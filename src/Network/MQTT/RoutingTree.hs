{-# LANGUAGE TypeFamilies, FlexibleInstances, OverloadedStrings #-}
module Network.MQTT.RoutingTree
  ( Filter (..)
  , Topic (..)
  , RoutingTree ()
  , RoutingTreeElement ()
  , null
  , empty
  , singleton
  , insert
  , insertWith
  , map
  , adjust
  , delete
  , unionWith
  , differenceWith
  , subscriptions
  ) where

import Data.Functor.Identity

import Data.Maybe
import Data.Monoid
import Data.List.NonEmpty ( NonEmpty(..) )
import qualified Data.List.NonEmpty as NL

import qualified Data.ByteString.Short as BS
import qualified Data.Map as M
import qualified Data.IntSet as S

import Prelude hiding ( map, null )

newtype Filter        = Filter (NL.NonEmpty BS.ShortByteString) deriving (Eq, Ord, Show)
newtype Topic         = Topic (NL.NonEmpty BS.ShortByteString) deriving (Eq, Ord, Show)

newtype RoutingTree a = RoutingTree (M.Map BS.ShortByteString (RoutingTreeNode a))

instance (RoutingTreeElement a, Monoid a) => Monoid (RoutingTree a) where
  mempty  = empty
  mappend = unionWith mappend

empty :: RoutingTree a
empty = RoutingTree mempty

null  :: RoutingTree a -> Bool
null  (RoutingTree m) = M.null m

singleton :: RoutingTreeElement a => Filter -> a -> RoutingTree a
singleton (Filter (x:|xs)) a
  | rtvNull a  = empty
  | otherwise = RoutingTree $ M.singleton x $ case xs of
      []     -> rtvFromTreeAndValue empty a
      (y:ys) -> rtvFromTree (singleton (Filter $ y:|ys) a)

insert :: RoutingTreeElement a => Filter -> a -> RoutingTree a -> RoutingTree a
insert  = insertWith const

insertWith :: RoutingTreeElement a => (a -> a -> a) -> Filter -> a -> RoutingTree a -> RoutingTree a
insertWith f (Filter (x:|xs)) a (RoutingTree m)
  | rtvNull a  = RoutingTree m
  | otherwise = RoutingTree $ M.alter g x m
  where
    g mn = Just $ case xs of
      []     -> case mn of
        Nothing -> rtvFromTreeAndValue empty a
        Just n  -> rtvFromTreeAndValue (rtvTree n) $ fromMaybe a $ f a <$> rtvValue n
      (y:ys) -> rtvFromTree $ insertWith f (Filter $ y:|ys) a $ fromMaybe empty $ rtvTree <$> mn

adjust :: RoutingTreeElement a => (a -> a) -> Filter -> RoutingTree a -> RoutingTree a
adjust f (Filter (x:|xs)) (RoutingTree m) =
  RoutingTree $ M.update g x m
  where
    g n = case xs of
      [] -> case rtvValue n of
        Just v  ->
          let t' = rtvTree n; v' = f v in
          if null t' && rtvNull v'
            then Nothing
            else Just $ rtvFromTreeAndValue t' v'
        Nothing -> Just n -- rtvTree is non-empty by invariant
      (y:ys) ->
        let t'@(RoutingTree m') = adjust f (Filter $ y:|ys) (rtvTree n) in
        case rtvValue n of
          Just v  -> Just $ rtvFromTreeAndValue t' v
          Nothing -> if M.null m' then Nothing else Just (rtvFromTree t')

delete :: RoutingTreeElement a => Filter -> RoutingTree a -> RoutingTree a
delete (Filter (x:|xs)) (RoutingTree m) =
  RoutingTree $ M.update g x m
  where
    g n = case xs of
      [] | null (rtvTree n) -> Nothing
         | otherwise     -> Just $ rtvFromTree $ rtvTree n
      y:ys -> let t = delete (Filter $ y:|ys) (rtvTree n) in
       case rtvValue n of
         Nothing | null t    -> Nothing
                 | otherwise -> Just $ rtvFromTree t
         Just v -> Just $ rtvFromTreeAndValue t v

map :: (RoutingTreeElement a, RoutingTreeElement b) => (a -> b) -> RoutingTree a -> RoutingTree b
map f (RoutingTree m) = RoutingTree $ fmap g m
    where
      g n = let t = map f (rtvTree n) in case rtvValue n of
        Nothing -> rtvFromTree t
        Just a  -> let b = f a in if rtvNull b then rtvFromTree t else rtvFromTreeAndValue t b

unionWith :: (RoutingTreeElement a) => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
unionWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.unionWith g m1 m2)
  where
    g n1 n2 = h (unionWith f (rtvTree n1) (rtvTree n2)) (rtvValue n1) (rtvValue n2)
    h t (Just v1) (Just v2) = rtvFromTreeAndValue t (f v1 v2)
    h t (Just v1)  _        = rtvFromTreeAndValue t v1
    h t  _        (Just v2) = rtvFromTreeAndValue t v2
    h t  _         _        = rtvFromTree         t

differenceWith :: RoutingTreeElement a => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
differenceWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.differenceWith g m1 m2)
  where
    g n1 n2 = k (differenceWith f (rtvTree n1) (rtvTree n2)) (d (rtvValue n1) (rtvValue n2))
    d (Just v1) (Just v2)              = Just (f v1 v2)
    d (Just v1)  _                     = Just v1
    d  _         _                     = Nothing
    k t Nothing  | null t              = Nothing
                 | otherwise           = Just (rtvFromTree t)
    k t (Just v) | null t && rtvNull v = Nothing
                 | otherwise           = Just (rtvFromTreeAndValue t v)

subscriptions :: (RoutingTreeElement a, Monoid a) => Topic -> RoutingTree a -> a
subscriptions (Topic (x:|[])) (RoutingTree m) =
  case M.lookup x m of
    Nothing -> mempty
    Just n  -> let RoutingTree m' = rtvTree n in fromMaybe mempty $ case M.lookup "#" m' of
      Nothing -> rtvValue n
      Just n' -> rtvValue n <> rtvValue n'
subscriptions (Topic (x:|y:zs)) (RoutingTree m) =
  matchComponent <> matchSingleLevelWildcard <> matchMultiLevelWildcard
  where
    matchComponent =
      case M.lookup x m of
        Nothing -> mempty
        Just n  -> subscriptions (Topic $ y:|zs) (rtvTree n)
    matchSingleLevelWildcard =
      case M.lookup "+" m of
        Nothing -> mempty
        Just n  -> subscriptions (Topic $ y:|zs) (rtvTree n)
    matchMultiLevelWildcard =
      case M.lookup "#" m of
        Nothing -> mempty
        Just n  -> fromMaybe mempty $ rtvValue n

--------------------------------------------------------------------------------
-- Specialised rtvTree implemenations using data families
--------------------------------------------------------------------------------

class RoutingTreeElement a where
  data RoutingTreeNode a
  rtvNull             :: a -> Bool
  rtvTree             :: RoutingTreeNode a -> RoutingTree a
  rtvValue            :: RoutingTreeNode a -> Maybe a
  rtvFromTree         :: RoutingTree a -> RoutingTreeNode a
  rtvFromTreeAndValue :: RoutingTree a -> a -> RoutingTreeNode a

instance RoutingTreeElement S.IntSet where
  data RoutingTreeNode S.IntSet
    = IntSetRoutingTreeNode !(RoutingTree S.IntSet) !S.IntSet
  rtvNull                              = S.null
  rtvTree (IntSetRoutingTreeNode t _)  = t
  rtvValue (IntSetRoutingTreeNode _ v) | rtvNull v  = Nothing
                                       | otherwise = Just v
  rtvFromTree t                        = IntSetRoutingTreeNode t mempty
  rtvFromTreeAndValue                  = IntSetRoutingTreeNode

instance RoutingTreeElement (Identity a) where
  data RoutingTreeNode (Identity a)
    = TreeNode          !(RoutingTree (Identity a))
    | TreeNodeWithValue !(RoutingTree (Identity a)) !(Identity a)
  rtvNull                             = const False
  rtvTree  (TreeNode          t  )    = t
  rtvTree  (TreeNodeWithValue t _)    = t
  rtvValue (TreeNode          _  )    = Nothing
  rtvValue (TreeNodeWithValue _ v)    = Just v
  rtvFromTree                         = TreeNode
  rtvFromTreeAndValue                 = TreeNodeWithValue
