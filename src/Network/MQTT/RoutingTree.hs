{-# LANGUAGE TypeFamilies, FlexibleInstances, OverloadedStrings #-}
module Network.MQTT.RoutingTree
  ( Filter (..)
  , Topic (..)
  , RoutingTree ()
  , RoutingTreeValue ()
  , null
  , empty
  , singleton
  , lookupWith
  , insert
  , insertWith
  , map
  , adjust
  , delete
  , unionWith
  , differenceWith
  ) where

import Data.Functor.Identity

import Data.Maybe
import Data.Monoid
import Data.List.NonEmpty ( NonEmpty(..) )
import qualified Data.List.NonEmpty as NL
import qualified Data.Sequence as S

import qualified Data.ByteString.Short as BS
import qualified Data.Map as M
import qualified Data.IntSet as IS

import Prelude hiding ( map, null )

newtype Filter        = Filter (NL.NonEmpty BS.ShortByteString) deriving (Eq, Ord, Show)
newtype Topic         = Topic (NL.NonEmpty BS.ShortByteString) deriving (Eq, Ord, Show)

newtype RoutingTree a = RoutingTree (M.Map BS.ShortByteString (RoutingTreeNode a))

instance (RoutingTreeValue a, Monoid a) => Monoid (RoutingTree a) where
  mempty  = empty
  mappend = unionWith mappend

empty :: RoutingTree a
empty = RoutingTree mempty

null  :: RoutingTree a -> Bool
null  (RoutingTree m) = M.null m

singleton :: RoutingTreeValue a => Filter -> a -> RoutingTree a
singleton (Filter (x:|xs)) a
  | rtvNull a  = empty
  | otherwise = RoutingTree $ M.singleton x $ case xs of
      []     -> rtvFromTreeAndValue empty a
      (y:ys) -> rtvFromTree (singleton (Filter $ y:|ys) a)

insert :: RoutingTreeValue a => Filter -> a -> RoutingTree a -> RoutingTree a
insert  = insertWith const

insertWith :: RoutingTreeValue a => (a -> a -> a) -> Filter -> a -> RoutingTree a -> RoutingTree a
insertWith f (Filter (x:|xs)) a (RoutingTree m)
  | rtvNull a  = RoutingTree m
  | otherwise = RoutingTree $ M.alter g x m
  where
    g mn = Just $ case xs of
      []     -> case mn of
        Nothing -> rtvFromTreeAndValue empty a
        Just n  -> rtvFromTreeAndValue (rtvTree n) $ fromMaybe a $ f a <$> rtvValue n
      (y:ys) -> rtvFromTree $ insertWith f (Filter $ y:|ys) a $ fromMaybe empty $ rtvTree <$> mn

adjust :: RoutingTreeValue a => (a -> a) -> Filter -> RoutingTree a -> RoutingTree a
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

delete :: RoutingTreeValue a => Filter -> RoutingTree a -> RoutingTree a
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

map :: (RoutingTreeValue a, RoutingTreeValue b) => (a -> b) -> RoutingTree a -> RoutingTree b
map f (RoutingTree m) = RoutingTree $ fmap g m
    where
      g n = let t = map f (rtvTree n) in case rtvValue n of
        Nothing -> rtvFromTree t
        Just a  -> let b = f a in if rtvNull b then rtvFromTree t else rtvFromTreeAndValue t b

unionWith :: (RoutingTreeValue a) => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
unionWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.unionWith g m1 m2)
  where
    g n1 n2 = h (unionWith f (rtvTree n1) (rtvTree n2)) (rtvValue n1) (rtvValue n2)
    h t (Just v1) (Just v2) = rtvFromTreeAndValue t (f v1 v2)
    h t (Just v1)  _        = rtvFromTreeAndValue t v1
    h t  _        (Just v2) = rtvFromTreeAndValue t v2
    h t  _         _        = rtvFromTree         t

differenceWith :: RoutingTreeValue a => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
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

lookupWith :: (RoutingTreeValue a) => (a -> a -> a) -> Topic -> RoutingTree a -> Maybe a
lookupWith f (Topic (x:|[])) (RoutingTree m) =
  case M.lookup x m of
    Nothing -> Nothing
    Just n  -> let RoutingTree m' = rtvTree n in
      case M.lookup "#" m' of
        Nothing -> rtvValue n
        Just n' -> rtvValue n `h` rtvValue n'
  where
    h (Just v1) (Just v2) = Just (f v1 v2)
    h (Just v1) _         = Just v1
    h _         (Just v2) = Just v2
    h _         _         = Nothing
lookupWith f (Topic (x:|y:zs)) (RoutingTree m) =
  matchComponent `h` matchSingleLevelWildcard `h` matchMultiLevelWildcard
  where
    matchComponent =
      M.lookup x m >>= lookupWith f (Topic $ y:|zs) . rtvTree
    matchSingleLevelWildcard =
      M.lookup "+" m >>= lookupWith f (Topic $ y:|zs) . rtvTree
    matchMultiLevelWildcard = M.lookup "#" m >>= rtvValue
    h (Just v1) (Just v2) = Just (f v1 v2)
    h (Just v1) _         = Just v1
    h _         (Just v2) = Just v2
    h _         _         = Nothing

--------------------------------------------------------------------------------
-- Specialised rtvTree implemenations using data families
--------------------------------------------------------------------------------

class RoutingTreeValue a where
  data RoutingTreeNode a
  rtvNull             :: a -> Bool
  rtvTree             :: RoutingTreeNode a -> RoutingTree a
  rtvValue            :: RoutingTreeNode a -> Maybe a
  rtvFromTree         :: RoutingTree a -> RoutingTreeNode a
  rtvFromTreeAndValue :: RoutingTree a -> a -> RoutingTreeNode a

instance RoutingTreeValue IS.IntSet where
  data RoutingTreeNode IS.IntSet
    = IntSetRoutingTreeNode !(RoutingTree IS.IntSet) !IS.IntSet
  rtvNull                              = IS.null
  rtvTree (IntSetRoutingTreeNode t _)  = t
  rtvValue (IntSetRoutingTreeNode _ v) | rtvNull v  = Nothing
                                       | otherwise = Just v
  rtvFromTree t                        = IntSetRoutingTreeNode t mempty
  rtvFromTreeAndValue                  = IntSetRoutingTreeNode

instance RoutingTreeValue (Identity a) where
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
