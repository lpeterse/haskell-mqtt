{-# LANGUAGE TypeFamilies, FlexibleInstances, OverloadedStrings #-}
module Network.MQTT.RoutingTree where

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

newtype RoutingTree a = RoutingTree { untree :: M.Map BS.ShortByteString (RoutingTreeNode a) }

instance (RoutingTreeElement a, Monoid a) => Monoid (RoutingTree a) where
  mempty  = empty
  mappend = unionWith mappend

empty :: RoutingTree a
empty = RoutingTree mempty

nullq :: RoutingTree a -> Bool
nullq (RoutingTree m) = M.null m

singleton :: RoutingTreeElement a => Filter -> a -> RoutingTree a
singleton (Filter (x:|xs)) a
  | null a    = empty
  | otherwise = RoutingTree $ M.singleton x $ case xs of
      []     -> fromTreeAndValue empty a
      (y:ys) -> fromTree (singleton (Filter $ y:|ys) a)

insert :: RoutingTreeElement a => Filter -> a -> RoutingTree a -> RoutingTree a
insert  = insertWith const

insertWith :: RoutingTreeElement a => (a -> a -> a) -> Filter -> a -> RoutingTree a -> RoutingTree a
insertWith f (Filter (x:|xs)) a (RoutingTree m)
  | null a    = RoutingTree m
  | otherwise = RoutingTree $ M.alter g x m
  where
    g mn = Just $ case xs of
      []     -> case mn of
        Nothing -> fromTreeAndValue empty a
        Just n  -> fromTreeAndValue (tree n) $ fromMaybe a $ f a <$> value n
      (y:ys) -> fromTree $ insertWith f (Filter $ y:|ys) a $ fromMaybe empty $ tree <$> mn


adjust :: RoutingTreeElement a => (a -> a) -> Filter -> RoutingTree a -> RoutingTree a
adjust f (Filter (x:|xs)) (RoutingTree m) =
  RoutingTree $ M.update g x m
  where
    g n = case xs of
      [] -> case value n of
        Just v  ->
          let t'@(RoutingTree m') = tree n; v' = f v in
          if M.null m' && null v'
            then Nothing
            else Just $ fromTreeAndValue t' v'
        Nothing -> Just n -- tree is non-empty by invariant
      (y:ys) ->
        let t'@(RoutingTree m') = adjust f (Filter $ y:|ys) (tree n) in
        case value n of
          Just v  -> Just $ fromTreeAndValue t' v
          Nothing -> if M.null m' then Nothing else Just (fromTree t')

delete :: RoutingTreeElement a => Filter -> RoutingTree a -> RoutingTree a
delete (Filter (x:|xs)) (RoutingTree m) =
  RoutingTree $ M.update g x m
  where
    g n = case xs of
      [] | M.null $ untree $ tree n -> Nothing
         | otherwise                -> Just $ fromTree $ tree n
      y:ys -> let t@(RoutingTree m') = delete (Filter $ y:|ys) (tree n) in
       case value n of
         Nothing | M.null m' -> Nothing
                 | otherwise -> Just $ fromTree t
         Just v -> Just $ fromTreeAndValue t v

map :: (RoutingTreeElement a, RoutingTreeElement b) => (a -> b) -> RoutingTree a -> RoutingTree b
map f (RoutingTree m) = RoutingTree $ fmap g m
    where
      g n = let t = map f (tree n) in case value n of
        Nothing -> fromTree t
        Just a  -> let b = f a in if null b then fromTree t else fromTreeAndValue t b

unionWith :: (RoutingTreeElement a, Monoid a) => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
unionWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.unionWith g m1 m2)
  where
    g n1 n2 = h (unionWith f (tree n1) (tree n2)) (value n1) (value n2)
    h t (Just v1) (Just v2) = fromTreeAndValue t (f v1 v2)
    h t (Just v1)  _        = fromTreeAndValue t v1
    h t  _        (Just v2) = fromTreeAndValue t v2
    h t  _         _        = fromTree         t

differenceWith :: RoutingTreeElement a => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
differenceWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.differenceWith g m1 m2)
  where
    g n1 n2 = k (differenceWith f (tree n1) (tree n2)) (d (value n1) (value n2))
    d (Just v1) (Just v2)            = Just (f v1 v2)
    d (Just v1)  _                   = Just v1
    d  _         _                   = Nothing
    k t Nothing  | nullq t           = Nothing
                 | otherwise         = Just (fromTree t)
    k t (Just v) | nullq t && null v = Nothing
                 | otherwise         = Just (fromTreeAndValue t v)

subscriptions :: (RoutingTreeElement a, Monoid a) => Topic -> RoutingTree a -> a
subscriptions (Topic (x:|[])) (RoutingTree m) =
  case M.lookup x m of
    Nothing -> mempty
    Just n  -> fromMaybe mempty $ case M.lookup "#" (untree $ tree n) of
      Nothing -> value n
      Just n' -> value n <> value n'
subscriptions (Topic (x:|y:zs)) (RoutingTree m) =
  matchComponent <> matchSingleLevelWildcard <> matchMultiLevelWildcard
  where
    matchComponent =
      case M.lookup x m of
        Nothing -> mempty
        Just n  -> subscriptions (Topic $ y:|zs) (tree n)
    matchSingleLevelWildcard =
      case M.lookup "+" m of
        Nothing -> mempty
        Just n  -> subscriptions (Topic $ y:|zs) (tree n)
    matchMultiLevelWildcard =
      case M.lookup "#" m of
        Nothing -> mempty
        Just n  -> fromMaybe mempty $ value n

--------------------------------------------------------------------------------
-- Specialised tree implemenations using data families
--------------------------------------------------------------------------------

class RoutingTreeElement a where
  data RoutingTreeNode a
  null             :: a -> Bool
  tree             :: RoutingTreeNode a -> RoutingTree a
  value            :: RoutingTreeNode a -> Maybe a
  fromTree         :: RoutingTree a -> RoutingTreeNode a
  fromTreeAndValue :: RoutingTree a -> a -> RoutingTreeNode a

instance RoutingTreeElement S.IntSet where
  data RoutingTreeNode S.IntSet
    = IntSetRoutingTreeNode !(RoutingTree S.IntSet) !S.IntSet
  null                              = S.null
  tree (IntSetRoutingTreeNode t _)  = t
  value (IntSetRoutingTreeNode _ v) | null v    = Nothing
                                    | otherwise = Just v
  fromTree t                        = IntSetRoutingTreeNode t mempty
  fromTreeAndValue                  = IntSetRoutingTreeNode

instance RoutingTreeElement (Identity a) where
  data RoutingTreeNode (Identity a)
    = TreeNode          !(RoutingTree (Identity a))
    | TreeNodeWithValue !(RoutingTree (Identity a)) !(Identity a)
  null                             = const False
  tree  (TreeNode          t  )    = t
  tree  (TreeNodeWithValue t _)    = t
  value (TreeNode          _  )    = Nothing
  value (TreeNodeWithValue _ v)    = Just v
  fromTree                         = TreeNode
  fromTreeAndValue                 = TreeNodeWithValue
