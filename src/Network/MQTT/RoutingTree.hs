{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.RoutingTree
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.RoutingTree (
  -- * RoutingTree
    RoutingTree (..)
  , RoutingTreeValue (..)
  -- ** null
  , null
  -- ** empty
  , empty
  -- ** size
  , size
  -- ** singleton
  , singleton
  -- ** matchTopic
  , matchTopic
  -- ** matchFilter
  , matchFilter
  -- ** lookup
  , lookup
  -- ** findMaxBounded
  , findMaxBounded
  -- ** insert
  , insert
  -- ** insertWith
  , insertWith
  -- ** insertFoldable
  , insertFoldable
  -- ** map
  , map
  -- ** mapMaybe
  , mapMaybe
  -- ** delete
  , delete
  -- ** union
  , union
  -- ** unionWith
  , unionWith
  -- ** differenceWith
  , differenceWith
  ) where

import           Control.Applicative   ((<|>))
import           Data.Functor.Identity
import qualified Data.IntSet           as IS
import           Data.List.NonEmpty    (NonEmpty (..))
import qualified Data.Map.Strict       as M
import           Data.Maybe            hiding (mapMaybe)
import           Data.Monoid
import qualified Data.List              as L
import           Network.MQTT.Topic
import           Prelude               hiding (lookup, map, null)

-- | The `RoutingTree` is a map-like data structure designed to hold elements
--   that can efficiently be queried according to the matching rules specified
--   by MQTT.
--   The primary purpose is to manage client subscriptions, but it can just
--   as well be used to manage permissions etc.
--
--   The tree consists of nodes that may or may not contain values. The edges
--   are filter components. As some value types have the concept of a null
--   (i.e. an empty set) the `RoutingTreeValue` is a class defining the data
--   family `RoutingTreeNode`. This is a performance and size optimization to
--   avoid unnecessary boxing and case distinction.
newtype RoutingTree a = RoutingTree { branches :: M.Map Level (RoutingTreeNode a) }

class RoutingTreeValue a where
  data RoutingTreeNode a
  node                 :: RoutingTree a -> Maybe a -> RoutingTreeNode a
  nodeNull             :: a -> Bool
  nodeTree             :: RoutingTreeNode a -> RoutingTree a
  nodeValue            :: RoutingTreeNode a -> Maybe a

instance (RoutingTreeValue a, Monoid a) => Monoid (RoutingTree a) where
  mempty  = empty
  mappend = unionWith mappend

instance (RoutingTreeValue a, Eq a) => Eq (RoutingTree a) where
  RoutingTree m1 == RoutingTree m2 =
    M.size m1 == M.size m2 && and (zipWith f (M.toAscList m1) (M.toAscList m2))
    where
      f (l1,n1) (l2,n2) = l1 == l2 && nodeValue n1 == nodeValue n2 && nodeTree n1 == nodeTree n2

instance (RoutingTreeValue a, Show a) => Show (RoutingTree a) where
  show (RoutingTree m) = "RoutingTree [" ++ L.intercalate ", " (f <$> M.toAscList m) ++ "]"
    where
      f (l,n) = "(" ++ show l ++ ", Node (" ++ show (nodeValue n) ++ ") (" ++ show (nodeTree n) ++ ")"

empty :: RoutingTree a
empty  = RoutingTree mempty

null  :: RoutingTree a -> Bool
null (RoutingTree m) = M.null m

size  :: RoutingTreeValue a => RoutingTree a -> Int
size (RoutingTree m) = M.foldl' f 0 m
  where
    f accum n = 1 + accum + size (nodeTree n)

singleton :: RoutingTreeValue a => Filter -> a -> RoutingTree a
singleton tf = singleton' (filterLevels tf)
  where
    singleton' (x:|xs) a
      | nodeNull a  = empty
      | otherwise   = RoutingTree $ M.singleton x $ case xs of
          []     -> node empty (Just a)
          (y:ys) -> node (singleton' (y:|ys) a) Nothing

insert :: RoutingTreeValue a => Filter -> a -> RoutingTree a -> RoutingTree a
insert  = insertWith const

insertWith :: RoutingTreeValue a => (a -> a -> a) -> Filter -> a -> RoutingTree a -> RoutingTree a
insertWith f tf a = insertWith' (filterLevels tf)
  where
    insertWith' (x:|xs) (RoutingTree m)
      | nodeNull a  = RoutingTree m
      | otherwise   = RoutingTree $ M.alter g x m
      where
        g mn = Just $ case xs of
          []     -> case mn of
            Nothing -> node empty (Just a)
            Just n  -> node (nodeTree n) $ (f a <$> nodeValue n) <|> Just a
          (y:ys) -> node (insertWith' (y:|ys) $ fromMaybe empty $ nodeTree <$> mn) Nothing

insertFoldable :: (RoutingTreeValue a, Foldable t) => t (Filter, a) -> RoutingTree a -> RoutingTree a
insertFoldable  = flip $ foldr $ uncurry insert

delete :: RoutingTreeValue a => Filter -> RoutingTree a -> RoutingTree a
delete tf = delete' (filterLevels tf)
  where
    delete' (x:|xs) (RoutingTree m) = RoutingTree $ M.update g x m
      where
        g n = case xs of
          [] | null (nodeTree n) -> Nothing
             | otherwise         -> Just $ node (nodeTree n) Nothing
          y:ys -> let t = delete' (y:|ys) (nodeTree n) in
           case nodeValue n of
             Nothing | null t    -> Nothing
                     | otherwise -> Just $ node t Nothing
             Just v -> Just $ node t (Just v)

map :: (RoutingTreeValue a, RoutingTreeValue b) => (a -> b) -> RoutingTree a -> RoutingTree b
map f (RoutingTree m) = RoutingTree $ fmap g m
  where
    g n = let t = map f (nodeTree n) in node t (f <$> nodeValue n)

-- FIXME: Review. Does not honour invariants!
mapMaybe :: (RoutingTreeValue a, RoutingTreeValue b) => (a -> Maybe b) -> RoutingTree a -> RoutingTree b
mapMaybe f (RoutingTree m) = RoutingTree $ fmap g m
  where
    g n = let t = mapMaybe f (nodeTree n) in node t (nodeValue n >>= f)

union     :: (RoutingTreeValue a, Monoid a) => RoutingTree a -> RoutingTree a -> RoutingTree a
union (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.unionWith g m1 m2)
  where
    g n1 n2 = node (nodeTree n1 `union` nodeTree n2) (nodeValue n1 <> nodeValue n2)

unionWith :: (RoutingTreeValue a) => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
unionWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.unionWith g m1 m2)
  where
    g n1 n2 = node (unionWith f (nodeTree n1) (nodeTree n2)) (nodeValue n1 `merge` nodeValue n2)
    merge (Just v1) (Just v2) = Just (f v1 v2)
    merge      mv1       mv2  = mv1 <|> mv2

differenceWith :: (RoutingTreeValue a, RoutingTreeValue b) => (a -> b -> Maybe a) -> RoutingTree a -> RoutingTree b -> RoutingTree a
differenceWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.differenceWith g m1 m2)
  where
    g n1 n2 = k (differenceWith f (nodeTree n1) (nodeTree n2)) (d (nodeValue n1) (nodeValue n2))
    d (Just v1) (Just v2) = f v1 v2
    d (Just v1)  _        = Just v1
    d  _         _        = Nothing
    k t Nothing  | null t               = Nothing
                 | otherwise            = Just $ node t Nothing
    k t (Just v) | null t && nodeNull v = Nothing
                 | otherwise            = Just $ node t $ Just v

-- | Collect all values of nodes that match a given topic (according to the
--   matching rules specified by the MQTT protocol).
lookup :: (RoutingTreeValue a, Monoid a) => Topic -> RoutingTree a -> a
lookup tf = fromMaybe mempty . lookupHead (topicLevels tf)
  where
    -- If the first level starts with $ then it must not be matched against + and #.
    lookupHead (x:|xs) t@(RoutingTree m)
      | startsWithDollar x = case xs of
          []     -> M.lookup x m >>= nodeValue
          (y:ys) -> M.lookup x m >>= lookupTail y ys . nodeTree
      | otherwise = lookupTail x xs t
    lookupTail x [] (RoutingTree m) =
      matchSingleLevelWildcard <> matchMultiLevelWildcard <> matchComponent
      where
        matchSingleLevelWildcard = M.lookup singleLevelWildcard m >>= nodeValue
        matchMultiLevelWildcard  = M.lookup multiLevelWildcard  m >>= nodeValue
        matchComponent           = M.lookup x                   m >>= \n->
          case M.lookup multiLevelWildcard $ branches $ nodeTree n of
            -- component match, but no additional multiLevelWildcard below
            Nothing -> nodeValue n
            -- component match and multiLevelWildcard match below
            Just n' -> nodeValue n <> nodeValue n'
    lookupTail x (y:ys) (RoutingTree m) =
      matchSingleLevelWildcard <> matchMultiLevelWildcard <> matchComponent
      where
        matchSingleLevelWildcard = M.lookup singleLevelWildcard m >>= lookupTail y ys . nodeTree
        matchMultiLevelWildcard  = M.lookup multiLevelWildcard  m >>= nodeValue
        matchComponent           = M.lookup                   x m >>= lookupTail y ys . nodeTree

findMaxBounded :: (RoutingTreeValue a, Ord a, Bounded a) => Topic -> RoutingTree a -> Maybe a
findMaxBounded topic = findHead (topicLevels topic)
  where
    findHead (x:|xs) t@(RoutingTree m)
      | startsWithDollar x = case xs of
          []     -> M.lookup x m >>= nodeValue
          (y:ys) -> M.lookup x m >>= findTail y ys . nodeTree
      | otherwise = findTail x xs t
    findTail x [] (RoutingTree m) =
      matchSingleLevelWildcard `maxBounded` matchMultiLevelWildcard `maxBounded` matchComponent
      where
        matchSingleLevelWildcard = M.lookup singleLevelWildcard m >>= nodeValue
        matchMultiLevelWildcard  = M.lookup multiLevelWildcard  m >>= nodeValue
        matchComponent           = M.lookup x                   m >>= \n->
          case M.lookup multiLevelWildcard $ branches $ nodeTree n of
            -- component match, but no additional multiLevelWildcard below
            Nothing -> nodeValue n
            -- component match and multiLevelWildcard match below
            Just n' -> nodeValue n `maxBounded` nodeValue n'
    findTail x (y:ys) (RoutingTree m) =
      matchSingleLevelWildcard `maxBounded` matchMultiLevelWildcard `maxBounded` matchComponent
      where
        matchSingleLevelWildcard = M.lookup singleLevelWildcard m >>= findTail y ys . nodeTree
        matchMultiLevelWildcard  = M.lookup multiLevelWildcard  m >>= nodeValue
        matchComponent           = M.lookup                   x m >>= findTail y ys . nodeTree

    maxBounded :: (Ord a, Bounded a) => Maybe a -> Maybe a -> Maybe a
    maxBounded a b
      | a == Just maxBound = a
      | otherwise          = max a b

-- | Match a `Topic` against a `RoutingTree`.
--
--   The function returns true iff the tree contains at least one node that
--   matches the topic /and/ contains a value (including nodes that are
--   indirectly matched by wildcard characters like `+` and `#` as described
--   in the MQTT specification).
matchTopic :: RoutingTreeValue a => Topic -> RoutingTree a -> Bool
matchTopic tf = matchTopicHead (topicLevels tf)
  where
    -- The '#' is always a terminal node and therefore does not contain subtrees.
    -- By invariant, a '#' node only exists if it contains a value. For this
    -- reason it does not need to be checked for a value here, but just for
    -- existence.
    -- A '+' node on the other hand may contain subtrees and may not carry a value
    -- itself. This needs to be checked.
    matchTopicHead (x:|xs) t@(RoutingTree m)
      | startsWithDollar x = case xs of
          []     -> matchExact x m
          (y:ys) -> fromMaybe False $ matchTopicTail y ys . nodeTree <$> M.lookup x m
      | otherwise = matchTopicTail x xs t
    matchTopicTail x [] (RoutingTree m) =
      matchExact x m  || matchPlus || matchHash
      where
        matchPlus = isJust ( nodeValue =<< M.lookup singleLevelWildcard m )
        matchHash = M.member multiLevelWildcard m
    matchTopicTail x (y:ys) (RoutingTree m) =
      M.member multiLevelWildcard m || case M.lookup x m of
        -- Same is true for '#' node here. In case no '#' hash node is present it is
        -- first tried to match the exact topic and then to match any '+' node.
        Nothing -> matchPlus
        Just n  -> matchTopicTail y ys (nodeTree n) || matchPlus
      where
        -- A '+' node matches any topic element.
        matchPlus = fromMaybe False
                  $ matchTopicTail y ys . nodeTree <$> M.lookup singleLevelWildcard m
    -- An exact match is the case if the map contains a node for the key and
    -- the node is not empty _or_ the node's subtree contains a wildcard key (wildcards)
    -- always also match the parent node.
    matchExact x m = case M.lookup x m of
      Nothing -> False
      Just n  -> isJust (nodeValue n) || let RoutingTree m' = nodeTree n in  M.member multiLevelWildcard m'

-- | Match a `Filter` against a `RoutingTree`.
--
--   The function returns true iff the tree contains a path that is
--   /less or equally specific/ than the filter and the terminal node contains
--   a value that is not `nodeNull`.
--
--   > match (singleton "#") "a"     = True
--   > match (singleton "#") "+"     = True
--   > match (singleton "#") "a/+/b" = True
--   > match (singleton "#") "a/+/#" = True
--   > match (singleton "+") "a"     = True
--   > match (singleton "+") "+"     = True
--   > match (singleton "+") "+/a"   = False
--   > match (singleton "+") "#"     = False
--   > match (singleton "a") "a"     = True
--   > match (singleton "a") "b"     = False
--   > match (singleton "a") "+"     = False
--   > match (singleton "a") "#"     = False
matchFilter :: RoutingTreeValue a => Filter -> RoutingTree a -> Bool
matchFilter tf = matchFilter' (filterLevels tf)
  where
    matchFilter' (x:|[]) (RoutingTree m)
      | x == multiLevelWildcard  = matchMultiLevelWildcard
      | x == singleLevelWildcard = matchMultiLevelWildcard || matchSingleLevelWildcard
      | otherwise                = matchMultiLevelWildcard || matchSingleLevelWildcard || matchExact
      where
        matchMultiLevelWildcard  = M.member multiLevelWildcard m
        matchSingleLevelWildcard = isJust ( nodeValue =<< M.lookup singleLevelWildcard m )
        matchExact               = case M.lookup x m of
          Nothing -> False
          Just n' -> isJust (nodeValue n') || let RoutingTree m' = nodeTree n' in M.member multiLevelWildcard m'
    matchFilter' (x:|y:zs) (RoutingTree m)
      | x == multiLevelWildcard  = matchMultiLevelWildcard
      | x == singleLevelWildcard = matchMultiLevelWildcard || matchSingleLevelWildcard
      | otherwise                = matchMultiLevelWildcard || matchSingleLevelWildcard || matchExact
      where
        matchMultiLevelWildcard  = M.member multiLevelWildcard  m
        matchSingleLevelWildcard = fromMaybe False $ matchFilter' (y:|zs) . nodeTree <$> M.lookup singleLevelWildcard m
        matchExact               = fromMaybe False $ matchFilter' (y:|zs) . nodeTree <$> M.lookup x m

--------------------------------------------------------------------------------
-- Specialised nodeTree implemenations using data families
--------------------------------------------------------------------------------

instance RoutingTreeValue IS.IntSet where
  data RoutingTreeNode IS.IntSet = IntSetRoutingTreeNode !(RoutingTree IS.IntSet) !IS.IntSet
  node t                                = IntSetRoutingTreeNode t . fromMaybe mempty
  nodeNull                              = IS.null
  nodeTree (IntSetRoutingTreeNode t _)  = t
  nodeValue (IntSetRoutingTreeNode _ v) | nodeNull v = Nothing
                                        | otherwise = Just v

instance RoutingTreeValue (Identity a) where
  data RoutingTreeNode (Identity a) = IdentityNode !(RoutingTree (Identity a)) !(Maybe (Identity a))
  node t n@Nothing              = IdentityNode t n
  node t n@(Just v)             = v `seq` IdentityNode t n
  nodeNull                      = const False
  nodeTree  (IdentityNode t _)  = t
  nodeValue (IdentityNode _ mv) = mv

instance RoutingTreeValue () where
  data RoutingTreeNode ()  = UnitNode {-# UNPACK #-} !Int !(RoutingTree ())
  node t Nothing           = UnitNode 0 t
  node t _                 = UnitNode 1 t
  nodeNull                 = const False
  nodeTree  (UnitNode _ t) = t
  nodeValue (UnitNode 0 _) = Nothing
  nodeValue (UnitNode _ _) = Just ()

instance RoutingTreeValue Bool where
  data RoutingTreeNode Bool = BoolNode {-# UNPACK #-} !Int !(RoutingTree Bool)
  node t Nothing               = BoolNode 0 t
  node t (Just False)          = BoolNode 1 t
  node t (Just True)           = BoolNode 2 t
  nodeNull                     = const False
  nodeTree  (BoolNode _ t)     = t
  nodeValue (BoolNode 1 _)     = Just False
  nodeValue (BoolNode 2 _)     = Just True
  nodeValue (BoolNode _ _)     = Nothing
