{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE BangPatterns      #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Trie
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Trie (
  -- * Trie
    Trie (..)
  , TrieValue (..)
  -- ** null
  , null
  -- ** empty
  , empty
  -- ** size
  , size
  -- ** sizeWith
  , sizeWith
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
  -- ** foldl'
  , foldl'
  -- ** delete
  , delete
  -- ** union
  , union
  -- ** unionWith
  , unionWith
  -- ** differenceWith
  , differenceWith
  ) where

import           Control.Applicative        ((<|>))
import qualified Data.Binary                as B
import           Data.Functor.Identity
import qualified Data.IntSet                as IS
import qualified Data.List                  as L
import           Data.List.NonEmpty         (NonEmpty (..))
import qualified Data.Map.Strict            as M
import           Data.Maybe                 hiding (mapMaybe)
import           Data.Monoid
import           Prelude                    hiding (lookup, map, null)

import           Network.MQTT.Message.Topic

-- | The `Trie` is a map-like data structure designed to hold elements
--   that can efficiently be queried according to the matching rules specified
--   by MQTT.
--   The primary purpose is to manage client subscriptions, but it can just
--   as well be used to manage permissions etc.
--
--   The tree consists of nodes that may or may not contain values. The edges
--   are filter components. As some value types have the concept of a null
--   (i.e. an empty set) the `TrieValue` is a class defining the data
--   family `TrieNode`. This is a performance and size optimization to
--   avoid unnecessary boxing and case distinction.
newtype Trie a = Trie { branches :: M.Map Level (TrieNode a) }

class TrieValue a where
  data TrieNode a
  node                 :: Trie a -> Maybe a -> TrieNode a
  nodeNull             :: a -> Bool
  nodeTree             :: TrieNode a -> Trie a
  nodeValue            :: TrieNode a -> Maybe a

instance (TrieValue a, Monoid a) => Monoid (Trie a) where
  mempty  = empty
  mappend = unionWith mappend

instance (TrieValue a, Eq a) => Eq (Trie a) where
  Trie m1 == Trie m2 =
    M.size m1 == M.size m2 && and (zipWith f (M.toAscList m1) (M.toAscList m2))
    where
      f (l1,n1) (l2,n2) = l1 == l2 && nodeValue n1 == nodeValue n2 && nodeTree n1 == nodeTree n2

instance (TrieValue a, Show a) => Show (Trie a) where
  show (Trie m) = "Trie [" ++ L.intercalate ", " (f <$> M.toAscList m) ++ "]"
    where
      f (l,n) = "(" ++ show l ++ ", Node (" ++ show (nodeValue n) ++ ") (" ++ show (nodeTree n) ++ ")"

instance B.Binary (Trie ()) where
  put _ = pure ()
  get = pure empty

empty :: Trie a
empty  = Trie mempty

null  :: Trie a -> Bool
null (Trie m) = M.null m

-- | Count all trie nodes that are not `nodeNull`.
size :: TrieValue a => Trie a -> Int
size  = sizeWith (const 1)

sizeWith :: TrieValue a => (a -> Int) -> Trie a -> Int
sizeWith sz = countTrie 0
  where
    -- Depth-first search through the tree.
    -- This implementation uses an accumulator in order to not defer
    -- the evaluation of the additions.
    countTrie !accum t =
      M.foldl' countNode accum (branches t)
    countNode !accum n =
      case nodeValue n of
        Nothing -> countTrie accum (nodeTree n)
        Just v  -> countTrie (accum + sz v) (nodeTree n)

singleton :: TrieValue a => Filter -> a -> Trie a
singleton tf = singleton' (filterLevels tf)
  where
    singleton' (x:|xs) a
      | nodeNull a  = empty
      | otherwise   = Trie $ M.singleton x $ case xs of
          []     -> node empty (Just a)
          (y:ys) -> node (singleton' (y:|ys) a) Nothing

insert :: TrieValue a => Filter -> a -> Trie a -> Trie a
insert  = insertWith const

insertWith :: TrieValue a => (a -> a -> a) -> Filter -> a -> Trie a -> Trie a
insertWith f tf a = insertWith' (filterLevels tf)
  where
    insertWith' (x:|xs) (Trie m)
      | nodeNull a  = Trie m
      | otherwise   = Trie $ M.alter g x m
      where
        g mn = Just $ case xs of
          []     -> case mn of
            Nothing -> node empty (Just a)
            Just n  -> node (nodeTree n) $ (f a <$> nodeValue n) <|> Just a
          (y:ys) -> node (insertWith' (y:|ys) $ fromMaybe empty $ nodeTree <$> mn) Nothing

insertFoldable :: (TrieValue a, Foldable t) => t (Filter, a) -> Trie a -> Trie a
insertFoldable  = flip $ foldr $ uncurry insert

delete :: TrieValue a => Filter -> Trie a -> Trie a
delete tf = delete' (filterLevels tf)
  where
    delete' (x:|xs) (Trie m) = Trie $ M.update g x m
      where
        g n = case xs of
          [] | null (nodeTree n) -> Nothing
             | otherwise         -> Just $ node (nodeTree n) Nothing
          y:ys -> let t = delete' (y:|ys) (nodeTree n) in
           case nodeValue n of
             Nothing | null t    -> Nothing
                     | otherwise -> Just $ node t Nothing
             Just v -> Just $ node t (Just v)

map :: (TrieValue a, TrieValue b) => (a -> b) -> Trie a -> Trie b
map f (Trie m) = Trie $ fmap g m
  where
    g n = let t = map f (nodeTree n) in node t (f <$> nodeValue n)

-- | Applies a functor to a try and removes nodes for which the mapping
--   function returns `Nothing`.
mapMaybe :: (TrieValue a, TrieValue b) => (a -> Maybe b) -> Trie a -> Trie b
mapMaybe f (Trie m) = Trie (M.mapMaybe g m)
  where
    g n | isNothing v' && null t' = Nothing
        | otherwise               = Just (node t' v')
      where
        v' = nodeValue n >>= f
        t' = mapMaybe f $ nodeTree n

foldl'  :: (TrieValue b) => (a -> b -> a) -> a -> Trie b -> a
foldl' f acc (Trie m) = M.foldl' g acc m
  where
    g acc' n = flip (foldl' f) (nodeTree n) $! case nodeValue n of
      Nothing    -> acc'
      Just value -> f acc' value

union     :: (TrieValue a, Monoid a) => Trie a -> Trie a -> Trie a
union (Trie m1) (Trie m2) = Trie (M.unionWith g m1 m2)
  where
    g n1 n2 = node (nodeTree n1 `union` nodeTree n2) (nodeValue n1 <> nodeValue n2)

unionWith :: (TrieValue a) => (a -> a -> a) -> Trie a -> Trie a -> Trie a
unionWith f (Trie m1) (Trie m2) = Trie (M.unionWith g m1 m2)
  where
    g n1 n2 = node (unionWith f (nodeTree n1) (nodeTree n2)) (nodeValue n1 `merge` nodeValue n2)
    merge (Just v1) (Just v2) = Just (f v1 v2)
    merge      mv1       mv2  = mv1 <|> mv2

differenceWith :: (TrieValue a, TrieValue b) => (a -> b -> Maybe a) -> Trie a -> Trie b -> Trie a
differenceWith f (Trie m1) (Trie m2) = Trie (M.differenceWith g m1 m2)
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
lookup :: (TrieValue a, Monoid a) => Topic -> Trie a -> a
lookup tf = fromMaybe mempty . lookupHead (topicLevels tf)
  where
    -- If the first level starts with $ then it must not be matched against + and #.
    lookupHead (x:|xs) t@(Trie m)
      | startsWithDollar x = case xs of
          []     -> M.lookup x m >>= nodeValue
          (y:ys) -> M.lookup x m >>= lookupTail y ys . nodeTree
      | otherwise = lookupTail x xs t
    lookupTail x [] (Trie m) =
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
    lookupTail x (y:ys) (Trie m) =
      matchSingleLevelWildcard <> matchMultiLevelWildcard <> matchComponent
      where
        matchSingleLevelWildcard = M.lookup singleLevelWildcard m >>= lookupTail y ys . nodeTree
        matchMultiLevelWildcard  = M.lookup multiLevelWildcard  m >>= nodeValue
        matchComponent           = M.lookup                   x m >>= lookupTail y ys . nodeTree

-- | Find the greatest value in a trie that matches the topic.
--
--   * Stops search as soon as a `maxBound` element has been found.
--   * Doesn't match into `$` topics.
findMaxBounded :: (TrieValue a, Ord a, Bounded a) => Topic -> Trie a -> Maybe a
findMaxBounded topic = findHead (topicLevels topic)
  where
    findHead (x:|xs) t@(Trie m)
      | startsWithDollar x = case xs of
          []     -> M.lookup x m >>= nodeValue
          (y:ys) -> M.lookup x m >>= findTail y ys . nodeTree
      | otherwise = findTail x xs t
    findTail x [] (Trie m) =
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
    findTail x (y:ys) (Trie m) =
      matchSingleLevelWildcard `maxBounded` matchMultiLevelWildcard `maxBounded` matchComponent
      where
        matchSingleLevelWildcard = M.lookup singleLevelWildcard m >>= findTail y ys . nodeTree
        matchMultiLevelWildcard  = M.lookup multiLevelWildcard  m >>= nodeValue
        matchComponent           = M.lookup                   x m >>= findTail y ys . nodeTree

    maxBounded :: (Ord a, Bounded a) => Maybe a -> Maybe a -> Maybe a
    maxBounded a b
      | a == Just maxBound = a
      | otherwise          = max a b

-- | Match a `Topic` against a `Trie`.
--
--   The function returns true iff the tree contains at least one node that
--   matches the topic /and/ contains a value (including nodes that are
--   indirectly matched by wildcard characters like `+` and `#` as described
--   in the MQTT specification).
matchTopic :: TrieValue a => Topic -> Trie a -> Bool
matchTopic tf = matchTopicHead (topicLevels tf)
  where
    -- The '#' is always a terminal node and therefore does not contain subtrees.
    -- By invariant, a '#' node only exists if it contains a value. For this
    -- reason it does not need to be checked for a value here, but just for
    -- existence.
    -- A '+' node on the other hand may contain subtrees and may not carry a value
    -- itself. This needs to be checked.
    matchTopicHead (x:|xs) t@(Trie m)
      | startsWithDollar x = case xs of
          []     -> matchExact x m
          (y:ys) -> fromMaybe False $ matchTopicTail y ys . nodeTree <$> M.lookup x m
      | otherwise = matchTopicTail x xs t
    matchTopicTail x [] (Trie m) =
      matchExact x m  || matchPlus || matchHash
      where
        matchPlus = isJust ( nodeValue =<< M.lookup singleLevelWildcard m )
        matchHash = M.member multiLevelWildcard m
    matchTopicTail x (y:ys) (Trie m) =
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
      Just n  -> isJust (nodeValue n) || let Trie m' = nodeTree n in  M.member multiLevelWildcard m'

-- | Match a `Filter` against a `Trie`.
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
matchFilter :: TrieValue a => Filter -> Trie a -> Bool
matchFilter tf = matchFilter' (filterLevels tf)
  where
    matchFilter' (x:|[]) (Trie m)
      | x == multiLevelWildcard  = matchMultiLevelWildcard
      | x == singleLevelWildcard = matchMultiLevelWildcard || matchSingleLevelWildcard
      | otherwise                = matchMultiLevelWildcard || matchSingleLevelWildcard || matchExact
      where
        matchMultiLevelWildcard  = M.member multiLevelWildcard m
        matchSingleLevelWildcard = isJust ( nodeValue =<< M.lookup singleLevelWildcard m )
        matchExact               = case M.lookup x m of
          Nothing -> False
          Just n' -> isJust (nodeValue n') || let Trie m' = nodeTree n' in M.member multiLevelWildcard m'
    matchFilter' (x:|y:zs) (Trie m)
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

instance TrieValue IS.IntSet where
  data TrieNode IS.IntSet = IntSetTrieNode !(Trie IS.IntSet) !IS.IntSet
  node t                                = IntSetTrieNode t . fromMaybe mempty
  nodeNull                              = IS.null
  nodeTree (IntSetTrieNode t _) = t
  nodeValue (IntSetTrieNode _ v)
    | nodeNull v = Nothing
    | otherwise  = Just v

instance TrieValue (Identity a) where
  data TrieNode (Identity a) = IdentityNode !(Trie (Identity a)) !(Maybe (Identity a))
  node t n@Nothing  = IdentityNode t n
  node t n@(Just v) = v `seq` IdentityNode t n
  nodeNull                      = const False
  nodeTree  (IdentityNode t _)  = t
  nodeValue (IdentityNode _ mv) = mv

instance TrieValue () where
  data TrieNode ()  = UnitNode {-# UNPACK #-} !Int !(Trie ())
  node t Nothing = UnitNode 0 t
  node t _       = UnitNode 1 t
  nodeNull                 = const False
  nodeTree  (UnitNode _ t) = t
  nodeValue (UnitNode 0 _) = Nothing
  nodeValue (UnitNode _ _) = Just ()

instance TrieValue Bool where
  data TrieNode Bool = BoolNode {-# UNPACK #-} !Int !(Trie Bool)
  node t Nothing      = BoolNode 0 t
  node t (Just False) = BoolNode 1 t
  node t (Just True)  = BoolNode 2 t
  nodeNull                     = const False
  nodeTree  (BoolNode _ t)     = t
  nodeValue (BoolNode 1 _) = Just False
  nodeValue (BoolNode 2 _) = Just True
  nodeValue (BoolNode _ _) = Nothing
