{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
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
  -- ** map
  , map
  -- ** mapMaybe
  , mapMaybe
  -- ** foldl'
  , foldl'
  -- ** union
  , union
  -- ** unionWith
  , unionWith
  -- ** differenceWith
  , differenceWith
  -- ** toList
  , toList
  -- ** Debugging & Testing
  , valid
  ) where

import           Control.Applicative        ((<|>))
import qualified Data.Binary                as B
import           Data.Functor.Identity
import qualified Data.IntSet                as IS
import qualified Data.List                  as L
import           Data.List.NonEmpty         (NonEmpty (..))
import qualified Data.List.NonEmpty         as NE
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
  nodeTree             :: TrieNode a -> Trie a
  nodeValue            :: TrieNode a -> Maybe a

instance (TrieValue a, Semigroup a) =>  Semigroup (Trie a) where
  (<>) = unionWith (<>)

instance (TrieValue a, Monoid a) => Monoid (Trie a) where
  mempty  = empty
  mappend = (<>)

instance (TrieValue a, Eq a) => Eq (Trie a) where
  Trie m1 == Trie m2 =
    M.size m1 == M.size m2 && and (zipWith f (M.toAscList m1) (M.toAscList m2))
    where
      f (l1,n1) (l2,n2) = l1 == l2 && nodeValue n1 == nodeValue n2 && nodeTree n1 == nodeTree n2

instance (TrieValue a, Show a) => Show (Trie a) where
  show (Trie m) = "Trie [" ++ L.intercalate ", " (f <$> M.toAscList m) ++ "]"
    where
      f (l,n) = "(" ++ show l ++ ", Node (" ++ show (nodeValue n) ++ ") (" ++ show (nodeTree n) ++ ")"

instance (TrieValue a, B.Binary a) => B.Binary (Trie a) where
  put (Trie m) = B.put m
  get          = Trie <$> B.get

instance (TrieValue a, B.Binary a) => B.Binary (TrieNode a) where
  put n        = B.put (nodeTree n) >> B.put (nodeValue n)
  get          = node <$> B.get <*> B.get

-- | Constructs an empty `Trie`.
empty :: Trie a
empty  = Trie mempty

-- | Check whether a `Trie` is empty.
--
--   * O(1)
--   * The trie is empty iff it doesn't contain any values.
--     The invariants guarantee that leaf nodes (if they exist) must contain
--     values.
null  :: Trie a -> Bool
null (Trie m) = M.null m

-- | Count all trie nodes that contain values.
--
--   * O(n)
--   * Convenience for `sizeWith (const 1)`.
size :: TrieValue a => Trie a -> Int
size  = sizeWith (const 1)

-- | Sum up the size the size of all nodes that contain values using a supplied
--   size function.
--
--   * O(n)
--   * This function is strict and uses an accumulator.
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

-- | Construct a `Trie` holding a single value.
singleton :: TrieValue a => Filter -> a -> Trie a
singleton tf = singleton' (filterLevels tf)
  where
    singleton' (x:|xs) a =
      Trie $ M.singleton x $ case xs of
          []     -> node empty (Just a)
          (y:ys) -> node (singleton' (y:|ys) a) Nothing

-- | Insert a value into the trie.
--
--   * O(length(filter))
--   * Convenience for `insertWith const`.
--   * Existing element at the specified position will be replaced.
insert :: TrieValue a => Filter -> a -> Trie a -> Trie a
insert  = insertWith const

-- | Insert a value into the trie and apply a combining function on collision.
--
--   * O(length(filter)) + cost of combining function
insertWith :: TrieValue a => (a -> a -> a) -> Filter -> a -> Trie a -> Trie a
insertWith f tf a = insertWith' (filterLevels tf)
  where
    insertWith' (x:|xs) (Trie m) = Trie $ M.alter g x m
      where
        g mn = Just $ case xs of
          []     -> case mn of
            Nothing -> node empty (Just a)
            Just n  -> node (nodeTree n) $ (f a <$> nodeValue n) <|> Just a
          (y:ys) -> node (insertWith' (y:|ys) $ fromMaybe empty $ nodeTree <$> mn) Nothing

-- | Applies a function to each value in the `Trie`.
--
--   * Removes leaf nodes for which the mapping function return
--     `b` where `nodeNull b`.
map :: (TrieValue a, TrieValue b) => (a -> b) -> Trie a -> Trie b
map f (Trie m) = Trie $ fmap g m
  where
    g n = let t = map f (nodeTree n) in node t (f <$> nodeValue n)

-- | Applies a function to each value in the `Trie` and eventually removes node.
--
--  * Removes leaf nodes for which the mapping function returns `Nothing`
--    or `Just b` where `nodeNull b`.
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

-- | Subtract one `Trie` from another by using a combining function for values with equal keys.
--
--   * If a node does not have a value in the first `Trie`, it won't have a value in the resulting `Trie`.
--   * If a node does not exist in the second `Trie` or doesn't have a value, the value of the first `Trie` is preserved (if any).
--   * If a node has a value in both `Trie`s, the combining function is applied.
differenceWith :: (TrieValue a, TrieValue b) => (a -> b -> Maybe a) -> Trie a -> Trie b -> Trie a
differenceWith f (Trie m1) (Trie m2) = Trie (M.differenceWith g m1 m2)
  where
    g n1 n2 = k (differenceWith f (nodeTree n1) (nodeTree n2)) (d (nodeValue n1) (nodeValue n2))
    d (Just v1) (Just v2) = f v1 v2
    d (Just v1)  _        = Just v1
    d  _         _        = Nothing
    k t Nothing  | null t               = Nothing
                 | otherwise            = Just $ node t Nothing
    k t mv                              = Just $ node t mv

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
      matchMultiLevelWildcard `maxBounded` matchSingleLevelWildcard `maxBounded` matchComponent
      where
        matchMultiLevelWildcard  = M.lookup multiLevelWildcard  m >>=
          nodeValue
        matchSingleLevelWildcard = M.lookup singleLevelWildcard m >>= \n->
          nodeValue n `maxBounded` (nodeValue =<< M.lookup multiLevelWildcard (branches $ nodeTree n))
        matchComponent           = M.lookup x                   m >>= \n->
          nodeValue n `maxBounded` (nodeValue =<< M.lookup multiLevelWildcard (branches $ nodeTree n))
    findTail x (y:ys) (Trie m) =
      matchMultiLevelWildcard `maxBounded` matchSingleLevelWildcard `maxBounded` matchComponent
      where
        matchMultiLevelWildcard  = M.lookup multiLevelWildcard  m >>= nodeValue
        matchSingleLevelWildcard = M.lookup singleLevelWildcard m >>= findTail y ys . nodeTree
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

toList :: (TrieValue a) => Trie a -> [(Filter, a)]
toList = fmap (\(x,y)-> (Filter (NE.reverse x), y)) . accumTrie []
  where
    accumTrie     :: (TrieValue a) => [(NonEmpty Level, a)] -> Trie a -> [(NonEmpty Level, a)]
    accumTrie accum (Trie m) =
      M.foldrWithKey accumTrieNode accum m

    accumTrieNode :: (TrieValue a) =>  Level -> TrieNode a -> [(NonEmpty Level, a)] ->[(NonEmpty Level, a)]
    accumTrieNode l n accum =
      accumTrie' prefix accum' (nodeTree n)
      where
        prefix = l :| []
        accum' = case nodeValue n of
          Nothing -> accum
          Just v  -> (prefix, v):accum

    accumTrie'     :: (TrieValue a) => NonEmpty Level -> [(NonEmpty Level, a)] -> Trie a -> [(NonEmpty Level, a)]
    accumTrie' prefix accum (Trie m) =
      M.foldrWithKey (accumTrieNode' prefix) accum m

    accumTrieNode' :: (TrieValue a) => NonEmpty Level -> Level -> TrieNode a -> [(NonEmpty Level, a)] -> [(NonEmpty Level, a)]
    accumTrieNode' prefix l n accum =
      accumTrie' (l NE.<| prefix) accum' (nodeTree n)
      where
        prefix' = l NE.<| prefix
        accum' = case nodeValue n of
          Nothing -> accum
          Just v  -> (prefix', v):accum

valid :: TrieValue a => Trie a -> Bool
valid (Trie m)
  = all validAndNotEmpty m
  where
    validAndNotEmpty n =
      valid t && (isJust v || not (null t))
      where
        t = nodeTree n
        v = nodeValue n

--------------------------------------------------------------------------------
-- Specialised TrieNode implemenations using data families
--------------------------------------------------------------------------------

instance TrieValue IS.IntSet where
  data TrieNode IS.IntSet = IntSetTrieNode !(Trie IS.IntSet) !IS.IntSet
  node t                        = IntSetTrieNode t . fromMaybe mempty
  nodeTree (IntSetTrieNode t _) = t
  nodeValue (IntSetTrieNode _ v)
    | IS.null v                 = Nothing
    | otherwise                 = Just v

instance TrieValue (Identity a) where
  data TrieNode (Identity a)    = IdentityNode !(Trie (Identity a)) !(Maybe (Identity a))
  node t n@Nothing  = IdentityNode t n
  node t n@(Just _) = IdentityNode t n
  nodeTree  (IdentityNode t _)  = t
  nodeValue (IdentityNode _ mv) = mv

instance TrieValue () where
  data TrieNode ()         = UnitNode {-# UNPACK #-} !Int !(Trie ())
  node t Nothing = UnitNode 0 t
  node t _       = UnitNode 1 t
  nodeTree  (UnitNode _ t) = t
  nodeValue (UnitNode 0 _) = Nothing
  nodeValue (UnitNode _ _) = Just ()

instance TrieValue Bool where
  data TrieNode Bool       = BoolNode {-# UNPACK #-} !Int !(Trie Bool)
  node t Nothing      = BoolNode 0 t
  node t (Just False) = BoolNode 1 t
  node t (Just True)  = BoolNode 2 t
  nodeTree  (BoolNode _ t) = t
  nodeValue (BoolNode 1 _) = Just False
  nodeValue (BoolNode 2 _) = Just True
  nodeValue (BoolNode _ _) = Nothing

--------------------------------------------------------------------------------
-- Untested code snippets for future use
--------------------------------------------------------------------------------

{-
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
-}
