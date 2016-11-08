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
  -- ** singleton
  , singleton
  -- ** matchTopic
  , matchTopic
  -- ** matchFilter
  , matchFilter
  -- ** lookupWith
  , lookupWith
  -- ** insert
  , insert
  -- ** insertWith
  , insertWith
  -- ** insertFoldable
  , insertFoldable
  -- ** map
  , map
  -- ** adjust
  , adjust
  -- ** delete
  , delete
  -- ** unionWith
  , unionWith
  -- ** differenceWith
  , differenceWith
  ) where

import           Data.Functor.Identity
import qualified Data.IntSet           as IS
import           Data.List.NonEmpty    (NonEmpty (..))
import qualified Data.Map              as M
import           Data.Maybe
import           Data.Monoid
import           Network.MQTT.Topic
import           Prelude               hiding (map, null)

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
newtype RoutingTree a = RoutingTree (M.Map Level (RoutingTreeNode a))

class RoutingTreeValue a where
  data RoutingTreeNode a
  nodeNull             :: a -> Bool
  nodeTree             :: RoutingTreeNode a -> RoutingTree a
  nodeValue            :: RoutingTreeNode a -> Maybe a
  nodeFromTree         :: RoutingTree a -> RoutingTreeNode a
  nodeFromTreeAndValue :: RoutingTree a -> a -> RoutingTreeNode a

instance (RoutingTreeValue a, Monoid a) => Monoid (RoutingTree a) where
  mempty  = empty
  mappend = unionWith mappend

empty :: RoutingTree a
empty  = RoutingTree mempty

null  :: RoutingTree a -> Bool
null (RoutingTree m) = M.null m

singleton :: RoutingTreeValue a => Filter -> a -> RoutingTree a
singleton tf = singleton' (filterLevels tf)
  where
    singleton' (x:|xs) a
      | nodeNull a  = empty
      | otherwise   = RoutingTree $ M.singleton x $ case xs of
          []     -> nodeFromTreeAndValue empty a
          (y:ys) -> nodeFromTree (singleton' (y:|ys) a)

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
            Nothing -> nodeFromTreeAndValue empty a
            Just n  -> nodeFromTreeAndValue (nodeTree n) $ fromMaybe a $ f a <$> nodeValue n
          (y:ys) -> nodeFromTree $ insertWith' (y:|ys) $ fromMaybe empty $ nodeTree <$> mn

insertFoldable :: (RoutingTreeValue a, Foldable t) => t (Filter, a) -> RoutingTree a -> RoutingTree a
insertFoldable  = flip $ foldr $ uncurry insert

adjust :: RoutingTreeValue a => (a -> a) -> Filter -> RoutingTree a -> RoutingTree a
adjust f tf = adjust' (filterLevels tf)
  where
    adjust' (x:|xs) (RoutingTree m) = RoutingTree $ M.update g x m
      where
        g n = case xs of
          [] -> case nodeValue n of
            Just v  ->
              let t' = nodeTree n; v' = f v in
              if null t' && nodeNull v'
                then Nothing
                else Just $ nodeFromTreeAndValue t' v'
            Nothing -> Just n -- nodeTree is non-empty by invariant
          (y:ys) ->
            let t'@(RoutingTree m') = adjust' (y:|ys) (nodeTree n) in
            case nodeValue n of
              Just v  -> Just $ nodeFromTreeAndValue t' v
              Nothing -> if M.null m' then Nothing else Just (nodeFromTree t')

delete :: RoutingTreeValue a => Filter -> RoutingTree a -> RoutingTree a
delete tf = delete' (filterLevels tf)
  where
    delete' (x:|xs) (RoutingTree m) = RoutingTree $ M.update g x m
      where
        g n = case xs of
          [] | null (nodeTree n) -> Nothing
             | otherwise     -> Just $ nodeFromTree $ nodeTree n
          y:ys -> let t = delete' (y:|ys) (nodeTree n) in
           case nodeValue n of
             Nothing | null t    -> Nothing
                     | otherwise -> Just $ nodeFromTree t
             Just v -> Just $ nodeFromTreeAndValue t v

map :: (RoutingTreeValue a, RoutingTreeValue b) => (a -> b) -> RoutingTree a -> RoutingTree b
map f (RoutingTree m) = RoutingTree $ fmap g m
    where
      g n = let t = map f (nodeTree n) in case nodeValue n of
        Nothing -> nodeFromTree t
        Just a  -> let b = f a in
          if nodeNull b then nodeFromTree t else nodeFromTreeAndValue t b

unionWith :: (RoutingTreeValue a) => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
unionWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.unionWith g m1 m2)
  where
    merge t (Just v1) (Just v2) = nodeFromTreeAndValue t (f v1 v2)
    merge t (Just v1)  _        = nodeFromTreeAndValue t v1
    merge t  _        (Just v2) = nodeFromTreeAndValue t v2
    merge t  _         _        = nodeFromTree         t
    g n1 n2 = merge (unionWith f (nodeTree n1) (nodeTree n2)) (nodeValue n1) (nodeValue n2)

differenceWith :: (RoutingTreeValue a, RoutingTreeValue b) => (a -> b -> Maybe a) -> RoutingTree a -> RoutingTree b -> RoutingTree a
differenceWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.differenceWith g m1 m2)
  where
    g n1 n2 = k (differenceWith f (nodeTree n1) (nodeTree n2)) (d (nodeValue n1) (nodeValue n2))
    d (Just v1) (Just v2) = f v1 v2
    d (Just v1)  _        = Just v1
    d  _         _        = Nothing
    k t Nothing  | null t               = Nothing
                 | otherwise            = Just (nodeFromTree t)
    k t (Just v) | null t && nodeNull v = Nothing
                 | otherwise            = Just (nodeFromTreeAndValue t v)

lookupWith :: (RoutingTreeValue a) => (a -> a -> a) -> Topic -> RoutingTree a -> Maybe a
lookupWith f tf = lookupWith'(topicLevels tf)
  where
    merge (Just v1) (Just v2) = Just (f v1 v2)
    merge (Just v1) _         = Just v1
    merge _         (Just v2) = Just v2
    merge _         _         = Nothing
    lookupWith' (x:|[]) (RoutingTree m) = case M.lookup x m of
      Nothing -> Nothing
      Just n  -> let RoutingTree m' = nodeTree n; v' = nodeValue n
                 in  fromMaybe v' $ merge v' . nodeValue <$> M.lookup multiLevelWildcard m'
    lookupWith' (x:|y:zs) (RoutingTree m) =
      matchComponent `merge` matchSingleLevelWildcard `merge` matchMultiLevelWildcard
      where
        matchComponent =
          M.lookup x m >>= lookupWith' ( y:|zs) . nodeTree
        matchSingleLevelWildcard =
          M.lookup singleLevelWildcard m >>= lookupWith' (y:|zs) . nodeTree
        matchMultiLevelWildcard = M.lookup multiLevelWildcard m >>= nodeValue

-- | Match a `Topic` against a `RoutingTree`.
--
--   The function returns true iff the tree contains at least one node that
--   matches the topic /and/ contains a value (including nodes that are
--   indirectly matched by wildcard characters like `+` and `#` as described
--   in the MQTT specification).
matchTopic :: RoutingTreeValue a => Topic -> RoutingTree a -> Bool
matchTopic tf = matchTopic' (topicLevels tf)
  where
    matchTopic' (x:|[]) (RoutingTree m) =
      match || matchPlus || matchHash
      -- The '#' is always a terminal node and therefore does not contain subtrees.
      -- By invariant, a '#' node only exists if it contains a value. For this
      -- reason it does not need to be checked for a value here, but just for
      -- existence.
      -- A '+' node on the other hand may contain subtrees and may not carry a value
      -- itself. This needs to be checked.
      where
        matchPlus    = isJust ( nodeValue =<< M.lookup singleLevelWildcard m )
        matchHash    = M.member multiLevelWildcard m
        match        = case M.lookup x m of
          Nothing -> False
          Just n  -> isJust (nodeValue n)
                  || let RoutingTree m' = nodeTree n
                     in  M.member multiLevelWildcard m'
    matchTopic' (x:|y:zs) (RoutingTree m) =
      M.member multiLevelWildcard m || case M.lookup x m of
        -- Same is true for '#' node here. In case no '#' hash node is present it is
        -- first tried to match the exact topic and then to match any '+' node.
        Nothing -> matchPlus
        Just n  -> matchTopic' (y:|zs) (nodeTree n) || matchPlus
      where
        -- A '+' node matches any topic element.
        matchPlus = fromMaybe False
                  $ matchTopic' (y:|zs) . nodeTree <$> M.lookup singleLevelWildcard m

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
  data RoutingTreeNode IS.IntSet
    = IntSetRoutingTreeNode !(RoutingTree IS.IntSet) !IS.IntSet
  nodeNull                              = IS.null
  nodeTree (IntSetRoutingTreeNode t _)  = t
  nodeValue (IntSetRoutingTreeNode _ v) | nodeNull v = Nothing
                                        | otherwise = Just v
  nodeFromTree t                        = IntSetRoutingTreeNode t mempty
  nodeFromTreeAndValue                  = IntSetRoutingTreeNode

instance RoutingTreeValue (Identity a) where
  data RoutingTreeNode (Identity a)
    = TreeNode          !(RoutingTree (Identity a))
    | TreeNodeWithValue !(RoutingTree (Identity a)) !(Identity a)
  nodeNull                             = const False
  nodeTree  (TreeNode          t  )    = t
  nodeTree  (TreeNodeWithValue t _)    = t
  nodeValue (TreeNode          _  )    = Nothing
  nodeValue (TreeNodeWithValue _ v)    = Just v
  nodeFromTree                         = TreeNode
  nodeFromTreeAndValue                 = TreeNodeWithValue

instance RoutingTreeValue () where
  data RoutingTreeNode () = UnitNode {-# UNPACK #-} !Int !(RoutingTree ())
  nodeNull                     = const False
  nodeTree  (UnitNode _ t)     = t
  nodeValue (UnitNode 0 _)     = Nothing
  nodeValue (UnitNode _ _)     = Just ()
  nodeFromTree                 = UnitNode 0
  nodeFromTreeAndValue t _     = UnitNode 1 t

instance RoutingTreeValue Bool where
  data RoutingTreeNode Bool = BoolNode {-# UNPACK #-} !Int !(RoutingTree Bool)
  nodeNull                     = const False
  nodeTree  (BoolNode _ t)     = t
  nodeValue (BoolNode 0 _)     = Just False
  nodeValue (BoolNode 1 _)     = Just True
  nodeValue (BoolNode _ _)     = Nothing
  nodeFromTree                 = BoolNode (-1)
  nodeFromTreeAndValue t False = BoolNode 0 t
  nodeFromTreeAndValue t True  = BoolNode 1 t
