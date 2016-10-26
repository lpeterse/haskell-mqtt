{-# LANGUAGE TypeFamilies, FlexibleInstances, OverloadedStrings #-}
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
  -- ** lookupWith
  , lookupWith
  -- ** matchTopic
  , matchTopic
  -- ** matchFilter
  , matchFilter
  -- ** insert
  , insert
  -- ** insertWith
  , insertWith
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

import Prelude hiding ( map, null )

import Data.Maybe
import Data.Monoid
import Data.List.NonEmpty ( NonEmpty(..) )
import Data.Functor.Identity

import qualified Data.List.NonEmpty as NL
import qualified Data.Sequence as S

import qualified Data.ByteString.Short as BS
import qualified Data.Map as M
import qualified Data.IntSet as IS

import Network.MQTT.TopicFilter

-- | The `RoutingTree` is a map-like data structure designed to hold elements
--   that can efficiently be queried according to the matching rules specified
--   by MQTT.
--   The primary purpose is to manage client subscriptions, but it can just
--   as well be used to manage permissions etc.
--
--   The tree consists of nodes that may or may not contain values. The edges
--   are filter components. As some value types have the concept of a null
--   (i.e. an empty set) the `RoutingTreeValue` is a class defining the data
--   family `RoutingTreeNode`. This a performance and size optimization to avoid
--   unnecessary boxing and case distinction.
newtype RoutingTree a = RoutingTree (M.Map BS.ShortByteString (RoutingTreeNode a))

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
singleton (Filter (x:|xs)) a
  | nodeNull a  = empty
  | otherwise   = RoutingTree $ M.singleton x $ case xs of
      []     -> nodeFromTreeAndValue empty a
      (y:ys) -> nodeFromTree (singleton (Filter $ y:|ys) a)

insert :: RoutingTreeValue a => Filter -> a -> RoutingTree a -> RoutingTree a
insert  = insertWith const

insertWith :: RoutingTreeValue a => (a -> a -> a) -> Filter -> a -> RoutingTree a -> RoutingTree a
insertWith f (Filter (x:|xs)) a (RoutingTree m)
  | nodeNull a  = RoutingTree m
  | otherwise   = RoutingTree $ M.alter g x m
  where
    g mn = Just $ case xs of
      []     -> case mn of
        Nothing -> nodeFromTreeAndValue empty a
        Just n  -> nodeFromTreeAndValue (nodeTree n) $ fromMaybe a $ f a <$> nodeValue n
      (y:ys) -> nodeFromTree $ insertWith f (Filter $ y:|ys) a $ fromMaybe empty $ nodeTree <$> mn

adjust :: RoutingTreeValue a => (a -> a) -> Filter -> RoutingTree a -> RoutingTree a
adjust f (Filter (x:|xs)) (RoutingTree m) =
  RoutingTree $ M.update g x m
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
        let t'@(RoutingTree m') = adjust f (Filter $ y:|ys) (nodeTree n) in
        case nodeValue n of
          Just v  -> Just $ nodeFromTreeAndValue t' v
          Nothing -> if M.null m' then Nothing else Just (nodeFromTree t')

delete :: RoutingTreeValue a => Filter -> RoutingTree a -> RoutingTree a
delete (Filter (x:|xs)) (RoutingTree m) =
  RoutingTree $ M.update g x m
  where
    g n = case xs of
      [] | null (nodeTree n) -> Nothing
         | otherwise     -> Just $ nodeFromTree $ nodeTree n
      y:ys -> let t = delete (Filter $ y:|ys) (nodeTree n) in
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
    g n1 n2 = merge
      (unionWith f (nodeTree n1) (nodeTree n2))
      (nodeValue n1) (nodeValue n2)
    merge t (Just v1) (Just v2) = nodeFromTreeAndValue t (f v1 v2)
    merge t (Just v1)  _        = nodeFromTreeAndValue t v1
    merge t  _        (Just v2) = nodeFromTreeAndValue t v2
    merge t  _         _        = nodeFromTree         t

differenceWith :: RoutingTreeValue a => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
differenceWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.differenceWith g m1 m2)
  where
    g n1 n2 = k (differenceWith f (nodeTree n1) (nodeTree n2)) (d (nodeValue n1) (nodeValue n2))
    d (Just v1) (Just v2)               = Just (f v1 v2)
    d (Just v1)  _                      = Just v1
    d  _         _                      = Nothing
    k t Nothing  | null t               = Nothing
                 | otherwise            = Just (nodeFromTree t)
    k t (Just v) | null t && nodeNull v = Nothing
                 | otherwise            = Just (nodeFromTreeAndValue t v)

lookupWith :: (RoutingTreeValue a) => (a -> a -> a) -> Topic -> RoutingTree a -> Maybe a
lookupWith f (Topic (x:|[])) (RoutingTree m) =
  case M.lookup x m of
    Nothing -> Nothing
    Just n  -> let RoutingTree m' = nodeTree n; v' = nodeValue n
               in  fromMaybe v' $ merge v' . nodeValue <$> M.lookup hashElement m
  where
    merge (Just v1) (Just v2) = Just (f v1 v2)
    merge (Just v1) _         = Just v1
    merge _         (Just v2) = Just v2
    merge _         _         = Nothing
lookupWith f (Topic (x:|y:zs)) (RoutingTree m) =
  matchComponent `merge` matchSingleLevelWildcard `merge` matchMultiLevelWildcard
  where
    merge (Just v1) (Just v2) = Just (f v1 v2)
    merge (Just v1) _         = Just v1
    merge _         (Just v2) = Just v2
    merge _         _         = Nothing
    matchComponent =
      M.lookup x m >>= lookupWith f (Topic $ y:|zs) . nodeTree
    matchSingleLevelWildcard =
      M.lookup plusElement m >>= lookupWith f (Topic $ y:|zs) . nodeTree
    matchMultiLevelWildcard = M.lookup hashElement m >>= nodeValue

-- | Match a `Topic` against a `RoutingTree`.
--
--   The function returns true iff the tree contains at least one node that
--   matches the topic /and/ contains a value (including nodes that are
--   indirectly matched by wildcard characters like `+` and `#` as described
--   in the MQTT specification).
matchTopic :: RoutingTreeValue a => Topic -> RoutingTree a -> Bool
matchTopic (Topic (x:|[])) (RoutingTree m) =
  -- The '#' is always a terminal node and therefore does not contain subtrees.
  -- By invariant, a '#' node only exists if it contains a value. For this
  -- reason it does not need to be checked for a value here, but just for
  -- existence.
  -- A '+' node on the other hand may contain subtrees and may not carry a value
  -- itself. This needs to be checked.
  M.member hashElement m || fromMaybe False
    ( not . nodeNull <$> ( nodeValue =<< M.lookup plusElement m ) )
matchTopic (Topic (x:|y:zs)) (RoutingTree m) =
  -- Same is true for '#' node here. In case no '#' hash node is present it is
  -- first tried to match the exact topic and then to match any '+' node.
  M.member hashElement m || case M.lookup x m of
      Nothing -> matchPlus
      Just n  -> matchTopic (Topic (y:|zs)) (nodeTree n) || matchPlus
  where
    -- A '+' node matches any topic element.
    matchPlus = fromMaybe False
              $ matchTopic (Topic (y:|zs)) . nodeTree <$> M.lookup plusElement m

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
matchFilter (Filter (x:|[])) (RoutingTree m) =
  fromMaybe False $ not . nodeNull <$> ( nodeValue =<< M.lookup x m )
matchFilter (Filter (x:|(y:zs))) (RoutingTree m) =
  fromMaybe False $ matchFilter (Filter $ y:|zs) . nodeTree <$> M.lookup x m

--------------------------------------------------------------------------------
-- Internal Utilities
--------------------------------------------------------------------------------

hashElement :: BS.ShortByteString
hashElement  = "#"

plusElement :: BS.ShortByteString
plusElement  = "+"

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
  nodeNull                             = const False
  nodeTree  (UnitNode _ t)             = t
  nodeValue (UnitNode 0 _)             = Nothing
  nodeValue (UnitNode _ _)             = Just ()
  nodeFromTree                         = UnitNode 0
  nodeFromTreeAndValue t _             = UnitNode 1 t

instance RoutingTreeValue Bool where
  data RoutingTreeNode Bool = BoolNode {-# UNPACK #-} !Int !(RoutingTree Bool)
  nodeNull                             = const False
  nodeTree  (BoolNode _ t)             = t
  nodeValue (BoolNode 0 _)             = Just False
  nodeValue (BoolNode 1 _)             = Just True
  nodeValue (BoolNode _ _)             = Nothing
  nodeFromTree                         = BoolNode (-1)
  nodeFromTreeAndValue t False         = BoolNode 0 t
  nodeFromTreeAndValue t True          = BoolNode 1 t
