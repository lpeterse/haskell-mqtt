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
  , randomTree
  ) where

import Data.Functor.Identity

import Data.Maybe
import Data.Monoid
import Data.List.NonEmpty ( NonEmpty(..) )
import Control.Monad ( foldM )
import qualified Data.List.NonEmpty as NL
import qualified Data.Sequence as S

import qualified Data.ByteString.Short as BS
import qualified Data.Map as M
import qualified Data.IntSet as IS

import System.Random ( randomIO )
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
  | nodeNull a  = empty
  | otherwise = RoutingTree $ M.singleton x $ case xs of
      []     -> nodeFromTreeAndValue empty a
      (y:ys) -> nodeFromTree (singleton (Filter $ y:|ys) a)

insert :: RoutingTreeValue a => Filter -> a -> RoutingTree a -> RoutingTree a
insert  = insertWith const

insertWith :: RoutingTreeValue a => (a -> a -> a) -> Filter -> a -> RoutingTree a -> RoutingTree a
insertWith f (Filter (x:|xs)) a (RoutingTree m)
  | nodeNull a  = RoutingTree m
  | otherwise = RoutingTree $ M.alter g x m
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
        Just a  -> let b = f a in if nodeNull b then nodeFromTree t else nodeFromTreeAndValue t b

unionWith :: (RoutingTreeValue a) => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
unionWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.unionWith g m1 m2)
  where
    g n1 n2 = h (unionWith f (nodeTree n1) (nodeTree n2)) (nodeValue n1) (nodeValue n2)
    h t (Just v1) (Just v2) = nodeFromTreeAndValue t (f v1 v2)
    h t (Just v1)  _        = nodeFromTreeAndValue t v1
    h t  _        (Just v2) = nodeFromTreeAndValue t v2
    h t  _         _        = nodeFromTree         t

differenceWith :: RoutingTreeValue a => (a -> a -> a) -> RoutingTree a -> RoutingTree a -> RoutingTree a
differenceWith f (RoutingTree m1) (RoutingTree m2) = RoutingTree (M.differenceWith g m1 m2)
  where
    g n1 n2 = k (differenceWith f (nodeTree n1) (nodeTree n2)) (d (nodeValue n1) (nodeValue n2))
    d (Just v1) (Just v2)              = Just (f v1 v2)
    d (Just v1)  _                     = Just v1
    d  _         _                     = Nothing
    k t Nothing  | null t              = Nothing
                 | otherwise           = Just (nodeFromTree t)
    k t (Just v) | null t && nodeNull v = Nothing
                 | otherwise           = Just (nodeFromTreeAndValue t v)

lookupWith :: (RoutingTreeValue a) => (a -> a -> a) -> Topic -> RoutingTree a -> Maybe a
lookupWith f (Topic (x:|[])) (RoutingTree m) =
  case M.lookup x m of
    Nothing -> Nothing
    Just n  -> let RoutingTree m' = nodeTree n in
      case M.lookup "#" m' of
        Nothing -> nodeValue n
        Just n' -> nodeValue n `h` nodeValue n'
  where
    h (Just v1) (Just v2) = Just (f v1 v2)
    h (Just v1) _         = Just v1
    h _         (Just v2) = Just v2
    h _         _         = Nothing
lookupWith f (Topic (x:|y:zs)) (RoutingTree m) =
  matchComponent `h` matchSingleLevelWildcard `h` matchMultiLevelWildcard
  where
    matchComponent =
      M.lookup x m >>= lookupWith f (Topic $ y:|zs) . nodeTree
    matchSingleLevelWildcard =
      M.lookup "+" m >>= lookupWith f (Topic $ y:|zs) . nodeTree
    matchMultiLevelWildcard = M.lookup "#" m >>= nodeValue
    h (Just v1) (Just v2) = Just (f v1 v2)
    h (Just v1) _         = Just v1
    h _         (Just v2) = Just v2
    h _         _         = Nothing

--------------------------------------------------------------------------------
-- Test functions
--------------------------------------------------------------------------------

randomTree :: Int -> Int -> IO (RoutingTree IS.IntSet)
randomTree 0     branching = RoutingTree <$> pure mempty
randomTree depth branching = RoutingTree <$> foldM (\m e->
  flip (M.insert e) m <$> (nodeFromTreeAndValue
  <$> randomTree (depth - 1) branching
  <*> randomSet :: IO (RoutingTreeNode IS.IntSet))) M.empty (take branching elements)
  where
    randomSet :: IO IS.IntSet
    randomSet = f 0 IS.empty
      where
        f :: Int -> IS.IntSet -> IO IS.IntSet
        f i accum = do
          p <- randomIO :: IO Double
          if p < 0.25
            then pure accum
            else f (succ i) $! IS.insert i accum

elements  :: [BS.ShortByteString]
elements =
  [ "a","b","c","d","e","f","g","h","i","j","k","l","m" ]

--------------------------------------------------------------------------------
-- Specialised nodeTree implemenations using data families
--------------------------------------------------------------------------------

class RoutingTreeValue a where
  data RoutingTreeNode a
  nodeNull             :: a -> Bool
  nodeTree             :: RoutingTreeNode a -> RoutingTree a
  nodeValue            :: RoutingTreeNode a -> Maybe a
  nodeFromTree         :: RoutingTree a -> RoutingTreeNode a
  nodeFromTreeAndValue :: RoutingTree a -> a -> RoutingTreeNode a

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
  nodeFromTree                         = BoolNode 2
  nodeFromTreeAndValue t False         = BoolNode 0 t
  nodeFromTreeAndValue t True          = BoolNode 1 t
