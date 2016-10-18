{-# LANGUAGE OverloadedStrings #-}
module Network.MQTT.SubscriptionTree where

import Control.Concurrent.MVar
import Data.Unique
import Data.Monoid
import Data.Maybe ( fromMaybe )
import Data.List ( tails )

import qualified Data.Set as S
import qualified Data.Map as M
import qualified Data.Text as T

type FilterComponent    = T.Text
type TopicComponent     = T.Text

data SubscriptionTree
   = SubscriptionTree
     { subscriberSet :: S.Set Unique
     , subtreeMap    :: M.Map FilterComponent SubscriptionTree
     } deriving (Eq, Ord)

instance Monoid SubscriptionTree where
  mempty  = SubscriptionTree S.empty M.empty
  mappend (SubscriptionTree s1 m1) (SubscriptionTree s2 m2) =
    SubscriptionTree (S.union s1 s2) (M.unionWith mappend m1 m2)

union :: SubscriptionTree -> SubscriptionTree -> SubscriptionTree
union = mappend

difference :: SubscriptionTree -> SubscriptionTree -> SubscriptionTree
difference (SubscriptionTree s1 m1) (SubscriptionTree s2 m2) =
  SubscriptionTree (S.difference s1 s2) (M.differenceWith f m1 m2)
  where
    f t1 t2 | diff == mempty = Nothing
            | otherwise      = Just diff
            where
              diff = difference t1 t2

subscribe :: Unique -> [FilterComponent] -> SubscriptionTree -> SubscriptionTree
subscribe unique [] (SubscriptionTree s m) =
  SubscriptionTree (S.insert unique s) m
subscribe unique (t:ts) (SubscriptionTree s m) =
  SubscriptionTree s $ M.insert t (subscribe unique ts
  $ fromMaybe mempty $ M.lookup t m) m

unsubscribe :: Unique -> [FilterComponent] -> SubscriptionTree -> SubscriptionTree
unsubscribe unique ts tree
  = difference tree $ subscribe unique ts mempty

subscribers :: SubscriptionTree -> [TopicComponent] -> S.Set Unique
subscribers  (SubscriptionTree s m) [] =
  s <> matchMultiLevelWildcard
  where
    matchMultiLevelWildcard  = case M.lookup "#" m of
      Nothing      -> mempty
      Just subtree -> subscriberSet subtree
subscribers  (SubscriptionTree _ m) (t:ts) =
  matchComponent <> matchSingleLevelWildcard <> matchMultiLevelWildcard
  where
    matchComponent           = case M.lookup  t  m of
      Nothing      -> mempty
      Just subtree -> subscribers subtree ts
    matchSingleLevelWildcard = case M.lookup "+" m of
      Nothing      -> mempty
      Just subtree -> subscribers subtree ts
    matchMultiLevelWildcard  = case M.lookup "#" m of
      Nothing      -> mempty
      Just subtree -> subscriberSet subtree
