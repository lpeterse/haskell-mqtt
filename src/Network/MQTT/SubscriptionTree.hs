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
subscribe unique [] (SubscriptionTree subscriberSet' subtreeMap') =
  SubscriptionTree (S.insert unique subscriberSet') subtreeMap'
subscribe unique (t:ts) (SubscriptionTree subscriberSet' subtreeMap') =
  SubscriptionTree subscriberSet' $ M.insert t (subscribe unique ts
  $ fromMaybe mempty $ M.lookup t subtreeMap') subtreeMap'

unsubscribe :: Unique -> [FilterComponent] -> SubscriptionTree -> SubscriptionTree
unsubscribe unique ts tree
  = difference tree $ subscribe unique ts mempty

subscribers :: SubscriptionTree -> [TopicComponent] -> S.Set Unique
subscribers  (SubscriptionTree subscribers _) [] =
  subscribers
subscribers  (SubscriptionTree _ subtrees) tts@(t:ts) =
  match <> skipOne <> skipMany
  where
    match    = case M.lookup  t  subtrees of
      Nothing      -> mempty
      Just subtree -> subscribers subtree ts
    skipOne  = case M.lookup "+" subtrees of
      Nothing      -> mempty
      Just subtree -> subscribers subtree ts
    skipMany = case M.lookup "#" subtrees of
      Nothing      -> mempty
      -- TODO: Think about the implications of exponential explosion here!
      Just subtree -> S.unions $ map (subscribers subtree) (tails tts)
