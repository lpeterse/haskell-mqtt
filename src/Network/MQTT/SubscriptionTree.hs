{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving #-}
module Network.MQTT.SubscriptionTree where

import Control.Concurrent.MVar
import Data.Unique
import Data.Monoid
import Data.Maybe ( fromMaybe )
import Data.List ( tails )
import Data.String

import qualified Data.Set as S
import qualified Data.Map as M
import qualified Data.Text as T

newtype Filter             = Filter [T.Text] deriving (Eq, Ord, Show)
newtype Topic              = Topic [T.Text] deriving (Eq, Ord, Show)

data SubscriptionTree a
   = SubscriptionTree
     { subscriberSet :: S.Set a
     , subtreeMap    :: M.Map T.Text (SubscriptionTree a)
     } deriving (Eq, Ord)

instance Ord a => Monoid (SubscriptionTree a) where
  mempty  = SubscriptionTree S.empty M.empty
  mappend (SubscriptionTree s1 m1) (SubscriptionTree s2 m2) =
    SubscriptionTree (S.union s1 s2) (M.unionWith mappend m1 m2)

union :: Ord a => SubscriptionTree a -> SubscriptionTree a -> SubscriptionTree a
union = mappend

difference :: Ord a => SubscriptionTree a -> SubscriptionTree a -> SubscriptionTree a
difference (SubscriptionTree s1 m1) (SubscriptionTree s2 m2) =
  SubscriptionTree (S.difference s1 s2) (M.differenceWith f m1 m2)
  where
    f t1 t2 | diff == mempty = Nothing
            | otherwise      = Just diff
            where
              diff = difference t1 t2

subscribe :: Ord a => a -> Filter -> SubscriptionTree a -> SubscriptionTree a
subscribe unique (Filter []) (SubscriptionTree s m) =
  SubscriptionTree (S.insert unique s) m
subscribe unique (Filter (t:ts)) (SubscriptionTree s m) =
  SubscriptionTree s $ M.insert t (subscribe unique (Filter ts)
  $ fromMaybe mempty $ M.lookup t m) m

unsubscribe :: Ord a => a -> Filter -> SubscriptionTree a -> SubscriptionTree a
unsubscribe unique ts tree
  = difference tree $ subscribe unique ts mempty

subscribers :: Ord a => Topic -> SubscriptionTree a -> S.Set a
subscribers (Topic []) (SubscriptionTree s m) =
  fromMaybe s $ (s <>) . subscriberSet <$> M.lookup "#" m
subscribers (Topic (t:ts)) (SubscriptionTree _ m) =
  matchComponent <> matchSingleLevelWildcard <> matchMultiLevelWildcard
  where
    matchComponent           = fromMaybe mempty $ subscribers (Topic ts) <$> M.lookup t m
    matchSingleLevelWildcard = fromMaybe mempty $ subscribers (Topic ts) <$> M.lookup "+" m
    matchMultiLevelWildcard  = fromMaybe mempty $ subscriberSet  <$> M.lookup "#" m
