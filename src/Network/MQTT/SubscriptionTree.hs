{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving #-}
module Network.MQTT.SubscriptionTree where

import Control.DeepSeq
import Control.Concurrent.MVar
import Data.Unique
import Data.Monoid
import Data.Maybe ( fromMaybe )
import Data.List ( tails )
import Data.String

import qualified Data.IntSet as S
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.ByteString.Short as BS

newtype Filter             = Filter [BS.ShortByteString] deriving (Eq, Ord, Show)
newtype Topic              = Topic [BS.ShortByteString] deriving (Eq, Ord, Show)

data SubscriptionTree
   = SubscriptionTree
     { subscriberSet :: !(S.IntSet)
     , subtreeMap    :: !(M.Map BS.ShortByteString (SubscriptionTree))
     } deriving (Eq, Ord)

instance Monoid (SubscriptionTree) where
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

subscribe :: Int -> Filter -> SubscriptionTree -> SubscriptionTree
subscribe unique (Filter []) (SubscriptionTree s m) =
  SubscriptionTree (S.insert unique s) m
subscribe unique (Filter (t:ts)) (SubscriptionTree s m) =
  SubscriptionTree s $ M.insert t (subscribe unique (Filter ts)
  $ fromMaybe mempty $ M.lookup t m) m

unsubscribe :: Int -> Filter -> SubscriptionTree -> SubscriptionTree
unsubscribe unique ts tree
  = difference tree $ subscribe unique ts mempty

subscribers :: Topic -> SubscriptionTree -> S.IntSet
subscribers (Topic []) (SubscriptionTree s m) =
  fromMaybe s $ (s <>) . subscriberSet <$> M.lookup "#" m
subscribers (Topic (t:ts)) (SubscriptionTree _ m) =
  matchComponent <> matchSingleLevelWildcard <> matchMultiLevelWildcard
  where
    matchComponent           = fromMaybe mempty $ subscribers (Topic ts) <$> M.lookup t m
    matchSingleLevelWildcard = fromMaybe mempty $ subscribers (Topic ts) <$> M.lookup "+" m
    matchMultiLevelWildcard  = fromMaybe mempty $ subscriberSet          <$> M.lookup "#" m
