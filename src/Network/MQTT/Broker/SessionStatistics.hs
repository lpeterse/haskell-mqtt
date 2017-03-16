{-# LANGUAGE DeriveGeneric #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker.SessionStatistics
-- Copyright   :  (c) Lars Petersen 2017
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Broker.SessionStatistics
  ( Statistics ()
  , StatisticsSnapshot (..)
  , new
  , snapshot
  , accountPublicationsAccepted
  , accountPublicationsDropped
  , accountSubscriptionsAccepted
  , accountSubscriptionsDenied
  , accountRetentionsAccepted
  , accountRetentionsDropped ) where

import           Control.Concurrent.MVar
import qualified Data.Binary             as B
import           GHC.Generics

data Statistics = Statistics
   { stPublicationsAccepted   :: MVar Word
   , stPublicationsDropped    :: MVar Word
   , stSubscriptionsAccepted  :: MVar Word
   , stSubscriptionsDenied    :: MVar Word
   , stRetentionsAccepted     :: MVar Word
   , stRetentionsDropped      :: MVar Word
   }

data StatisticsSnapshot = StatisticsSnapshot
   { publicationsAccepted   :: Word
   , publicationsDropped    :: Word
   , subscriptionsAccepted  :: Word
   , subscriptionsDenied    :: Word
   , retentionsAccepted     :: Word
   , retentionsDropped      :: Word
   } deriving (Eq, Ord, Show, Generic)

instance B.Binary StatisticsSnapshot

new :: IO Statistics
new = Statistics
  <$> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0

snapshot :: Statistics -> IO StatisticsSnapshot
snapshot st = StatisticsSnapshot
  <$> readMVar (stPublicationsAccepted st)
  <*> readMVar (stPublicationsDropped st)
  <*> readMVar (stSubscriptionsAccepted st)
  <*> readMVar (stSubscriptionsDenied st)
  <*> readMVar (stRetentionsAccepted st)
  <*> readMVar (stRetentionsDropped st)

accountPublicationsAccepted :: Statistics -> Word -> IO ()
accountPublicationsAccepted ss c =
  modifyMVar_ (stPublicationsAccepted ss) $ \i-> pure $! i + c

accountPublicationsDropped   :: Statistics -> Word -> IO ()
accountPublicationsDropped ss c =
  modifyMVar_ (stPublicationsDropped ss) $ \i-> pure $! i + c

accountSubscriptionsAccepted   :: Statistics -> Word -> IO ()
accountSubscriptionsAccepted ss c =
  modifyMVar_ (stSubscriptionsAccepted ss) $ \i-> pure $! i + c

accountSubscriptionsDenied   :: Statistics -> Word -> IO ()
accountSubscriptionsDenied ss c =
  modifyMVar_ (stSubscriptionsDenied ss) $ \i-> pure $! i + c

accountRetentionsAccepted   :: Statistics -> Word -> IO ()
accountRetentionsAccepted ss c =
  modifyMVar_ (stRetentionsAccepted ss) $ \i-> pure $! i + c

accountRetentionsDropped   :: Statistics -> Word -> IO ()
accountRetentionsDropped ss c =
  modifyMVar_ (stRetentionsDropped ss) $ \i-> pure $! i + c
