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
  , accountMessagePublished
  , accountMessageDropped ) where

import           Control.Concurrent.MVar
import qualified Data.Binary             as B
import           Data.Word
import           GHC.Generics

data Statistics = Statistics
   { stMessagesPublished :: MVar Word64
   , stMessagesDropped   :: MVar Word64
   }

data StatisticsSnapshot = StatisticsSnapshot
   { messagesPublished :: Word64
   , messagesDropped   :: Word64
   } deriving (Eq, Ord, Show, Generic)

instance B.Binary StatisticsSnapshot

new :: IO Statistics
new = Statistics
  <$> newMVar 0
  <*> newMVar 0

snapshot :: Statistics -> IO StatisticsSnapshot
snapshot st = StatisticsSnapshot
  <$> readMVar (stMessagesPublished st)
  <*> readMVar (stMessagesDropped st)

accountMessagePublished :: Statistics -> IO ()
accountMessagePublished ss =
  modifyMVar_ (stMessagesPublished ss) $ \i-> pure $! i + 1

accountMessageDropped   :: Statistics -> IO ()
accountMessageDropped ss =
  modifyMVar_ (stMessagesDropped ss) $ \i-> pure $! i + 1
