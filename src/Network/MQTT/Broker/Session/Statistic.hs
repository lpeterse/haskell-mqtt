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
module Network.MQTT.Broker.Session.Statistic
  ( Statistic ()
  , SessionStatistic (..)
  , newStatistic
  , snapshot
  , accountPacketsSent
  , accountPacketsReceived
  , accountPublicationsAccepted
  , accountPublicationsDropped
  , accountRetentionsAccepted
  , accountRetentionsDropped
  , accountSubscriptionsAccepted
  , accountSubscriptionsRejected
  ) where

import           Control.Concurrent.MVar
import qualified Data.Binary                  as B
import qualified Data.Sequence                as Seq
import           Data.Word
import           GHC.Generics

import           Network.MQTT.Broker.Internal

data SessionStatistic = SessionStatistic
   { ssPacketsSent           :: Word64
   , ssPacketsReceived       :: Word64
   , ssPublicationsAccepted  :: Word64
   , ssPublicationsDropped   :: Word64
   , ssRetentionsAccepted    :: Word64
   , ssRetentionsDropped     :: Word64
   , ssSubscriptionsAccepted :: Word64
   , ssSubscriptionsRejected :: Word64
   , ssQueueQoS0Length       :: Word64
   , ssQueueQoS0Dropped      :: Word64
   , ssQueueQoS1Length       :: Word64
   , ssQueueQoS1Dropped      :: Word64
   , ssQueueQoS2Length       :: Word64
   , ssQueueQoS2Dropped      :: Word64
   } deriving (Eq, Ord, Show, Generic)

instance B.Binary SessionStatistic

newStatistic :: IO Statistic
newStatistic = Statistic
  <$> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0
  <*> newMVar 0

snapshot :: Session auth -> IO SessionStatistic
snapshot session = do
  q <- readMVar (sessionQueue session)
  SessionStatistic
    <$> readMVar (stPacketsSent           st)
    <*> readMVar (stPacketsReceived       st)
    <*> readMVar (stPublicationsAccepted  st)
    <*> readMVar (stPublicationsDropped   st)
    <*> readMVar (stRetentionsAccepted    st)
    <*> readMVar (stRetentionsDropped     st)
    <*> readMVar (stSubscriptionsAccepted st)
    <*> readMVar (stSubscriptionsRejected st)
    <*> pure (fromIntegral $ Seq.length $ queueQoS0 q)
    <*> readMVar (stQueueQoS0Dropped st)
    <*> pure (fromIntegral $ Seq.length $ queueQoS1 q)
    <*> readMVar (stQueueQoS1Dropped st)
    <*> pure (fromIntegral $ Seq.length $ queueQoS2 q)
    <*> readMVar (stQueueQoS2Dropped st)
  where
    st = sessionStatistic session

accountPublicationsAccepted :: Statistic -> Word64 -> IO ()
accountPublicationsAccepted ss c =
  modifyMVar_ (stPublicationsAccepted ss) $ \i-> pure $! i + c

accountPublicationsDropped   :: Statistic -> Word64 -> IO ()
accountPublicationsDropped ss c =
  modifyMVar_ (stPublicationsDropped ss) $ \i-> pure $! i + c

accountSubscriptionsAccepted   :: Statistic -> Word64 -> IO ()
accountSubscriptionsAccepted ss c =
  modifyMVar_ (stSubscriptionsAccepted ss) $ \i-> pure $! i + c

accountSubscriptionsRejected   :: Statistic -> Word64 -> IO ()
accountSubscriptionsRejected ss c =
  modifyMVar_ (stSubscriptionsRejected ss) $ \i-> pure $! i + c

accountRetentionsAccepted   :: Statistic -> Word64 -> IO ()
accountRetentionsAccepted ss c =
  modifyMVar_ (stRetentionsAccepted ss) $ \i-> pure $! i + c

accountRetentionsDropped   :: Statistic -> Word64 -> IO ()
accountRetentionsDropped ss c =
  modifyMVar_ (stRetentionsDropped ss) $ \i-> pure $! i + c

accountPacketsSent        :: Statistic -> Word64 -> IO ()
accountPacketsSent ss c =
  modifyMVar_ (stPacketsSent ss) $ \i-> pure $! i + c

accountPacketsReceived    :: Statistic -> Word64 -> IO ()
accountPacketsReceived ss c =
  modifyMVar_ (stPacketsReceived ss) $ \i-> pure $! i + c
