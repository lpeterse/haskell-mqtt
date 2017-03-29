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
import           GHC.Generics

import           Network.MQTT.Broker.Internal

data SessionStatistic = SessionStatistic
   { ssPublicationsAccepted  :: Word
   , ssPublicationsDropped   :: Word
   , ssRetentionsAccepted    :: Word
   , ssRetentionsDropped     :: Word
   , ssSubscriptionsAccepted :: Word
   , ssSubscriptionsRejected :: Word
   , ssQueueQoS0Length       :: Word
   , ssQueueQoS0Dropped      :: Word
   , ssQueueQoS1Length       :: Word
   , ssQueueQoS1Dropped      :: Word
   , ssQueueQoS2Length       :: Word
   , ssQueueQoS2Dropped      :: Word
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

snapshot :: Session auth -> IO SessionStatistic
snapshot session = do
  q <- readMVar (sessionQueue session)
  SessionStatistic
    <$> readMVar (stPublicationsAccepted  st)
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

accountPublicationsAccepted :: Statistic -> Word -> IO ()
accountPublicationsAccepted ss c =
  modifyMVar_ (stPublicationsAccepted ss) $ \i-> pure $! i + c

accountPublicationsDropped   :: Statistic -> Word -> IO ()
accountPublicationsDropped ss c =
  modifyMVar_ (stPublicationsDropped ss) $ \i-> pure $! i + c

accountSubscriptionsAccepted   :: Statistic -> Word -> IO ()
accountSubscriptionsAccepted ss c =
  modifyMVar_ (stSubscriptionsAccepted ss) $ \i-> pure $! i + c

accountSubscriptionsRejected   :: Statistic -> Word -> IO ()
accountSubscriptionsRejected ss c =
  modifyMVar_ (stSubscriptionsRejected ss) $ \i-> pure $! i + c

accountRetentionsAccepted   :: Statistic -> Word -> IO ()
accountRetentionsAccepted ss c =
  modifyMVar_ (stRetentionsAccepted ss) $ \i-> pure $! i + c

accountRetentionsDropped   :: Statistic -> Word -> IO ()
accountRetentionsDropped ss c =
  modifyMVar_ (stRetentionsDropped ss) $ \i-> pure $! i + c
