{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT where

import Data.Int
import Data.Source
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS

import Control.Monad
import Control.Concurrent.MVar
import Control.Concurrent.Async
import Control.Concurrent.BoundedChan

import Network.MQTT.Message
import Network.MQTT.Message.RemainingLength
import Network.MQTT.SubscriptionTree

data MqttClient c
   = MqttClient
     { clientIdentifier        :: ClientIdentifier
     , clientKeepAlive         :: KeepAlive
     , clientOutputQueue       :: MVar Message
     , clientNewConnection     :: IO c
     , clientThread            :: MVar (Maybe (Async ()))
     }


class Channel a where
  type ChannelException a
  send    :: a -> BS.Builder -> IO ()
  receive :: a -> IO BS.ByteString
  close   :: a -> IO ()

newMqttClient :: IO (MqttClient a)
newMqttClient = do
  undefined

connect :: MqttClient a -> IO ()
connect c = modifyMVar_ (clientThread c) $ \mt->
  case mt of
    Just t  -> pure mt -- already connected, do nothing
    Nothing -> do
      undefined
  where
    maintainConnection :: Channel c => c -> IO ()
    maintainConnection c = do
      keepAlive `race_` processInput c `race_` processOutput c
    keepAlive :: IO ()
    keepAlive = do
      undefined
    processInput :: Channel c => c -> IO ()
    processInput channel =
      undefined
    processOutput :: Channel c => c -> IO ()
    processOutput channel =
      forever $ takeMVar (clientOutputQueue c) >>= send channel . bMessage

disconnect :: MqttClient a -> IO ()
disconnect c =  modifyMVar_ (clientThread c) $ \mt->
  case mt of
    Nothing     -> pure mt -- already disconnected, do nothing
    Just thread -> cancel thread >> pure mt

publishQoS0 :: MqttClient a -> Retain -> Topic -> Message -> IO ()
publishQoS0  = undefined

publishQoS1 :: MqttClient a -> Retain -> Topic -> Message -> IO ()
publishQoS1  = undefined

publishQoS2 :: MqttClient a -> Retain -> Topic -> Message -> IO ()
publishQoS2  = undefined
