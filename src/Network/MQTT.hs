{-# LANGUAGE OverloadedStrings #-}
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

import Control.Concurrent.MVar
import Control.Concurrent.Async
import Control.Concurrent.BoundedChan

import Network.MQTT.Message
import Network.MQTT.Message.RemainingLength
import Network.MQTT.SubscriptionTree

data MqttClient
   = MqttClient
     { clientIdentifier  :: ClientIdentifier
     , clientKeepAlive   :: KeepAlive
     , clientOutputQueue :: BoundedChan Message
     , clientInputSource :: Source IO BS.ByteString Message
     , timeLastMessageReceived :: MVar Int64
     }

runMqttClient :: MqttClient -> IO ()
runMqttClient c = do
  send $ Connect (clientIdentifier c) True (clientKeepAlive c) Nothing
  msg <- receive
  case msg of
    ConnectAcknowledgement (Left connectionRefusal) -> undefined
    ConnectAcknowledgement (Right session) -> undefined
    _ -> undefined
  where
    send = undefined
    receive = undefined

publishQoS0 :: MqttClient -> Retain -> Topic -> Message -> IO ()
publishQoS0  = undefined

publishQoS1 :: MqttClient -> Retain -> Topic -> Message -> IO ()
publishQoS1  = undefined

publishQoS2 :: MqttClient -> Retain -> Topic -> Message -> IO ()
publishQoS2  = undefined
