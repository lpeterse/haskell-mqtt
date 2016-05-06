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

import Data.Typeable
import Control.Exception

import Network.MQTT.Message

data Message
   = Message
     { qos      :: QoS
     , retained :: Bool
     , topic    :: Topic
     , payload  :: Payload
     } deriving (Eq, Ord, Show)

data QoS
   = QoS0
   | QoS1
   | QoS2
   deriving (Eq, Ord, Show)

data MqttException
   = ParserError String
   | ProtocolViolation String
   | ConnectionRefused ConnectionRefusal
   deriving (Eq, Ord, Show, Typeable)

instance Exception MqttException where
