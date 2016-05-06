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

data MqttException
   = ProtocolViolation String
   | ConnectionRefused ConnectionRefusal
   deriving (Show, Typeable)

instance Exception MqttException where
