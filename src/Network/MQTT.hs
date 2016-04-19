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

import Data.Source
import qualified Data.ByteString as BS

import Network.MQTT.Message
import Network.MQTT.Message.RemainingLength
import Network.MQTT.SubscriptionTree

data MQTT
type Interface m a b = Source m a a -> (b -> m ()) -> m ()

withMQTT    :: Interface IO BS.ByteString BS.ByteString
            -> (MQTT -> IO a)
            -> IO a
withMQTT     = undefined

publish     :: MQTT -> Retain -> Topic -> Message -> IO ()
publish      = undefined

publishQoS1 :: MQTT -> Retain -> Topic -> Message -> IO ()
publishQoS1  = undefined

publishQoS2 :: MQTT -> Retain -> Topic -> Message -> IO ()
publishQoS2  = undefined
