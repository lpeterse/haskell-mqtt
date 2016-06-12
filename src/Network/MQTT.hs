--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
{-# LANGUAGE TypeFamilies #-}
module Network.MQTT where

import Control.Exception

import Data.Typeable
import qualified Data.ByteString as BS

import Network.MQTT.Message

data Message = Message
     { topic     :: Topic
     , payload   :: Payload
     , qos       :: QoS
     , retained  :: Bool
     , duplicate :: Bool
     }
   deriving (Eq, Ord, Show)

class StreamTransmitter s where
  type StreamTransmitterException s
  send    :: s -> BS.ByteString -> IO ()
  receive :: s -> IO BS.ByteString
  close   :: s -> IO ()
