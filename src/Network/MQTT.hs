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
  transmit :: s -> BS.ByteString -> IO ()

class StreamReceiver s where
  receive :: s -> IO BS.ByteString

class Connectable s where
  type ConnectableAddress s
  connect :: s -> ConnectableAddress s -> IO ()

class Closable s where
  close   :: s -> IO ()
