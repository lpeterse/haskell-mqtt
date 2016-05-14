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

import Control.Exception

import Data.Typeable
import qualified Data.ByteString as BS
import System.Socket as S
import System.Socket.Family.Inet as S
import System.Socket.Type.Stream as S
import System.Socket.Protocol.TCP as S

import Network.MQTT.Message

data Message
   = Message
     { qos      :: QoS
     , retained :: Bool
     , topic    :: Topic
     , payload  :: Payload
     } deriving (Eq, Ord, Show)

data Connection
  = Connection
    { receive :: IO BS.ByteString
    , send    :: BS.ByteString -> IO ()
    , close   :: IO ()
    , sock    :: !(S.Socket S.Inet S.Stream S.TCP)
    }

data MqttException
   = ParserError String
   | ProtocolViolation String
   | ConnectionRefused ConnectionRefusal
   | ConnectionClosed
   | ClientLostSession
   | ServerLostSession
   deriving (Eq, Ord, Show, Typeable)

instance Exception MqttException where
