{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies, StandaloneDeriving        #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Server
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Server where

import qualified Control.Exception as E
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Builder  as BS
import qualified Data.ByteString.Lazy     as BSL
import           Data.IORef
import qualified Data.Serialize.Get       as SG
import           Data.Typeable
import           Network.MQTT.Message
import qualified Network.Stack.Server     as SS

instance (Typeable transport) => E.Exception (SS.ServerException (MQTT transport))

data MQTT transport

instance (SS.ServerStack transport, SS.ServerMessage transport ~ BS.ByteString) => SS.ServerStack (MQTT transport) where
  type ServerMessage (MQTT transport) = RawMessage
  data Server (MQTT transport) = MqttServer
    { mqttTransportServer     :: SS.Server transport
    , mqttConfig              :: SS.ServerConfig (MQTT transport)
    }
  data ServerConfig (MQTT transport) = MqttServerConfig
    { mqttTransportConfig     :: SS.ServerConfig transport
    }
  data ServerConnection (MQTT transport) = MqttServerConnection
    { mqttTransportConnection :: SS.ServerConnection transport
    , mqttTransportLeftover   :: IORef BS.ByteString
    }
  data ServerConnectionInfo (MQTT transport) = MqttServerConnectionInfo
    { mqttTransportServerConnectionInfo :: SS.ServerConnectionInfo transport
    }
  data ServerException (MQTT transport)
    = ProtocolViolation String
    | ConnectionRefused ConnectionRefusal
    deriving (Eq, Ord, Show, Typeable)
  withServer config handle =
    SS.withServer (mqttTransportConfig config) $ \server->
      handle (MqttServer server config)
  withConnection server handleConnection =
    SS.withConnection (mqttTransportServer server) $ \connection info->
      flip handleConnection (MqttServerConnectionInfo info) =<< MqttServerConnection
        <$> pure connection
        <*> newIORef mempty
  flush connection = SS.flush (mqttTransportConnection connection)
  send connection =
    SS.send (mqttTransportConnection connection) . BSL.toStrict . BS.toLazyByteString . bRawMessage
  receive connection i =
    parse <$> readIORef (mqttTransportLeftover connection) >>= process
    where
      parse = SG.runGetPartial pRawMessage
      fetch = SS.receive (mqttTransportConnection connection) i
      process (SG.Partial continuation) = continuation <$> fetch >>= process
      process (SG.Fail failure _)       = E.throwIO (ProtocolViolation failure :: SS.ServerException (MQTT transport))
      process (SG.Done msg bs)          = writeIORef (mqttTransportLeftover connection) bs >> pure msg

deriving instance Show (SS.ServerConnectionInfo a) => Show (SS.ServerConnectionInfo (MQTT a))
