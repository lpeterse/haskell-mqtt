{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeFamilies        #-}
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

import           Control.Concurrent.MVar
import qualified Control.Exception       as E
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.Serialize.Get      as SG
import           Data.Typeable
import           Network.MQTT.Message
import qualified Network.Stack.Server    as SS

instance (Typeable transport) => E.Exception (SS.ServerException (MQTT transport))

data MQTT transport

instance (SS.StreamServerStack transport) => SS.ServerStack (MQTT transport) where
  data Server (MQTT transport) = MqttServer
    { mqttTransportServer     :: SS.Server transport
    , mqttConfig              :: SS.ServerConfig (MQTT transport)
    }
  data ServerConfig (MQTT transport) = MqttServerConfig
    { mqttTransportConfig     :: SS.ServerConfig transport
    }
  data ServerConnection (MQTT transport) = MqttServerConnection
    { mqttTransportConnection :: SS.ServerConnection transport
    , mqttTransportLeftover   :: MVar BS.ByteString
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
        <*> newMVar mempty

instance (SS.StreamServerStack transport) => SS.MessageServerStack (MQTT transport) where
  type SendMessage (MQTT transport) = DownstreamMessage
  type ReceiveMessage (MQTT transport) = UpstreamMessage
  sendMessage connection =
    SS.sendStreamLazy (mqttTransportConnection connection) . BS.toLazyByteString . buildDownstreamMessage
  receiveMessage connection =
    modifyMVar (mqttTransportLeftover connection) (execute . decode)
    where
      fetch  = SS.receiveStream (mqttTransportConnection connection)
      decode = SG.runGetPartial parseUpstreamMessage
      execute (SG.Partial continuation) = execute =<< continuation <$> fetch
      execute (SG.Fail failure _)       = E.throwIO (ProtocolViolation failure :: SS.ServerException (MQTT transport))
      execute (SG.Done msg leftover')   = pure (leftover', msg)
  consumeMessages connection consume =
    modifyMVar_ (mqttTransportLeftover connection) (execute . decode)
    where
      fetch  = SS.receiveStream (mqttTransportConnection connection)
      decode = SG.runGetPartial parseUpstreamMessage
      execute (SG.Partial continuation) = execute =<< continuation <$> fetch
      execute (SG.Fail failure _)       = E.throwIO (ProtocolViolation failure :: SS.ServerException (MQTT transport))
      execute (SG.Done msg leftover')   = do
        done <- consume msg
        if done
          then pure leftover'
          else execute (decode leftover')

deriving instance Show (SS.ServerConnectionInfo a) => Show (SS.ServerConnectionInfo (MQTT a))
