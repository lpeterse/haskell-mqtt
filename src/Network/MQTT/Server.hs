{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE ScopedTypeVariables #-}
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

import           Control.Exception
import           Control.Monad               (join)
import qualified Data.ByteString             as BS
import qualified Data.ByteString.Builder     as BS
import qualified Data.ByteString.Lazy        as BSL
import           Data.IORef
import qualified Data.Serialize.Get          as SG
import qualified Data.Text                   as T
import           Data.Typeable
import           Network.MQTT.Authentication
import qualified Network.MQTT.Broker         as Broker
import           Network.MQTT.Message
import qualified Network.MQTT.ServerStack    as SS

instance (Typeable auth, Typeable transport) => Exception (SS.ServerException (MQTT auth transport))

data MQTT auth transport

instance (Typeable auth, Typeable transport, SS.ServerStack transport, SS.ServerMessage transport ~ BS.ByteString, Request (SS.ServerConnection transport), Authenticator auth) => SS.ServerStack (MQTT auth transport) where
  type ServerMessage (MQTT auth transport) = RawMessage
  data Server (MQTT auth transport) = MqttServer
    { mqttTransportServer    :: SS.Server transport
    , mqttConfig             :: SS.ServerConfig (MQTT auth transport)
    }
  data ServerConfig (MQTT auth transport) = MqttServerConfig
    { mqttTransportConfig    :: SS.ServerConfig transport
    , mqttBroker             :: Broker.Broker auth
    }
  data ServerConnection (MQTT auth transport) = MqttServerConnection
    { mqttTransportConnection :: SS.ServerConnection transport
    , mqttTransportLeftover   :: IORef BS.ByteString
    , mqttUsername            :: Maybe T.Text
    , mqttPassword            :: Maybe BS.ByteString
    }
  data ServerException (MQTT auth transport)
    = ProtocolViolation String
    | ConnectionRefused ConnectionRefusal
    deriving (Eq, Ord, Show, Typeable)
  new config = MqttServer <$> SS.new (mqttTransportConfig config) <*> pure config
  start server = SS.start (mqttTransportServer server)
  stop server = SS.stop (mqttTransportServer server)
  accept server = bracket (SS.accept $ mqttTransportServer server) SS.close f
    where
      f transportConnection = parse <$> fetch >>= process
        where
          parse = SG.runGetPartial pRawMessage
          fetch = SS.receive transportConnection 4096
          process (SG.Partial continuation) = continuation <$> fetch >>= process
          process (SG.Fail failure _)       = throwIO (ProtocolViolation failure :: SS.ServerException (MQTT auth transport))
          process (SG.Done msg leftover)    = case msg of
            c@Connect {} -> do
              mqttConnection <- MqttServerConnection
                            <$> pure transportConnection
                            <*> newIORef leftover
                            <*> pure (fst <$> connectCredentials c)
                            <*> pure (join $ snd <$> connectCredentials c)
              mp <- authenticate (Broker.brokerAuthenticator $ mqttBroker $ mqttConfig server) mqttConnection
              case mp of
                Just _principal ->
                  pure mqttConnection
                Nothing -> do
                  SS.send transportConnection $ BSL.toStrict $ BS.toLazyByteString $ bRawMessage $ ConnectAcknowledgement (Left NotAuthorized)
                  throwIO (ConnectionRefused NotAuthorized :: SS.ServerException (MQTT auth transport))
            _ -> throwIO (ProtocolViolation "Expected CONN message, got something else." :: SS.ServerException (MQTT auth transport))
  flush connection     = SS.flush (mqttTransportConnection connection)
  close connection =
    -- DISCONNECT shall only be sent from the client from the server, so
    -- nothing to do but closing the transport here.
    SS.close (mqttTransportConnection connection)
  send connection =
    SS.send (mqttTransportConnection connection) . BSL.toStrict . BS.toLazyByteString . bRawMessage
  receive connection i =
    parse <$> readIORef (mqttTransportLeftover connection) >>= process
    where
      parse = SG.runGetPartial pRawMessage
      fetch = SS.receive (mqttTransportConnection connection) i
      process (SG.Partial continuation) = continuation <$> fetch >>= process
      process (SG.Fail failure _)       = throwIO (ProtocolViolation failure :: SS.ServerException (MQTT auth transport))
      process (SG.Done msg bs)          = writeIORef (mqttTransportLeftover connection) bs >> pure msg

instance (Request (SS.ServerConnection transport)) => Request (SS.ServerConnection (MQTT auth transport)) where
  requestSecure           = requestSecure . mqttTransportConnection
  requestUsername         = mqttUsername
  requestPassword         = mqttPassword
  requestHeaders          = requestHeaders . mqttTransportConnection
  requestCertificateChain = requestCertificateChain . mqttTransportConnection
