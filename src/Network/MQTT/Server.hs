{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
  accept server = bracket (SS.accept $ mqttTransportServer server) SS.close $ \transportConnection->
    fetchRequest transportConnection =<< (SG.runGetPartial pRawMessage <$> SS.receive transportConnection 4096)
    where
      fetchRequest transportConnection x = case x of
        SG.Partial continuation -> fetchRequest transportConnection =<< (continuation <$> SS.receive transportConnection 4096)
        SG.Done rawMessage _    -> decideRequest transportConnection rawMessage
        SG.Fail e _             -> throwIO (ProtocolViolation e :: SS.ServerException (MQTT auth transport))
      decideRequest transportConnection c@Connect {} = do
        let mqttConnection = MqttServerConnection transportConnection (fst <$> connectCredentials c) (join $ snd <$> connectCredentials c)
        mp <- authenticate (Broker.brokerAuthenticator $ mqttBroker $ mqttConfig server) mqttConnection
        case mp of
          Just principal -> pure mqttConnection
          Nothing -> do
            SS.send transportConnection $ BSL.toStrict $ BS.toLazyByteString $ bRawMessage $ ConnectAcknowledgement (Left NotAuthorized)
            throwIO (ConnectionRefused NotAuthorized :: SS.ServerException (MQTT auth transport))
      decideRequest _ _ = throwIO (ProtocolViolation "Expected CONN message, got something else." :: SS.ServerException (MQTT auth transport))
  flush connection   = SS.flush (mqttTransportConnection connection)
  close connection   = SS.close (mqttTransportConnection connection)
  send connection    = undefined
  receive connection = undefined

instance (Request (SS.ServerConnection transport)) => Request (SS.ServerConnection (MQTT auth transport)) where
  requestSecure           = requestSecure . mqttTransportConnection
  requestUsername         = mqttUsername
  requestPassword         = mqttPassword
  requestHeaders          = requestHeaders . mqttTransportConnection
  requestCertificateChain = requestCertificateChain . mqttTransportConnection
