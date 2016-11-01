{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies     #-}
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

import           Control.Concurrent
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

data ServerException
   = ProtocolViolation String
   | ConnectionRefused ConnectionRefusal
   deriving (Eq, Ord, Show, Typeable)

instance Exception ServerException

data MqttConnection a = MqttConnection {
    mqttTransport :: a
  , mqttUsername  :: Maybe T.Text
  , mqttPassword  :: Maybe BS.ByteString
  }

instance (Request a) => Request (MqttConnection a) where
  requestSecure           = requestSecure . mqttTransport
  requestUsername         = mqttUsername
  requestPassword         = mqttPassword
  requestHeaders          = requestHeaders . mqttTransport
  requestCertificateChain = requestCertificateChain . mqttTransport

handle :: (SS.ServerStack a, Request (SS.ServerConnection a), Authenticator auth) => Broker.Broker auth -> SS.ServerConnection a -> IO ()
handle broker connection = do
  bs <- parseRequest =<< (SG.runGetPartial pRawMessage <$> SS.receive connection 4096)
  print bs
  where
    parseRequest :: SG.Result RawMessage -> IO BS.ByteString
    parseRequest x = case x of
      SG.Partial continuation -> parseRequest =<< (continuation <$> SS.receive connection 4096)
      SG.Done rawMessage bs   -> acceptRequest rawMessage >> pure bs
      SG.Fail e _             -> throwIO $ ProtocolViolation e
    acceptRequest c@Connect {} = do
      let mqttConnection = MqttConnection connection (fst <$> connectCredentials c) (join $ snd <$> connectCredentials c)
      mp <- authenticate (Broker.brokerAuthenticator broker) mqttConnection
      case mp of
        Nothing -> do
          SS.send connection $ BSL.toStrict $ BS.toLazyByteString $ bRawMessage $ ConnectAcknowledgement (Left NotAuthorized)
          throwIO $ ConnectionRefused NotAuthorized
        Just principal -> do
          print principal
          threadDelay 10000000
    acceptRequest _            = throwIO $ ProtocolViolation "Expected CONN message, got something else."
