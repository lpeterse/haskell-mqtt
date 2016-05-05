{-# LANGUAGE OverloadedStrings, TypeFamilies, BangPatterns #-}
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

import Data.Int
import Data.Source
import Data.Source.Attoparsec
import Data.Typeable
import qualified Data.Map as M
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as LBS

import Control.Exception
import Control.Monad
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.Async
import Control.Concurrent.BoundedChan

import Network.MQTT.Message
import Network.MQTT.Message.RemainingLength
import Network.MQTT.PacketIdentifier

data MqttClient c
   = MqttClient
     { clientIdentifier              :: ClientIdentifier
     , clientKeepAlive               :: KeepAlive
     , clientWill                    :: Maybe Will
     , clientUsernamePassword        :: Maybe (Username, Maybe Password)
     , clientRecentActivity          :: MVar Bool
     , clientOutputQueue             :: MVar Message
     , clientSubscriptions           :: MVar (M.Map TopicFilter (Maybe QoS))
     , clientPacketIdentifierPool    :: PacketIdentifierPool Message
     , clientNewConnection           :: IO Connection
     , clientProcessor               :: MVar (Async ())
     }

class Runnable a where
  start  :: a -> IO ()
  stop   :: a -> IO ()
  status :: a -> IO RunState

data RunState
   = Stopped
   | Stopping
   | Starting
   | Running
   deriving (Eq, Ord, Show)

data Connection
   = Connection
     { receive :: IO Message
     , send    :: Message -> IO ()
     }

connect :: MqttClient a -> IO ()
connect c = modifyMVar_ (clientProcessor c) $ \p->
  poll p >>= \m-> case m of
    -- Processing thread is stil running, no need to connect.
    Nothing -> pure p
    -- Processing thread not running, create a new one with new connection
    Just _  -> clientNewConnection c >>= async . establishConnection
  where
    establishConnection :: Connection -> IO ()
    establishConnection connection = do
      send connection Connect
        { connectClientIdentifier = clientIdentifier c
        , connectCleanSession     = True
        , connectKeepAlive        = clientKeepAlive c
        , connectWill             = clientWill c
        , connectUsernamePassword = clientUsernamePassword c
        }
      message <- receive connection
      case message of
        ConnectAcknowledgement a -> case a of
          Right sessionPresent   -> maintainConnection connection -- TODO: handle sessionPresent
          Left connectionRefusal -> throwIO $ ConnectionRefused connectionRefusal
        _ -> throwIO $ ProtocolViolation "Expected CONNACK, got something else for CONNECT."
      undefined
    maintainConnection :: Connection -> IO ()
    maintainConnection c =
      keepAlive `race_` processOutput c `race_` processInput c
    -- The keep alive thread wakes up every `keepAlive/2` seconds.
    -- It reads and unsets the recent-activity flag.
    -- When it finds the recent-activity flag unset, it sends a PINGREQ to the
    -- server. The interval between the last message and the PINGREQ is at
    -- most `keepAlive` seconds (assuming we get woken up on time by the RTS).
    keepAlive :: IO ()
    keepAlive = forever $ do
      threadDelay $ 500000 * fromIntegral (clientKeepAlive c)
      activity <- swapMVar (clientRecentActivity c) False
      unless activity $ putMVar (clientOutputQueue c) PingRequest
    processOutput :: Connection -> IO ()
    processOutput connection = forever $ do
      void $ swapMVar (clientRecentActivity c) True
      send connection =<< takeMVar (clientOutputQueue c)
    processInput :: Connection -> IO ()
    processInput connection = forever $ do
      message <- receive connection
      case message of
        Publish {} -> pure ()
        -- The following packet types are responses to earlier requests.
        -- We need to dispatch them to the waiting threads.
        PublishAcknowledgement i ->
          resolvePacketIdentifier (clientPacketIdentifierPool c) i message
        PublishReceived i ->
          resolvePacketIdentifier (clientPacketIdentifierPool c) i message
        PublishRelease i ->
          resolvePacketIdentifier (clientPacketIdentifierPool c) i message
        PublishComplete i ->
          resolvePacketIdentifier (clientPacketIdentifierPool c) i message
        SubscribeAcknowledgement i _ ->
          resolvePacketIdentifier (clientPacketIdentifierPool c) i message
        UnsubscribeAcknowledgement i ->
          resolvePacketIdentifier (clientPacketIdentifierPool c) i message
        -- Do nothing on a PINGREQ.
        PingResponse -> pure ()
        -- The following packets must not be sent from the server to the client.
        _ -> throwIO $ ProtocolViolation "Unexpected packet type received from the server."

publish :: MqttClient a -> Maybe QoS -> Retain -> Topic -> BS.ByteString -> IO ()
publish client mqos !retain !topic !body = case mqos of
  Nothing  -> putMVar (clientOutputQueue client)
    Publish {
      publishDuplicate = False,
      publishRetain = retain,
      publishQoS = Nothing,
      publishTopic = topic,
      publishBody = body }
  Just qos -> bracket takeIdentifier returnIdentifier $
    maybe (withoutIdentifier qos) (withIdentifier qos)
  where
    takeIdentifier =
      takePacketIdentifier (clientPacketIdentifierPool client)
    returnIdentifier Nothing =
      pure ()
    returnIdentifier (Just (i, _)) =
      returnPacketIdentifier (clientPacketIdentifierPool client) i
    withIdentifier qos (i, mresponse) = do
      putMVar (clientOutputQueue client) (message qos i)
      response <- takeMVar mresponse
      case qos of
        AtLeastOnce ->
          case response of
            PublishAcknowledgement _ -> pure ()
            _ -> throwIO $ ProtocolViolation
              "Expected PUBACK, got something else for PUBLISH with QoS 1."
        ExactlyOnce ->
          case response of
            PublishReceived _ -> do
              putMVar (clientOutputQueue client) (PublishRelease i)
              response' <- takeMVar mresponse
              case response' of
                PublishComplete _ -> pure ()
                _ -> throwIO $ ProtocolViolation
                  "Expected PUBREL, got something else for PUBLISH with QoS 2."
            _ -> throwIO $ ProtocolViolation
              "Expected PUBREC, got something else for PUBLISH with QoS 2."
    withoutIdentifier qos = do
      -- We cannot easily wait for when packet identifiers are available again.
      -- On the other hand throwing an exception seems too drastic. So (for the
      -- extremely unlikely) case of packet identifier exhaustion, we shall wait
      -- 1 second and then try again.
      threadDelay 1000000
      publish client (Just qos) retain topic body
    message qos i = Publish {
      publishDuplicate = False,
      publishRetain = retain,
      publishQoS = Just (qos, i),
      publishTopic = topic,
      publishBody = body }

publishQoS2 :: MqttClient a -> Retain -> Topic -> BS.ByteString -> IO ()
publishQoS2  = undefined

subscribe   :: MqttClient a -> M.Map TopicFilter (Maybe QoS) -> IO ()
subscribe    = undefined

data MqttException
   = ProtocolViolation String
   | ConnectionRefused ConnectionRefusal
   deriving (Show, Typeable)

instance Exception MqttException where
