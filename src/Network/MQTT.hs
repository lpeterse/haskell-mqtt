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
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS

import Control.Exception
import Control.Monad
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.Async
import Control.Concurrent.BoundedChan

import Network.MQTT.Message
import Network.MQTT.Message.RemainingLength
import Network.MQTT.SubscriptionTree
import Network.MQTT.PacketIdentifier

data MqttClient c
   = MqttClient
     { clientIdentifier              :: ClientIdentifier
     , clientKeepAlive               :: KeepAlive
     , clientRecentActivity          :: MVar Bool
     , clientOutputQueue             :: MVar Message
     , clientPacketIdentifierPool    :: PacketIdentifierPool Message
     , clientNewConnection           :: IO c
     , clientThread                  :: MVar (Maybe (Async ()))
     , clientRecentLifeSign          :: MVar Bool
     }

class Channel a where
  type ChannelException a
  send    :: a -> BS.Builder -> IO ()
  receive :: a -> IO (Maybe BS.ByteString)
  close   :: a -> IO ()

newMqttClient :: IO (MqttClient a)
newMqttClient = do
  undefined

connect :: MqttClient a -> IO ()
connect c = modifyMVar_ (clientThread c) $ \mt->
  case mt of
    Just t  -> pure mt -- already connected, do nothing
    Nothing -> do
      undefined
  where
    maintainConnection :: Channel c => c -> IO ()
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
    processOutput :: Channel c => c -> IO ()
    processOutput channel = forever $ do
      void $ swapMVar (clientRecentActivity c) True
      takeMVar (clientOutputQueue c) >>= send channel . bMessage
    processInput :: Channel c => c -> IO ()
    processInput =
      drain . messageProcessor . parse pMessage . sample . receive
      where
        -- This 'Transducer' decides what to do with the input.
        -- When a message is encoutered that is not supposed to be sent from the
        -- server to the client, a 'ProtocolViolation' exception will be thrown.
        messageProcessor :: Transducer IO BS.ByteString Message Message
        messageProcessor = each $ \message-> case message of
            Publish {} -> do
              pure ()
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
            -- Do nothing on a ping response. We don't care.
            PingResponse ->
              pure ()
            -- The following messages should not be sent from the server to the client.
            _ -> throwIO $ ProtocolViolation "Unexpected packet type received from the server."

disconnect :: MqttClient a -> IO ()
disconnect c =  modifyMVar_ (clientThread c) $ \mt->
  case mt of
    Nothing     -> pure mt -- already disconnected, do nothing
    Just thread -> cancel thread >> pure mt

publishQoS0 :: MqttClient a -> Retain -> Topic -> BS.ByteString -> IO ()
publishQoS0 client retain !topic !body =
  putMVar (clientOutputQueue client) message
  where
    message = Publish {
      publishDuplicate = False,
      publishRetain = retain,
      publishQoS = Nothing,
      publishTopic = topic,
      publishBody = body }

publish' :: MqttClient a -> QoS -> Retain -> Topic -> BS.ByteString -> IO ()
publish' client qos retain !topic !body = -- Note the BangPatterns!
  bracket takeIdentifier returnIdentifier $
    maybe withoutIdentifier withIdentifier
  where
    takeIdentifier =
      takePacketIdentifier (clientPacketIdentifierPool client)
    returnIdentifier Nothing =
      pure ()
    returnIdentifier (Just (i, _)) =
      returnPacketIdentifier (clientPacketIdentifierPool client) i
    withIdentifier (i, mresponse) = do
      putMVar (clientOutputQueue client) (message i)
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
    withoutIdentifier  = do
      -- We cannot easily wait for when packet identifiers are available again.
      -- On the other hand throwing an exception seems too drastic. So (for the
      -- extremely unlikely) case of packet identifier exhaustion, we shall wait
      -- 1 second and then try again.
      threadDelay 1000000
      publish' client qos retain topic body
    message i = Publish {
      publishDuplicate = False,
      publishRetain = retain,
      publishQoS = Just (qos, i),
      publishTopic = topic,
      publishBody = body }

publishQoS2 :: MqttClient a -> Retain -> Topic -> BS.ByteString -> IO ()
publishQoS2  = undefined

data MqttException
   = ProtocolViolation String
   deriving (Show, Typeable)

instance Exception MqttException where
