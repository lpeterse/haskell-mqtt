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

import System.Clock

import Network.MQTT.Message
import Network.MQTT.Message.RemainingLength
import Network.MQTT.SubscriptionTree
import Network.MQTT.PacketIdentifier

data MqttClient c
   = MqttClient
     { clientIdentifier              :: ClientIdentifier
     , clientKeepAlive               :: KeepAlive
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
    keepAlive :: IO ()
    keepAlive = do
      undefined
    processOutput :: Channel c => c -> IO ()
    processOutput channel =
      forever $ takeMVar (clientOutputQueue c) >>= send channel . bMessage
    processInput :: Channel c => c -> IO ()
    processInput =
      drain . messageProcessor . parse pMessage . lifeSignRegistrator . sample . receive
      where
        -- This 'Transducer' sets the recent-life-sign flag on every piece of
        -- input. This solutions was chosen as it causes the least performance
        -- penalty. Any other alternative I can think of would require getting
        -- the current time here which would require a system call.
        lifeSignRegistrator :: Transducer IO BS.ByteString BS.ByteString BS.ByteString
        lifeSignRegistrator = each $ const $ void $ swapMVar (clientRecentLifeSign c) True
        -- This 'Transducer' decides what to do with the input.
        -- When a message is encoutered that is not supposed to be sent from the
        -- server to the client, a 'ProtocolViolation' exception will be thrown.
        messageProcessor :: Transducer IO BS.ByteString Message Message
        messageProcessor = each $ \message-> case message of
            Publish {} -> do
              pure ()
            PublishAcknowledgement {} -> do
              pure ()
            PublishReceived {} -> do
              pure ()
            PublishRelease {} -> do
              pure ()
            PublishComplete {} -> do
              pure ()
            SubscribeAcknowledgement {} -> do
              pure ()
            UnsubscribeAcknowledgement {} -> do
              pure ()
            PingResponse -> do
              pure ()
            -- The following messages should not be sent from the server to the client.
            _ -> throwIO $ ProtocolViolation $ "Unexpected message type ''" ++
              takeWhile (/= ' ') (show message) ++ "'' received from the server."

disconnect :: MqttClient a -> IO ()
disconnect c =  modifyMVar_ (clientThread c) $ \mt->
  case mt of
    Nothing     -> pure mt -- already disconnected, do nothing
    Just thread -> cancel thread >> pure mt

publishQoS0 :: MqttClient a -> Retain -> Topic -> Message -> IO ()
publishQoS0  = undefined

publishQoS1 :: MqttClient a -> Retain -> Topic -> BS.ByteString -> IO ()
publishQoS1 client retain !topic !body = -- Note the BangPatterns!
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
      case response of
        PublishAcknowledgement _ -> pure ()
        _                        -> throwIO $ ProtocolViolation
          "Got something other than PUBACK for PUBLISH with QoS 1."
    withoutIdentifier  = do
      -- We cannot easily wait for when packet identifiers are available again.
      -- On the other hand throwing an exception seems too drastic. So (for the
      -- extremely unlikely) case of packet identifier exhaustion, we shall wait
      -- 1 second and then try again.
      threadDelay 1000000
      publishQoS1 client retain topic body
    message i = Publish {
      publishDuplicate = False,
      publishRetain = retain,
      publishQoS = Just (AtLeastOnce, i),
      publishTopic = topic,
      publishBody = body }

publishQoS2 :: MqttClient a -> Retain -> Topic -> Message -> IO ()
publishQoS2  = undefined

data MqttException
   = ProtocolViolation String
   deriving (Show, Typeable)

instance Exception MqttException where
