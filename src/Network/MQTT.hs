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
import qualified Data.IntMap as IM
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
     , clientNewConnection           :: IO Connection
     , clientRecentActivity          :: MVar Bool
     , clientOutputQueue             :: MVar Message
     , clientOutput                  :: MVar (Either Message (PacketIdentifier -> (Message, NotAcknowledgedOutbound)))
     , clientProcessor               :: MVar (Async ())
     , clientNotAcknowledgedInbound  :: MVar (IM.IntMap NotAcknowledgedInbound)
     , clientNotAcknowledgedOutbound :: MVar (IM.IntMap NotAcknowledgedOutbound)
     }

data NotAcknowledgedOutbound
   = NotAcknowledgedPublish     Message (MVar ())
   | NotReceivedPublish         Message (MVar ())
   | NotCompletePublish         (MVar ())
   | NotAcknowledgedSubscribe   [(TopicFilter, Maybe QoS)] (MVar [Maybe QoS])
   | NotAcknowledgedUnsubscribe [TopicFilter] (MVar ())

data NotAcknowledgedInbound
   = NotReleasedPublish         (IM.IntMap (MVar ()))

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
        , connectCleanSession     = False
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
          modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
            case IM.lookup i im of
              Just (NotAcknowledgedPublish _ x) ->
                putMVar x () >> pure (IM.delete i im)
              _ -> throwIO $ ProtocolViolation "Expected PUBACK, got something else."
        PublishReceived i -> do
          modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
            case IM.lookup i im of
              Just (NotReceivedPublish _ x) ->
                pure (IM.insert i (NotCompletePublish x) im)
              _ -> throwIO $ ProtocolViolation "Expected PUBREC, got something else."
          putMVar (clientOutputQueue c) (PublishRelease i)
        PublishRelease i -> do
          modifyMVar_ (clientNotAcknowledgedInbound c) $ \im->
            case IM.lookup i im of
              Nothing -> -- probably a duplicate
                pure im
              Just (NotReleasedPublish _ x) ->
                pure (IM.insert i (NotCompletePublish x) im)
              _ -> throwIO $ ProtocolViolation "Expected PUBREL, got something else."
          putMVar (clientOutputQueue c) (PublishComplete i)
        PublishComplete i -> do
          modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
            case IM.lookup i im of
              Nothing                     -> pure im
              Just (NotCompletePublish x) -> pure (IM.delete i im)
              _ -> throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
          putMVar (clientOutputQueue c) (PublishComplete i)
        SubscribeAcknowledgement i as ->
          modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
            case IM.lookup i im of
              Nothing                           -> pure im
              Just (NotAcknowledgedSubscribe x) -> putMVar x as >> pure (IM.delete i im)
              _ -> throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
        UnsubscribeAcknowledgement i ->
          modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
            case IM.lookup i im of
              Nothing                             -> pure im
              Just (NotAcknowledgedUnsubscribe x) -> putMVar x >> pure (IM.delete i im)
              _ -> throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
        PingResponse -> pure ()
        -- The following packets must not be sent from the server to the client.
        _ -> throwIO $ ProtocolViolation "Unexpected packet type received from the server."

publish :: MqttClient a -> Maybe QoS -> Retain -> Topic -> BS.ByteString -> IO ()
publish client mqos !retain !topic !body = case mqos of
  Nothing  ->
    putMVar (clientOutput client) $ Left $ message Nothing
  Just qos -> do
    transmissionComplete <- newEmptyMVar
    putMVar (clientOutput client) $ Right $ f transmissionComplete qos
    takeMVar transmissionComplete
  where
    f transmissionComplete qos packetIdentifier =
      ( m, g m { publishDuplicate = True } transmissionComplete )
      where
        m = message $ Just (qos, packetIdentifier)
        g = case qos of
              AtLeastOnce -> NotAcknowledgedPublish
              ExactlyOnce -> NotReceivedPublish
    message qp = Publish {
      publishDuplicate = False,
      publishRetain    = retain,
      publishQoS       = qp,
      publishTopic     = topic,
      publishBody      = body }
{-
      withoutIdentifier qos = do
        -- We cannot easily wait for when packet identifiers are available again.
        -- On the other hand throwing an exception seems too drastic. So (for the
        -- extremely unlikely) case of packet identifier exhaustion, we shall wait
        -- 1 second and then try again.
        threadDelay 1000000
        publish client (Just qos) retain topic body
-}

publishQoS2 :: MqttClient a -> Retain -> Topic -> BS.ByteString -> IO ()
publishQoS2  = undefined

subscribe   :: MqttClient a -> M.Map TopicFilter (Maybe QoS) -> IO ()
subscribe    = undefined

data MqttException
   = ProtocolViolation String
   | ConnectionRefused ConnectionRefusal
   deriving (Show, Typeable)

instance Exception MqttException where
