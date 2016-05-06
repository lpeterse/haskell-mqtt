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
module Network.MQTT.Client where

import Data.Int
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

import Network.MQTT
import Network.MQTT.Message
import Network.MQTT.Message.RemainingLength
import Network.MQTT.PacketIdentifier

data MqttClient
   = MqttClient
     { clientIdentifier              :: ClientIdentifier
     , clientKeepAlive               :: KeepAlive
     , clientWill                    :: Maybe Will
     , clientUsernamePassword        :: Maybe (Username, Maybe Password)
     , clientNewConnection           :: IO Connection
     , clientRecentActivity          :: MVar Bool
     , clientOutputQueue             :: MVar RawMessage
     , clientOutput                  :: MVar (Either RawMessage (PacketIdentifier -> (RawMessage, NotAcknowledgedOutbound)))
     , clientProcessor               :: MVar (Async ())
     , clientNotAcknowledgedInbound  :: MVar (IM.IntMap NotAcknowledgedInbound)
     , clientNotAcknowledgedOutbound :: MVar (IM.IntMap NotAcknowledgedOutbound)
     , clientMessages                :: MVar Tail
     }

newtype Tail
      = Tail (MVar (Tail, Message))

newtype Messages
      = Messages (MVar Tail)

messages :: MqttClient -> IO Messages
messages client = do
  t <- readMVar (clientMessages client)
  Messages <$> newMVar t

message  :: Messages -> IO Message
message (Messages mt) =
  modifyMVar mt $ \(Tail next)-> readMVar next

data NotAcknowledgedOutbound
   = NotAcknowledgedPublish     RawMessage (MVar ())
   | NotReceivedPublish         RawMessage (MVar ())
   | NotCompletePublish         (MVar ())
   | NotAcknowledgedSubscribe   RawMessage (MVar [Maybe (Maybe QualityOfService)])
   | NotAcknowledgedUnsubscribe RawMessage (MVar ())

newtype NotAcknowledgedInbound
      = NotReleasedPublish      Message

data Connection
   = Connection
     { receive :: IO RawMessage
     , send    :: RawMessage -> IO ()
     }

connect :: MqttClient -> IO ()
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
        PublishAcknowledgement (PacketIdentifier i) ->
          modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
            case IM.lookup i im of
              Just (NotAcknowledgedPublish _ x) ->
                putMVar x () >> pure (IM.delete i im)
              _ -> throwIO $ ProtocolViolation "Expected PUBACK, got something else."
        PublishReceived (PacketIdentifier i) -> do
          modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
            case IM.lookup i im of
              Just (NotReceivedPublish _ x) ->
                pure (IM.insert i (NotCompletePublish x) im)
              _ -> throwIO $ ProtocolViolation "Expected PUBREC, got something else."
          putMVar (clientOutputQueue c) (PublishRelease (PacketIdentifier i))
        PublishRelease (PacketIdentifier i) -> do
          modifyMVar_ (clientNotAcknowledgedInbound c) $ \im->
            case IM.lookup i im of
              Just (NotReleasedPublish msg) -> do
                publishLocal c msg -- Publish exactly once here!
                pure (IM.delete i im)
              Nothing -> -- Duplicate, don't publish again.
                pure im
          putMVar (clientOutputQueue c) (PublishComplete (PacketIdentifier i))
        PublishComplete (PacketIdentifier i) -> do
          modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
            case IM.lookup i im of
              Nothing                     ->
                pure im
              Just (NotCompletePublish x) ->
                pure (IM.delete i im)
              _ ->
                throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
          putMVar (clientOutputQueue c) (PublishComplete (PacketIdentifier i))
        SubscribeAcknowledgement (PacketIdentifier i) as ->
          modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
            case IM.lookup i im of
              Nothing                             ->
                pure im
              Just (NotAcknowledgedSubscribe m x) -> do
                putMVar x as
                pure (IM.delete i im)
              _ ->
                throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
        UnsubscribeAcknowledgement (PacketIdentifier i) ->
          modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
            case IM.lookup i im of
              Nothing                               ->
                pure im
              Just (NotAcknowledgedUnsubscribe m x) -> do
                putMVar x ()
                pure (IM.delete i im)
              _ ->
                throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
        PingResponse -> pure ()
        -- The following packets must not be sent from the server to the client.
        _ ->
          throwIO $ ProtocolViolation "Unexpected packet type received from the server."

publish :: MqttClient -> Message -> IO ()
publish client (Message qos !retain !topic !body) = case qos of
  QoS0 -> putMVar (clientOutput client) $ Left $ message Nothing
  QoS1 -> register AtLeastOnce NotAcknowledgedPublish
  QoS2 -> register ExactlyOnce NotReceivedPublish
  where
    register qos' f = do
      confirmation <- newEmptyMVar
      putMVar (clientOutput client) $ Right $ qosMessage confirmation qos' f
      takeMVar confirmation
    qosMessage confirmation qos f packetIdentifier =
      ( m, f m { publishDuplicate = True } confirmation )
      where
        m = message $ Just (qos, packetIdentifier)
    message mqos' = Publish {
      publishDuplicate = False,
      publishRetain    = retain,
      publishQoS       = mqos',
      publishTopic     = topic,
      publishBody      = body }

publishLocal :: MqttClient -> Message -> IO ()
publishLocal client msg = modifyMVar_ (clientMessages client) $
  \(Tail currentTail)-> do
    nextTail <- Tail <$> newEmptyMVar     -- a new (unresolved) tail
    putMVar currentTail (nextTail, msg)   -- resolve the current head
    pure nextTail                         -- new tail replaces current

{-
      withoutIdentifier qos = do
        -- We cannot easily wait for when packet identifiers are available again.
        -- On the other hand throwing an exception seems too drastic. So (for the
        -- extremely unlikely) case of packet identifier exhaustion, we shall wait
        -- 1 second and then try again.
        threadDelay 1000000
        publish client (Just qos) retain topic body
-}

subscribe :: MqttClient -> [(TopicFilter, QoS)] -> IO [Maybe QoS]
subscribe client [] = pure []
subscribe client topics = do
  response <- newEmptyMVar
  putMVar (clientOutput client) $ Right $ f response
  fmap (fmap toQoS) <$> takeMVar response
  where
    f response i = (m, NotAcknowledgedSubscribe m response)
      where
        m = Subscribe i $ fmap fromQoS topics
    fromQoS (x, QoS0) = (x, Nothing)
    fromQoS (x, QoS1) = (x, Just AtLeastOnce)
    fromQoS (x, QoS2) = (x, Just ExactlyOnce)
    toQoS Nothing = QoS0
    toQoS (Just AtLeastOnce) = QoS1
    toQoS (Just ExactlyOnce) = QoS2

unsubscribe :: MqttClient -> [TopicFilter] -> IO ()
unsubscribe client [] = pure ()
unsubscribe client topics = do
  confirmation <- newEmptyMVar
  putMVar (clientOutput client) $ Right $ f confirmation
  takeMVar confirmation
  where
    f confirmation i = (m, NotAcknowledgedUnsubscribe m confirmation)
      where
        m = Unsubscribe i topics
