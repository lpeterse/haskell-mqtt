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
import qualified Data.Attoparsec.ByteString as A

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
     { clientIdentifier              :: MVar ClientIdentifier
     , clientKeepAlive               :: MVar KeepAlive
     , clientWill                    :: MVar (Maybe Will)
     , clientUsernamePassword        :: MVar (Maybe (Username, Maybe Password))
     , clientRecentActivity          :: MVar Bool
     , clientOutput                  :: MVar (Either RawMessage (PacketIdentifier -> (RawMessage, NotAcknowledgedOutbound)))
     , clientNotAcknowledgedInbound  :: MVar (IM.IntMap NotAcknowledgedInbound)
     , clientNotAcknowledgedOutbound :: MVar (IM.IntMap NotAcknowledgedOutbound)
     , clientMessages                :: MVar Tail
     , clientNewConnection           :: MVar (IO Connection)
     , clientThreads                 :: MVar (Async ())
     }

newtype Tail
      = Tail (MVar (Tail, Message))

newtype Messages
      = Messages (MVar Tail)

messages :: MqttClient -> IO Messages
messages client =
  Messages <$> (newMVar =<< readMVar (clientMessages client))

message  :: Messages -> IO Message
message (Messages mt) =
  modifyMVar mt $ \(Tail next)-> readMVar next

data NotAcknowledgedOutbound
   = NotAcknowledgedPublish     RawMessage (MVar ())
   | NotReceivedPublish         RawMessage (MVar ())
   | NotCompletePublish         (MVar ())
   | NotAcknowledgedSubscribe   RawMessage (MVar [Maybe QoS])
   | NotAcknowledgedUnsubscribe RawMessage (MVar ())

newtype NotAcknowledgedInbound
      = NotReleasedPublish      Message

-- | Disconnects the client.
--   * The operation returns after the DISCONNECT packet has been written to the
--     connection and the connection has been closed. (FIXME it doesn't do so right now).
disconnect :: MqttClient -> IO ()
disconnect c = putMVar (clientOutput c) (Left Disconnect)

connect :: MqttClient -> IO ()
connect c = modifyMVar_ (clientThreads c) $ \p->
  poll p >>= \m-> case m of
    -- Processing thread is stil running, no need to connect.
    Nothing -> pure p
    -- Processing thread not running, create a new one with new connection
    Just _  -> join (readMVar (clientNewConnection c)) >>= async . handleConnection
  where
    handleConnection :: Connection -> IO ()
    handleConnection connection =
      sendConnect >> receiveConnectAcknowledgement >>= maintainConnection
      where
        sendConnect :: IO ()
        sendConnect = do
          ci <- readMVar (clientIdentifier c)
          ck <- readMVar (clientKeepAlive c)
          cw <- readMVar (clientWill c)
          cu <- readMVar (clientUsernamePassword c)
          send connection $ LBS.toStrict $ BS.toLazyByteString $ bRawMessage Connect
            { connectClientIdentifier = ci
            , connectCleanSession     = False
            , connectKeepAlive        = ck
            , connectWill             = cw
            , connectUsernamePassword = cu
            }

        receiveConnectAcknowledgement :: IO BS.ByteString
        receiveConnectAcknowledgement = do
          result <- A.parseWith (receive connection) pRawMessage =<< receive connection
          case result of
            A.Done j message -> f message >> pure j
            A.Fail _ _ e     -> throwIO $ ParserError e
            A.Partial _      -> throwIO $ ParserError "Expected CONNACK, got end of input."
          where
            f (ConnectAcknowledgement a) = case a of
                Right sessionPresent   -> pure ()
                Left connectionRefusal -> throwIO $ ConnectionRefused connectionRefusal
            f _ = throwIO $ ProtocolViolation "Expected CONNACK, got something else."

        maintainConnection :: BS.ByteString -> IO ()
        maintainConnection i =
          keepAlive `race_` handleOutput `race_` handleInput i
          where
            -- The keep alive thread wakes up every `keepAlive/2` seconds.
            -- It reads and unsets the recent-activity flag.
            -- When it finds the recent-activity flag unset, it sends a PINGREQ to the
            -- server. The interval between the last message and the PINGREQ is at
            -- most `keepAlive` seconds (assuming we get woken up on time by the RTS).
            keepAlive :: IO ()
            keepAlive = do
              interval <- (500000*) . fromIntegral <$> readMVar (clientKeepAlive c)
              forever $ do
                threadDelay interval
                activity <- swapMVar (clientRecentActivity c) False
                unless activity $ putMVar (clientOutput c) $ Left PingRequest

            handleOutput :: IO ()
            handleOutput = do
              void $ swapMVar (clientRecentActivity c) True
              x <- takeMVar (clientOutput c)
              case x of
                Left message ->
                  send connection $ LBS.toStrict $ BS.toLazyByteString $ bRawMessage message
                Right imessage ->
                  send connection . LBS.toStrict . BS.toLazyByteString . bRawMessage =<< assignPacketIdentifier imessage
              where
                assignPacketIdentifier :: (PacketIdentifier -> (RawMessage, NotAcknowledgedOutbound)) -> IO RawMessage
                assignPacketIdentifier f = do
                  mm <- modifyMVar (clientNotAcknowledgedOutbound c) (`g` [0x0000 .. 0xffff])
                  case mm of
                    Nothing -> threadDelay 100000 >> assignPacketIdentifier f
                    Just m  -> pure m
                  where
                    g p []      = pure (p, Nothing)
                    g p (i:is)  | IM.member i p = g p is
                                | otherwise    = let (m, a) = f (PacketIdentifier i)
                                                 in pure (IM.insert i a p, Just m)

            handleInput :: BS.ByteString -> IO ()
            handleInput i = do
              result <- A.parseWith (receive connection) pRawMessage i
              case result of
                A.Done j message -> f message >> handleInput j
                A.Fail _ _ e     -> throwIO $ ParserError e
                A.Partial _      -> throwIO $ ParserError "Unexpected end of input."
              where
                f msg@Publish {} = case publishQoS msg of
                  Nothing -> publishLocal c Message
                    { qos      = QoS0
                    , retained = publishRetain msg
                    , topic    = publishTopic msg
                    , payload  = publishBody msg
                    }
                  Just (AtLeastOnce, i) -> do
                    publishLocal c Message
                      { qos      = QoS1
                      , retained = publishRetain msg
                      , topic    = publishTopic msg
                      , payload  = publishBody msg
                      }
                    putMVar (clientOutput c) $ Left $ PublishAcknowledgement i
                  Just (ExactlyOnce, PacketIdentifier i) ->
                    modifyMVar_ (clientNotAcknowledgedInbound c) $
                      pure . IM.insert i (NotReleasedPublish Message
                        { qos      = QoS2
                        , retained = publishRetain msg
                        , topic    = publishTopic msg
                        , payload  = publishBody msg
                        })
                -- The following packet types are responses to earlier requests.
                -- We need to dispatch them to the waiting threads.
                f (PublishAcknowledgement (PacketIdentifier i)) =
                  modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
                    case IM.lookup i im of
                      Just (NotAcknowledgedPublish _ x) ->
                        putMVar x () >> pure (IM.delete i im)
                      _ -> throwIO $ ProtocolViolation "Expected PUBACK, got something else."
                f (PublishReceived (PacketIdentifier i)) = do
                  modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
                    case IM.lookup i im of
                      Just (NotReceivedPublish _ x) ->
                        pure (IM.insert i (NotCompletePublish x) im)
                      _ -> throwIO $ ProtocolViolation "Expected PUBREC, got something else."
                  putMVar (clientOutput c) (Left $ PublishRelease (PacketIdentifier i))
                f (PublishRelease (PacketIdentifier i)) = do
                  modifyMVar_ (clientNotAcknowledgedInbound c) $ \im->
                    case IM.lookup i im of
                      Just (NotReleasedPublish msg) -> do
                        publishLocal c msg -- Publish exactly once here!
                        pure (IM.delete i im)
                      Nothing -> -- Duplicate, don't publish again.
                        pure im
                  putMVar (clientOutput c) (Left $ PublishComplete (PacketIdentifier i))
                f (PublishComplete (PacketIdentifier i)) = do
                  modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
                    case IM.lookup i im of
                      Nothing ->
                        pure im
                      Just (NotCompletePublish x) ->
                        pure (IM.delete i im)
                      _ ->
                        throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
                  putMVar (clientOutput c) (Left $ PublishComplete (PacketIdentifier i))
                f (SubscribeAcknowledgement (PacketIdentifier i) as) =
                  modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
                    case IM.lookup i im of
                      Nothing ->
                        pure im
                      Just (NotAcknowledgedSubscribe m x) -> do
                        putMVar x as
                        pure (IM.delete i im)
                      _ ->
                        throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
                f (UnsubscribeAcknowledgement (PacketIdentifier i)) =
                  modifyMVar_ (clientNotAcknowledgedOutbound c) $ \im->
                    case IM.lookup i im of
                      Nothing                               ->
                        pure im
                      Just (NotAcknowledgedUnsubscribe m x) -> do
                        putMVar x ()
                        pure (IM.delete i im)
                      _ ->
                        throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
                f PingResponse =
                  pure ()
                -- The following packets must not be sent from the server to the client.
                _ =
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
  takeMVar response
  where
    f response i = (m, NotAcknowledgedSubscribe m response)
      where
        m = Subscribe i topics

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
