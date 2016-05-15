{-# LANGUAGE OverloadedStrings, TypeFamilies, BangPatterns, TupleSections #-}
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
import Data.Word
import Data.Typeable
import qualified Data.Map as M
import qualified Data.IntMap as IM
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Builder.Extra as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import qualified Data.Serialize.Get as SG

import Foreign.Ptr
import Foreign.Marshal.Alloc

import Control.Exception
import Control.Monad
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.Async

import System.Random
import qualified System.Socket as S

import Network.MQTT
import Network.MQTT.IO
import Network.MQTT.Message
import Network.MQTT.Message.RemainingLength

data MqttClient
   = MqttClient
     { clientIdentifier              :: MVar ClientIdentifier
     , clientKeepAlive               :: MVar KeepAlive
     , clientWill                    :: MVar (Maybe Will)
     , clientUsernamePassword        :: MVar (Maybe (Username, Maybe Password))
     , clientRecentActivity          :: MVar Bool
     , clientOutput                  :: MVar (Either RawMessage (PacketIdentifier -> (RawMessage, OutboundState)))
     , clientInboundState            :: MVar (IM.IntMap InboundState)
     , clientOutboundState           :: MVar ([Int], IM.IntMap OutboundState)
     , clientMessages                :: MVar Tail
     , clientNewConnection           :: MVar (IO Connection)
     , clientThreads                 :: MVar (Async ())
     }

newMqttClient :: IO Connection -> IO MqttClient
newMqttClient ioc = MqttClient
  <$> (newMVar =<< ClientIdentifier . T.pack . take 23 . ("haskell-" ++) . randomRs ('a','z') <$> newStdGen)
  <*> newMVar 60
  <*> newMVar Nothing
  <*> newMVar Nothing
  <*> newMVar False
  <*> newEmptyMVar
  <*> newMVar IM.empty
  <*> newMVar ([0..10000], IM.empty)
  <*> (newMVar =<< (Tail <$> newEmptyMVar))
  <*> newMVar ioc
  <*> (newMVar =<< async (pure ()))

type ClientSessionPresent = Bool

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

data OutboundState
   = NotAcknowledgedPublish     RawMessage (MVar ())
   | NotReceivedPublish         RawMessage (MVar ())
   | NotCompletePublish         (MVar ())
   | NotAcknowledgedSubscribe   RawMessage (MVar [Maybe QoS])
   | NotAcknowledgedUnsubscribe RawMessage (MVar ())

newtype InboundState = NotReleasedPublish Message

-- | Disconnects the client.
--   * The operation returns after the DISCONNECT packet has been written to the
--     connection and the connection has been closed.
disconnect :: MqttClient -> IO ()
disconnect c = do
  t <- readMVar (clientThreads c)
  putMVar (clientOutput c) (Left Disconnect)
  wait t

connect :: MqttClient -> IO ()
connect c = modifyMVar_ (clientThreads c) $ \p->
  poll p >>= \m-> case m of
    -- Processing thread is stil running, no need to connect.
    Nothing -> pure p
    -- Processing thread not running, create a new one with new connection
    Just _  -> join (readMVar (clientNewConnection c)) >>= handleConnection False
  where
    handleConnection :: ClientSessionPresent -> Connection -> IO (Async ())
    handleConnection clientSessionPresent connection = do
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
          bs <- S.receive (sock connection) 4096 S.msgNoSignal
          case SG.runGetPartial pRawMessage bs of
            SG.Done message bs' -> f message >> pure bs'
            SG.Fail e bs'       -> throwIO $ ParserError e
            SG.Partial _        -> throwIO $ ParserError "Expected CONNACK, got end of input."
          where
            f (ConnectAcknowledgement a) = case a of
                Right serverSessionPresent
                  | serverSessionPresent && not clientSessionPresent ->
                      -- This should not happen unless an old client identifier
                      -- has been chosen or the server is wicked.
                      throwIO ClientLostSession
                  | not serverSessionPresent && clientSessionPresent ->
                      -- This may happen if the server lost its memory, either
                      -- acidentially or for administrative reasons.
                      throwIO ServerLostSession
                  | otherwise -> pure ()
                Left connectionRefusal -> throwIO $ ConnectionRefused connectionRefusal
            f _ = throwIO $ ProtocolViolation "Expected CONNACK, got something else."

        maintainConnection :: BS.ByteString -> IO (Async ())
        maintainConnection i = async $ do
          print "MAINTAIN"
          keepAlive `race_` handleOutput `race_` (handleInput i `catch` \e-> print (e :: SomeException))
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
            handleOutput = bufferedOutput connection getMessage getMaybeMessage (send connection)
              where
                getMessage :: IO RawMessage
                getMessage = do
                  msg <- f =<< takeMVar (clientOutput c)
                  void $ swapMVar (clientRecentActivity c) True
                  pure msg

                getMaybeMessage :: IO (Maybe RawMessage)
                getMaybeMessage = do
                  memsg <- tryTakeMVar (clientOutput c)
                  case memsg of
                    Nothing   -> pure Nothing
                    Just emsg -> Just <$> f emsg

                f :: Either RawMessage (PacketIdentifier -> (RawMessage, OutboundState)) -> IO RawMessage
                f (Left msg)   = pure msg
                f (Right imsg) = assignPacketIdentifier imsg

                assignPacketIdentifier :: (PacketIdentifier -> (RawMessage, OutboundState)) -> IO RawMessage
                assignPacketIdentifier x =
                  modifyMVar (clientOutboundState c) assign >>= \mm-> case mm of
                    Just m -> pure m
                    -- We cannot easily wait for when packet identifiers are available again.
                    -- On the other hand throwing an exception seems too drastic. So (for the
                    -- extremely unlikely) case of packet identifier exhaustion, we shall wait
                    -- 100ms and then try again.
                    Nothing -> threadDelay 100000 >> assignPacketIdentifier x
                  where
                    assign im@([], _) = pure (im, Nothing)
                    assign (i:is, m)  = let (msg, st) = x (PacketIdentifier i)
                                        in  pure ((is, IM.insert i st m), Just msg)

            handleInput :: BS.ByteString -> IO ()
            handleInput bs
              | BS.null bs = handleInput' =<< S.receive (sock connection) 4096 S.msgNoSignal
              | otherwise  = handleInput' bs
              where
                handleInput' bs' = do
                  -- print $ "handle input" ++ show (BS.unpack bs')
                  g (SG.runGetPartial pRawMessage bs')
                g (SG.Done message bs') = do
                  -- print message
                  f message >> handleInput' bs'
                g (SG.Partial     cont) = do
                  --print "Partial"
                  bs' <- S.receive (sock connection) 4096 S.msgNoSignal
                  if BS.null bs'
                    then throwIO ConnectionClosed
                    else g $ cont bs'
                g (SG.Fail         e _) = do
                  --print $ "FAIL" ++ show e
                  throwIO $ ParserError e

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
                    modifyMVar_ (clientInboundState c) $
                      pure . IM.insert i (NotReleasedPublish Message
                        { qos      = QoS2
                        , retained = publishRetain msg
                        , topic    = publishTopic msg
                        , payload  = publishBody msg
                        })
                -- The following packet types are responses to earlier requests.
                -- We need to dispatch them to the waiting threads.
                f (PublishAcknowledgement (PacketIdentifier i)) =
                  modifyMVar_ (clientOutboundState c) $ \(is,im)->
                    case IM.lookup i im of
                      Just (NotAcknowledgedPublish _ promise) ->
                        putMVar promise () >> pure (i:is, IM.delete i im)
                      _ -> throwIO $ ProtocolViolation "Expected PUBACK, got something else."
                f (PublishReceived (PacketIdentifier i)) = do
                  modifyMVar_ (clientOutboundState c) $ \(is,im)->
                    case IM.lookup i im of
                      Just (NotReceivedPublish _ promise) ->
                        pure (is, IM.insert i (NotCompletePublish promise) im)
                      _ -> throwIO $ ProtocolViolation "Expected PUBREC, got something else."
                  putMVar (clientOutput c) (Left $ PublishRelease (PacketIdentifier i))
                f (PublishRelease (PacketIdentifier i)) = do
                  modifyMVar_ (clientInboundState c) $ \im->
                    case IM.lookup i im of
                      Just (NotReleasedPublish msg) -> do
                        publishLocal c msg -- Publish exactly once here!
                        pure (IM.delete i im)
                      Nothing -> -- Duplicate, don't publish again.
                        pure im
                  putMVar (clientOutput c) (Left $ PublishComplete (PacketIdentifier i))
                f (PublishComplete (PacketIdentifier i)) = do
                  modifyMVar_ (clientOutboundState c) $ \p@(is,im)->
                    case IM.lookup i im of
                      Nothing ->
                        pure p
                      Just (NotCompletePublish future) -> do
                        putMVar future ()
                        pure (i:is, IM.delete i im)
                      _ ->
                        throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
                f (SubscribeAcknowledgement (PacketIdentifier i) as) =
                  modifyMVar_ (clientOutboundState c) $ \p@(is,im)->
                    case IM.lookup i im of
                      Nothing -> pure p
                      Just (NotAcknowledgedSubscribe m promise) -> do
                        putMVar promise as
                        pure (i:is, IM.delete i im)
                      _ -> throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
                f (UnsubscribeAcknowledgement (PacketIdentifier i)) =
                  modifyMVar_ (clientOutboundState c) $ \p@(is,im)->
                    case IM.lookup i im of
                      Nothing -> pure p
                      Just (NotAcknowledgedUnsubscribe m promise) -> do
                        putMVar promise ()
                        pure (i:is, IM.delete i im)
                      _ -> throwIO $ ProtocolViolation "Expected PUBCOMP, got something else."
                f PingResponse = pure ()
                -- The following packets must not be sent from the server to the client.
                _ = throwIO $ ProtocolViolation "Unexpected packet type received from the server."

publishLocal :: MqttClient -> Message -> IO ()
publishLocal client msg = modifyMVar_ (clientMessages client) $
  \(Tail currentTail)-> do
    nextTail <- Tail <$> newEmptyMVar     -- a new (unresolved) tail
    putMVar currentTail (nextTail, msg)   -- resolve the current head
    pure nextTail                         -- new tail replaces current

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

subscribe :: MqttClient -> [(TopicFilter, QoS)] -> IO [Maybe QoS]
subscribe client [] = pure []
subscribe client topics = do
  response <- newEmptyMVar
  putMVar (clientOutput client) $ Right $ f response
  takeMVar response
  where
    f response i =
      let message = Subscribe i topics
      in (message, NotAcknowledgedSubscribe message response)

unsubscribe :: MqttClient -> [TopicFilter] -> IO ()
unsubscribe client [] = pure ()
unsubscribe client topics = do
  confirmation <- newEmptyMVar
  putMVar (clientOutput client) $ Right $ f confirmation
  takeMVar confirmation
  where
    f confirmation i =
      let message = Unsubscribe i topics
      in (message, NotAcknowledgedUnsubscribe message confirmation)
