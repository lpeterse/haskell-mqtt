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
module Network.MQTT.Client
  ( Client ()
  , ClientEventStream ()
  , ClientEvent (..)
  , ClientException (..)
  , EventEmitter (..)
  , EventStream ()
  , new
  , start
  , stop
  , publish
  , subscribe
  , unsubscribe
  , hookEventStream
  , takeEvent
  ) where

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

import Network.MQTT
import Network.MQTT.IO
import Network.MQTT.Message

import qualified Network.Transceiver as T

data Client s a
   = Client
     { clientServerAddress           :: MVar a
     , clientIdentifier              :: MVar ClientIdentifier
     , clientKeepAlive               :: MVar KeepAlive
     , clientWill                    :: MVar (Maybe Will)
     , clientUsernamePassword        :: MVar (Maybe (Username, Maybe Password))
     , clientRecentActivity          :: MVar Bool
     , clientOutput                  :: MVar (Either RawMessage (PacketIdentifier -> (RawMessage, OutboundState)))
     , clientInboundState            :: MVar (IM.IntMap InboundState)
     , clientOutboundState           :: MVar ([Int], IM.IntMap OutboundState)
     , clientMessages                :: MVar Tail
     , clientNewConnection           :: MVar (IO s)
     , clientThreads                 :: MVar (Async ())
     }

new :: IO s -> a -> IO (Client s a)
new ios a = Client
  <$> newMVar a
  <*> (newMVar =<< ClientIdentifier . T.pack . take 23 . ("haskell-" ++) . randomRs ('a','z') <$> newStdGen)
  <*> newMVar 60
  <*> newMVar Nothing
  <*> newMVar Nothing
  <*> newMVar False
  <*> newEmptyMVar
  <*> newMVar IM.empty
  <*> newMVar ([0..10000], IM.empty)
  <*> (newMVar =<< (Tail <$> newEmptyMVar))
  <*> newMVar ios
  <*> (newMVar =<< async (pure ()))

type ClientSessionPresent = Bool

newtype Tail
      = Tail (MVar (Tail, ClientEvent))

newtype ClientEventStream
      = ClientEventStream (MVar Tail)

hookEventStream :: Client s a -> IO ClientEventStream
hookEventStream client =
  ClientEventStream <$> (newMVar =<< readMVar (clientMessages client))

takeEvent :: ClientEventStream -> IO ClientEvent
takeEvent (ClientEventStream mt) =
  modifyMVar mt $ \(Tail next)-> readMVar next

data OutboundState
   = NotAcknowledgedPublish     RawMessage (MVar ())
   | NotReceivedPublish         RawMessage (MVar ())
   | NotCompletePublish         (MVar ())
   | NotAcknowledgedSubscribe   RawMessage (MVar [Maybe QoS])
   | NotAcknowledgedUnsubscribe RawMessage (MVar ())

newtype InboundState = NotReleasedPublish Message

-- | Starts the `Client` if it is not already running.
--
--   * The `Started` event will be added to the event stream.
--   * The `Connecting` event will be added to the event stream.
--   * A new `Connectable` will be created.
--   * A connection will be tried to establish (includes `connect` as well as the MQTT handshake).
--   * The `Connected` event will be added to the event stream.
--   * Messages will be processed until a `Disconnect` occurs.
--
--   At every step after the `Connecting` event, a `Disconnect` may occur.
--   The client will then try to reconnect automatically.
start :: (T.Connectable s, T.Address s ~ a,  T.StreamConnection s, T.Closable s, T.Data s ~ BS.ByteString) => Client s a -> IO ()
start c = modifyMVar_ (clientThreads c) $ \p->
  poll p >>= \m-> case m of
    -- Processing thread is stil running, no need to connect.
    Nothing -> pure p
    -- Processing thread not running, create a new one with new connection
    Just _  -> do
      publishLocal c Started
      async $ forever $ do
        run -- `catch` (\e-> print (e :: SomeException) >> print "RECONNECT")
        threadDelay 1000000

  where
    run :: IO ()
    run  = join (readMVar (clientNewConnection c)) >>= handleConnection False

    -- handleConnection :: (StreamTransmitter s, StreamReceiver s, Connectable s, Closable s) => ClientSessionPresent -> s -> IO ()
    handleConnection clientSessionPresent connection = do
      publishLocal c Connecting
      connectTransmitter
      publishLocal c Connected
      sendConnect
      receiveConnectAcknowledgement >>= maintainConnection
      where
        connectTransmitter :: IO ()
        connectTransmitter =
          T.connect connection =<< readMVar (clientServerAddress c)

        sendConnect :: IO ()
        sendConnect = do
          ci <- readMVar (clientIdentifier c)
          ck <- readMVar (clientKeepAlive c)
          cw <- readMVar (clientWill c)
          cu <- readMVar (clientUsernamePassword c)
          T.sendChunks connection $ BS.toLazyByteString $ bRawMessage Connect
            { connectClientIdentifier = ci
            , connectCleanSession     = False
            , connectKeepAlive        = ck
            , connectWill             = cw
            , connectCredentials      = cu
            }

        receiveConnectAcknowledgement :: IO BS.ByteString
        receiveConnectAcknowledgement = do
          bs <- T.receiveChunk connection
          case SG.runGetPartial pRawMessage bs of
            SG.Done message bs' -> f message >> pure bs'
            SG.Fail e bs'       -> throwIO $ ProtocolViolation e
            SG.Partial _        -> throwIO $ ProtocolViolation "Expected CONNACK, got end of input."
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

        maintainConnection :: BS.ByteString -> IO ()
        maintainConnection i =
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
            handleOutput = bufferedOutput connection getMessage getMaybeMessage (T.sendChunk connection)
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
              | BS.null bs = handleInput' =<< T.receiveChunk connection
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
                  bs' <- T.receiveChunk connection
                  if BS.null bs'
                    then throwIO ServerClosedConnection
                    else g $ cont bs'
                g (SG.Fail         e _) = do
                  --print $ "FAIL" ++ show e
                  throwIO $ ProtocolViolation e

                f msg@Publish {} = case publishQoS msg of
                  PublishQoS0 -> publishLocal c $ Received Message
                    { topic    = publishTopic msg
                    , payload  = publishBody msg
                    , qos      = QoS0
                    , retained = publishRetain msg
                    , duplicate = False
                    }
                  PublishQoS1 dup i -> do
                    publishLocal c $ Received Message
                      { topic    = publishTopic msg
                      , payload  = publishBody msg
                      , qos      = QoS1
                      , retained = publishRetain msg
                      , duplicate = False
                      }
                    putMVar (clientOutput c) $ Left $ PublishAcknowledgement i
                  PublishQoS2 (PacketIdentifier i) ->
                    modifyMVar_ (clientInboundState c) $
                      pure . IM.insert i (NotReleasedPublish Message
                        { topic     = publishTopic msg
                        , payload   = publishBody msg
                        , qos       = QoS2
                        , retained  = publishRetain msg
                        , duplicate = False
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
                        publishLocal c $ Received msg -- Publish exactly once here!
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

-- | Stops the `Client` if it is not already stopped.
--
--   * A graceful DISCONNECT packet will be sent to the server and the
--     connection will be `close`d.
--   * All internal client threads will be terminated.
--   * The `Stopped` event will be added to the event stream.
stop :: T.Closable s => Client s a -> IO ()
stop c = do
  t <- readMVar (clientThreads c)
  putMVar (clientOutput c) (Left Disconnect)
  wait t

publishLocal :: Client s a -> ClientEvent -> IO ()
publishLocal client msg = modifyMVar_ (clientMessages client) $
  \(Tail currentTail)-> do
    nextTail <- Tail <$> newEmptyMVar     -- a new (unresolved) tail
    putMVar currentTail (nextTail, msg)   -- resolve the current head
    pure nextTail                         -- new tail replaces current

publish :: Client s a -> Topic -> QoS -> Retain -> Payload -> IO ()
publish client !topic !qos !retain !payload = case qos of
  QoS0 -> undefined -- putMVar (clientOutput client) $ Left $ message Nothing
  QoS1 -> undefined {-- register AtLeastOnce NotAcknowledgedPublish
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
      publishBody      = body } -}

subscribe :: Client s a -> [(TopicFilter, QoS)] -> IO [Maybe QoS]
subscribe client [] = pure []
subscribe client topics = do
  response <- newEmptyMVar
  putMVar (clientOutput client) $ Right $ f response
  takeMVar response
  where
    f response i =
      let message = Subscribe i topics
      in (message, NotAcknowledgedSubscribe message response)

unsubscribe :: Client s a -> [TopicFilter] -> IO ()
unsubscribe client [] = pure ()
unsubscribe client topics = do
  confirmation <- newEmptyMVar
  putMVar (clientOutput client) $ Right $ f confirmation
  takeMVar confirmation
  where
    f confirmation i =
      let message = Unsubscribe i topics
      in (message, NotAcknowledgedUnsubscribe message confirmation)

data ClientEvent
   = Started
   | Connecting
   | Connected
   | Ping
   | Pong
   | Received Message
   | Disconnected ClientException
   | Stopped
   deriving (Eq, Ord, Show)

data ClientException
   = ProtocolViolation String
   | ConnectionRefused ConnectionRefusal
   | ClientLostSession
   | ServerLostSession
   | ClientClosedConnection
   | ServerClosedConnection
   deriving (Eq, Ord, Show, Typeable)

instance Exception ClientException where

class EventEmitter a where
  type Event a
  hook   :: a -> EventStream a
  listen :: EventStream a -> Event a

data EventStream a

instance EventEmitter (Client s a) where
  type Event (Client s a) = ClientEvent
  hook   = undefined
  listen = undefined
