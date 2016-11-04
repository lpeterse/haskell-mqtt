{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE TypeFamilies      #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Client
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Client where
{-
  ( Client ()
  , ClientConfiguration (..)
  -- * Events
  , ClientEvent (..)
  , listenEvents
  , acceptEvent
  -- * Exceptions
  , ClientException (..)
  , newClient
  , start
  , stop
  , publish
  , subscribe
  , unsubscribe
  ) where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.Broadcast
import           Control.Exception
import           Control.Monad
import qualified Data.ByteString              as BS
import qualified Data.ByteString.Builder      as BS
import qualified Data.IntMap                  as IM
import qualified Data.Serialize.Get           as SG
import qualified Data.Text                    as T
import qualified Data.Text.Encoding           as T
import           Data.Typeable
import           Data.Word
import           Network.MQTT
import           Network.MQTT.IO
import           Network.MQTT.Message
import qualified Network.Transceiver          as T
import           Network.URI
import           System.Random

data ClientConfiguration t
   = ClientConfiguration
     { -- | @mqtt://user:password\@server.example:1234@
       clientURI                      :: URI
     , clientWill                     :: Maybe Message
     , clientKeepAlive                :: KeepAlive
     , clientIdentifierPrefix         :: String
     , clientMaxUnacknowlegedMessages :: Word16
     , clientNewTransceiver           :: IO t
     }

validateClientConfiguration :: ClientConfiguration t -> IO ()
validateClientConfiguration config = do
  validateUriScheme
  validateUriAuthority
  where
    validateUriScheme = case uriScheme (clientURI config) of
      "mqtt"  -> pure ()
      "mqtts" -> pure ()
      "ws"    -> pure ()
      "wss"   -> pure ()
      _       -> err "clientURI: unsupported scheme"
    validateUriAuthority = case uriAuthority (clientURI config) of
      Nothing   -> err "clientURI: missing authority"
      Just auth -> pure ()
    err = throwIO . ClientConfigurationException

data ClientConfigurationException
   = ClientConfigurationException String
   deriving (Typeable, Show)

instance Exception ClientConfigurationException

uriUsername :: URI -> Maybe Username
uriUsername uri =
  f $ takeWhile (/=':') . uriUserInfo  <$> uriAuthority uri
  where
    f (Just []) = Nothing
    f (Just xs) = Just (T.pack xs)
    f _         = Nothing

uriPassword :: URI -> Maybe Password
uriPassword uri = f $ drop 1 . dropWhile (/=':') . uriUserInfo <$> uriAuthority uri
  where
    f (Just []) = Nothing
    f (Just xs) = Just (T.encodeUtf8 $ T.pack xs)
    f _         = Nothing

data Client t
   = Client
     { clientIdentifier              :: ClientIdentifier
     , clientEventBroadcast          :: Broadcast ClientEvent
     , clientConfiguration           :: MVar (ClientConfiguration t)
     , clientRecentActivity          :: MVar Bool
     , clientOutput                  :: MVar (Either UpstreamMessage (PacketIdentifier -> (UpstreamMessage, OutboundState)))
     , clientInboundState            :: MVar (IM.IntMap InboundState)
     , clientOutboundState           :: MVar ([Int], IM.IntMap OutboundState)
     , clientThreads                 :: MVar (Async ())
     }

newClient :: ClientConfiguration t -> IO (Client t)
newClient configuration = Client
  <$> (ClientIdentifier . T.pack . take clientIdentifierLength
    . (clientIdentifierPrefix configuration ++)
    . randomRs clientIdentifierCharacterRange <$> newStdGen)
  <*> newBroadcast
  <*> newMVar configuration
  <*> newMVar False
  <*> newEmptyMVar
  <*> newMVar IM.empty
  <*> newMVar ([0..fromIntegral (clientMaxUnacknowlegedMessages configuration)], IM.empty)
  <*> (newMVar =<< async (pure ()))
  where
    clientIdentifierLength = 23
    clientIdentifierCharacterRange = ('a','z')

type ClientSessionPresent = Bool

data OutboundState
   = NotAcknowledgedPublish     UpstreamMessage (MVar ())
   | NotReceivedPublish         UpstreamMessage (MVar ())
   | NotCompletePublish         (MVar ())
   | NotAcknowledgedSubscribe   UpstreamMessage (MVar [Maybe QualityOfService])
   | NotAcknowledgedUnsubscribe UpstreamMessage (MVar ())

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
start :: (T.Connectable s, T.Address s ~ a,  T.StreamConnection s, T.Closable s, T.Data s ~ BS.ByteString) => Client s -> IO ()
start c = modifyMVar_ (clientThreads c) $ \p->
  poll p >>= \m-> case m of
    -- Processing thread is stil running, no need to connect.
    Nothing -> pure p
    -- Processing thread not running, create a new one with new connection
    Just _  -> do
      broadcast (clientEventBroadcast c) Started
      async $ forever $ do
        run -- `catch` (\e-> print (e :: SomeException) >> print "RECONNECT")
        threadDelay 1000000

  where
    run :: IO ()
    run  = join (clientNewTransceiver <$> readMVar (clientConfiguration c)) >>= handleConnection False

    -- handleConnection :: (StreamTransmitter s, StreamReceiver s, Connectable s, Closable s) => ClientSessionPresent -> s -> IO ()
    handleConnection clientSessionPresent connection = do
      broadcast (clientEventBroadcast c) Connecting
      connectTransmitter
      broadcast (clientEventBroadcast c) Connected
      sendConnect
      receiveConnectAcknowledgement >>= maintainConnection
      where
        connectTransmitter :: IO ()
        connectTransmitter =
          T.connect connection =<< readMVar undefined

        sendConnect :: IO ()
        sendConnect = do
          conf <- readMVar (clientConfiguration c)
          T.sendChunks connection $ BS.toLazyByteString $ buildUpstreamMessage Connect
            { connectClientIdentifier = clientIdentifier c
            , connectCleanSession     = False
            , connectKeepAlive        = clientKeepAlive conf
            , connectWill             = clientWill conf
            , connectCredentials      = undefined
            }

        receiveConnectAcknowledgement :: IO BS.ByteString
        receiveConnectAcknowledgement = do
          bs <- T.receiveChunk connection
          case SG.runGetPartial parseDownstreamMessage bs of
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
              interval <- (500000*) . fromIntegral . clientKeepAlive <$> readMVar (clientConfiguration c)
              forever $ do
                threadDelay interval
                activity <- swapMVar (clientRecentActivity c) False
                unless activity $ putMVar (clientOutput c) $ Left PingRequest

            handleOutput :: IO ()
            handleOutput = bufferedOutput connection getMessage getMaybeMessage (T.sendChunk connection)
              where
                getMessage :: IO UpstreamMessage
                getMessage = do
                  msg <- f =<< takeMVar (clientOutput c)
                  void $ swapMVar (clientRecentActivity c) True
                  pure msg

                getMaybeMessage :: IO (Maybe UpstreamMessage)
                getMaybeMessage = do
                  memsg <- tryTakeMVar (clientOutput c)
                  case memsg of
                    Nothing   -> pure Nothing
                    Just emsg -> Just <$> f emsg

                f :: Either UpstreamMessage (PacketIdentifier -> (UpstreamMessage, OutboundState)) -> IO UpstreamMessage
                f (Left msg)   = pure msg
                f (Right imsg) = assignPacketIdentifier imsg

                assignPacketIdentifier :: (PacketIdentifier -> (UpstreamMessage, OutboundState)) -> IO UpstreamMessage
                assignPacketIdentifier x =
                  modifyMVar (clientOutboundState c) assign >>= \mm-> case mm of
                    Just m  -> pure m
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
                  g (SG.runGetPartial parseDownstreamMessage bs')
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
                  PublishQoS0 -> broadcast (clientEventBroadcast c) $ Received Message
                    { topic    = publishTopic msg
                    , payload  = publishBody msg
                    , qos      = QoS0
                    , retained = publishRetain msg
                    , duplicate = False
                    }
                  {-PublishQoS1 dup i -> do
                    broadcast (clientEventBroadcast c) $ Received Message
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
                        broadcast (clientEventBroadcast c) $ Received msg -- Publish exactly once here!
                        pure (IM.delete i im)
                      Nothing -> -- Duplicate, don't publish again.
                        pure im
                  putMVar (clientOutput c) (Left $ PublishComplete (PacketIdentifier i)) -}
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
stop :: T.Closable s => Client s -> IO ()
stop c = do
  t <- readMVar (clientThreads c)
  putMVar (clientOutput c) (Left Disconnect)
  wait t

publish :: Client s -> Topic -> QoS -> Retain -> Payload -> IO ()
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

subscribe :: Client s -> [(TopicFilter, QoS)] -> IO [Maybe QoS]
subscribe client [] = pure []
subscribe client topics = do
  response <- newEmptyMVar
  putMVar (clientOutput client) $ Right $ f response
  takeMVar response
  where
    f response i =
      let message = Subscribe i topics
      in (message, NotAcknowledgedSubscribe message response)

unsubscribe :: Client s -> [TopicFilter] -> IO ()
unsubscribe client [] = pure ()
unsubscribe client topics = do
  confirmation <- newEmptyMVar
  putMVar (clientOutput client) $ Right $ f confirmation
  takeMVar confirmation
  where
    f confirmation i =
      let message = Unsubscribe i topics
      in (message, NotAcknowledgedUnsubscribe message confirmation)

listenEvents :: Client s -> IO (BroadcastListener ClientEvent)
listenEvents c = listen (clientEventBroadcast c)

acceptEvent  :: BroadcastListener ClientEvent -> IO ClientEvent
acceptEvent   = accept

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
-}
