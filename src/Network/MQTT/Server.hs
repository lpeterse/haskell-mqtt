{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeFamilies        #-}
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
import           Control.Concurrent.Async
import qualified Control.Exception           as E
import           Control.Monad
import qualified Data.Binary.Get             as SG
import qualified Data.ByteString             as BS
import           Data.CaseInsensitive
import           Data.IORef
import           Data.Typeable
import           Network.MQTT.Authentication
import qualified Network.MQTT.Broker         as Broker
import           Network.MQTT.Message
import qualified Network.MQTT.Session        as Session
import qualified Network.Stack.Server        as SS
import qualified Network.WebSockets          as WS
import qualified System.Socket               as S

instance (Typeable transport) => E.Exception (SS.ServerException (MQTT transport))

data MQTT transport
type RecentActivity = IORef Bool

class SS.ServerStack a => MqttServerTransportStack a where
  connHttpPath    :: SS.ServerConnectionInfo a -> Maybe BS.ByteString
  connHttpHeaders :: SS.ServerConnectionInfo a -> [(CI BS.ByteString, BS.ByteString)]

instance (Typeable f, Typeable t, Typeable p, S.Family f, S.Protocol p, S.Type t) => MqttServerTransportStack (S.Socket f t p) where
  connHttpPath    = const Nothing
  connHttpHeaders = const []

instance SS.StreamServerStack a => MqttServerTransportStack (SS.WebSocket a) where
  connHttpPath    = Just . WS.requestPath . SS.wsRequestHead
  connHttpHeaders = WS.requestHeaders . SS.wsRequestHead

instance SS.StreamServerStack a => MqttServerTransportStack (SS.TLS a) where
  connHttpPath    = const Nothing
  connHttpHeaders = const []

instance (SS.StreamServerStack transport) => SS.ServerStack (MQTT transport) where
  data Server (MQTT transport) = MqttServer
    { mqttTransportServer     :: SS.Server transport
    , mqttConfig              :: SS.ServerConfig (MQTT transport)
    }
  data ServerConfig (MQTT transport) = MqttServerConfig
    { mqttTransportConfig     :: SS.ServerConfig transport
    }
  data ServerConnection (MQTT transport) = MqttServerConnection
    { mqttTransportConnection :: SS.ServerConnection transport
    , mqttTransportLeftover   :: MVar BS.ByteString
    }
  data ServerConnectionInfo (MQTT transport) = MqttServerConnectionInfo
    { mqttTransportServerConnectionInfo :: SS.ServerConnectionInfo transport
    }
  data ServerException (MQTT transport)
    = ProtocolViolation String
    | ConnectionRefused ConnectionRefusal
    | KeepAliveTimeoutException
    deriving (Eq, Ord, Show, Typeable)
  withServer config handle =
    SS.withServer (mqttTransportConfig config) $ \server->
      handle (MqttServer server config)
  withConnection server handler =
    SS.withConnection (mqttTransportServer server) $ \connection info->
      flip handler (MqttServerConnectionInfo info) =<< MqttServerConnection
        <$> pure connection
        <*> newMVar mempty

instance (SS.StreamServerStack transport) => SS.MessageServerStack (MQTT transport) where
  type ClientMessage (MQTT transport) = ClientMessage
  type ServerMessage (MQTT transport) = ServerMessage
  sendMessage connection =
    SS.sendStreamBuilder (mqttTransportConnection connection) 8192 . serverMessageBuilder
  sendMessages connection msgs =
    SS.sendStreamBuilder (mqttTransportConnection connection) 8192 $ foldl (\b m-> b `mappend` serverMessageBuilder m) mempty msgs
  receiveMessage connection =
    modifyMVar (mqttTransportLeftover connection) (execute . SG.pushChunk decode)
    where
      fetch  = do
        bs <- SS.receiveStream (mqttTransportConnection connection) 4096
        pure $ if BS.null bs then Nothing else Just bs
      decode = SG.runGetIncremental clientMessageParser
      execute (SG.Partial continuation) = execute =<< continuation <$> fetch
      execute (SG.Fail _ _ failure)     = E.throwIO (ProtocolViolation failure :: SS.ServerException (MQTT transport))
      execute (SG.Done leftover' _ msg) = pure (leftover', msg)
  consumeMessages connection consume =
    modifyMVar_ (mqttTransportLeftover connection) (execute . SG.pushChunk decode)
    where
      fetch  = do
        bs <- SS.receiveStream (mqttTransportConnection connection) 4096
        pure $ if BS.null bs then Nothing else Just bs
      decode = SG.runGetIncremental clientMessageParser
      execute (SG.Partial continuation) = execute =<< continuation <$> fetch
      execute (SG.Fail _ _ failure)     = E.throwIO (ProtocolViolation failure :: SS.ServerException (MQTT transport))
      execute (SG.Done leftover' _ msg) = do
        done <- consume msg
        if done
          then pure leftover'
          else execute (SG.pushChunk decode leftover')

deriving instance Show (SS.ServerConnectionInfo transport) => Show (SS.ServerConnectionInfo (MQTT transport))

handleConnection :: forall transport authenticator. (SS.StreamServerStack transport, MqttServerTransportStack transport, Authenticator authenticator) => Broker.Broker authenticator -> SS.ServerConnection (MQTT transport) -> SS.ServerConnectionInfo (MQTT transport) -> IO ()
handleConnection broker conn connInfo =
  E.handle (\e-> do
    print "HUHU"
    print (e :: E.SomeException) >> E.throwIO e
   ) $ do
    print "handleConnection"
    recentActivity <- newIORef True
    msg <- SS.receiveMessage conn
    print msg
    case msg of
      ClientConnect {} ->
        let sessionHandler session sessionPresent = do
              void $ SS.sendMessage conn (ConnectAck $ Right sessionPresent)
              print "Client accepted."
              foldl1 race_
                [ handleInput recentActivity session
                , handleOutput session
                , keepAlive recentActivity (connectKeepAlive msg)
                ]
            sessionUnauthorizedHandler = do
              print "Client not authorized."
              void $ SS.sendMessage conn (ConnectAck $ Left NotAuthorized)
            sessionErrorHandler = do
              print "Server unavailable."
              void $ SS.sendMessage conn (ConnectAck $ Left ServerUnavailable)
            request = Request
              { requestClientIdentifier = connectClientIdentifier msg
              , requestCleanSession     = connectCleanSession msg
              , requestCredentials      = connectCredentials msg
              , requestHttpPath         = connHttpPath (mqttTransportServerConnectionInfo connInfo)
              , requestHttpHeaders      = connHttpHeaders (mqttTransportServerConnectionInfo connInfo)
              , requestSecure           = False
              , requestCertificateChain = Nothing
              }
          in Broker.withSession broker request sessionUnauthorizedHandler sessionErrorHandler sessionHandler
      _ -> E.throwIO (ProtocolViolation "Expected CONN packet." :: SS.ServerException (MQTT transport))
  where
    -- The keep alive thread wakes up every `keepAlive/2` seconds.
    -- When it detects no recent activity, it sleeps one more full `keepAlive`
    -- interval and checks again. When it still finds no recent activity, it
    -- throws an exception.
    -- That way a timeout will be detected between 1.5 and 2 `keep alive`
    -- intervals after the last actual client activity.
    keepAlive :: RecentActivity -> KeepAliveInterval -> IO ()
    keepAlive recentActivity interval = forever $ do
      writeIORef recentActivity False
      threadDelay regularInterval
      activity <- readIORef recentActivity
      unless activity $ do
        -- Alert state: The client must get active within the next interval.
        threadDelay alertInterval
        activity' <- readIORef recentActivity
        unless activity' $ E.throwIO (KeepAliveTimeoutException :: SS.ServerException (MQTT transport))
      where
        regularInterval = fromIntegral interval * 500000
        alertInterval   = fromIntegral interval * 1000000
    handleInput :: RecentActivity -> Session.Session -> IO ()
    handleInput recentActivity session = do
      print "Start consuming messages."
      SS.consumeMessages conn $ \packet-> do
        writeIORef recentActivity True
        case packet of
          ClientConnect {} ->
            E.throwIO (ProtocolViolation "Unexpected CONN packet." :: SS.ServerException (MQTT transport))
          ClientPublish msg -> do
            Broker.publishUpstream broker session msg
            pure False
          ClientPublish' pid msg -> do
            case msgQos msg of
              Qos0 -> pure () -- should not happen (invalid state)
              Qos1 -> do
                Broker.publishUpstream broker session msg
                Session.enqueuePublishAcknowledged session pid
              Qos2 -> do
                Session.holdQos2Message session pid msg
                Session.enqueuePublishReceived session pid
            pure False
          ClientPublishRelease pid -> do
            mmsg <- Session.releaseQos2Message session pid
            case mmsg of
              Nothing ->
                pure () -- WARNING
              Just msg -> do
                Broker.publishUpstream broker session msg
                Session.enqueuePublishCompleted session pid
            pure False
          ClientSubscribe pid filters -> do
            Broker.subscribe broker session pid filters
            pure False
          ClientUnsubscribe {} ->
            pure False
          ClientPingRequest {} -> do
            void $ SS.sendMessage conn ServerPingResponse
            pure False
          ClientDisconnect ->
            pure True
          _ -> pure False -- FIXME
    handleOutput session = forever $ do
      -- The `dequeue` operation is blocking until messages get available.
      msgs <- Session.dequeue session
      --threadDelay 10
      SS.sendMessages conn msgs
