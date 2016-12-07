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
import           Data.Int
import           Data.IORef
import           Data.Typeable
import           Network.MQTT.Authentication
import qualified Network.MQTT.Broker         as Broker
import           Network.MQTT.Message
import qualified Network.MQTT.Session        as Session
import qualified Network.Stack.Server        as SS
import qualified Network.WebSockets          as WS
import qualified System.Log.Logger           as Log
import qualified System.Socket               as S

instance (Typeable transport) => E.Exception (SS.ServerException (MQTT transport))

data MQTT transport
type RecentActivity = IORef Bool

class SS.ServerStack a => MqttServerTransportStack a where
  getConnectionRequest :: SS.ServerConnectionInfo a -> IO ConnectionRequest

instance (Typeable f, Typeable t, Typeable p, S.Family f, S.Protocol p, S.Type t, S.HasNameInfo f) => MqttServerTransportStack (S.Socket f t p) where
  getConnectionRequest (SS.SocketServerConnectionInfo addr) = do
    remoteAddr <- S.hostName <$> S.getNameInfo addr (S.niNumericHost `mappend` S.niNumericService)
    pure ConnectionRequest {
        requestClientIdentifier = mempty
      , requestSecure = False
      , requestCleanSession = True
      , requestCredentials = Nothing
      , requestHttp = Nothing
      , requestCertificateChain = Nothing
      , requestRemoteAddress = Just remoteAddr
      }

instance (SS.StreamServerStack a, MqttServerTransportStack a) => MqttServerTransportStack (SS.WebSocket a) where
  getConnectionRequest (SS.WebSocketServerConnectionInfo tci rh) = do
    req <- getConnectionRequest tci
    pure req {
        requestHttp = Just (WS.requestPath rh, WS.requestHeaders rh)
      }

instance (SS.StreamServerStack a, MqttServerTransportStack a) => MqttServerTransportStack (SS.TLS a) where
  getConnectionRequest (SS.TlsServerConnectionInfo tci mcc) = do
    req <- getConnectionRequest tci
    pure req {
        requestSecure = True
      , requestCertificateChain = mcc
      }

instance (SS.StreamServerStack transport) => SS.ServerStack (MQTT transport) where
  data Server (MQTT transport) = MqttServer
    { mqttTransportServer     :: SS.Server transport
    , mqttConfig              :: SS.ServerConfig (MQTT transport)
    }
  data ServerConfig (MQTT transport) = MqttServerConfig
    { mqttTransportConfig     :: SS.ServerConfig transport
    , mqttMaxMessageSize      :: Int64
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
    | MessageTooLong
    | ConnectionRejected ConnectionRejectReason
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

-- TODO: eventually too strict with message size tracking
instance (SS.StreamServerStack transport) => SS.MessageServerStack (MQTT transport) where
  type ClientMessage (MQTT transport) = ClientMessage
  type ServerMessage (MQTT transport) = ServerMessage
  sendMessage connection =
    SS.sendStreamBuilder (mqttTransportConnection connection) 8192 . serverMessageBuilder
  sendMessages connection msgs =
    SS.sendStreamBuilder (mqttTransportConnection connection) 8192 $ foldl (\b m-> b `mappend` serverMessageBuilder m) mempty msgs
  receiveMessage connection maxMsgSize =
    modifyMVar (mqttTransportLeftover connection) (execute 0 . SG.pushChunk decode)
    where
      fetch  = SS.receiveStream (mqttTransportConnection connection) 4096
      decode = SG.runGetIncremental clientMessageParser
      execute received result
        | received > maxMsgSize = E.throwIO (MessageTooLong :: SS.ServerException (MQTT transport))
        | otherwise = case result of
            SG.Partial continuation -> do
              bs <- fetch
              if BS.null bs
                then execute received (continuation Nothing)
                else execute (received + fromIntegral (BS.length bs)) (continuation $ Just bs)
            SG.Fail _ _ failure ->
              E.throwIO (ProtocolViolation failure :: SS.ServerException (MQTT transport))
            SG.Done leftover' _ msg ->
              pure (leftover', msg)
  consumeMessages connection maxMsgSize consume =
    modifyMVar_ (mqttTransportLeftover connection) (execute 0 . SG.pushChunk decode)
    where
      fetch  = SS.receiveStream (mqttTransportConnection connection) 4096
      decode = SG.runGetIncremental clientMessageParser
      execute received result
        | received > maxMsgSize = E.throwIO (MessageTooLong :: SS.ServerException (MQTT transport))
        | otherwise = case result of
            SG.Partial continuation -> do
              bs <- fetch
              if BS.null bs
                then execute received (continuation Nothing)
                else execute (received + fromIntegral (BS.length bs)) (continuation $ Just bs)
            SG.Fail _ _ failure ->
              E.throwIO (ProtocolViolation failure :: SS.ServerException (MQTT transport))
            SG.Done leftover' _ msg -> do
              done <- consume msg
              if done
                then pure leftover'
                else execute 0 (SG.pushChunk decode leftover')

deriving instance Show (SS.ServerConnectionInfo transport) => Show (SS.ServerConnectionInfo (MQTT transport))

handleConnection :: forall transport auth. (SS.StreamServerStack transport, MqttServerTransportStack transport, Authenticator auth) => Broker.Broker auth -> SS.ServerConfig (MQTT transport) -> SS.ServerConnection (MQTT transport) -> SS.ServerConnectionInfo (MQTT transport) -> IO ()
handleConnection broker cfg conn connInfo = do
    recentActivity <- newIORef True
    msg <- SS.receiveMessage conn (mqttMaxMessageSize cfg)
      `E.catch` \e-> do
        Log.warningM "Server.connectionRequest" $ show (e :: SS.ServerException (MQTT transport))
        E.throwIO e
    case msg of
      ClientConnect {} ->
        let sessionErrorHandler e = do
              Log.errorM "Server.connectionRequest" $ show e
              void $ SS.sendMessage conn (ServerConnectionRejected ServerUnavailable)
            sessionUnauthorizedHandler = do
              Log.warningM "Server.connectionRequest" "Authentication failed."
              void $ SS.sendMessage conn (ServerConnectionRejected NotAuthorized)
            sessionHandler session sessionPresent principal = do
              Log.infoM "Server.connection" $ "Session " ++ show (Session.sessionIdentifier session)
                ++  ": Associated " ++ show principal
                ++ (if sessionPresent then " with existing session." else " with new session.")
              void $ SS.sendMessage conn (ServerConnectionAccepted sessionPresent)
              foldl1 race_
                [ handleInput recentActivity session
                , handleOutput session
                , keepAlive recentActivity (connectKeepAlive msg) session
                ] `E.catch` (\e-> do
                  Log.warningM "Server.connection" $"Session " ++ show (Session.sessionIdentifier session)
                    ++ ": Connection terminated with exception: " ++ show (e :: E.SomeException)
                  E.throwIO e
                )
              Log.infoM "Server.connection" $
                "Session " ++ show (Session.sessionIdentifier session) ++ ": Graceful disconnect."
          in do
            req <- getConnectionRequest (mqttTransportServerConnectionInfo connInfo)
            let request = req {
                requestClientIdentifier = connectClientIdentifier msg
              , requestCleanSession     = connectCleanSession msg
              , requestCredentials      = connectCredentials msg
              }
            Log.infoM "Server.connectionRequest" $ show request
            Broker.withSession broker request sessionUnauthorizedHandler sessionErrorHandler sessionHandler
      _ -> E.throwIO (ProtocolViolation "Expected CONN packet." :: SS.ServerException (MQTT transport))
  where
    -- The keep alive thread wakes up every `keepAlive/2` seconds.
    -- When it detects no recent activity, it sleeps one more full `keepAlive`
    -- interval and checks again. When it still finds no recent activity, it
    -- throws an exception.
    -- That way a timeout will be detected between 1.5 and 2 `keep alive`
    -- intervals after the last actual client activity.
    keepAlive :: RecentActivity -> KeepAliveInterval -> Session.Session auth -> IO ()
    keepAlive recentActivity interval session = forever $ do
      writeIORef recentActivity False
      threadDelay regularInterval
      activity <- readIORef recentActivity
      unless activity $ do
        threadDelay regularInterval
        activity' <- readIORef recentActivity
        unless activity' $ do
          -- Alert state: The client must get active within the next interval.
          Log.warningM "Server.connection.keepAlive" $ "Session " ++ show (Session.sessionIdentifier session) ++ ": Client is overdue."
          threadDelay regularInterval
          activity'' <- readIORef recentActivity
          unless activity'' $ E.throwIO (KeepAliveTimeoutException :: SS.ServerException (MQTT transport))
      where
        regularInterval = fromIntegral interval *  500000
    handleInput :: RecentActivity -> Session.Session auth -> IO ()
    handleInput recentActivity session =
      SS.consumeMessages conn (mqttMaxMessageSize cfg) $ \packet-> do
        writeIORef recentActivity True
        --Log.debugM "Server.connection.handleInput" $ take 50 $ show packet
        case packet of
          ClientConnect {} ->
            E.throwIO (ProtocolViolation "Unexpected CONN packet." :: SS.ServerException (MQTT transport))
          ClientPublish pid msg -> do
            case msgQos msg of
              Qos0 ->
                Broker.publishUpstream broker session msg
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
                pure ()
              Just msg -> do
                Broker.publishUpstream broker session msg
                Session.enqueuePublishCompleted session pid
            pure False
          ClientSubscribe pid filters -> do
            Broker.subscribe broker session pid filters
            pure False
          ClientUnsubscribe pid filters -> do
            Broker.unsubscribe broker session pid filters
            pure False
          ClientPingRequest -> do
            Log.debugM "Server.connection.handleInput" $ "Session " ++ show (Session.sessionIdentifier session) ++ ": Received ping."
            void $ SS.sendMessage conn ServerPingResponse
            pure False
          ClientDisconnect ->
            pure True
    handleOutput session = forever $ do
      -- The `dequeue` operation is blocking until messages get available.
      msgs <- Session.dequeue session
      SS.sendMessages conn msgs
