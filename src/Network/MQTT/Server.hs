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
  req <- getConnectionRequest (mqttTransportServerConnectionInfo connInfo)
  msg <- SS.receiveMessage conn (mqttMaxMessageSize cfg)
  case msg of
    ClientConnectUnsupported -> do
      Log.warningM "Server.connection" $ "Connection from "
        ++ show (requestRemoteAddress req) ++ " rejected: UnacceptableProtocolVersion"
      void $ SS.sendMessage conn (ServerConnectionRejected UnacceptableProtocolVersion)
      -- Communication ends here gracefully. The caller shall close the connection.
    ClientConnect {} -> do
      let -- | This one is called when the authenticator decided to reject the request.
          sessionRejectHandler reason = do
            Log.warningM "Server.connection" $ "Connection rejected: " ++ show reason
            void $ SS.sendMessage conn (ServerConnectionRejected reason)
            -- Communication ends here gracefully. The caller shall close the connection.

          -- | This part is where the threads for a connection are created
          --   (one for input, one for output and one watchdog thread).
          sessionAcceptHandler session sessionPresent principal = do
            Log.infoM "Server.connection" $ "Connection accepted: Associated "
              ++ show principal ++ (if sessionPresent then " with existing session "
              ++ show (Session.sessionIdentifier session) ++  "." else " with new session.")
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
          -- Extend the request object with information gathered from the connect packet.
          request = req {
              requestClientIdentifier = connectClientIdentifier msg
            , requestCleanSession     = connectCleanSession msg
            , requestCredentials      = connectCredentials msg
            }
      Log.infoM "Server.connection" $ "Connection request: " ++ show request
      Broker.withSession broker request sessionRejectHandler sessionAcceptHandler
    _ -> pure () -- TODO: Don't parse not-CONN packets in the first place!
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
          ClientConnectUnsupported ->
            E.throwIO (ProtocolViolation "Unexpected CONN packet (of unsupported protocol version)." :: SS.ServerException (MQTT transport))
          ClientPublish pid dup msg -> do
            Session.processPublish session pid dup msg publish
            pure False
          ClientPublishAcknowledged pid -> do
            Session.processPublishAcknowledged session pid
            pure False
          ClientPublishReceived pid -> do
            Session.processPublishReceived session pid
            pure False
          ClientPublishRelease pid -> do
            Session.processPublishRelease session pid publish
            pure False
          ClientPublishComplete pid -> do
            Session.processPublishComplete session pid
            pure False
          ClientSubscribe pid filters -> do
            Broker.subscribe broker session pid filters
            pure False
          ClientUnsubscribe pid filters -> do
            Broker.unsubscribe broker session pid filters
            pure False
          ClientPingRequest -> do
            Log.debugM "Server.connection.handleInput" $ "Session " ++ show (Session.sessionIdentifier session) ++ ": Received ping."
            Session.enqueuePingResponse session
            pure False
          ClientDisconnect ->
            pure True
      where
        -- | A message is only sent upstream if the principal has publish
        --   permission. Otherwise the message is discarded silently.
        publish :: Message -> IO ()
        publish msg = do
          isPermitted <- hasPublishPermission
            (Broker.brokerAuthenticator broker)
            (Session.sessionPrincipal session)
            (msgTopic msg)
          when isPermitted (Broker.publishUpstream broker msg)
    handleOutput session = forever $ do
      -- The `waitPending` operation is blocking until messages get available.
      Session.waitPending session
      msgs <- Session.dequeue session
      SS.sendMessages conn msgs
