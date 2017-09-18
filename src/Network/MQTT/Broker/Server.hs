{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeFamilies        #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker.Server
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Broker.Server
  ( serveConnection
  , MQTT ()
  , MqttServerTransportStack (..)
  , SS.Server ( .. )
  , SS.ServerConfig ( .. )
  , SS.ServerConnection ( .. )
  ) where

import           Control.Concurrent
import           Control.Concurrent.Async
import qualified Control.Exception                     as E
import           Control.Monad
import qualified Data.Binary.Get                       as SG
import qualified Data.ByteString                       as BS
import           Data.Int
import           Data.IORef
import qualified Data.Sequence                         as Seq
import           Data.Typeable
import qualified Network.Stack.Server                  as SS
import qualified Network.WebSockets                    as WS
import qualified System.Socket                         as S

import qualified Network.MQTT.Broker                   as Broker
import           Network.MQTT.Broker.Authentication
import qualified Network.MQTT.Broker.Internal          as Session
import qualified Network.MQTT.Broker.Session           as Session
import qualified Network.MQTT.Broker.Session.Statistic as Session
import           Network.MQTT.Message

data MQTT transport
data MqttServerException
  = ProtocolViolation String
  | MessageTooLong
  | ConnectionRejected RejectReason
  | KeepAliveTimeoutException
  deriving (Eq, Ord, Show, Typeable)
instance E.Exception MqttServerException

class SS.ServerStack a => MqttServerTransportStack a where
  getConnectionRequest :: SS.ServerConnectionInfo a -> IO ConnectionRequest

instance (Typeable f, Typeable t, Typeable p, S.Family f, S.Protocol p, S.Type t, S.HasNameInfo f) => MqttServerTransportStack (S.Socket f t p) where
  getConnectionRequest (SS.SocketServerConnectionInfo addr) = do
    remoteAddr <- S.hostName <$> S.getNameInfo addr (S.niNumericHost `mappend` S.niNumericService)
    pure ConnectionRequest {
        requestClientIdentifier = ClientIdentifier mempty
      , requestSecure = False
      , requestCleanSession = True
      , requestCredentials = Nothing
      , requestHttp = Nothing
      , requestCertificateChain = Nothing
      , requestRemoteAddress = Just remoteAddr
      , requestWill = Nothing
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
  withServer config handle =
    SS.withServer (mqttTransportConfig config) $ \server->
      handle (MqttServer server)
  serveOnce server handler =
    SS.serveOnce (mqttTransportServer server) $ \connection info->
      flip handler (MqttServerConnectionInfo info) =<< MqttServerConnection
        <$> pure connection
        <*> newMVar mempty
  serveForever server handler =
    SS.serveForever (mqttTransportServer server) $ \connection info->
      flip handler (MqttServerConnectionInfo info) =<< MqttServerConnection
        <$> pure connection
        <*> newMVar mempty

-- TODO: eventually too strict with message size tracking
instance (SS.StreamServerStack transport) => SS.MessageServerStack (MQTT transport) where
  type ClientMessage (MQTT transport) = ClientPacket
  type ServerMessage (MQTT transport) = ServerPacket
  sendMessage connection =
    SS.sendStreamBuilder (mqttTransportConnection connection) 8192 . serverPacketBuilder
  sendMessages connection msgs =
    SS.sendStreamBuilder (mqttTransportConnection connection) 8192 $ foldl (\b m-> b `mappend` serverPacketBuilder m) mempty msgs
  receiveMessage connection maxMsgSize =
    modifyMVar (mqttTransportLeftover connection) (execute 0 . SG.pushChunk decode)
    where
      fetch  = SS.receiveStream (mqttTransportConnection connection) 4096
      decode = SG.runGetIncremental clientPacketParser
      execute received result
        | received > maxMsgSize = E.throwIO MessageTooLong
        | otherwise = case result of
            SG.Partial continuation -> do
              bs <- fetch
              if BS.null bs
                then execute received (continuation Nothing)
                else execute (received + fromIntegral (BS.length bs)) (continuation $ Just bs)
            SG.Fail _ _ failure ->
              E.throwIO (ProtocolViolation failure)
            SG.Done leftover' _ msg ->
              pure (leftover', msg)
  consumeMessages connection maxMsgSize consume =
    modifyMVar_ (mqttTransportLeftover connection) (execute 0 . SG.pushChunk decode)
    where
      fetch  = SS.receiveStream (mqttTransportConnection connection) 4096
      decode = SG.runGetIncremental clientPacketParser
      execute received result
        | received > maxMsgSize = E.throwIO MessageTooLong
        | otherwise = case result of
            SG.Partial continuation -> do
              bs <- fetch
              if BS.null bs
                then execute received (continuation Nothing)
                else execute (received + fromIntegral (BS.length bs)) (continuation $ Just bs)
            SG.Fail _ _ failure ->
              E.throwIO (ProtocolViolation failure)
            SG.Done leftover' _ msg -> do
              done <- consume msg
              if done
                then pure leftover'
                else execute 0 (SG.pushChunk decode leftover')

deriving instance Show (SS.ServerConnectionInfo transport) => Show (SS.ServerConnectionInfo (MQTT transport))

serveConnection :: forall transport auth. (SS.StreamServerStack transport, MqttServerTransportStack transport, Authenticator auth) => Broker.Broker auth -> SS.ServerConnection (MQTT transport) -> SS.ServerConnectionInfo (MQTT transport) -> IO ()
serveConnection broker conn connInfo = do
  recentActivity <- newIORef True
  req <- getConnectionRequest (mqttTransportServerConnectionInfo connInfo)
  msg <- SS.receiveMessage conn maxInitialPacketSize
  case msg of
    ClientConnectUnsupported -> do
      Broker.onConnectionRejected cbs req UnacceptableProtocolVersion
      void $ SS.sendMessage conn (ServerConnectionRejected UnacceptableProtocolVersion)
      -- Communication ends here gracefully. The caller shall close the connection.
    ClientConnect {} -> do
      let -- | This one is called when the authenticator decided to reject the request.
          sessionRejectHandler reason = do
            Broker.onConnectionRejected cbs request reason
            void $ SS.sendMessage conn (ServerConnectionRejected reason)
            -- Communication ends here gracefully. The caller shall close the connection.

          -- | This part is where the threads for a connection are created
          --   (one for input, one for output and one watchdog thread).
          sessionAcceptHandler session sessionPresent = do
            Broker.onConnectionAccepted cbs request session
            void $ SS.sendMessage conn (ServerConnectionAccepted sessionPresent)
            foldl1 race_
              [ handleInput recentActivity session
              , handleOutput session
              , keepAlive recentActivity (connectKeepAlive msg)
              ] `E.catch` (\e-> do
                Broker.onConnectionFailed cbs session e
                E.throwIO e
              )
            Broker.onConnectionClosed cbs session
          -- Extend the request object with information gathered from the connect packet.
          request = req {
              requestClientIdentifier = connectClientIdentifier msg
            , requestCleanSession     = cleanSession
            , requestCredentials      = connectCredentials msg
            , requestWill             = connectWill msg
            }
            where
              CleanSession cleanSession = connectCleanSession msg
      Broker.withSession broker request sessionRejectHandler sessionAcceptHandler
    _ -> pure () -- TODO: Don't parse not-CONN packets in the first place!
  where
    cbs = Session.brokerCallbacks broker

    -- The size of the initial CONN packet shall somewhat be limited to a moderate size.
    -- This value is assumed to make no problems while still protecting
    -- the servers resources against exaustion attacks.
    maxInitialPacketSize :: Int64
    maxInitialPacketSize  = 65535

    -- The keep alive thread wakes up every `keepAlive/2` seconds.
    -- When it detects no recent activity, it sleeps one more full `keepAlive`
    -- interval and checks again. When it still finds no recent activity, it
    -- throws an exception.
    -- That way a timeout will be detected between 1.5 and 2 `keep alive`
    -- intervals after the last actual client activity.
    keepAlive :: IORef Bool -> KeepAliveInterval -> IO ()
    keepAlive recentActivity (KeepAliveInterval interval) = forever $ do
      writeIORef recentActivity False
      threadDelay regularInterval
      activity <- readIORef recentActivity
      unless activity $ do
        threadDelay regularInterval
        activity' <- readIORef recentActivity
        unless activity' $ do
          threadDelay regularInterval
          activity'' <- readIORef recentActivity
          unless activity'' $ E.throwIO KeepAliveTimeoutException
      where
        regularInterval = fromIntegral interval *  500000

    -- | This thread is responsible for continuously processing input.
    --   It blocks on reading the input stream until input becomes available.
    --   Input is consumed no faster than it can be processed.
    --
    --   * Read packet from the input stream.
    --   * Note that there was activity (TODO: a timeout may occur when a packet
    --     takes too long to transmit due to its size).
    --   * Process and dispatch the message internally.
    --   * Repeat and eventually wait again.
    --   * Eventually throws `ServerException`s.
    handleInput :: IORef Bool -> Session.Session auth -> IO ()
    handleInput recentActivity session = do
      maxPacketSize <- fromIntegral . quotaMaxPacketSize . principalQuota <$> Session.getPrincipal session
      SS.consumeMessages conn maxPacketSize $ \packet-> do
        writeIORef recentActivity True
        Session.accountPacketsReceived (Session.sessionStatistic session) 1
        case packet of
          ClientDisconnect ->
            pure True
          ClientConnect {} ->
            E.throwIO $ ProtocolViolation "Unexpected CONN packet."
          ClientConnectUnsupported ->
            E.throwIO $ ProtocolViolation "Unexpected CONN packet (of unsupported protocol version)."
          _ -> Session.process session packet >> pure False

    -- | This thread is responsible for continuously transmitting to the client
    --   and reading from the output queue.
    --
    --   * It blocks on Session.waitPending until output gets available.
    --   * When output is available, it fetches a whole sequence of messages
    --     from the output queue.
    --   * It then uses the optimized SS.sendMessages operation which fills
    --     a whole chunk with as many messages as possible and sends the chunks
    --     each with a single system call. This is _very_ important for high
    --     throughput.
    --   * Afterwards, it repeats and eventually waits again.
    handleOutput :: Session.Session auth -> IO ()
    handleOutput session = forever $ do
      -- The `waitPending` operation is blocking until messages get available.
      Session.wait session
      msgs <- Session.poll session
      void $ SS.sendMessages conn msgs
      Session.accountPacketsSent (Session.sessionStatistic session) (fromIntegral $ Seq.length msgs)
