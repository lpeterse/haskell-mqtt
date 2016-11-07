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

import Data.IORef
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import qualified Control.Exception        as E
import           Control.Monad
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Builder  as BS
import qualified Data.Serialize.Get       as SG
import           Data.Typeable
import qualified Network.MQTT.Broker      as Broker
import qualified Network.MQTT.Session     as Session
import           Network.MQTT.Message
import qualified Network.Stack.Server     as SS

instance (Typeable transport) => E.Exception (SS.ServerException (MQTT transport))

data MQTT transport

type RecentActivity = IORef Bool

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
  withConnection server handleConnection =
    SS.withConnection (mqttTransportServer server) $ \connection info->
      flip handleConnection (MqttServerConnectionInfo info) =<< MqttServerConnection
        <$> pure connection
        <*> newMVar mempty

instance (SS.StreamServerStack transport) => SS.MessageServerStack (MQTT transport) where
  type ClientMessage (MQTT transport) = ClientMessage
  type ServerMessage (MQTT transport) = ServerMessage
  sendMessage connection =
    SS.sendStreamLazy (mqttTransportConnection connection) . BS.toLazyByteString . buildServerMessage
  receiveMessage connection =
    modifyMVar (mqttTransportLeftover connection) (execute . decode)
    where
      fetch  = SS.receiveStream (mqttTransportConnection connection)
      decode = SG.runGetPartial parseClientMessage
      execute (SG.Partial continuation) = execute =<< continuation <$> fetch
      execute (SG.Fail failure _)       = E.throwIO (ProtocolViolation failure :: SS.ServerException (MQTT transport))
      execute (SG.Done msg leftover')   = pure (leftover', msg)
  consumeMessages connection consume =
    modifyMVar_ (mqttTransportLeftover connection) (execute . decode)
    where
      fetch  = SS.receiveStream (mqttTransportConnection connection)
      decode = SG.runGetPartial parseClientMessage
      execute (SG.Partial continuation) = execute =<< continuation <$> fetch
      execute (SG.Fail failure _)       = E.throwIO (ProtocolViolation failure :: SS.ServerException (MQTT transport))
      execute (SG.Done msg leftover')   = do
        done <- consume msg
        if done
          then pure leftover'
          else execute (decode leftover')

deriving instance Show (SS.ServerConnectionInfo transport) => Show (SS.ServerConnectionInfo (MQTT transport))

handleConnection :: forall transport authenticator. (SS.StreamServerStack transport) => Broker.Broker authenticator -> SS.ServerConnection (MQTT transport) -> SS.ServerConnectionInfo (MQTT transport) -> IO ()
handleConnection broker conn _connInfo =
  E.handle (\e->
    print (e :: E.SomeException)
   ) $ do
    recentActivity <- newIORef True
    msg <- SS.receiveMessage conn
    print msg
    case msg of
      ClientConnect {} ->
        let sessionHandler session sessionPresent = do
              SS.sendMessage conn (ConnectAck $ Right sessionPresent)
              foldl1 race_
                [ handleInput recentActivity session
                , handleOutput session
                , keepAlive recentActivity (connectKeepAlive msg)
                ]
            sessionUnauthorizedHandler =
              SS.sendMessage conn (ConnectAck $ Left NotAuthorized)
            sessionErrorHandler =
              SS.sendMessage conn (ConnectAck $ Left ServerUnavailable)
            sessionRequest = Broker.SessionRequest
              { Broker.sessionRequestClientIdentifier = connectClientIdentifier msg
              , Broker.sessionRequestCredentials      = connectCredentials msg
              , Broker.sessionRequestConnectionInfo   = ()
              , Broker.sessionClean                   = connectCleanSession msg
              }
          in Broker.withSession broker sessionRequest sessionUnauthorizedHandler sessionErrorHandler sessionHandler
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
    handleInput recentActivity session = SS.consumeMessages conn $ \packet-> do
      writeIORef recentActivity True
      case packet of
        ClientConnect {} ->
          E.throwIO (ProtocolViolation "Unexpected CONN packet." :: SS.ServerException (MQTT transport))
        ClientPublish msg -> do
          Broker.publishUpstream broker session msg
          pure False
        ClientSubscribe pid filters -> do
          Broker.subscribe broker session pid filters
          pure False
        ClientUnsubscribe {} ->
          pure False
        ClientPingRequest {} -> do
          SS.sendMessage conn PingResponse
          pure False
        ClientDisconnect ->
          pure True
    handleOutput session = forever $
      -- The `dequeue` operation is blocking until messages get available.
      mapM_ (SS.sendMessage conn) =<< Session.dequeue session
