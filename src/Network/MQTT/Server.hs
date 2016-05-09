{-# LANGUAGE OverloadedStrings #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Server where

import Data.Int
import Data.Typeable
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.IntMap as IM
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Attoparsec.ByteString as A
import qualified Data.Text as T

import Control.Exception
import Control.Monad
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.Async
import qualified Control.Concurrent.BoundedChan as BC

import System.Random

import Network.MQTT
import Network.MQTT.Message
import Network.MQTT.Message.RemainingLength
import Network.MQTT.SubscriptionTree

type  SessionIdentifier = Int

data  MqttServerSessions
   =  MqttServerSessions
      { maxSession    :: SessionIdentifier
      , subscriptions :: SubscriptionTree
      , session       :: IM.IntMap MqttServerSession
      }

data  MqttServer
   =  MqttServer
      { serverMaxSessionIdentifier    :: MVar SessionIdentifier
      , serverSessions                :: MVar (IM.IntMap MqttServerSession)
      , serverAuthenticate            :: Maybe (Username, Maybe Password) -> IO (Maybe Identity)
      }

data  MqttServerSession
    = MqttServerSession
      { sessionServer                  :: MqttServer
      , sessionConnection              :: MVar (Async ())
      , sessionOutputBuffer            :: MVar RawMessage
      , sessionBestEffortQueue         :: BC.BoundedChan Message
      , sessionGuaranteedDeliveryQueue :: BC.BoundedChan Message
      , sessionInboundPacketState      :: MVar (IM.IntMap InboundPacketState)
      , sessionOutboundPacketState     :: MVar (IM.IntMap OutboundPacketState)
      , sessionSubscriptions           :: S.Set TopicFilter
      }

data  Identity
data  InboundPacketState

data  OutboundPacketState
   =  NotAcknowledgedPublishQoS1 Message
   |  NotReceivedPublishQoS2     Message
   |  NotCompletePublishQoS2     Message

data MConnection
   = MConnection
     { msend    :: Message -> IO ()
     , mreceive :: IO Message
     , mclose   :: IO ()
     }

publish :: MqttServerSession -> Message -> IO ()
publish session message = case qos message of
  -- For QoS0 messages, the queue will simply overflow and messages will get
  -- lost. This is the desired behaviour and allowed by contract.
  QoS0 ->
    void $ BC.writeChan (sessionBestEffortQueue session) message
  -- For QoS1 and QoS2 messages, an overflow will kill the connection and
  -- delete the session. We cannot otherwise signal the client that we are
  -- unable to further serve the contract.
  _ -> do
    success <- BC.tryWriteChan (sessionGuaranteedDeliveryQueue session) message
    unless success undefined -- sessionTerminate session

dispatchConnection :: MqttServer -> Connection -> IO ()
dispatchConnection server connection =
  withConnect $ \clientIdentifier cleanSession keepAlive mwill muser j-> do
    -- Client sent a valid CONNECT packet. Next, authenticate the client.
    midentity <- serverAuthenticate server muser
    case midentity of
      -- Client authentication failed. Send CONNACK with `NotAuthorized`.
      Nothing -> send $ ConnectAcknowledgement $ Left NotAuthorized
      -- Cient authenticaion successfull.
      Just identity -> do
        -- Retrieve session; create new one if necessary.
        (session, sessionPresent) <- getSession server clientIdentifier
        -- Now knowing the session state, we can send the success CONNACK.
        send $ ConnectAcknowledgement $ Right sessionPresent
        -- Replace (and shutdown) existing connections.
        modifyMVar_ (sessionConnection session) $ \previousConnection-> do
          cancel previousConnection
          async $ maintainConnection session `finally` close connection
  where
    -- Tries to receive the first packet and (if applicable) extracts the
    -- CONNECT information to call the contination with.
    withConnect :: (ClientIdentifier -> CleanSession -> KeepAlive -> Maybe Will -> Maybe (Username, Maybe Password) -> BS.ByteString -> IO ()) -> IO ()
    withConnect  = undefined

    send :: RawMessage -> IO ()
    send  = undefined

    maintainConnection :: MqttServerSession -> IO ()
    maintainConnection session =
      processKeepAlive `race_` processInput `race_` processOutput
        `race_` processBestEffortQueue `race_` processGuaranteedDeliveryQueue

      where
        processKeepAlive = undefined
        processInput     = undefined
        processOutput    = undefined
        processBestEffortQueue = forever $ do
          message <- BC.readChan (sessionBestEffortQueue session)
          putMVar (sessionOutputBuffer session) Publish
            { publishDuplicate = False
            , publishRetain    = retained message
            , publishQoS       = Nothing
            , publishTopic     = topic message
            , publishBody      = payload message
            }
        processGuaranteedDeliveryQueue = undefined

getSession :: MqttServer -> ClientIdentifier -> IO (MqttServerSession, SessionPresent)
getSession server clientIdentifier = undefined
{--
  modifyMVar (serverSessions server) $ \ms->
    case M.lookup clientIdentifier ms of
      Just session -> pure (ms, (session, True))
      Nothing      -> do
        mthread <- newMVar =<< async (pure ())
        session <- MqttServerSession
          <$> pure server
          <*> pure mthread
          <*> newEmptyMVar
          <*> BC.newBoundedChan 1000
          <*> BC.newBoundedChan 1000
          <*> newEmptyMVar
          <*> newEmptyMVar
        pure (M.insert clientIdentifier session ms, (session, False))
-}
