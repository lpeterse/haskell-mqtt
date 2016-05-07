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
module Network.MQTT.Server where

import Data.Int
import Data.Typeable
import qualified Data.Map as M
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

data  MqttSessions
   =  MqttSessions
      { sessions                       :: MVar (M.Map ClientIdentifier MqttSession)
      , sessionsAuthentication         :: Maybe (Username, Maybe Password) -> IO (Maybe Identity)
      }

data  MqttSession
    = MqttSession
      { sessionConnection              :: MVar (Async ())
      , sessionOutput                  :: MVar (Either RawMessage ())
      , sessionQoS0Queue               :: BC.BoundedChan Message
      , sessionQoS1Queue               :: BC.BoundedChan Message
      , sessionQoS2Queue               :: BC.BoundedChan Message
      , sessionTerminate               :: IO ()
      }

data  Identity

publish :: MqttSession -> Message -> IO ()
publish session message = case qos message of
  -- For QoS0 messages, the queue will simply overflow and messages will get
  -- lost. This is the desired behaviour and allowed by contract.
  QoS0 ->
    void $ BC.writeChan (sessionQoS0Queue session) message
  -- For QoS1 and QoS2 messages, an overflow will kill the connection and
  -- delete the session. We cannot otherwise signal the client that we are
  -- unable to further serve the contract.
  QoS1 -> do
    success <- BC.tryWriteChan (sessionQoS1Queue session) message
    unless success $ sessionTerminate session
  QoS2 -> do
    success <- BC.tryWriteChan (sessionQoS2Queue session) message
    unless success $ sessionTerminate session

dispatchConnection :: Connection -> MqttSessions -> IO ()
dispatchConnection connection ms =
  withConnect $ \clientIdentifier cleanSession keepAlive mwill muser j-> do
    midentity <- sessionsAuthentication ms muser
    case midentity of
      Nothing -> send $ ConnectAcknowledgement $ Left NotAuthorized
      Just identity -> do
        (session, sessionPresent) <- getSession ms clientIdentifier
        withMVar (sessionConnection session) cancel -- Cancel existing connections associated with this session.
        void $ swapMVar (sessionOutput session) (Left $ ConnectAcknowledgement $ Right sessionPresent)
  where
    withConnect :: (ClientIdentifier -> CleanSession -> KeepAlive -> Maybe Will -> Maybe (Username, Maybe Password) -> BS.ByteString -> IO ()) -> IO ()
    withConnect  = undefined

    send :: RawMessage -> IO ()
    send  = undefined

getSession :: MqttSessions -> ClientIdentifier -> IO (MqttSession, SessionPresent)
getSession mqttSessions clientIdentifier =
  modifyMVar (sessions mqttSessions) $ \ms->
    case M.lookup clientIdentifier ms of
      Just session -> pure (ms, (session, True))
      Nothing      -> do
        mthread <- newMVar =<< async (pure ())
        session <- MqttSession
          <$> pure mthread
          <*> newEmptyMVar
          <*> BC.newBoundedChan 1000
          <*> BC.newBoundedChan 1000
          <*> BC.newBoundedChan 1000
          <*> pure (removeSession >> withMVar mthread cancel)
        pure (M.insert clientIdentifier session ms, (session, False))
  where
    removeSession =
      modifyMVar_ (sessions mqttSessions) $ pure . M.delete clientIdentifier
