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
      { sessions                      :: MVar (M.Map ClientIdentifier MqttSession)
      }

data  MqttSession
    = MqttSession
      { sessionConnection              :: MVar (Async ())
      , sessionOutput                  :: MVar (Either RawMessage ())
      , sessionQoS0Queue               :: MVar (BC.BoundedChan Message)
      , sessionQoS1Queue               :: MVar (BC.BoundedChan Message)
      , sessionQoS2Queue               :: MVar (BC.BoundedChan Message)
      }

publish :: MqttSession -> Message -> IO ()
publish session message = case qos message of
  -- For QoS0 messages, the queue will simply overflow and messages will get
  -- lost. This is the desired behaviour for QoS0.
  QoS0 ->
    void $ BC.writeChan (sessionQoS0Queue session) message
  -- For QoS1 and QoS2 messages, an overflow will kill the connection and
  -- delete the session. We cannot otherwise signal the client that we are
  -- unable to further serve the contract.
  QoS1 -> do
    success <- BC.writeChan (sessionQoS1Queue session) message
    unless success terminateSession
  QoS2 -> do
    success <- BC.writeChan (sessionQoS2Queue session) message
    unless success terminateSession
  where
    terminateSession = undefined


dispatchConnection :: Connection -> MqttSessions -> IO ()
dispatchConnection connection ms =
  withConnect $ \clientIdentifier cleanSession keepAlive mwill muser j-> do
    (session, sessionPresent) <- modifyMVar (sessions ms) $ \ss->
      case M.lookup clientIdentifier ss of
        Nothing      -> do
          session <- newMqttSession
          pure (M.insert clientIdentifier session ss, (session, False))
        Just session ->
          pure (ss, (session, True))
    cancel =<< readMVar (sessionConnection session) -- Cancel existing connections associated with this session.
    void $ swapMVar (sessionOutput session) (Left $ ConnectAcknowledgement $ Right sessionPresent)
  where
    withConnect :: (ClientIdentifier -> CleanSession -> KeepAlive -> Maybe Will -> Maybe (Username, Maybe Password) -> BS.ByteString -> IO ()) -> IO ()
    withConnect  = undefined

    send :: RawMessage -> IO ()
    send  = undefined

newMqttSession :: IO MqttSession
newMqttSession = undefined
