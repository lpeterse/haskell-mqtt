{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE DeriveGeneric     #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker.Internal
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Broker.Internal where

import           Control.Concurrent.MVar
import           Control.Concurrent.PrioritySemaphore
import qualified Control.Concurrent.PrioritySemaphore  as PrioritySemaphore
import           Control.Exception
import           Control.Monad
import qualified Data.ByteString                       as BS
import qualified Data.Binary                           as B
import           Data.Functor.Identity
import           Data.Int
import qualified Data.IntMap.Strict                    as IM
import qualified Data.IntSet                           as IS
import qualified Data.Map.Strict                       as M
import           Data.Maybe
import           System.Clock
import qualified System.Log.Logger                     as Log
import           GHC.Generics                          (Generic)
import qualified Data.Sequence                         as Seq

import           Network.MQTT.Broker.Authentication    hiding (getPrincipal)
import           Network.MQTT.Broker.Authentication    (AuthenticationException,
                                                        Authenticator,
                                                        ConnectionRequest (..),
                                                        PrincipalIdentifier,
                                                        Quota (..),
                                                        authenticate,
                                                        getPrincipal,
                                                        principalPublishPermissions,
                                                        principalQuota,
                                                        principalRetainPermissions,
                                                        principalSubscribePermissions)
import qualified Network.MQTT.Broker.RetainedMessages  as RM
import qualified Network.MQTT.Broker.SessionStatistics as SS
import           Network.MQTT.Message                  (ClientIdentifier,
                                                        Message (..),
                                                        PacketIdentifier,
                                                        RejectReason (..),
                                                        SessionPresent (..))
import           Network.MQTT.Message
import qualified Network.MQTT.Message                  as Message
import           Network.MQTT.Message.Topic
import qualified Network.MQTT.Trie                     as R

data Broker auth
   = Broker
   { brokerCreatedAt     :: Int64
   , brokerAuthenticator :: auth
   , brokerRetainedStore :: RM.RetainedStore
   , brokerState         :: MVar (BrokerState auth)
   }

data BrokerState auth
   = BrokerState
   { brokerMaxSessionIdentifier   :: !SessionIdentifier
   , brokerSubscriptions          :: !(R.Trie IS.IntSet)
   , brokerSessions               :: !(IM.IntMap (Session auth))
   , brokerSessionsByPrincipals   :: !(M.Map (PrincipalIdentifier, ClientIdentifier) Int)
   }

type SessionIdentifier = Int

data Session auth
   = Session
   { sessionBroker              :: !(Broker auth)
   , sessionIdentifier          :: !SessionIdentifier
   , sessionClientIdentifier    :: !ClientIdentifier
   , sessionPrincipalIdentifier :: !PrincipalIdentifier
   , sessionCreatedAt           :: !Int64
   , sessionConnection          :: !(MVar Connection)
   , sessionPrincipal           :: !(MVar Principal)
   , sessionSemaphore           :: !PrioritySemaphore
   , sessionSubscriptions       :: !(MVar (R.Trie QoS))
   , sessionQueue               :: !(MVar ServerQueue)
   , sessionQueuePending        :: !(MVar ())
   , sessionStatistics          :: !SS.Statistics
   }

data Connection
   = Connection
   { connectionCreatedAt     :: !Int64
   , connectionCleanSession  :: !Bool
   , connectionSecure        :: !Bool
   , connectionWebSocket     :: !Bool
   , connectionRemoteAddress :: !(Maybe BS.ByteString)
   } deriving (Eq, Ord, Show, Generic)

data ServerQueue
   = ServerQueue
   { queuePids       :: !(Seq.Seq PacketIdentifier)
   , outputBuffer    :: !(Seq.Seq ServerPacket)
   , queueQoS0       :: !(Seq.Seq Message)
   , queueQoS1       :: !(Seq.Seq Message)
   , queueQoS2       :: !(Seq.Seq Message)
   , notAcknowledged :: !(IM.IntMap Message) -- We sent a `QoS1` message and have not yet received the @PUBACK@.
   , notReceived     :: !(IM.IntMap Message) -- We sent a `QoS2` message and have not yet received the @PUBREC@.
   , notReleased     :: !(IM.IntMap Message) -- We received as `QoS2` message, sent the @PUBREC@ and wait for the @PUBREL@.
   , notComplete     :: !IS.IntSet           -- We sent a @PUBREL@ and have not yet received the @PUBCOMP@.
   }

instance B.Binary Connection

instance Eq (Session auth) where
 (==) s1 s2 = (==) (sessionIdentifier s1) (sessionIdentifier s2)

instance Ord (Session auth) where
 compare s1 s2 = compare (sessionIdentifier s1) (sessionIdentifier s2)

instance Show (Session auth) where
 show session =
   "Session { identifier = " ++ show (sessionIdentifier session)
   ++ ", principal = " ++ show (sessionPrincipalIdentifier session)
   ++ ", client = " ++ show (sessionClientIdentifier session) ++ " }"

-- | Inject a message downstream into the broker. It will be delivered
--   to all subscribed sessions within this broker instance.
publishDownstream :: Broker auth -> Message -> IO ()
publishDownstream broker msg = do
 RM.store msg (brokerRetainedStore broker)
 let topic = msgTopic msg
 st <- readMVar (brokerState broker)
 forM_ (IS.elems $ R.lookup topic $ brokerSubscriptions st) $ \key->
   case IM.lookup (key :: Int) (brokerSessions st) of
     Nothing      ->
       putStrLn "WARNING: dead session reference"
     Just session -> publishMessage session msg

-- | Publish a message upstream on the broker.
--
--   * As long as the broker is not clustered upstream=downstream.
--   * FUTURE NOTE: In clustering mode this shall distribute the message
--     to other brokers or upwards when the brokers form a hierarchy.
publishUpstream :: Broker auth -> Message -> IO ()
publishUpstream = publishDownstream

notePending   :: Session auth -> IO ()
notePending    = void . flip tryPutMVar () . sessionQueuePending

waitPending   :: Session auth -> IO ()
waitPending    = void . readMVar . sessionQueuePending

emptyServerQueue :: Int -> ServerQueue
emptyServerQueue i = ServerQueue
 { queuePids         = Seq.fromList $ fmap PacketIdentifier [0 .. min i 65535]
 , outputBuffer      = mempty
 , queueQoS0         = mempty
 , queueQoS1         = mempty
 , queueQoS2         = mempty
 , notAcknowledged   = mempty
 , notReceived       = mempty
 , notReleased       = mempty
 , notComplete       = mempty
 }

publishMessage :: Session auth -> Message -> IO ()
publishMessage session msg = do
 subscriptions <- readMVar (sessionSubscriptions session)
 case R.findMaxBounded (msgTopic msg) subscriptions of
   Nothing  -> pure ()
   Just qos -> enqueueMessage session msg { msgQoS = qos }

publishMessages :: Foldable t => Session auth -> t Message -> IO ()
publishMessages session msgs =
 forM_ msgs (publishMessage session)

-- | This enqueues a message for transmission to the client. This operation does not block.
enqueueMessage :: Session auth -> Message -> IO ()
enqueueMessage session msg = do
 quota <- principalQuota <$> readMVar (sessionPrincipal session)
 modifyMVar_ (sessionQueue session) $ \queue->
   pure $! case msgQoS msg of
     QoS0 -> queue { queueQoS0 = Seq.take (fromIntegral $ quotaMaxQueueSizeQoS0 quota) $ queueQoS0 queue Seq.|> msg }
     -- TODO: Terminate session on queue overflow!
     QoS1 -> queue { queueQoS1 = queueQoS1 queue Seq.|> msg }
     QoS2 -> queue { queueQoS2 = queueQoS2 queue Seq.|> msg }
 -- IMPORTANT: Notify the sending thread that something has been enqueued!
 notePending session

-- TODO: make more efficient
enqueueMessages :: Foldable t => Session auth -> t Message -> IO ()
enqueueMessages session msgs =
 forM_ msgs (enqueueMessage session)
