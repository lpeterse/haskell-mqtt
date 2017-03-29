{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
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
import           Control.Monad
import qualified Data.Binary                           as B
import qualified Data.ByteString                       as BS
import           Data.Int
import qualified Data.IntMap.Strict                    as IM
import qualified Data.IntSet                           as IS
import qualified Data.Map.Strict                       as M
import qualified Data.Sequence                         as Seq
import           GHC.Generics                          (Generic)

import           Network.MQTT.Broker.Authentication    hiding (getPrincipal)
import qualified Network.MQTT.Broker.RetainedMessages  as RM
import           Network.MQTT.Message
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
   , brokerSessionsByPrincipals   :: !(M.Map (PrincipalIdentifier, ClientIdentifier) SessionIdentifier)
   }

newtype SessionIdentifier = SessionIdentifier Int deriving (Eq, Ord, Show, Enum, Generic)

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
   , sessionStatistic           :: !Statistic
   }

data Statistic
   = Statistic
   { stPublicationsAccepted   :: MVar Word
   , stPublicationsDropped    :: MVar Word
   , stRetentionsAccepted     :: MVar Word
   , stRetentionsDropped      :: MVar Word
   , stSubscriptionsAccepted  :: MVar Word
   , stSubscriptionsRejected  :: MVar Word
   , stQueueQoS0Dropped       :: MVar Word
   , stQueueQoS1Dropped       :: MVar Word
   , stQueueQoS2Dropped       :: MVar Word
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

instance B.Binary SessionIdentifier
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
   Just qos -> enqueue session msg { msgQoS = qos }

publishMessages :: Foldable t => Session auth -> t Message -> IO ()
publishMessages session msgs =
 forM_ msgs (publishMessage session)

-- | This enqueues a message for transmission to the client.
--
--   * This operations eventually terminates the session on queue overflow.
--     The caller will not notice this and the operation will not throw an exception.
enqueue :: Session auth -> Message -> IO ()
enqueue session msg = do
  quota <- principalQuota <$> readMVar (sessionPrincipal session)
  overflow <- modifyMVar (sessionQueue session) $ \queue-> case msgQoS msg of
    QoS0 -> if fromIntegral (quotaMaxQueueSizeQoS0 quota) > Seq.length (queueQoS0 queue)
      then pure $ (, False) $! queue { queueQoS0 = queueQoS0 queue Seq.|> msg }
      else pure $ (, True)  $! queue { queueQoS0 = Seq.drop 1 $ queueQoS0 queue Seq.|> msg }
    QoS1 -> if fromIntegral (quotaMaxQueueSizeQoS1 quota) > Seq.length (queueQoS1 queue)
      then pure $ (, False) $! queue { queueQoS1 = queueQoS1 queue Seq.|> msg }
      else pure $ (, True)  $! queue { queueQoS1 = Seq.drop 1 $ queueQoS1 queue Seq.|> msg }
    QoS2 -> if fromIntegral (quotaMaxQueueSizeQoS2 quota) > Seq.length (queueQoS2 queue)
      then pure $ (, False) $! queue { queueQoS2 = queueQoS2 queue Seq.|> msg }
      else pure $ (, True)  $! queue { queueQoS2 = Seq.drop 1 $ queueQoS2 queue Seq.|> msg }
  when overflow (accountOverflow $ sessionStatistic session)
  -- Notify the sending thread that something has been enqueued!
  notePending session
  where
    accountOverflow :: Statistic -> IO ()
    accountOverflow ss = case msgQoS msg of
      QoS0 -> modifyMVar_ (stQueueQoS0Dropped ss) $ \i-> pure $! i + 1
      QoS1 -> modifyMVar_ (stQueueQoS1Dropped ss) $ \i-> pure $! i + 1
      QoS2 -> modifyMVar_ (stQueueQoS2Dropped ss) $ \i-> pure $! i + 1

-- | Terminate a session.
--
--   * An eventually connected client gets disconnected.
--   * The session subscriptions are removed from the subscription tree
--     which means that it will receive no more messages.
--   * The session will be unlinked from the broker which means
--     that clients cannot resume it anymore under this client identifier.
terminate :: Session auth -> IO ()
terminate session =
  -- This assures that the client gets disconnected. The code is executed
  -- _after_ the current client handler that has terminated.
  -- TODO Race: New clients may try to connect while we are in here.
  -- This would not make the state inconsistent, but kill this thread.
  -- What we need is another `exclusivelyUninterruptible` function for
  -- the priority semaphore.
  exclusively (sessionSemaphore session) $
    modifyMVarMasked_ (brokerState $ sessionBroker session) $ \st->
      withMVarMasked (sessionSubscriptions session) $ \subscriptions->
        pure st
          { brokerSessions = IM.delete sid ( brokerSessions st )
            -- Remove the session id from the (principal, client) -> sessionid
            -- mapping. Remove empty leaves in this mapping, too.
          , brokerSessionsByPrincipals = M.delete
              ( sessionPrincipalIdentifier session
              , sessionClientIdentifier session)
              (brokerSessionsByPrincipals st)
            -- Remove the session id from each set that the session
            -- subscription tree has a corresponding value for (which is ignored).
          , brokerSubscriptions = R.differenceWith
              (\b _-> Just (IS.delete sid b) )
              ( brokerSubscriptions st ) subscriptions
          }
  where
    SessionIdentifier sid = sessionIdentifier session
