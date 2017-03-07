{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Session
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Server.Session where

import           Control.Concurrent.MVar
import           Control.Concurrent.PrioritySemaphore
import           Control.Monad
import qualified Data.Binary                           as B
import           Data.Bool
import qualified Data.ByteString                       as BS
import           Data.Int
import qualified Data.IntMap                           as IM
import qualified Data.IntSet                           as IS
import           Data.Monoid
import qualified Data.Sequence                         as Seq
import           GHC.Generics                          (Generic)

import           Network.MQTT.Message
import qualified Network.MQTT.RoutingTree              as R
import           Network.MQTT.Server.Authentication    hiding (getPrincipal)
import qualified Network.MQTT.Server.SessionStatistics as SS

type Identifier = Int

data Session auth = Session
  { sessionIdentifier          :: !Identifier
  , sessionClientIdentifier    :: !ClientIdentifier
  , sessionPrincipalIdentifier :: !PrincipalIdentifier
  , sessionCreatedAt           :: !Int64
  , sessionConnection          :: !(MVar Connection)
  , sessionPrincipal           :: !(MVar Principal)
  , sessionSemaphore           :: !PrioritySemaphore
  , sessionSubscriptions       :: !(MVar (R.RoutingTree QoS))
  , sessionQueue               :: !(MVar ServerQueue)
  , sessionQueuePending        :: !(MVar ())
  , sessionStatistics          :: SS.Statistics
  }

data Connection = Connection
  { connectionCreatedAt     :: !Int64
  , connectionCleanSession  :: !Bool
  , connectionSecure        :: !Bool
  , connectionWebSocket     :: !Bool
  , connectionRemoteAddress :: !(Maybe BS.ByteString)
  } deriving (Eq, Ord, Show, Generic)

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

-- | Reset the session state after a reconnect.
--
--   * All output buffers will be cleared.
--   * Output buffers will be filled with retransmissions.
reset :: Session auth -> IO ()
reset session =
  modifyMVar_ (sessionQueue session) (\q-> pure $! resetQueue q)

notePending   :: Session auth -> IO ()
notePending    = void . flip tryPutMVar () . sessionQueuePending

waitPending   :: Session auth -> IO ()
waitPending    = void . readMVar . sessionQueuePending

publishMessage :: Session auth -> Message -> IO ()
publishMessage session msg = do
  subscriptions <- readMVar (sessionSubscriptions session)
  case R.findMaxBounded (msgTopic msg) subscriptions of
    Nothing  -> pure ()
    Just qos -> enqueueMessage session msg { msgQoS = qos }

publishMessages :: Foldable t => Session auth -> t Message -> IO ()
publishMessages session msgs =
  forM_ msgs (publishMessage session)

-- | Enqueue a PINGRESP to be sent as soon as the output thread is available.
--
--   The PINGRESP will be inserted with highest priority in front of all other enqueued messages.
enqueuePingResponse :: Session auth -> IO ()
enqueuePingResponse session = do
  modifyMVar_ (sessionQueue session) $ \queue->
    pure $! queue { outputBuffer = ServerPingResponse Seq.<| outputBuffer queue }
  -- IMPORTANT: Notify the sending thread that something has been enqueued!
  notePending session

-- | This enqueues a message for transmission to the client. This operation does not block.
enqueueMessage :: Session auth -> Message -> IO ()
enqueueMessage session msg = do
  quota <- principalQuota <$> getPrincipal session
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

enqueueSubscribeAcknowledged :: Session auth -> PacketIdentifier -> [Maybe QoS] -> IO ()
enqueueSubscribeAcknowledged session pid mqoss = do
  modifyMVar_ (sessionQueue session) $ \queue->
    pure $! queue { outputBuffer = outputBuffer queue Seq.|> ServerSubscribeAcknowledged pid mqoss }
  notePending session

enqueueUnsubscribeAcknowledged :: Session auth -> PacketIdentifier -> IO ()
enqueueUnsubscribeAcknowledged session pid = do
  modifyMVar_ (sessionQueue session) $ \queue->
    pure $! queue { outputBuffer = outputBuffer queue Seq.|> ServerUnsubscribeAcknowledged pid}
  notePending session

-- | Blocks until messages are available and prefers non-qos0 messages over
--  qos0 messages.
dequeue :: Session auth -> IO (Seq.Seq ServerPacket)
dequeue session =
  modifyMVar (sessionQueue session) $ \queue-> do
    let q = normalizeQueue queue
    if | not (Seq.null $ outputBuffer q) -> pure (q { outputBuffer = mempty }, outputBuffer q)
       | not (Seq.null $ queueQoS0    q) -> pure (q { queueQoS0    = mempty }, fmap (ServerPublish (PacketIdentifier (-1)) (Duplicate False)) (queueQoS0 q))
       | otherwise                       -> clearPending >> pure (q, mempty)
  where
    -- | In case all queues are empty, we need to clear the `pending` variable.
    -- ATTENTION: An implementation error would cause the `dequeue` operation
    -- to rush right through the blocking call leading to enourmous CPU usage.
    clearPending :: IO ()
    clearPending  = void $ tryTakeMVar (sessionQueuePending session)

-- | Process a @PUB@ message received from the peer.
--
--   Different handling depending on message qos.
processPublish :: Session auth -> PacketIdentifier -> Duplicate -> Message -> (Message -> IO ()) -> IO ()
processPublish session pid@(PacketIdentifier p) _dup msg forward =
  case msgQoS msg of
    QoS0 ->
      forward msg
    QoS1 -> do
      forward msg
      modifyMVar_ (sessionQueue session) $ \q-> pure $! q {
          outputBuffer = outputBuffer q Seq.|> ServerPublishAcknowledged pid
        }
      notePending session
    QoS2 -> do
      modifyMVar_ (sessionQueue session) $ \q-> pure $! q {
          outputBuffer = outputBuffer q Seq.|> ServerPublishReceived pid
        , notReleased  = IM.insert p msg (notReleased q)
        }
      notePending session

-- | Note that a QoS1 message has been received by the peer.
--
--   This shall be called when a @PUBACK@ has been received by the peer.
--   We release the message from our buffer and the transaction is then complete.
processPublishAcknowledged :: Session auth -> PacketIdentifier -> IO ()
processPublishAcknowledged session (PacketIdentifier pid) = do
  modifyMVar_ (sessionQueue session) $ \q-> pure $! q {
      -- The packet identifier is free for reuse only if it actually was in the set of notAcknowledged messages.
      queuePids = bool (queuePids q) (PacketIdentifier pid Seq.<| queuePids q) (IM.member pid (notAcknowledged q))
    , notAcknowledged = IM.delete pid (notAcknowledged q)
    }
  -- See code of `processPublishComplete` for explanation.
  notePending session

-- | Note that a QoS2 message has been received by the peer.
--
--   This shall be called when a @PUBREC@ has been received from the peer.
--   This is the completion of the second step in a 4-way handshake.
--   The state changes from _not received_ to _not completed_.
--   We will send a @PUBREL@ to the client and expect a @PUBCOMP@ in return.
processPublishReceived :: Session auth -> PacketIdentifier -> IO ()
processPublishReceived session (PacketIdentifier pid) = do
  modifyMVar_ (sessionQueue session) $ \q->
    pure $! q {
      notReceived  = IM.delete pid (notReceived q)
    , notComplete  = IS.insert pid (notComplete q)
    , outputBuffer = outputBuffer q Seq.|> ServerPublishRelease (PacketIdentifier pid)
    }
  notePending session

-- | Release a `QoS2` message.
--
--   This shall be called when @PUBREL@ has been received from the peer.
--   It enqueues an outgoing @PUBCOMP@.
--   The message is only released if the handler returned without exception.
--   The handler is only executed if there still is a message (it is a valid scenario
--   that it might already have been released).
processPublishRelease :: Session auth -> PacketIdentifier -> (Message -> IO ()) -> IO ()
processPublishRelease session (PacketIdentifier pid) upstream = do
  modifyMVar_ (sessionQueue session) $ \q->
    case IM.lookup pid (notReleased q) of
      Nothing  ->
        pure q
      Just msg -> do
        upstream msg
        pure $! q { notReleased  = IM.delete pid (notReleased q)
                  , outputBuffer = outputBuffer q Seq.|> ServerPublishComplete (PacketIdentifier pid)
                  }
  notePending session

-- | Complete the transmission of a QoS2 message.
--
--   This shall be called when a @PUBCOMP@ has been received from the peer
--   to finally free the packet identifier.
processPublishComplete :: Session auth -> PacketIdentifier -> IO ()
processPublishComplete session (PacketIdentifier pid) = do
  modifyMVar_ (sessionQueue session) $ \q-> pure $! q {
      -- The packet identifier is now free for reuse.
      queuePids   = PacketIdentifier pid Seq.<| queuePids q
    , notComplete = IS.delete pid (notComplete q)
    }
  -- Although we did not enqueue something it might still be the case
  -- that we have unsent data that couldn't be sent by now because no more
  -- packet identifiers were available.
  -- In case the output queues are actually empty, the thread will check
  -- them once and immediately sleep again.
  notePending session

getSubscriptions :: Session auth -> IO (R.RoutingTree QoS)
getSubscriptions session =
  readMVar (sessionSubscriptions session)

getConnection :: Session auth -> IO (Maybe Connection)
getConnection session =
  tryReadMVar (sessionConnection session)

getPrincipal  :: Session auth -> IO Principal
getPrincipal session =
  readMVar (sessionPrincipal session)

getFreePacketIdentifiers :: Session auth -> IO (Seq.Seq PacketIdentifier)
getFreePacketIdentifiers session =
  queuePids <$> readMVar (sessionQueue session)

resetQueue :: ServerQueue -> ServerQueue
resetQueue q = q {
    outputBuffer = (rePublishQoS1 . rePublishQoS2 . reReleaseQoS2) mempty
  }
  where
    rePublishQoS1 s = IM.foldlWithKey (\s' pid msg-> s' Seq.|> ServerPublish         (PacketIdentifier pid) (Duplicate True) msg) s (notAcknowledged q)
    rePublishQoS2 s = IM.foldlWithKey (\s' pid msg-> s' Seq.|> ServerPublish         (PacketIdentifier pid) (Duplicate True) msg) s (notReceived     q)
    reReleaseQoS2 s = IS.foldl        (\s' pid->     s' Seq.|> ServerPublishRelease  (PacketIdentifier pid)                     ) s (notComplete     q)

-- | This function fills the output buffer with as many messages
--   as possible (this is limited by the available packet identifiers).
normalizeQueue :: ServerQueue -> ServerQueue
normalizeQueue = takeQoS1 . takeQoS2
  where
    takeQoS1 q
      | Seq.null msgs     = q
      | otherwise         = q
        { outputBuffer    = outputBuffer q <> Seq.zipWith (flip ServerPublish (Duplicate False)) pids' msgs'
        , queuePids       = pids''
        , queueQoS1       = msgs''
        , notAcknowledged = foldr (\(PacketIdentifier pid, msg)-> IM.insert pid msg)
                                  (notAcknowledged q)
                                  (Seq.zipWith (,) pids' msgs')
        }
      where
        pids              = queuePids q
        msgs              = queueQoS1 q
        n                 = min (Seq.length pids) (Seq.length msgs)
        (pids', pids'')   = Seq.splitAt n pids
        (msgs', msgs'')   = Seq.splitAt n msgs
    takeQoS2 q
      | Seq.null msgs     = q
      | otherwise         = q
        { outputBuffer    = outputBuffer q <> Seq.zipWith (flip ServerPublish (Duplicate False)) pids' msgs'
        , queuePids       = pids''
        , queueQoS2       = msgs''
        , notReceived     = foldr (\(PacketIdentifier pid, msg)-> IM.insert pid msg)
                                  (notReceived q)
                                  (Seq.zipWith (,) pids' msgs')
        }
      where
        pids              = queuePids q
        msgs              = queueQoS2 q
        n                 = min (Seq.length pids) (Seq.length msgs)
        (pids', pids'')   = Seq.splitAt n pids
        (msgs', msgs'')   = Seq.splitAt n msgs
