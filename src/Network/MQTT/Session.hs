{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE MultiWayIf        #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Session
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Session where

import           Control.Concurrent.MVar
import           Control.Concurrent.PrioritySemaphore
import           Control.Monad
import           Data.Bool
import qualified Data.IntSet              as IS
import qualified Data.IntMap              as IM
import           Data.Monoid
import qualified Data.Sequence            as Seq
import           Network.MQTT.Message
import qualified Network.MQTT.RoutingTree as R
import           Network.MQTT.Authentication

type Identifier = Int

data Session auth = Session
  { sessionPrincipal           :: !(Principal auth)
  , sessionClientIdentifier    :: !ClientIdentifier
  , sessionIdentifier          :: !Identifier
  , sessionSemaphore           :: !PrioritySemaphore
  , sessionSubscriptions       :: !(MVar (R.RoutingTree QualityOfService))
  , sessionQueue               :: !(MVar ServerQueue)
  , sessionQueuePending        :: !(MVar ())
  , sessionQueueLimitQos0      :: Int
  }

instance Eq (Session auth) where
  (==) s1 s2 = (==) (sessionIdentifier s1) (sessionIdentifier s2)

instance Ord (Session auth) where
  compare s1 s2 = compare (sessionIdentifier s1) (sessionIdentifier s2)

instance (Authenticator auth) => Show (Session auth) where
  show session =
    "Session { identifier = " ++ show (sessionIdentifier session)
    ++ ", principal = " ++ show (sessionPrincipal session)
    ++ ", client = " ++ show (sessionClientIdentifier session) ++ " }"

data ServerQueue
  = ServerQueue
  { queuePids           :: !(Seq.Seq Int)
  , outputBuffer        :: !(Seq.Seq ServerMessage)
  , queueQos0           :: !(Seq.Seq Message)
  , queueQos1           :: !(Seq.Seq Message)
  , queueQos2           :: !(Seq.Seq Message)
  , notAcknowledged     :: !(IM.IntMap Message) -- | We sent a `Qos1` message and have not yet received the @PUBACK@.
  , notReceived         :: !(IM.IntMap Message) -- | We sent a `Qos2` message and have not yet received the @PUBREC@.
  , notReleased         :: !(IM.IntMap Message) -- | We received as `Qos2` message, sent the @PUBREC@ and wait for the @PUBREL@.
  , notComplete         :: !IS.IntSet           -- | We sent a @PUBREL@ and have not yet received the @PUBCOMP@.
  }

emptyServerQueue :: Int -> ServerQueue
emptyServerQueue i = ServerQueue
  { queuePids         = Seq.fromList [0..min i 65535]
  , outputBuffer      = mempty
  , queueQos0         = mempty
  , queueQos1         = mempty
  , queueQos2         = mempty
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
    Just qos -> enqueueMessage session msg { msgQos = qos }

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
  modifyMVar_ (sessionQueue session) $ \queue->
    pure $! case msgQos msg of
      Qos0 -> queue { queueQos0 = Seq.take (sessionQueueLimitQos0 session) $ queueQos0 queue Seq.|> msg }
      Qos1 -> queue { queueQos1 = queueQos1 queue Seq.|> msg }
      Qos2 -> queue { queueQos2 = queueQos2 queue Seq.|> msg }
  -- IMPORTANT: Notify the sending thread that something has been enqueued!
  notePending session

-- TODO: make more efficient
enqueueMessages :: Foldable t => Session auth -> t Message -> IO ()
enqueueMessages session msgs =
  forM_ msgs (enqueueMessage session)

enqueueSubscribeAcknowledged :: Session auth -> PacketIdentifier -> [Maybe QualityOfService] -> IO ()
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
dequeue :: Session auth -> IO (Seq.Seq ServerMessage)
dequeue session =
  modifyMVar (sessionQueue session) $ \queue-> do
    let q = normalizeQueue queue
    if | not (Seq.null $ outputBuffer q) -> pure (q { outputBuffer = mempty }, outputBuffer q)
       | not (Seq.null $ queueQos0    q) -> pure (q { queueQos0    = mempty }, fmap (ServerPublish (-1) False) (queueQos0 q))
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
processPublish session pid _dup msg forward =
  case msgQos msg of
    Qos0 ->
      forward msg
    Qos1 -> do
      forward msg
      modifyMVar_ (sessionQueue session) $ \q-> pure $! q {
          outputBuffer = outputBuffer q Seq.|> ServerPublishAcknowledged pid
        }
      notePending session
    Qos2 -> do
      modifyMVar_ (sessionQueue session) $ \q-> pure $! q {
          outputBuffer = outputBuffer q Seq.|> ServerPublishReceived pid
        , notReleased  = IM.insert pid msg (notReleased q)
        }
      notePending session

-- | Note that a Qos1 message has been received by the peer.
--
--   This shall be called when a @PUBACK@ has been received by the peer.
--   We release the message from our buffer and the transaction is then complete.
processPublishAcknowledged :: Session auth -> PacketIdentifier -> IO ()
processPublishAcknowledged session pid = do
  modifyMVar_ (sessionQueue session) $ \q-> pure $! q {
      -- The packet identifier is free for reuse only if it actually was in the set of notAcknowledged messages.
      queuePids = bool (queuePids q) (pid Seq.<| queuePids q) (IM.member pid (notAcknowledged q))
    , notAcknowledged = IM.delete pid (notAcknowledged q)
    }
  -- See code of `processPublishComplete` for explanation.
  notePending session

-- | Note that a Qos2 message has been received by the peer.
--
--   This shall be called when a @PUBREC@ has been received from the peer.
--   This is the completion of the second step in a 4-way handshake.
--   The state changes from _not received_ to _not completed_.
--   We will send a @PUBREL@ to the client and expect a @PUBCOMP@ in return.
processPublishReceived :: Session auth -> PacketIdentifier -> IO ()
processPublishReceived session pid = do
  modifyMVar_ (sessionQueue session) $ \q->
    pure $! q {
      notReceived  = IM.delete pid (notReceived q)
    , notComplete  = IS.insert pid (notComplete q)
    , outputBuffer = outputBuffer q Seq.|> ServerPublishRelease pid
    }
  notePending session

-- | Release a `Qos2` message.
--
--   This shall be called when @PUBREL@ has been received from the peer.
--   It enqueues an outgoing @PUBCOMP@.
--   The message is only released if the handler returned without exception.
--   The handler is only executed if there still is a message (it is a valid scenario
--   that it might already have been released).
processPublishRelease :: Session auth -> PacketIdentifier -> (Message -> IO ()) -> IO ()
processPublishRelease session pid upstream = do
  modifyMVar_ (sessionQueue session) $ \q->
    case IM.lookup pid (notReleased q) of
      Nothing  ->
        pure q
      Just msg -> do
        upstream msg
        pure $! q { notReleased  = IM.delete pid (notReleased q)
                  , outputBuffer = outputBuffer q Seq.|> ServerPublishComplete pid
                  }
  notePending session

-- | Complete the transmission of a Qos2 message.
--
--   This shall be called when a @PUBCOMP@ has been received from the peer
--   to finally free the packet identifier.
processPublishComplete :: Session auth -> PacketIdentifier -> IO ()
processPublishComplete session pid = do
  modifyMVar_ (sessionQueue session) $ \q-> pure $! q {
      -- The packet identifier is now free for reuse.
      queuePids   = pid Seq.<| queuePids q
    , notComplete = IS.delete pid (notComplete q)
    }
  -- Although we did not enqueue something it might still be the case
  -- that we have unsent data that couldn't be sent by now because no more
  -- packet identifiers were available.
  -- In case the output queues are actually empty, the thread will check
  -- them once and immediately sleep again.
  notePending session

getSubscriptions :: Session auth -> IO (R.RoutingTree QualityOfService)
getSubscriptions session =
  readMVar (sessionSubscriptions session)

getFreePacketIdentifiers :: Session auth -> IO (Seq.Seq PacketIdentifier)
getFreePacketIdentifiers session =
  queuePids <$> readMVar (sessionQueue session)

resetQueue :: ServerQueue -> ServerQueue
resetQueue q = q {
    outputBuffer = (rePublishQos1 . rePublishQos2 . reReleaseQos2) mempty
  }
  where
    rePublishQos1 s = IM.foldlWithKey (\s' pid msg-> s' Seq.|> ServerPublish         pid True msg) s (notAcknowledged q)
    rePublishQos2 s = IM.foldlWithKey (\s' pid msg-> s' Seq.|> ServerPublish         pid True msg) s (notReceived     q)
    reReleaseQos2 s = IS.foldl        (\s' pid->     s' Seq.|> ServerPublishRelease  pid)          s (notComplete     q)

-- | This function fills the output buffer with as many messages
--   as possible (this is limited by the available packet identifiers).
normalizeQueue :: ServerQueue -> ServerQueue
normalizeQueue = takeQos1 . takeQos2
  where
    takeQos1 q
      | Seq.null msgs     = q
      | otherwise         = q
        { outputBuffer    = outputBuffer q <> Seq.zipWith (flip ServerPublish False) pids' msgs'
        , queuePids       = pids''
        , queueQos1       = msgs''
        , notAcknowledged = foldr (uncurry IM.insert) (notAcknowledged q)
                                  (Seq.zipWith (,) pids' msgs')
        }
      where
        pids              = queuePids q
        msgs              = queueQos1 q
        n                 = min (Seq.length pids) (Seq.length msgs)
        (pids', pids'')   = Seq.splitAt n pids
        (msgs', msgs'')   = Seq.splitAt n msgs
    takeQos2 q
      | Seq.null msgs     = q
      | otherwise         = q
        { outputBuffer    = outputBuffer q <> Seq.zipWith (flip ServerPublish False) pids' msgs'
        , queuePids       = pids''
        , queueQos2       = msgs''
        , notReceived     = foldr (uncurry IM.insert) (notReceived q)
                                  (Seq.zipWith (,) pids' msgs')
        }
      where
        pids              = queuePids q
        msgs              = queueQos2 q
        n                 = min (Seq.length pids) (Seq.length msgs)
        (pids', pids'')   = Seq.splitAt n pids
        (msgs', msgs'')   = Seq.splitAt n msgs
