{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker.Session
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Broker.Session (
  -- * Session
    Session ()
  , SessionIdentifier (..)
  , Connection (..)
  -- ** wait
  , wait
  -- ** poll
  , poll
  -- ** reset
  , reset
  -- ** process
  , process
  -- ** disconnect
  , disconnect
  -- ** terminate
  , terminate
  -- ** enqueue
  , enqueue

  -- * Misc
  , createdAt
  , identifier
  , clientIdentifier
  , principalIdentifier

  , getSubscriptions
  , getConnection
  , getPrincipal
  , getFreePacketIdentifiers
  , getStatistic

  -- * SessionStatistic
  , SessionStatistic (..)
  ) where

import           Control.Concurrent.MVar
import           Control.Concurrent.PrioritySemaphore
import           Control.Monad
import           Data.Bool
import           Data.Functor.Identity
import           Data.Int
import qualified Data.IntMap                           as IM
import qualified Data.IntSet                           as IS
import           Data.Maybe
import           Data.Monoid
import qualified Data.Sequence                         as Seq

import           Network.MQTT.Broker.Authentication    hiding (getPrincipal)
import           Network.MQTT.Broker.Internal
import qualified Network.MQTT.Broker.RetainedMessages  as RM
import           Network.MQTT.Broker.Session.Statistic
import           Network.MQTT.Message
import qualified Network.MQTT.Trie                     as R

-- | Process a packet received from the client.
process :: Session auth -> ClientPacket -> IO ()
process session (ClientPublish pid dup msg) =
  processPublish session pid dup msg
process session (ClientPublishAcknowledged pid) =
  processPublishAcknowledged session pid
process session (ClientPublishReceived pid) =
  processPublishReceived session pid
process session (ClientPublishRelease pid) =
  processPublishRelease session pid
process session (ClientPublishComplete pid) =
  processPublishComplete session pid
process session (ClientSubscribe pid filters) =
  processSubscribe session pid filters
process session (ClientUnsubscribe pid filters) =
  processUnsubscribe session pid filters
process session ClientPingRequest =
  processPingRequest session
process _ _ = pure ()

-- | Process a @PUB@ packet received from the peer.
--
--   Different handling depending on message qos.
processPublish :: Session auth -> PacketIdentifier -> Duplicate -> Message -> IO ()
processPublish session pid@(PacketIdentifier p) _dup msg =
  case msgQoS msg of
    QoS0 ->
      publish session msg
    QoS1 -> do
      publish session msg
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

processSubscribe :: Session auth -> PacketIdentifier -> [(Filter, QoS)] -> IO ()
processSubscribe session pid filters = do
  principal <- readMVar (sessionPrincipal session)
  checkedFilters <- mapM (checkPermission principal) filters
  let subscribeFilters = mapMaybe (\(filtr,mqos)->(filtr,) <$> mqos) checkedFilters
      qosTree = R.insertFoldable subscribeFilters R.empty
      sidTree = R.map (const $ IS.singleton sid) qosTree
  -- Do the accounting for the session statistics.
  -- TODO: Do this as a transaction below.
  let countAccepted = length subscribeFilters
  let countRejected = length filters - countAccepted
  accountSubscriptionsAccepted (sessionStatistic session) $ fromIntegral countAccepted
  accountSubscriptionsRejected (sessionStatistic session) $ fromIntegral countRejected
  -- Force the `qosTree` in order to lock the broker as little as possible.
  -- The `sidTree` is left lazy.
  qosTree `seq` do
    modifyMVarMasked_ (brokerState $ sessionBroker session) $ \bst-> do
      modifyMVarMasked_
        ( sessionSubscriptions session )
        ( pure . R.unionWith max qosTree )
      pure $ bst { brokerSubscriptions = R.unionWith IS.union (brokerSubscriptions bst) sidTree }
    enqueueSubscribeAcknowledged session pid (fmap snd checkedFilters)
    forM_ checkedFilters $ \(filtr,_qos)->
      publishMessages session =<< RM.retrieve filtr (brokerRetainedStore $ sessionBroker session)
  where
    SessionIdentifier sid = sessionIdentifier session
    checkPermission principal (filtr, qos) = do
      let isPermitted = R.matchFilter filtr (principalSubscribePermissions principal)
      pure (filtr, if isPermitted then Just qos else Nothing)

processUnsubscribe :: Session auth -> PacketIdentifier -> [Filter] -> IO ()
processUnsubscribe session pid filters =
  -- Force the `unsubBrokerTree` first in order to lock the broker as little as possible.
  unsubBrokerTree `seq` do
    modifyMVarMasked_ (brokerState $ sessionBroker session) $ \bst-> do
      modifyMVarMasked_
        ( sessionSubscriptions session )
        ( pure . flip (R.differenceWith (const . const Nothing)) unsubBrokerTree )
      pure $ bst { brokerSubscriptions = R.differenceWith
                    (\is (Identity i)-> Just (IS.delete i is))
                    (brokerSubscriptions bst) unsubBrokerTree }
    enqueueUnsubscribeAcknowledged session pid
  where
    SessionIdentifier sid = sessionIdentifier session
    unsubBrokerTree  = R.insertFoldable ( fmap (,Identity sid) filters ) R.empty

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
processPublishRelease :: Session auth -> PacketIdentifier -> IO ()
processPublishRelease session (PacketIdentifier pid) = do
  modifyMVar_ (sessionQueue session) $ \q->
    case IM.lookup pid (notReleased q) of
      Nothing  ->
        pure q
      Just msg -> do
        publish session msg
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

-- | Respond to a PING request with a PINGRESP.
processPingRequest :: Session auth -> IO ()
processPingRequest session = do
  modifyMVar_ (sessionQueue session) $ \queue->
    pure $! queue { outputBuffer = ServerPingResponse Seq.<| outputBuffer queue }
  -- IMPORTANT: Notify the sending thread that something has been enqueued!
  notePending session


-- | Disconnect a session.
disconnect :: Session auth -> IO ()
disconnect session =
  -- This assures that the client gets disconnected by interrupting
  -- the current client handler thread (if any).
  exclusively (sessionSemaphore session) (pure ())

-- | Reset the session state after a reconnect.
--
--   * All output buffers will be cleared.
--   * Output buffers will be filled with retransmissions.
reset :: Session auth -> IO ()
reset session =
  modifyMVar_ (sessionQueue session) (\q-> pure $! resetQueue q)

-- | Publish a message upstream with the permissions of a specific session.
publish :: Session auth -> Message -> IO ()
publish session msg = do
  principal <- readMVar (sessionPrincipal session)
  -- A topic is permitted if it yields a match in the publish permission tree.
  if R.matchTopic (msgTopic msg) (principalPublishPermissions principal)
    then do
      if retain && R.matchTopic (msgTopic msg) (principalRetainPermissions principal)
        then do
          RM.store msg (brokerRetainedStore $ sessionBroker session)
          accountRetentionsAccepted stats 1
        else
          accountRetentionsDropped stats 1
      publishUpstream (sessionBroker session) msg
      accountPublicationsAccepted stats 1
    else
      accountPublicationsDropped stats 1
  where
    stats = sessionStatistic session
    Retain retain = msgRetain msg

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

-- | Wait until packets are available for delivery.
wait :: Session auth -> IO ()
wait  = waitPending

-- | Poll a bunch of available messages for delivery to the client.
--
--   Priority: Qos0 packets will only be returned when all other packets
--   have already been polled.
poll :: Session auth -> IO (Seq.Seq ServerPacket)
poll session =
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

createdAt           :: Session auth -> Int64
createdAt            = sessionCreatedAt

identifier          :: Session auth -> SessionIdentifier
identifier           = sessionIdentifier

clientIdentifier    :: Session auth -> ClientIdentifier
clientIdentifier     = sessionClientIdentifier

principalIdentifier :: Session auth -> PrincipalIdentifier
principalIdentifier  = sessionPrincipalIdentifier

getSubscriptions :: Session auth -> IO (R.Trie QoS)
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

getStatistic :: Session auth -> IO SessionStatistic
getStatistic = snapshot

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
