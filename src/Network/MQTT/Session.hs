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
module Network.MQTT.Session where

import           Control.Concurrent.MVar
import           Control.Monad
import           Data.Functor.Identity
import qualified Data.IntMap              as IM
import           Data.Monoid
import qualified Data.Sequence            as Seq
import           Network.MQTT.Message
import qualified Network.MQTT.RoutingTree as R

type Identifier = Int

data Session = Session
  { sessionIdentifier     :: !Identifier
  , sessionSubscriptions  :: !(MVar (R.RoutingTree (Identity QualityOfService)))
  , sessionQueue          :: !(MVar ServerQueue)
  , sessionQueuePending   :: !(MVar ())
  , sessionQueueLimitQos0 :: Int
  }

instance Eq Session where
  (==) s1 s2 = (==) (sessionIdentifier s1) (sessionIdentifier s2)

instance Ord Session where
  compare s1 s2 = compare (sessionIdentifier s1) (sessionIdentifier s2)

data ServerQueue
  = ServerQueue
  { queuePids           :: !(Seq.Seq Int)
  , queueAcknowledged   :: !(Seq.Seq Int)
  , queueReceived       :: !(Seq.Seq Int)
  , queueCompleted      :: !(Seq.Seq Int)
  , queueSubscribed     :: !(Seq.Seq (Int, [Maybe QualityOfService]))
  , queueUnsubscribed   :: !(Seq.Seq Int)
  , queueQos0           :: !(Seq.Seq Message)
  , queueQos1           :: !(Seq.Seq Message)
  , queueQos2           :: !(Seq.Seq Message)
  , queueUnacknowledged :: !(IM.IntMap Message)
  , queueUnreceived     :: !(IM.IntMap Message)
  , queueUncompleted    :: !(IM.IntMap Message)
  }

emptyServerQueue :: Int -> ServerQueue
emptyServerQueue i = ServerQueue
  { queuePids = Seq.fromList [0..min i 65535]
  , queueAcknowledged = mempty
  , queueReceived = mempty
  , queueCompleted = mempty
  , queueSubscribed = mempty
  , queueUnsubscribed = mempty
  , queueQos0 = mempty
  , queueQos1 = mempty
  , queueQos2 = mempty
  , queueUnacknowledged = mempty
  , queueUnreceived = mempty
  , queueUncompleted = mempty
  }

notePending   :: Session -> IO ()
notePending    = void . flip tryPutMVar () . sessionQueuePending

enqueueMessage :: Session -> Message -> IO ()
enqueueMessage session msg = do
  modifyMVar_ (sessionQueue session) $ \queue->
    pure $! case msgQos msg of
      Qos0 -> queue { queueQos0 = Seq.take (sessionQueueLimitQos0 session) $ queueQos0 queue Seq.|> msg }
      Qos1 -> queue { queueQos1 = queueQos1 queue Seq.|> msg }
      Qos2 -> queue { queueQos1 = queueQos1 queue Seq.|> msg }
  notePending session

enqueueSubscribeAcknowledged :: Session -> PacketIdentifier -> [Maybe QualityOfService] -> IO ()
enqueueSubscribeAcknowledged session pid mqoss = do
  modifyMVar_ (sessionQueue session) $ \queue->
    pure $! queue { queueSubscribed = queueSubscribed queue Seq.|> (pid, mqoss) }
  notePending session

enqueueUnsubscribeAcknowledged :: Session -> PacketIdentifier -> IO ()
enqueueUnsubscribeAcknowledged session pid = do
  modifyMVar_ (sessionQueue session) $ \queue->
    pure $! queue { queueUnsubscribed = queueUnsubscribed queue Seq.|> pid}
  notePending session

-- | Blocks until messages are available and prefers non-qos0 messages over
--  qos0 messages.
dequeue :: Session -> IO (Seq.Seq ServerMessage)
dequeue session = do
  waitPending
  modifyMVar (sessionQueue session) $ \queue-> do
    let qs@(_, nonQos0Msgs) = dequeueNonQos0 queue
    if Seq.null nonQos0Msgs
      then do
        let qs'@(_, qos0Msgs) = dequeueQos0 queue
        if Seq.null qos0Msgs
          then clearPending >> pure (queue, mempty)
          else pure qs'
      else pure qs
  where
    -- | Blocks until some other thread puts `()` into the `pending` variable.
    -- A filled `pending` variable implies the output queue is non-empty in
    -- case there is only one consumer thread.
    waitPending :: IO ()
    waitPending  = void $ readMVar $ sessionQueuePending session
    -- | In case all queues are empty, we need to clear the `pending` variable.
    -- ATTENTION: An implementation error would cause the `dequeue` operation
    -- to rush right through the blocking call leading to enourmous CPU usage.
    clearPending :: IO ()
    clearPending  = void $ tryTakeMVar (sessionQueuePending session)

dequeueQos0    :: ServerQueue -> (ServerQueue, Seq.Seq ServerMessage)
dequeueQos0 queue =
  ( queue { queueQos0 = mempty }, fmap Publish (queueQos0 queue) )

dequeueNonQos0 :: ServerQueue -> (ServerQueue, Seq.Seq ServerMessage)
dequeueNonQos0
  = dequeueQos1
  . dequeueQos2
  . dequeueAcknowledged
  . dequeueReceived
  . dequeueCompleted
  . dequeueSubscribed
  . dequeueUnsubscribed
  . (, mempty)
  where
    dequeueAcknowledged qs@(q,s)
      | Seq.null (queueAcknowledged q) = qs
      | otherwise = ( q { queueAcknowledged = mempty }, s <> fmap PublishAcknowledged (queueAcknowledged q) )
    dequeueReceived qs@(q,s)
      | Seq.null (queueReceived q) = qs
      | otherwise = ( q { queueReceived = mempty }, s <> fmap PublishReceived (queueReceived q) )
    dequeueCompleted qs@(q,s)
      | Seq.null (queueCompleted q) = qs
      | otherwise = ( q { queueCompleted = mempty }, s <> fmap PublishReceived (queueCompleted q) )
    dequeueSubscribed qs@(q,s)
      | Seq.null (queueSubscribed q) = qs
      | otherwise = ( q { queueSubscribed = mempty }, s <> fmap (uncurry SubscribeAcknowledged) (queueSubscribed q) )
    dequeueUnsubscribed qs@(q,s)
      | Seq.null (queueUnsubscribed q) = qs
      | otherwise = ( q { queueUnsubscribed = mempty }, s <> fmap UnsubscribeAcknowledged (queueUnsubscribed q) )
    dequeueQos1 qs@(q,s)
      | Seq.null msgs = qs
      | otherwise = ( q { queuePids           = pids''
                        , queueQos1           = msgs''
                        , queueUnacknowledged = foldr (uncurry IM.insert) (queueUnacknowledged q)
                                                      (Seq.zipWith (,) pids' msgs')
                        }
                    , s <> Seq.zipWith Publish' pids' msgs' )
      where
        pids                    = queuePids q
        msgs                    = queueQos1 q
        n                       = min (Seq.length pids) (Seq.length msgs)
        (pids', pids'')         = Seq.splitAt n pids
        (msgs', msgs'')         = Seq.splitAt n msgs
    dequeueQos2 qs@(q,s)
      | Seq.null msgs = qs
      | otherwise = ( q { queuePids        = pids''
                        , queueQos2        = msgs''
                        , queueUnreceived  = foldr (uncurry IM.insert) (queueUnreceived q)
                                                   (Seq.zipWith (,) pids' msgs')
                        }
                      , s <> Seq.zipWith Publish' pids' msgs' )
      where
        pids                    = queuePids q
        msgs                    = queueQos2 q
        n                       = min (Seq.length pids) (Seq.length msgs)
        (pids', pids'')         = Seq.splitAt n pids
        (msgs', msgs'')         = Seq.splitAt n msgs
