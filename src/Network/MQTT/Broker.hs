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
module Network.MQTT.Broker where

import Data.Maybe
import Data.Functor.Identity
import qualified Data.IntSet as IS
import qualified Data.IntMap as IM
import qualified Data.Sequence as S

import Control.Monad
import Control.Concurrent

import qualified Network.MQTT.RoutingTree as R

type SessionKey = Int
type Message = ()

newtype Broker  = Broker  { unBroker  :: MVar BrokerState }
newtype Session = Session { unSession :: MVar SessionState }

data QosLevel
   = Qos0
   | Qos1
   | Qos2
   deriving (Eq, Ord, Show)

data BrokerState
  =  BrokerState
    { brokerMaxSessionKey           :: !SessionKey
    , brokerSubscriptions           :: !(R.RoutingTree IS.IntSet)
    , brokerSessions                :: !(IM.IntMap Session)
    }

data SessionState
  =  SessionState
    { sessionBroker                 :: !Broker
    , sessionKey                    :: !SessionKey
    , sessionTerminated             :: !Bool
    , sessionSubscriptions          :: !(R.RoutingTree (Identity QosLevel))
    , sessionQos0Queue              :: !(S.Seq (R.Topic, Message))
    , sessionQos0QueueMaxSize       :: !Int
    , sessionQos1Queue              :: !(S.Seq (R.Topic, Message))
    , sessionQos1QueueMaxSize       :: !Int
    , sessionQos2Queue              :: !(S.Seq (R.Topic, Message))
    , sessionQos2QueueMaxSize       :: !Int
    }

newBroker  :: IO Broker
newBroker =
  Broker <$> newMVar BrokerState
    { brokerMaxSessionKey = 0
    , brokerSubscriptions = mempty
    , brokerSessions      = mempty
    }

createSession :: Broker -> IO Session
createSession (Broker broker) =
  modifyMVar broker $ \brokerState-> do
    let newSessionKey    = brokerMaxSessionKey brokerState + 1
    newSession <- Session <$> newMVar SessionState
         { sessionBroker           = Broker broker
         , sessionKey              = newSessionKey
         , sessionTerminated       = False
         , sessionSubscriptions    = R.empty
         , sessionQos0Queue        = S.empty
         , sessionQos0QueueMaxSize = 100
         , sessionQos1Queue        = S.empty
         , sessionQos1QueueMaxSize = 100
         , sessionQos2Queue        = S.empty
         , sessionQos2QueueMaxSize = 100
         }
    let newBrokerState = brokerState
         { brokerMaxSessionKey  = newSessionKey
         , brokerSessions       = IM.insert newSessionKey newSession (brokerSessions brokerState)
         }
    pure (newBrokerState, newSession)

closeSession :: Session -> IO ()
closeSession (Session session) =
  withMVar session $ \sessionState->
    modifyMVar_ (unBroker $ sessionBroker sessionState) $ \brokerState->
      pure $ brokerState
        { brokerSubscriptions =
            R.differenceWith IS.difference
              ( brokerSubscriptions brokerState)
              ( R.map ( const $ IS.singleton $ sessionKey sessionState )
                      ( sessionSubscriptions sessionState ) )
        , brokerSessions =
            IM.delete
              ( sessionKey sessionState )
              ( brokerSessions brokerState)
        }

subscribeSession :: Session -> [(R.Filter, QosLevel)] -> IO ()
subscribeSession (Session session) filters =
  modifyMVar_ session $ \sessionState->
    modifyMVar (unBroker $ sessionBroker sessionState) $ \brokerState-> do
      let newSessionState = sessionState
           { sessionSubscriptions = foldr
              (\(f,q)-> R.insertWith max f (Identity q))
              (sessionSubscriptions sessionState) filters
           }
      let newBrokerState = brokerState
           { brokerSubscriptions = foldr
              (\(x,_)-> R.insertWith IS.union x (IS.singleton $ sessionKey sessionState))
              (brokerSubscriptions brokerState) filters
           }
      pure (newBrokerState, newSessionState)

unsubscribeSession :: Session -> [R.Filter] -> IO ()
unsubscribeSession (Session session) filters =
  modifyMVar_ session $ \sessionState->
    modifyMVar (unBroker $ sessionBroker sessionState) $ \brokerState-> do
      let newSessionState = sessionState
           { sessionSubscriptions = foldr R.delete (sessionSubscriptions sessionState) filters
           }
      let newBrokerState = brokerState
           { brokerSubscriptions = foldr
              (R.adjust (IS.delete $ sessionKey sessionState))
              (brokerSubscriptions brokerState) filters
           }
      pure (newBrokerState, newSessionState)

deliverSession  :: Session -> R.Topic -> Message -> IO ()
deliverSession session topic message =
  modifyMVar_ (unSession session) $ \sst->
    pure $ case R.lookupWith max topic (sessionSubscriptions sst) of
      Nothing   -> sst
      Just (Identity Qos0) -> sst { sessionQos0Queue = sessionQos0Queue sst S.|> (topic, message) }
      Just (Identity Qos1) -> sst { sessionQos1Queue = sessionQos1Queue sst S.|> (topic, message) }
      Just (Identity Qos2) -> sst { sessionQos2Queue = sessionQos2Queue sst S.|> (topic, message) }

publishBroker   :: Broker -> R.Topic -> Message -> IO ()
publishBroker (Broker broker) topic message = do
  brokerState <- readMVar broker
  forM_ (IS.elems $ fromMaybe IS.empty $ R.lookupWith IS.union topic $ brokerSubscriptions brokerState) $ \key->
    case IM.lookup (key :: Int) (brokerSessions brokerState) of
      Nothing      -> pure ()
      Just session -> deliverSession session topic message

{-
type  SessionKey = Int

data  MqttBrokerSessions
   =  MqttBrokerSessions
      { maxSession    :: SessionKey
      , subscriptions :: SubscriptionTree
      , session       :: IM.IntMap MqttBrokerSession
      }


data  MqttBrokerSession
    = MqttBrokerSession
      { sessionBroker                  :: MqttBroker
      , sessionConnection              :: MVar (Async ())
      , sessionOutputBuffer            :: MVar RawMessage
      , sessionBestEffortQueue         :: BC.BoundedChan Message
      , sessionGuaranteedDeliveryQueue :: BC.BoundedChan Message
      , sessionInboundPacketState      :: MVar (IM.IntMap InboundPacketState)
      , sessionOutboundPacketState     :: MVar (IM.IntMap OutboundPacketState)
      , sessionSubscriptions           :: IS.Set TopicFilter
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

publish :: MqttBrokerSession -> Message -> IO ()
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

dispatchConnection :: MqttBroker -> Connection -> IO ()
dispatchConnection broker connection =
  withConnect $ \clientIdentifier cleanSession keepAlive mwill muser j-> do
    -- Client sent a valid CONNECT packet. Next, authenticate the client.
    midentity <- brokerAuthenticate broker muser
    case midentity of
      -- Client authentication failed. Send CONNACK with `NotAuthorized`.
      Nothing -> send $ ConnectAcknowledgement $ Left NotAuthorized
      -- Cient authenticaion successfull.
      Just identity -> do
        -- Retrieve session; create new one if necessary.
        (session, sessionPresent) <- getSession broker clientIdentifier
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

    maintainConnection :: MqttBrokerSession -> IO ()
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
            , publishQoS       = undefined -- Nothing
            , publishTopic     = topic message
            , publishBody      = payload message
            }
        processGuaranteedDeliveryQueue = undefined

getSession :: MqttBroker -> ClientIdentifier -> IO (MqttBrokerSession, SessionPresent)
getSession broker clientIdentifier =
  modifyMVar (brokerSessions broker) $ \ms->
    case M.lookup clientIdentifier ms of
      Just session -> pure (ms, (session, True))
      Nothing      -> do
        mthread <- newMVar =<< async (pure ())
        session <- MqttBrokerSession
          <$> pure broker
          <*> pure mthread
          <*> newEmptyMVar
          <*> BC.newBoundedChan 1000
          <*> BC.newBoundedChan 1000
          <*> newEmptyMVar
          <*> newEmptyMVar
        pure (M.insert clientIdentifier session ms, (session, False))
-}
