{-# LANGUAGE OverloadedStrings #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Broker where

import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad
import           Data.Functor.Identity
import qualified Data.IntMap              as IM
import qualified Data.IntSet              as IS
import           Data.Maybe
import           Network.MQTT.Message
import qualified Network.MQTT.RoutingTree as R
import qualified Network.MQTT.Session     as Session
import           Network.MQTT.Topic
import           System.Random

data Broker authenticator  = Broker {
    brokerAuthenticator :: authenticator
  , brokerState         :: MVar (BrokerState authenticator)
  }

data BrokerState authenticator
  =  BrokerState
    { brokerMaxSessionIdentifier :: !Session.Identifier
    , brokerSubscriptions        :: !(R.RoutingTree IS.IntSet)
    , brokerSessions             :: !(IM.IntMap Session.Session)
    }

new :: authenticator -> IO (Broker authenticator)
new authenticator = do
  st <-newMVar BrokerState
    { brokerMaxSessionIdentifier = 0
    , brokerSubscriptions        = mempty
    , brokerSessions             = mempty
    }
  pure Broker {
      brokerAuthenticator = authenticator
    , brokerState         = st
    }

data SessionConfig
   = SessionConfig
     { sessionConfigQueue0MaxSize :: Int
     , sessionConfigQueue1MaxSize :: Int
     , sessionConfigQueue2MaxSize :: Int
     }

defaultSessionConfig :: SessionConfig
defaultSessionConfig = SessionConfig 100 100 100

data SessionRequest
   = SessionRequest
     { sessionRequestClientIdentifier :: !ClientIdentifier
     , sessionRequestCredentials      :: !(Maybe (Username, Maybe Password))
     , sessionRequestConnectionInfo   :: ()
     , sessionClean                   :: CleanSession
     }

withSession :: Broker auth -> SessionRequest -> IO () -> IO () -> (Session.Session -> SessionPresent -> IO ()) -> IO ()
withSession broker request sessionRejectHandler sessionErrorHandler sessionHandler
  | sessionRequestClientIdentifier request /= "mqtt-default" = sessionRejectHandler
  | otherwise = do
      r <- randomIO :: IO Double
      if r < 0.5
        then sessionErrorHandler
        else bracket
          ( createSession broker defaultSessionConfig )
          ( when (sessionClean request) . closeSession broker )
          ( \session-> sessionHandler session False )

createSession :: Broker auth -> SessionConfig -> IO Session.Session
createSession (Broker _ broker) config =
  modifyMVar broker $ \brokerState-> do
    subscriptions <- newMVar R.empty
    queue <- newMVar (Session.emptyServerQueue 1000)
    queuePending <- newEmptyMVar
    let newSessionIdentifier = brokerMaxSessionIdentifier brokerState + 1
        newSession = Session.Session
         { Session.sessionIdentifier       = newSessionIdentifier
         , Session.sessionSubscriptions    = subscriptions
         , Session.sessionQueue            = queue
         , Session.sessionQueuePending     = queuePending
         , Session.sessionQueueLimitQos0   = 256
         }
        newBrokerState = brokerState
         { brokerMaxSessionIdentifier = newSessionIdentifier
         , brokerSessions             = IM.insert newSessionIdentifier newSession (brokerSessions brokerState)
         }
    pure (newBrokerState, newSession)

closeSession :: Broker auth -> Session.Session -> IO ()
closeSession (Broker _ broker) session =
  modifyMVar_ broker $ \brokerState->
    withMVar (Session.sessionSubscriptions session) $ \subscriptions->
      pure $ brokerState
        { brokerSubscriptions =
            R.differenceWith IS.difference
              ( brokerSubscriptions brokerState)
              ( R.map
                  ( const $ IS.singleton $ Session.sessionIdentifier session )
                  subscriptions
              )
        , brokerSessions =
            IM.delete
              ( Session.sessionIdentifier session )
              ( brokerSessions brokerState)
        }

publishDownstream :: Broker auth -> Message -> IO ()
publishDownstream (Broker _auth broker) msg = do
  let topic = msgTopic msg
  brokerState <- readMVar broker
  forM_ (IS.elems $ fromMaybe IS.empty $ R.lookupWith IS.union topic $ brokerSubscriptions brokerState) $ \key->
    case IM.lookup (key :: Int) (brokerSessions brokerState) of
      Nothing      ->
        putStrLn "WARNING: dead session reference"
      Just session -> Session.enqueueMessage session msg

publishUpstream :: Broker auth -> Session.Session -> Message -> IO ()
publishUpstream broker session msg =
  publishDownstream broker msg

-- TODO: refactor
subscribe :: Broker auth -> Session.Session -> PacketIdentifier -> [(Filter, QualityOfService)] -> IO ()
subscribe (Broker _ broker) session pid filters = do
  modifyMVarMasked_ broker $ \bst-> do
    modifyMVarMasked_ (Session.sessionSubscriptions session) $ \subs->
      pure $ foldr (\(f,q)-> R.insertWith max f (Identity q)) subs filters
    pure $ bst
             { brokerSubscriptions = foldr
                (\(x,_)-> R.insertWith IS.union x (IS.singleton $ Session.sessionIdentifier session))
                (brokerSubscriptions bst) filters
             }
  Session.enqueueSubscribeAcknowledged session pid (fmap (Just . snd) filters)


{-
subscribeSession :: Session -> [(TopicFilter, QosLevel)] -> IO ()
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

unsubscribeSession :: Session -> [TopicFilter] -> IO ()
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

-- FIXME: What about downgrading message qos?
deliverSession :: Session -> Topic -> Message -> IO ()
deliverSession session topic message = do
  sst <- readMVar (unSession session)
  case R.lookupWith max topic (sessionSubscriptions sst) of
    Just (Identity Qos0) -> do
      success <- tryWriteChan (sessionQueue0 sst) (topic, message)
      unless success (closeSession session)
    Just (Identity Qos1) -> do
      success <- tryWriteChan (sessionQueue1 sst) (topic, message)
      unless success (closeSession session)
    Just (Identity Qos2) -> do
      success <- tryWriteChan (sessionQueue2 sst) (topic, message)
      unless success (closeSession session)
    _ -> pure ()


-}
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
      , sessionSubscriptions           :: IS.Set TopicTopicFilter
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
