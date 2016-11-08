{-# LANGUAGE OverloadedStrings, TupleSections #-}
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

import           Control.Arrow            as Arrow
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
withSession broker request sessionRejectHandler sessionErrorHandler sessionHandler = do
  r <- randomIO :: IO Double
  if r < 0.2
    then sessionErrorHandler
    else if r < 0.4
      then sessionRejectHandler
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
            -- Remove the session identifier from each set that the session subscription
            -- tree has a corresponding value for (which is ignored).
            R.differenceWith (\b _-> Just (IS.delete (Session.sessionIdentifier session) b))
              ( brokerSubscriptions brokerState) subscriptions
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

publishUpstream' :: Broker auth -> Message -> IO ()
publishUpstream' broker msg =
  publishDownstream broker msg

subscribe :: Broker auth -> Session.Session -> PacketIdentifier -> [(Filter, QualityOfService)] -> IO ()
subscribe (Broker _ broker) session pid filters =
  -- Force the `qosTree` in order to lock the broker as little as possible.
  -- The `sidTree` is left lazy.
  qosTree `seq` do
    modifyMVarMasked_ broker $ \bst-> do
      modifyMVarMasked_
        ( Session.sessionSubscriptions session )
        ( pure . R.unionWith max qosTree )
      pure $ bst { brokerSubscriptions = R.unionWith IS.union (brokerSubscriptions bst) sidTree }
    Session.enqueueSubscribeAcknowledged session pid (fmap (Just . snd) filters)
  where
    qosTree = R.insertFoldable (fmap (Arrow.second Identity) filters) R.empty
    sidTree = R.map (const $ IS.singleton $ Session.sessionIdentifier session) qosTree

unsubscribe :: Broker auth -> Session.Session -> PacketIdentifier -> [Filter] -> IO ()
unsubscribe (Broker _ broker) session pid filters =
  -- Force the `unsubBrokerTree` first in order to lock the broker as little as possible.
  unsubBrokerTree `seq` do
    modifyMVarMasked_ broker $ \bst-> do
      modifyMVarMasked_
        ( Session.sessionSubscriptions session )
        ( pure . flip (R.differenceWith (const . const Nothing)) unsubBrokerTree )
      pure $ bst { brokerSubscriptions = R.differenceWith
                    (\is (Identity i)-> Just (IS.delete i is))
                    (brokerSubscriptions bst) unsubBrokerTree }
    Session.enqueueUnsubscribeAcknowledged session pid
  where
    unsubBrokerTree  = R.insertFoldable
      ( fmap (,Identity $ Session.sessionIdentifier session) filters ) R.empty
