{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
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
import           Network.MQTT.Authentication ( Authenticator, AuthenticationException, Request(..), Principal, authenticate )
import           Network.MQTT.Message
import qualified Network.MQTT.RoutingTree as R
import qualified Network.MQTT.Session     as Session
import           Network.MQTT.Topic
import qualified System.Log.Logger        as Log
import qualified Data.Map as M

data Broker authenticator  = Broker {
    brokerAuthenticator :: authenticator
  , brokerState         :: MVar (BrokerState authenticator)
  }

data BrokerState authenticator
  =  BrokerState
    { brokerMaxSessionIdentifier :: !Session.Identifier
    , brokerSubscriptions        :: !(R.RoutingTree IS.IntSet)
    , brokerSessions             :: !(IM.IntMap Session.Session)
    , brokerPrincipals           :: !(M.Map Principal (M.Map ClientIdentifier Int))
    }

new :: Authenticator auth => auth -> IO (Broker auth)
new authenticator = do
  st <-newMVar BrokerState
    { brokerMaxSessionIdentifier = 0
    , brokerSubscriptions        = mempty
    , brokerSessions             = mempty
    , brokerPrincipals           = mempty
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

withSession :: (Authenticator auth) => Broker auth -> Request -> IO () -> (AuthenticationException auth -> IO ()) -> (Session.Session -> SessionPresent -> Principal -> IO ()) -> IO ()
withSession broker request sessionRejectHandler sessionErrorHandler sessionHandler = do
  emp <- try $ authenticate (brokerAuthenticator broker) request
  case emp of
    Left e -> sessionErrorHandler e
    Right mp -> case mp of
      Nothing -> sessionRejectHandler
      Just principal -> bracket
        ( modifyMVar (brokerState broker) $ getSession principal (requestClientIdentifier request) )
        (\(_present, session)-> when (requestCleanSession request) (closeSession broker session) )
        (\(present, session)-> sessionHandler session present principal )

getSession :: Principal -> ClientIdentifier -> BrokerState auth -> IO (BrokerState auth, (SessionPresent, Session.Session))
getSession principal cid st =
  case M.lookup principal (brokerPrincipals st) of
    Just mcis -> case M.lookup cid mcis of
      Just sid ->
        case IM.lookup sid (brokerSessions st) of
          -- Resuming an existing session..
          Just session -> pure (st, (True, session))
          -- Orphaned session id. This is illegal state.
          Nothing -> do
            Log.warningM "Broker.getSession" $ "Illegal state: Found orphanded session id " ++ show sid ++ "."
            createSession
      -- No session found for client identifier. Creating one.
      Nothing -> createSession
    -- No session entry found for principal. Creating one.
    Nothing -> createSession
  where
    --createSession :: IO (BrokerState auth, Session.Session)
    createSession = do
      incompleteQos2 <- newMVar IM.empty
      subscriptions <- newMVar R.empty
      queue <- newMVar (Session.emptyServerQueue 1000)
      queuePending <- newEmptyMVar
      let newSessionIdentifier = brokerMaxSessionIdentifier st + 1
          newSession = Session.Session
           { Session.sessionIdentifier       = newSessionIdentifier
           , Session.sessionClientIdentifier = cid
           , Session.sessionPrincipal        = principal
           , Session.sessionIncompleteQos2   = incompleteQos2
           , Session.sessionSubscriptions    = subscriptions
           , Session.sessionQueue            = queue
           , Session.sessionQueuePending     = queuePending
           , Session.sessionQueueLimitQos0   = 256
           }
          newBrokerState = st
           { brokerMaxSessionIdentifier = newSessionIdentifier
           , brokerSessions             = IM.insert newSessionIdentifier newSession (brokerSessions st)
           , brokerPrincipals           = M.unionWith M.union (brokerPrincipals st) (M.singleton principal $ M.singleton cid newSessionIdentifier)
           }
      Log.infoM "Broker.createSession" $ "Creating new session with id " ++ show newSessionIdentifier ++ " for " ++ show principal ++ "."
      pure (newBrokerState, (False, newSession))

closeSession :: Broker auth -> Session.Session -> IO ()
closeSession (Broker _ broker) session =
  modifyMVar_ broker $ \st->
    withMVar (Session.sessionSubscriptions session) $ \subscriptions->
      pure $ st
        { brokerSubscriptions =
            -- Remove the session identifier from each set that the session subscription
            -- tree has a corresponding value for (which is ignored).
            R.differenceWith (\b _-> Just (IS.delete (Session.sessionIdentifier session) b))
              ( brokerSubscriptions st) subscriptions
        , brokerSessions =
            IM.delete
              ( Session.sessionIdentifier session )
              ( brokerSessions st )
        , brokerPrincipals =
            M.update
              (\mcid-> let mcid' = M.delete (Session.sessionClientIdentifier session) mcid
                              in if M.null mcid' then Nothing else Just mcid')
              ( Session.sessionPrincipal session ) ( brokerPrincipals st )
        }

publishDownstream :: Broker auth -> Message -> IO ()
publishDownstream (Broker _auth broker) msg = do
  let topic = msgTopic msg
  st <- readMVar broker
  forM_ (IS.elems $ fromMaybe IS.empty $ R.lookupWith IS.union topic $ brokerSubscriptions st) $ \key->
    case IM.lookup (key :: Int) (brokerSessions st) of
      Nothing      ->
        putStrLn "WARNING: dead session reference"
      Just session -> Session.enqueueMessage session msg

publishUpstream :: Broker auth -> Session.Session -> Message -> IO ()
publishUpstream broker _session = publishDownstream broker

publishUpstream' :: Broker auth -> Message -> IO ()
publishUpstream'  = publishDownstream

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
