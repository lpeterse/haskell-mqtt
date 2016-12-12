{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Broker
  ( Broker ()
  , new
  , publishDownstream
  , publishUpstream
  , publishUpstream'
  , subscribe
  , unsubscribe
  , withSession
  , getSubscriptions
  ) where

import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad
import           Data.Functor.Identity
import qualified Data.IntMap                   as IM
import qualified Data.IntSet                   as IS
import qualified Data.Map                      as M
import           Data.Maybe
import           Network.MQTT.Authentication   (AuthenticationException,
                                                Authenticator,
                                                ConnectionRequest (..),
                                                Principal, authenticate,
                                                hasPublishPermission,
                                                hasSubscribePermission)
import           Network.MQTT.Message
import qualified Network.MQTT.RetainedMessages as RM
import qualified Network.MQTT.RoutingTree      as R
import qualified Network.MQTT.Session          as Session
import           Network.MQTT.Topic
import qualified System.Log.Logger             as Log

data Broker auth  = Broker {
    brokerAuthenticator    :: auth
  , brokerRetainedMessages :: MVar RM.RetainedTree
  , brokerState            :: MVar (BrokerState auth)
  }

data BrokerState auth
  =  BrokerState
    { brokerMaxSessionIdentifier :: !Session.Identifier
    , brokerSubscriptions        :: !(R.RoutingTree IS.IntSet)
    , brokerSessions             :: !(IM.IntMap (Session.Session auth))
    , brokerPrincipals           :: !(M.Map (Principal auth) (M.Map ClientIdentifier Int))
    }

new :: Authenticator auth => auth -> IO (Broker auth)
new authenticator = do
  rm <- newMVar RM.empty
  st <-newMVar BrokerState
    { brokerMaxSessionIdentifier = 0
    , brokerSubscriptions        = mempty
    , brokerSessions             = mempty
    , brokerPrincipals           = mempty
    }
  pure Broker {
      brokerAuthenticator    = authenticator
    , brokerRetainedMessages = rm
    , brokerState            = st
    }

withSession :: forall auth. (Authenticator auth) => Broker auth -> ConnectionRequest -> (ConnectionRejectReason -> IO ()) -> (Session.Session auth -> SessionPresent -> Principal auth -> IO ()) -> IO ()
withSession broker request sessionRejectHandler sessionAcceptHandler = do
  emp <- try $ authenticate (brokerAuthenticator broker) request :: IO (Either (AuthenticationException auth) (Maybe (Principal auth)))
  case emp of
    Left _ -> sessionRejectHandler ServerUnavailable
    Right mp -> case mp of
      Nothing -> sessionRejectHandler NotAuthorized
      Just principal -> bracket
        ( modifyMVar (brokerState broker) $ getSession principal (requestClientIdentifier request) )
        (\(_present, session)-> when (requestCleanSession request) (closeSession broker session) )
        (\(present, session)-> sessionAcceptHandler session present principal )

getSession :: Authenticator auth => Principal auth -> ClientIdentifier -> BrokerState auth -> IO (BrokerState auth, (SessionPresent, Session.Session auth))
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

closeSession :: Authenticator auth => Broker auth -> Session.Session auth -> IO ()
closeSession broker session =
  modifyMVar_ (brokerState broker) $ \st->
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
publishDownstream broker msg = do
  -- Log.debugM "Broker.publishDownstream" $ show msg
  when (msgRetain msg) $ do
    Log.debugM "Broker.publishDownstream" "retain"
    modifyMVar_ (brokerRetainedMessages broker) $ \rm->
      pure $! RM.insert msg rm
  let topic = msgTopic msg
  st <- readMVar (brokerState broker)
  forM_ (IS.elems $ R.lookup topic $ brokerSubscriptions st) $ \key->
    case IM.lookup (key :: Int) (brokerSessions st) of
      Nothing      ->
        putStrLn "WARNING: dead session reference"
      Just session -> Session.enqueueMessage session msg

-- | Publish a message on the broker regarding specific permissions of the session.
--
--   QUESTION: Where do qos 1 and 2 messages get acknowledged?
publishUpstream :: Authenticator auth => Broker auth -> Session.Session auth -> Message -> IO ()
publishUpstream broker session  msg = do
  isPermitted <- hasPublishPermission (brokerAuthenticator broker) (Session.sessionPrincipal session) (msgTopic msg)
  Log.debugM "Broker.publishUpstream" $ show (Session.sessionPrincipal session) ++ " publishes on "
    ++ show (msgTopic msg) ++ ": " ++ (if isPermitted then "OK" else "FORBIDDEN")
  if isPermitted
    then publishDownstream broker msg
    else pure ()

publishUpstream' :: Broker auth -> Message -> IO ()
publishUpstream'  = publishDownstream

subscribe :: Authenticator auth => Broker auth -> Session.Session auth -> PacketIdentifier -> [(Filter, QualityOfService)] -> IO ()
subscribe broker session pid filters = do
  checkedFilters <- mapM checkPermission filters
  let subscribeFilters = mapMaybe (\(filtr,mqos)-> (filtr,) . Identity <$> mqos) checkedFilters
      qosTree = R.insertFoldable subscribeFilters R.empty
      sidTree = R.map (const $ IS.singleton $ Session.sessionIdentifier session) qosTree
  -- Force the `qosTree` in order to lock the broker as little as possible.
  -- The `sidTree` is left lazy.
  qosTree `seq` do
    modifyMVarMasked_ (brokerState broker) $ \bst-> do
      modifyMVarMasked_
        ( Session.sessionSubscriptions session )
        ( pure . R.unionWith max qosTree )
      pure $ bst { brokerSubscriptions = R.unionWith IS.union (brokerSubscriptions bst) sidTree }
    Session.enqueueSubscribeAcknowledged session pid (fmap snd checkedFilters)
    rm <- readMVar (brokerRetainedMessages broker)
    -- TODO: downgrade qos
    forM_ subscribeFilters $ \(filtr,_qos)->
       Session.enqueueMessages session $ RM.lookupFilter filtr rm
  where
    checkPermission (filtr, qos) = do
      isPermitted <- hasSubscribePermission (brokerAuthenticator broker) (Session.sessionPrincipal session) filtr
      Log.debugM "Broker.subscribe" $ show (Session.sessionPrincipal session) ++ " subscribes "
        ++ show filtr ++ " with " ++ show qos ++ ": " ++ (if isPermitted then "OK" else "FORBIDDEN")
      pure (filtr, if isPermitted then Just qos else Nothing)

unsubscribe :: Broker auth -> Session.Session auth -> PacketIdentifier -> [Filter] -> IO ()
unsubscribe broker session pid filters =
  -- Force the `unsubBrokerTree` first in order to lock the broker as little as possible.
  unsubBrokerTree `seq` do
    modifyMVarMasked_ (brokerState broker) $ \bst-> do
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

getSubscriptions :: Broker auth -> IO (R.RoutingTree IS.IntSet)
getSubscriptions broker = brokerSubscriptions <$> readMVar (brokerState broker)
