{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE LambdaCase          #-}
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
  , getUptime
  , getSessions
  , getSubscriptions
  ) where

import           Control.Concurrent.MVar
import qualified Control.Concurrent.PrioritySemaphore as PrioritySemaphore
import           Control.Exception
import           Control.Monad
import           Data.Functor.Identity
import           Data.Int
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
import           System.Clock
import qualified System.Log.Logger             as Log

data Broker auth
   = Broker
   { brokerCreatedAt     :: Int64
   , brokerAuthenticator :: auth
   , brokerRetainedStore :: RM.RetainedStore
   , brokerState         :: MVar (BrokerState auth)
   }

data BrokerState auth
   = BrokerState
   { brokerMaxSessionIdentifier :: !Session.Identifier
   , brokerSubscriptions        :: !(R.RoutingTree IS.IntSet)
   , brokerSessions             :: !(IM.IntMap (Session.Session auth))
   , brokerPrincipals           :: !(M.Map (Principal auth) (M.Map ClientIdentifier Int))
   }

new :: Authenticator auth => auth -> IO (Broker auth)
new authenticator = do
  now <- sec <$> getTime Realtime
  rm <- RM.new
  st <-newMVar BrokerState
    { brokerMaxSessionIdentifier = 0
    , brokerSubscriptions        = mempty
    , brokerSessions             = mempty
    , brokerPrincipals           = mempty
    }
  pure Broker {
      brokerCreatedAt     = now
    , brokerAuthenticator = authenticator
    , brokerRetainedStore = rm
    , brokerState         = st
    }

withSession :: forall auth. (Authenticator auth) => Broker auth -> ConnectionRequest -> (ConnectionRejectReason -> IO ()) -> (Session.Session auth -> SessionPresent -> Principal auth -> IO ()) -> IO ()
withSession broker request sessionRejectHandler sessionAcceptHandler = do
  (try $ authenticate (brokerAuthenticator broker) request :: IO (Either (AuthenticationException auth) (Maybe (Principal auth)))) >>= \case
    Left _ -> sessionRejectHandler ServerUnavailable
    Right mp -> case mp of
      Nothing -> sessionRejectHandler NotAuthorized
      -- In case the principals identity could be determined, we'll either
      -- find an associated existing session or create a new one.
      Just principal -> bracket
        -- Getting/creating a session eventually modifies the broker state.
        ( modifyMVar (brokerState broker) $ getSession principal (requestClientIdentifier request) )
        -- This is executed when the current thread terminates (on connection loss).
        -- Cleanup actions are executed here (like removing the session when the clean session flag was set).
        (\(session, _)-> if requestCleanSession request
            then terminateSession broker (Session.sessionIdentifier session)
            else Session.reset session
        )
        -- This is where the actual connection handler code is invoked.
        -- We're using a `PrioritySemaphore` here. This allows other threads for
        -- this session to terminate the current one. This is usually the case
        -- when the client loses the connection and reconnects, but we have not
        -- yet noted the dead connection. The currently running handler thread
        -- will receive a `ThreadKilled` exception.
        (\(session, sessionPresent)-> PrioritySemaphore.exclusively (Session.sessionSemaphore session) $
          sessionAcceptHandler session sessionPresent principal
        )

getSession :: Authenticator auth => Principal auth -> ClientIdentifier -> BrokerState auth -> IO (BrokerState auth, (Session.Session auth, SessionPresent))
getSession principal cid st =
  case M.lookup principal (brokerPrincipals st) of
    Just mcis -> case M.lookup cid mcis of
      Just sid ->
        case IM.lookup sid (brokerSessions st) of
          -- Resuming an existing session..
          Just session -> do
            pure (st, (session, True))
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
      semaphore <- PrioritySemaphore.new
      subscriptions <- newMVar R.empty
      queue <- newMVar (Session.emptyServerQueue 1000)
      queuePending <- newEmptyMVar
      let newSessionIdentifier = brokerMaxSessionIdentifier st + 1
          newSession = Session.Session
           { Session.sessionIdentifier       = newSessionIdentifier
           , Session.sessionClientIdentifier = cid
           , Session.sessionPrincipal        = principal
           , Session.sessionSemaphore        = semaphore
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
      pure (newBrokerState, (newSession, False))

-- | Terminate a session.
--
--   * An eventually connected client gets disconnected.
--   * The session subscriptions are removed from the subscription tree
--     which means that it will receive no more messages.
--   * The session will be unlinked from the broker which means
--     that clients cannot resume it anymore under this client identifier.
terminateSession :: Authenticator auth => Broker auth -> Session.Identifier -> IO ()
terminateSession broker sessionid =
  IM.lookup sessionid <$> getSessions broker >>= \case
    -- Session does not exist (anymore). Nothing to do.
    Nothing -> pure ()
    Just session ->
      -- This assures that the client gets disconnected. The code is executed
      -- _after_ the current client handler that has terminated.
      -- TODO Race: New clients may try to connect while we are in here.
      -- This would not make the state inconsistent, but kill this thread.
      -- What we need is another `exclusivelyUninterruptible` function for
      -- the priority semaphore.
      PrioritySemaphore.exclusively (Session.sessionSemaphore session) $
        modifyMVarMasked_ (brokerState broker) $ \st->
          withMVarMasked (Session.sessionSubscriptions session) $ \subscriptions->
            pure st
              { brokerSessions = IM.delete sessionid (brokerSessions st)
                -- Remove the session id from the (principal, client) -> sessionid
                -- mapping. Remove empty leaves in this mapping, too.
              , brokerPrincipals = M.update
                  (\mcid-> let mcid' = M.delete (Session.sessionClientIdentifier session) mcid
                           in if M.null mcid' then Nothing else Just mcid')
                  ( Session.sessionPrincipal session ) ( brokerPrincipals st )
                -- Remove the session id from each set that the session
                -- subscription tree has a corresponding value for (which is ignored).
              , brokerSubscriptions = R.differenceWith
                  (\b _-> Just (IS.delete sessionid b) )
                  ( brokerSubscriptions st ) subscriptions
              }

publishDownstream :: Broker auth -> Message -> IO ()
publishDownstream broker msg = do
  RM.store msg (brokerRetainedStore broker)
  let topic = msgTopic msg
  st <- readMVar (brokerState broker)
  forM_ (IS.elems $ R.lookup topic $ brokerSubscriptions st) $ \key->
    case IM.lookup (key :: Int) (brokerSessions st) of
      Nothing      ->
        putStrLn "WARNING: dead session reference"
      Just session -> Session.publishMessage session msg

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
  let subscribeFilters = mapMaybe (\(filtr,mqos)->(filtr,) <$> mqos) checkedFilters
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
    forM_ checkedFilters $ \(filtr,_qos)->
       Session.publishMessages session =<< RM.retrieve filtr (brokerRetainedStore broker)
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

getUptime        :: Broker auth -> IO Int64
getUptime broker = do
  now <- sec <$> getTime Realtime
  pure $ now - brokerCreatedAt broker

getSessions      :: Broker auth -> IO (IM.IntMap (Session.Session auth))
getSessions broker = brokerSessions <$> readMVar (brokerState broker)

getSubscriptions :: Broker auth -> IO (R.RoutingTree IS.IntSet)
getSubscriptions broker = brokerSubscriptions <$> readMVar (brokerState broker)
