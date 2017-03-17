{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker.Internal
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Broker.Internal
  ( Broker ( brokerAuthenticator )
  , new
  , publishUpstream
  , publishDownstream
  , publish
  , subscribe
  , unsubscribe
  , withSession
  , disconnectSession
  , terminateSession
  , getUptime
  , getSessions
  , getSubscriptions
  ) where

import           Control.Concurrent.MVar
import qualified Control.Concurrent.PrioritySemaphore  as PrioritySemaphore
import           Control.Exception
import           Control.Monad
import           Data.Functor.Identity
import           Data.Int
import qualified Data.IntMap.Strict                    as IM
import qualified Data.IntSet                           as IS
import qualified Data.Map.Strict                       as M
import           Data.Maybe
import           System.Clock
import qualified System.Log.Logger                     as Log

import           Network.MQTT.Broker.Authentication    (AuthenticationException,
                                                        Authenticator,
                                                        ConnectionRequest (..),
                                                        PrincipalIdentifier,
                                                        Quota (..),
                                                        authenticate,
                                                        getPrincipal,
                                                        principalPublishPermissions,
                                                        principalQuota,
                                                        principalSubscribePermissions,
                                                        principalRetainPermissions)
import qualified Network.MQTT.Broker.RetainedMessages  as RM
import qualified Network.MQTT.Broker.Session           as Session
import qualified Network.MQTT.Broker.SessionStatistics as SS
import           Network.MQTT.Message                  (ClientIdentifier,
                                                        Message (..),
                                                        PacketIdentifier,
                                                        RejectReason (..),
                                                        SessionPresent (..))
import qualified Network.MQTT.Message                  as Message
import           Network.MQTT.Message.Topic
import qualified Network.MQTT.Trie                     as R

data Broker auth
   = Broker
   { brokerCreatedAt     :: Int64
   , brokerAuthenticator :: auth
   , brokerRetainedStore :: RM.RetainedStore
   , brokerState         :: MVar (BrokerState auth)
   }

data BrokerState auth
   = BrokerState
   { brokerMaxSessionIdentifier   :: !Session.Identifier
   , brokerSubscriptions          :: !(R.Trie IS.IntSet)
   , brokerSessions               :: !(IM.IntMap (Session.Session auth))
   , brokerSessionsByPrincipals   :: !(M.Map (PrincipalIdentifier, ClientIdentifier) Int)
   }

new :: auth -> IO (Broker auth)
new authenticator = do
  now <- sec <$> getTime Realtime
  rm <- RM.new
  st <-newMVar BrokerState
    { brokerMaxSessionIdentifier = 0
    , brokerSubscriptions        = mempty
    , brokerSessions             = mempty
    , brokerSessionsByPrincipals = mempty
    }
  pure Broker {
      brokerCreatedAt     = now
    , brokerAuthenticator = authenticator
    , brokerRetainedStore = rm
    , brokerState         = st
    }

withSession :: forall auth. (Authenticator auth) => Broker auth -> ConnectionRequest -> (RejectReason -> IO ()) -> (Session.Session auth -> SessionPresent -> IO ()) -> IO ()
withSession broker request sessionRejectHandler sessionAcceptHandler =
  (try $ authenticate (brokerAuthenticator broker) request :: IO (Either (AuthenticationException auth) (Maybe PrincipalIdentifier))) >>= \case
    Left _ -> sessionRejectHandler ServerUnavailable
    Right mp -> case mp of
      Nothing -> sessionRejectHandler NotAuthorized
      Just principalIdentifier -> bracket
            -- In case the principals identity could be determined, we'll either
            -- find an associated existing session or create a new one.
            -- Getting/creating a session eventually modifies the broker state.
            ( getSession broker (principalIdentifier, requestClientIdentifier request) )
            -- This is executed when the current thread terminates (on connection loss).
            -- Cleanup actions are executed here (like removing the session when the clean session flag was set).
            (\case
                Nothing -> pure ()
                Just (session, _) -> if requestCleanSession request
                  then terminateSession broker (Session.sessionIdentifier session)
                  else Session.reset session
            )
            -- This is where the actual connection handler code is invoked.
            -- We're using a `PrioritySemaphore` here. This allows other threads for
            -- this session to terminate the current one. This is usually the case
            -- when the client loses the connection and reconnects, but we have not
            -- yet noted the dead connection. The currently running handler thread
            -- will receive a `ThreadKilled` exception.
            (\case
                Nothing -> sessionRejectHandler NotAuthorized
                Just (session, sessionPresent)->
                  PrioritySemaphore.exclusively (Session.sessionSemaphore session) $ do
                    now <- sec <$> getTime Realtime
                    let connection = Session.Connection {
                        Session.connectionCreatedAt = now
                      , Session.connectionCleanSession = requestCleanSession request
                      , Session.connectionSecure = requestSecure request
                      , Session.connectionWebSocket = isJust (requestHttp request)
                      , Session.connectionRemoteAddress = requestRemoteAddress request
                      }
                    bracket_
                      ( putMVar (Session.sessionConnection session) connection )
                      ( void $ takeMVar (Session.sessionConnection session) )
                      ( sessionAcceptHandler session sessionPresent )
            )

-- | Either lookup or create a session if none is present (yet).
--
--   Principal is only looked up initially. Reconnects won't update the
--   permissions etc. Returns Nothing in case the principal identifier cannot
--   be mapped to a principal object.
getSession :: Authenticator auth => Broker auth -> (PrincipalIdentifier, ClientIdentifier) -> IO (Maybe (Session.Session auth, SessionPresent))
getSession broker pcid@(pid, cid) =
  modifyMVar (brokerState broker) $ \st->
    case M.lookup pcid (brokerSessionsByPrincipals st) of
      Just sid ->
        case IM.lookup sid (brokerSessions st) of
          -- Resuming an existing session..
          Just session ->
            pure (st, Just (session, SessionPresent True))
          -- Orphaned session id. This is illegal state.
          Nothing -> do
            Log.warningM "Broker.getSession" $ "Illegal state: Found orphanded session id " ++ show sid ++ "."
            createSession st
      -- No session entry found for principal. Creating one.
      Nothing -> createSession st
    where
      createSession st = getPrincipal (brokerAuthenticator broker) pid >>= \case
        Nothing -> pure (st, Nothing)
        Just principal -> do
          now <- sec <$> getTime Realtime
          semaphore <- PrioritySemaphore.new
          subscriptions <- newMVar R.empty
          queue <- newMVar (Session.emptyServerQueue $ fromIntegral $ quotaMaxInflightMessages $ principalQuota principal)
          queuePending <- newEmptyMVar
          mconnection <- newEmptyMVar
          mprincipal <- newMVar principal
          stats <- SS.new
          let newSessionIdentifier = brokerMaxSessionIdentifier st + 1
              newSession = Session.Session
               { Session.sessionIdentifier       = newSessionIdentifier
               , Session.sessionClientIdentifier = cid
               , Session.sessionPrincipalIdentifier = pid
               , Session.sessionCreatedAt        = now
               , Session.sessionConnection       = mconnection
               , Session.sessionPrincipal        = mprincipal
               , Session.sessionSemaphore        = semaphore
               , Session.sessionSubscriptions    = subscriptions
               , Session.sessionQueue            = queue
               , Session.sessionQueuePending     = queuePending
               , Session.sessionStatistics       = stats
               }
              newBrokerState = st
               { brokerMaxSessionIdentifier = newSessionIdentifier
               , brokerSessions             = IM.insert newSessionIdentifier newSession (brokerSessions st)
               , brokerSessionsByPrincipals = M.insert pcid newSessionIdentifier (brokerSessionsByPrincipals st)
               }
          Log.infoM "Broker.createSession" $ "Creating new session with id " ++ show newSessionIdentifier ++ " for " ++ show pid ++ "."
          pure (newBrokerState, Just (newSession, SessionPresent False))

-- | Disconnect a session.
disconnectSession :: Broker auth -> Session.Identifier -> IO ()
disconnectSession broker sessionid =
  IM.lookup sessionid <$> getSessions broker >>= \case
    -- Session does not exist (anymore). Nothing to do.
    Nothing -> pure ()
    Just session ->
      -- This assures that the client gets disconnected. The code is executed
      -- _after_ the current client handler that has terminated.
      PrioritySemaphore.exclusively (Session.sessionSemaphore session) (pure ())

-- | Terminate a session.
--
--   * An eventually connected client gets disconnected.
--   * The session subscriptions are removed from the subscription tree
--     which means that it will receive no more messages.
--   * The session will be unlinked from the broker which means
--     that clients cannot resume it anymore under this client identifier.
terminateSession :: Broker auth -> Session.Identifier -> IO ()
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
              , brokerSessionsByPrincipals = M.delete
                  ( Session.sessionPrincipalIdentifier session
                  , Session.sessionClientIdentifier session)
                  (brokerSessionsByPrincipals st)
                -- Remove the session id from each set that the session
                -- subscription tree has a corresponding value for (which is ignored).
              , brokerSubscriptions = R.differenceWith
                  (\b _-> Just (IS.delete sessionid b) )
                  ( brokerSubscriptions st ) subscriptions
              }

-- | Inject a message downstream into the broker. It will be delivered
--   to all subscribed sessions within this broker instance.
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

-- | Publish a message upstream on the broker.
--
--   * As long as the broker is not clustered upstream=downstream.
--   * FUTURE NOTE: In clustering mode this shall distribute the message
--     to other brokers or upwards when the brokers form a hierarchy.
publishUpstream :: Broker auth -> Message -> IO ()
publishUpstream = publishDownstream

publish   :: Broker auth -> Session.Session auth -> Message.Message -> IO ()
publish broker session msg = do
  principal <- readMVar (Session.sessionPrincipal session)
  -- A topic is permitted if it yields a match in the publish permission tree.
  if R.matchTopic (Message.msgTopic msg) (principalPublishPermissions principal)
    then do
      if retain && R.matchTopic (Message.msgTopic msg) (principalRetainPermissions principal)
        then do
          RM.store msg (brokerRetainedStore broker)
          SS.accountRetentionsAccepted stats 1
        else
          SS.accountRetentionsDropped stats 1
      publishUpstream broker msg
      SS.accountPublicationsAccepted stats 1
    else
      SS.accountPublicationsDropped stats 1
  where
    stats = Session.sessionStatistics session
    Message.Retain retain = msgRetain msg

subscribe :: Broker auth -> Session.Session auth -> PacketIdentifier -> [(Filter, Message.QoS)] -> IO ()
subscribe broker session pid filters = do
  principal <- readMVar (Session.sessionPrincipal session)
  checkedFilters <- mapM (checkPermission principal) filters
  let subscribeFilters = mapMaybe (\(filtr,mqos)->(filtr,) <$> mqos) checkedFilters
      qosTree = R.insertFoldable subscribeFilters R.empty
      sidTree = R.map (const $ IS.singleton $ Session.sessionIdentifier session) qosTree
  -- Do the accounting for the session statistics.
  -- TODO: Do this as a transaction below.
  let countAccepted = length subscribeFilters
  let countDenied   = length filters - countAccepted
  SS.accountSubscriptionsAccepted (Session.sessionStatistics session) $ fromIntegral countAccepted
  SS.accountSubscriptionsDenied   (Session.sessionStatistics session) $ fromIntegral countDenied
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
    checkPermission principal (filtr, qos) = do
      let isPermitted = R.matchFilter filtr (principalSubscribePermissions principal)
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

getSubscriptions :: Broker auth -> IO (R.Trie IS.IntSet)
getSubscriptions broker = brokerSubscriptions <$> readMVar (brokerState broker)
