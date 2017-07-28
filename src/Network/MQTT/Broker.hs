{-# LANGUAGE LambdaCase          #-}
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
  ( Broker (brokerAuthenticator)
  , newBroker
  , publishUpstream
  , publishDownstream
  , withSession
  , getUptime
  , getSessions
  , getSubscriptions
  , lookupSession
  , terminateExpiredSessions
  , terminateExpiredSessionsAt
  ) where

import           Control.Concurrent.InterruptibleLock
import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad                         (mapM_, void, when)
import           Data.Int
import qualified Data.IntMap.Strict                    as IM
import qualified Data.IntSet                           as IS
import qualified Data.Map.Strict                       as M
import           Data.Maybe
import           System.Clock
import qualified System.Log.Logger                     as Log

import           Network.MQTT.Broker.Authentication
import           Network.MQTT.Broker.Internal
import qualified Network.MQTT.Broker.RetainedMessages  as RM
import qualified Network.MQTT.Broker.Session           as Session
import qualified Network.MQTT.Broker.Session.Statistic as Session
import           Network.MQTT.Message
import qualified Network.MQTT.Trie                     as R

newBroker :: IO auth -> IO (Broker auth)
newBroker getAuthenticator = do
  now <- sec <$> getTime Realtime
  rm <- RM.new
  st <-newMVar BrokerState
    { brokerMaxSessionIdentifier = SessionIdentifier 0
    , brokerSubscriptions        = mempty
    , brokerSessions             = mempty
    , brokerSessionsByPrincipals = mempty
    }
  pure Broker {
      brokerCreatedAt     = now
    , brokerAuthenticator = getAuthenticator
    , brokerRetainedStore = rm
    , brokerState         = st
    }

withSession :: forall auth. (Authenticator auth) => Broker auth -> ConnectionRequest -> (RejectReason -> IO ()) -> (Session auth -> SessionPresent -> IO ()) -> IO ()
withSession broker request sessionRejectHandler sessionAcceptHandler = do
  authenticator <- brokerAuthenticator broker
  (try $ authenticate authenticator request :: IO (Either (AuthenticationException auth) (Maybe PrincipalIdentifier))) >>= \case
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
                  then Session.terminate session
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
                Just s  -> acceptAndServeConnection s
            )
  where
    acceptAndServeConnection :: (Session auth, SessionPresent) -> IO ()
    acceptAndServeConnection (session, sessionPresent) =
      exclusively (sessionLock session) $
        let serve = onConnect >> sessionAcceptHandler session sessionPresent >> onDisconnect Nothing
        in  serve `catch` (\e-> onDisconnect $ Just $ show (e :: SomeException) )
      where
        onConnect :: IO ()
        onConnect = do
          now <- sec <$> getTime Realtime
          modifyMVar_ (sessionConnectionState session) $ \case
            Connected {} ->
              throwIO $ AssertionFailed "Session shouldn't be marked as connected here."
            Disconnected {} ->
              pure Connected {
                  connectedAt            = now
                , connectedCleanSession  = requestCleanSession request
                , connectedSecure        = requestSecure request
                , connectedWebSocket     = isJust (requestHttp request)
                , connectedRemoteAddress = requestRemoteAddress request
                }
        onDisconnect :: Maybe String -> IO ()
        onDisconnect reason = do
          now <- sec <$> getTime Realtime
          ttl <- quotaMaxIdleSessionTTL . principalQuota <$> Session.getPrincipal session
          modifyMVar_ (sessionConnectionState session) $ const $
            pure Disconnected {
                disconnectedAt               = now
              , disconnectedSessionExpiresAt = now + fromIntegral ttl
              , disconnectedWith             = reason
              }

lookupSession :: SessionIdentifier -> Broker auth -> IO (Maybe (Session auth))
lookupSession (SessionIdentifier sid) broker =
  withMVar (brokerState broker) $ \st->
    pure $ IM.lookup sid (brokerSessions st)

terminateExpiredSessions :: Broker auth -> IO ()
terminateExpiredSessions broker = do
  now <- sec <$> getTime Realtime
  terminateExpiredSessionsAt broker now

terminateExpiredSessionsAt :: Broker auth -> Int64 -> IO ()
terminateExpiredSessionsAt broker timestamp =
  getSessions broker >>= mapM_ terminateIfExpired
  where
    terminateIfExpired :: Session auth -> IO ()
    terminateIfExpired session =
      Session.getConnectionState session >>= \case
        Connected {} -> pure ()
        Disconnected { disconnectedSessionExpiresAt = expiration } ->
          when ( timestamp >= expiration ) $ do
            Log.infoM "Broker" $ "Removing expired session " ++ show sid  ++ "."
            Session.terminate session
      where
        SessionIdentifier sid = Session.identifier session

-- | Either lookup or create a session if none is present (yet).
--
--   Principal is only looked up initially. Reconnects won't update the
--   permissions etc. Returns Nothing in case the principal identifier cannot
--   be mapped to a principal object.
getSession :: Authenticator auth => Broker auth -> (PrincipalIdentifier, ClientIdentifier) -> IO (Maybe (Session auth, SessionPresent))
getSession broker pcid@(pid, cid) = do
  authenticator <- brokerAuthenticator broker
  -- Resuming an existing session..
  -- Re-fetch the principal and its permissions.
  getPrincipal authenticator pid >>= \case
    Nothing -> pure Nothing
    Just principal ->
      modifyMVar (brokerState broker) $ \st->
        case M.lookup pcid (brokerSessionsByPrincipals st) of
          Just (SessionIdentifier sid) ->
            case IM.lookup sid (brokerSessions st) of
              Just session -> do
                void $ swapMVar (sessionPrincipal session) principal
                pure (st, Just (session, SessionPresent True))
              -- Orphaned session id. This is illegal state.
              Nothing -> do
                Log.warningM "Broker.getSession" $ "Illegal state: Found orphanded session id " ++ show sid ++ "."
                createSession principal st
          -- No session entry found for principal. Creating one.
          Nothing -> createSession principal st
    where
      createSession principal st = do
        now           <- sec <$> getTime Realtime
        lock          <- newInterruptibleLock
        subscriptions <- newMVar R.empty
        queue         <- newMVar (emptyServerQueue $ fromIntegral $ quotaMaxPacketIdentifiers $ principalQuota principal)
        queuePending  <- newEmptyMVar
        mconnection   <- newMVar $ Disconnected 0 0 mempty
        mprincipal    <- newMVar principal
        stats         <- Session.newStatistic
        let SessionIdentifier maxSessionIdentifier = brokerMaxSessionIdentifier st
            newSessionIdentifier = maxSessionIdentifier + 1
            newSession = Session
              { sessionBroker              = broker
              , sessionIdentifier          = SessionIdentifier newSessionIdentifier
              , sessionClientIdentifier    = cid
              , sessionPrincipalIdentifier = pid
              , sessionCreatedAt           = now
              , sessionConnectionState     = mconnection
              , sessionPrincipal           = mprincipal
              , sessionLock                = lock
              , sessionSubscriptions       = subscriptions
              , sessionQueue               = queue
              , sessionQueuePending        = queuePending
              , sessionStatistic           = stats
              }
            newBrokerState = st
              { brokerMaxSessionIdentifier = SessionIdentifier newSessionIdentifier
              , brokerSessions             = IM.insert newSessionIdentifier newSession (brokerSessions st)
              , brokerSessionsByPrincipals = M.insert pcid (SessionIdentifier newSessionIdentifier) (brokerSessionsByPrincipals st)
              }
        Log.infoM "Broker.createSession" $ "Creating new session with id " ++ show newSessionIdentifier ++ " for " ++ show pid ++ "."
        pure (newBrokerState, Just (newSession, SessionPresent False))

getUptime        :: Broker auth -> IO Int64
getUptime broker = do
  now <- sec <$> getTime Realtime
  pure $ now - brokerCreatedAt broker

getSessions      :: Broker auth -> IO (IM.IntMap (Session auth))
getSessions broker = brokerSessions <$> readMVar (brokerState broker)

getSubscriptions :: Broker auth -> IO (R.Trie IS.IntSet)
getSubscriptions broker = brokerSubscriptions <$> readMVar (brokerState broker)
