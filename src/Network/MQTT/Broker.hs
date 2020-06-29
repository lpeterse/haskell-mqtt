{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
  , Callbacks (..)
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
import           Control.Monad                         (void, when)
import           Data.Int
import qualified Data.IntMap.Strict                    as IM
import qualified Data.IntSet                           as IS
import qualified Data.Map.Strict                       as M
import           Data.Maybe
import           System.Clock

import           Network.MQTT.Broker.Authentication
import           Network.MQTT.Broker.Internal
import qualified Network.MQTT.Broker.RetainedMessages  as RM
import qualified Network.MQTT.Broker.Session           as Session
import qualified Network.MQTT.Broker.Session.Statistic as Session
import           Network.MQTT.Message
import qualified Network.MQTT.Trie                     as R

newBroker :: IO auth -> Callbacks auth -> IO (Broker auth)
newBroker getAuthenticator cbs = do
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
    , brokerCallbacks     = cbs
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
            ( getSession broker principalIdentifier (requestClientIdentifier request) )
            -- This is executed when the current thread terminates (on connection loss).
            -- Cleanup actions are executed here (like removing the session when the clean session flag was set).
            (\case
                Nothing -> pure ()
                Just (session, _, _) -> if requestCleanSession request
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
                Just (session, sessionPresent, maxSessionsExceeded )-> do
                  when maxSessionsExceeded $
                    terminateOldestSessionOfPrincipal broker principalIdentifier
                  acceptAndServeConnection session sessionPresent
            )
  where
    acceptAndServeConnection :: Session auth -> SessionPresent -> IO ()
    acceptAndServeConnection session sessionPresent =
      exclusively (sessionLock session) $
        let serve = onConnect' >> sessionAcceptHandler session sessionPresent >> onDisconnect' Nothing
        in  serve `catch` (\e-> onDisconnect' $ Just $ show (e :: SomeException) )
      where
        onConnect' :: IO ()
        onConnect' = do
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
                , connectedWill          = requestWill request
                }
        onDisconnect' :: Maybe String -> IO ()
        onDisconnect' reason = do
          now <- sec <$> getTime Realtime
          ttl <- quotaMaxIdleSessionTTL . principalQuota <$> Session.getPrincipal session
          case requestWill request of
              Nothing  -> pure ()
              Just msg
                | isJust reason -> Session.publish session msg
                | otherwise     -> pure ()
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
    terminateIfExpired session =
      Session.getConnectionState session >>= \case
        Connected {} -> pure ()
        Disconnected { disconnectedSessionExpiresAt = expiration } ->
          when ( timestamp >= expiration ) $ Session.terminate session

-- | Either lookup or create a session if none is present (yet).
--
--   Principal is only looked up initially. Reconnects won't update the
--   permissions etc. Returns Nothing in case the principal identifier cannot
--   be mapped to a principal object.
getSession :: Authenticator auth => Broker auth -> PrincipalIdentifier -> ClientIdentifier -> IO (Maybe (Session auth, SessionPresent, Bool))
getSession broker pid cid = do
  authenticator <- brokerAuthenticator broker
  -- Resuming an existing session..
  -- Re-fetch the principal and its permissions.
  getPrincipal authenticator pid >>= \case
    Nothing -> pure Nothing
    Just principal ->
      modifyMVar (brokerState broker) $ \st->
        case M.lookup pid (brokerSessionsByPrincipals st) of
          -- No session entry found for principal.
          -- Creating a new one.
          Nothing -> do
            (st', session) <- createSession principal st
            -- Session limit cannot be exceeded with 1 session.
            pure (st', Just (session, SessionPresent False, False))
          -- At least one session exists for this principal.
          -- Find the correct one or create a new one.
          Just cim -> case M.lookup cid cim of
            Nothing -> do
              (st', session) <- createSession principal st
              pure $ (st', Just (session, SessionPresent False, M.size cim >= quotaMaxSessions (principalQuota principal)))
            Just (SessionIdentifier sid) ->
              case IM.lookup sid (brokerSessions st) of
                Nothing -> throwIO $ AssertionFailed $
                  "Encountered orhphaned session id " ++ show sid ++
                  " for principal " ++ show pid ++" (" ++ show cid ++ ")."
                Just session -> do
                  void $ swapMVar (sessionPrincipal session) principal
                  -- Session limit cannot be exceeded when continuing an existing one.
                  pure (st, Just (session, SessionPresent True, False))
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
              , brokerSessionsByPrincipals = flip (M.insert pid) (brokerSessionsByPrincipals st) $! case M.lookup pid (brokerSessionsByPrincipals st) of
                                               Nothing  -> M.singleton cid (SessionIdentifier newSessionIdentifier)
                                               Just cim -> M.insert cid (SessionIdentifier newSessionIdentifier) cim
              }
        pure (newBrokerState, newSession)

getUptime        :: Broker auth -> IO Int64
getUptime broker = do
  now <- sec <$> getTime Realtime
  pure $ now - brokerCreatedAt broker

getSessions      :: Broker auth -> IO (IM.IntMap (Session auth))
getSessions broker = brokerSessions <$> readMVar (brokerState broker)

getSubscriptions :: Broker auth -> IO (R.Trie IS.IntSet)
getSubscriptions broker = brokerSubscriptions <$> readMVar (brokerState broker)

