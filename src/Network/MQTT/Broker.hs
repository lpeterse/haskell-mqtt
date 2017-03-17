{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE TupleSections       #-}
module Network.MQTT.Broker
  ( Broker (brokerAuthenticator)
  , newBroker
  , publishUpstream
  , publishDownstream
  , withSession
  , getUptime
  , getSessions
  , getSubscriptions
  ) where

import           Control.Concurrent.MVar
import           Control.Concurrent.PrioritySemaphore
import qualified Control.Concurrent.PrioritySemaphore  as PrioritySemaphore
import           Control.Exception
import           Control.Monad
import qualified Data.ByteString                       as BS
import qualified Data.Binary                           as B
import           Data.Functor.Identity
import           Data.Int
import qualified Data.IntMap.Strict                    as IM
import qualified Data.IntSet                           as IS
import qualified Data.Map.Strict                       as M
import           Data.Maybe
import           System.Clock
import qualified System.Log.Logger                     as Log
import           GHC.Generics                          (Generic)
import qualified Data.Sequence                         as Seq

import           Network.MQTT.Broker.Authentication    hiding (getPrincipal)
import           Network.MQTT.Broker.Authentication    (AuthenticationException,
                                                        Authenticator,
                                                        ConnectionRequest (..),
                                                        PrincipalIdentifier,
                                                        Quota (..),
                                                        authenticate,
                                                        getPrincipal,
                                                        principalPublishPermissions,
                                                        principalQuota,
                                                        principalRetainPermissions,
                                                        principalSubscribePermissions)
import qualified Network.MQTT.Broker.RetainedMessages  as RM
import qualified Network.MQTT.Broker.Session as Session
import qualified Network.MQTT.Broker.SessionStatistics as SS
import           Network.MQTT.Message                  (ClientIdentifier,
                                                        Message (..),
                                                        PacketIdentifier,
                                                        RejectReason (..),
                                                        SessionPresent (..))
import           Network.MQTT.Message
import qualified Network.MQTT.Message                  as Message
import           Network.MQTT.Message.Topic
import qualified Network.MQTT.Trie                     as R
import Network.MQTT.Broker.Internal

newBroker :: auth -> IO (Broker auth)
newBroker authenticator = do
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

withSession :: forall auth. (Authenticator auth) => Broker auth -> ConnectionRequest -> (RejectReason -> IO ()) -> (Session auth -> SessionPresent -> IO ()) -> IO ()
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
                Just (session, sessionPresent)->
                  PrioritySemaphore.exclusively (sessionSemaphore session) $ do
                    now <- sec <$> getTime Realtime
                    let connection = Connection {
                        connectionCreatedAt = now
                      , connectionCleanSession = requestCleanSession request
                      , connectionSecure = requestSecure request
                      , connectionWebSocket = isJust (requestHttp request)
                      , connectionRemoteAddress = requestRemoteAddress request
                      }
                    bracket_
                      ( putMVar (sessionConnection session) connection )
                      ( void $ takeMVar (sessionConnection session) )
                      ( sessionAcceptHandler session sessionPresent )
            )

-- | Either lookup or create a session if none is present (yet).
--
--   Principal is only looked up initially. Reconnects won't update the
--   permissions etc. Returns Nothing in case the principal identifier cannot
--   be mapped to a principal object.
getSession :: Authenticator auth => Broker auth -> (PrincipalIdentifier, ClientIdentifier) -> IO (Maybe (Session auth, SessionPresent))
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
          queue <- newMVar (emptyServerQueue $ fromIntegral $ quotaMaxInflightMessages $ principalQuota principal)
          queuePending <- newEmptyMVar
          mconnection <- newEmptyMVar
          mprincipal <- newMVar principal
          stats <- SS.new
          let newSessionIdentifier = brokerMaxSessionIdentifier st + 1
              newSession = Session
               { sessionBroker           = broker
               , sessionIdentifier       = newSessionIdentifier
               , sessionClientIdentifier = cid
               , sessionPrincipalIdentifier = pid
               , sessionCreatedAt        = now
               , sessionConnection       = mconnection
               , sessionPrincipal        = mprincipal
               , sessionSemaphore        = semaphore
               , sessionSubscriptions    = subscriptions
               , sessionQueue            = queue
               , sessionQueuePending     = queuePending
               , sessionStatistics       = stats
               }
              newBrokerState = st
               { brokerMaxSessionIdentifier = newSessionIdentifier
               , brokerSessions             = IM.insert newSessionIdentifier newSession (brokerSessions st)
               , brokerSessionsByPrincipals = M.insert pcid newSessionIdentifier (brokerSessionsByPrincipals st)
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
