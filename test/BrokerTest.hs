{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
module BrokerTest ( getTestTree ) where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import           Data.Default.Class
import qualified Data.IntMap.Strict                 as IM
import           Data.Maybe
import           Data.Monoid
import qualified Data.Sequence                      as Seq
import           Data.String
import           Data.Typeable
import           Data.UUID                          (UUID)
import           System.Clock
import           Test.Tasty
import           Test.Tasty.HUnit

import qualified Network.MQTT.Broker                as Broker
import           Network.MQTT.Broker.Authentication
import qualified Network.MQTT.Broker.Session        as Session
import           Network.MQTT.Message
import qualified Network.MQTT.Message               as Message
import qualified Network.MQTT.Trie                  as R

newtype TestAuthenticator = TestAuthenticator (AuthenticatorConfig TestAuthenticator)

instance Authenticator TestAuthenticator where
  data AuthenticatorConfig TestAuthenticator = TestAuthenticatorConfig
    { cfgAuthenticate           :: ConnectionRequest -> IO (Maybe UUID)
    , cfgGetPrincipal           :: UUID -> IO (Maybe Principal)
    }
  data AuthenticationException TestAuthenticator = TestAuthenticatorException deriving (Typeable, Show)
  newAuthenticator = pure . TestAuthenticator
  authenticate (TestAuthenticator cfg) = cfgAuthenticate cfg
  getPrincipal (TestAuthenticator cfg) = cfgGetPrincipal cfg
  getLastException = const $ pure Nothing

instance Exception (AuthenticationException TestAuthenticator)

getTestTree :: IO TestTree
getTestTree =
  pure $ testGroup "Broker"
    [ testGroup "Authentication"
      [ testCase "Reject with 'ServerUnavaible' when authentication throws exception" $ do
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigNoService) def
          let sessionRejectHandler                 = putMVar m1
              sessionAcceptHandler session present = putMVar m2 (session, present)
          Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
          tryReadMVar m1 >>= \x-> Just ServerUnavailable @?= x
          tryReadMVar m2 >>= \x-> Nothing                @?= x
      , testCase "Reject 'NotAuthorized' when authentication returned Nothing" $ do
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigNoAccess) def
          let sessionRejectHandler                 = putMVar m1
              sessionAcceptHandler session present = putMVar m2 (session, present)
          Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
          tryReadMVar m1 >>= \x-> Just NotAuthorized   @?= x
          tryReadMVar m2 >>= \x-> Nothing              @?= x
      ]
    , testGroup "Subscriptions"

      [ testCase "subscribe the same filter from 2 different sessions" $ do
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              req2 = connectionRequest { requestClientIdentifier = "2" }
              msg  = Message.Message "a/b" QoS0 (Retain False) ""
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _ ->
            Broker.withSession broker req2 (const $ pure ()) $ \session2 _ -> do
              Session.process session1 (ClientSubscribe (PacketIdentifier 42) [("a/b", QoS0)])
              Session.process session2 (ClientSubscribe (PacketIdentifier 47) [("a/b", QoS0)])
              Broker.publishDownstream broker msg
              queue1 <- (<>) <$> Session.poll session1 <*> Session.poll session1
              queue2 <- (<>) <$> Session.poll session2 <*> Session.poll session2
              queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged (PacketIdentifier 42) [Just QoS0], ServerPublish (PacketIdentifier (-1)) (Duplicate False) msg]
              queue2 @?= Seq.fromList [ ServerSubscribeAcknowledged (PacketIdentifier 47) [Just QoS0], ServerPublish (PacketIdentifier (-1)) (Duplicate False) msg]

      , testCase "get retained message on subscription (newer overrides older, issue #6)" $ do
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              msg1 = Message.Message "topic" QoS0 (Retain True) "test"
              msg2 = Message.Message "topic" QoS0 (Retain True) "toast"
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _-> do
            Broker.publishDownstream broker msg1
            Broker.publishDownstream broker msg2
            Session.process session1 (ClientSubscribe (PacketIdentifier 23) [("topic", QoS0)])
            queue1 <- (<>) <$> Session.poll session1 <*> Session.poll session1
            queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged (PacketIdentifier 23) [Just QoS0], ServerPublish (PacketIdentifier (-1)) (Duplicate False) msg2]

      , testCase "delete retained message when body is empty" $ do
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              msg1 = Message.Message "topic" QoS0 (Retain True) "test"
              msg2 = Message.Message "topic" QoS0 (Retain True) ""
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _ -> do
            Broker.publishDownstream broker msg1
            Broker.publishDownstream broker msg2
            Session.process session1 (ClientSubscribe (PacketIdentifier 23) [("topic", QoS0)])
            queue1 <- (<>) <$> Session.poll session1 <*> Session.poll session1
            queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged (PacketIdentifier 23) [Just QoS0] ]
      ]

    , testGroup "Last Will"

      [ testCase "Will shall be published on exceptional disconnect" $ do
          mvar0 <- newEmptyMVar
          mvar1 <- newEmptyMVar
          mvar2 <- newEmptyMVar
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          let msg0 = Message.Message "topic" QoS0 (Retain False) "test"
              req0 = connectionRequest { requestClientIdentifier = "0", requestWill = Nothing }
              req1 = connectionRequest { requestClientIdentifier = "1", requestWill = Just msg0 }
              ses0 = Broker.withSession broker req0 (const $ pure ()) $ \s _ -> do
                Session.process s (ClientSubscribe (PacketIdentifier 47) [("topic", QoS0)])
                void $ Session.poll s
                putMVar mvar0 ()
                void $ takeMVar mvar2
                putMVar mvar1 =<< Session.poll s
              ses1 = Broker.withSession broker req1 (const $ pure ()) $ \_ _ -> do
                void $ takeMVar mvar0
                error "close connection with error"
          withAsync ses0 $ \_-> withAsync (ses1 >> putMVar mvar2 ()) $ \_-> do
            queue <- takeMVar mvar1
            assertEqual "Expect queue to contain other connection's will."
              (Seq.singleton $ ServerPublish (PacketIdentifier (-1)) (Duplicate False) msg0) queue

      , testCase "Will shall not be published on regular client disconnect" $ do
          mvar0 <- newEmptyMVar
          mvar1 <- newEmptyMVar
          mvar2 <- newEmptyMVar
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          let msg0 = Message.Message "topic" QoS0 (Retain False) "test"
              req0 = connectionRequest { requestClientIdentifier = "0", requestWill = Nothing }
              req1 = connectionRequest { requestClientIdentifier = "1", requestWill = Just msg0 }
              ses0 = Broker.withSession broker req0 (const $ pure ()) $ \s _ -> do
                Session.process s (ClientSubscribe (PacketIdentifier 47) [("topic", QoS0)])
                void $ Session.poll s
                putMVar mvar0 ()
                void $ takeMVar mvar2
                putMVar mvar1 =<< Session.poll s
              ses1 = Broker.withSession broker req1 (const $ pure ()) $ \_ _ -> do
                void $ takeMVar mvar0
                pure ()
          withAsync ses0 $ \_-> withAsync (ses1 >> putMVar mvar2 ()) $ \_-> do
            queue <- takeMVar mvar1
            assertEqual "Expect queue to be empty." mempty queue
      ]

    , testGroup "Queue overflow handling"

      [ testCase "Barrel shift on overflowing QoS0 queue" $ do
          let msgs = [ Message.Message "topic" QoS0 (Retain False) (fromString $ show x) | x <- [(1 :: Int)..] ]
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          t1 <- newEmptyMVar
          t2 <- newEmptyMVar
          t3 <- newEmptyMVar
          t4 <- newEmptyMVar
          t5 <- newEmptyMVar
          let h session _ = do {
              Session.process session (ClientSubscribe (PacketIdentifier 0) [("topic", QoS0)]);
              putMVar t1 ();
              takeMVar t2;
              void $ Session.poll session; -- subscribe acknowledge
              putMVar t3 =<< Session.poll session;
              takeMVar t4;
              putMVar t5 =<< Session.poll session;
            }
          let w = Broker.withSession broker connectionRequest (const $ pure ()) h
          withAsync w $ \_-> do
            takeMVar t1
            forM_ (take 10 msgs) $ Broker.publishDownstream broker
            putMVar t2 ()
            p <- takeMVar t3
            assertEqual "Expect 10 packets being polld (first time)."
              (Seq.fromList $ fmap (ServerPublish (PacketIdentifier (-1)) (Duplicate False)) $ take 10 msgs) p
            forM_ (take 11 msgs) $ Broker.publishDownstream broker
            putMVar t4 ()
            q <- takeMVar t5
            assertEqual "Expect 10 packets being polld (second time)."
              (Seq.fromList $ fmap (ServerPublish (PacketIdentifier (-1)) (Duplicate False)) $ take 10 $ drop 1 msgs) q

      {-, testCase "Terminate session on overflowing QoS1 queue" $ do
          let msgs = [ Message.Message "topic" QoS1 (Retain False) (fromString $ show x) | x <- [(1 :: Int)..] ]
          broker <- Broker.newBroker $ TestAuthenticator authenticatorConfigAllAccess
          t1 <- newEmptyMVar
          t2 <- newEmptyMVar
          t3 <- newEmptyMVar
          t4 <- newEmptyMVar
          t5 <- newEmptyMVar
          let h session _ = do {
              Session.process session (ClientSubscribe (PacketIdentifier 0) [("topic", QoS1)]);
              putMVar t1 ();
              takeMVar t2;
              putMVar t3 =<< (Seq.drop 1 <$> Session.poll session); -- cut off subscribe acknowledge
              void $ takeMVar t4;
              putMVar t5 =<< Session.poll session;
            }
          let w = Broker.withSession broker connectionRequest (const $ pure ()) h
          withAsync w $ \as-> do
            takeMVar t1
            forM_ (take 10 msgs) $ Broker.publishDownstream broker
            putMVar t2 ()
            p <- takeMVar t3
            assertEqual "Expect 10 packets being polld (first time)."
              (Seq.fromList $ zipWith (\i m-> ServerPublish (PacketIdentifier i) (Duplicate False) m) [0..] $ take 10 msgs) p
            forM_ (take 11 msgs) $ Broker.publishDownstream broker
            assertEqual "Expect session handler thread to be killed." "Left thread killed" =<< (show <$> waitCatch as) -}

      {-, testCase "overflowing Qos2 queue (session termination)" $ do
          let msgs = [ Message.Message "topic" QoS2 (Retain False) (fromString $ show x) | x <- [(1 :: Int)..] ]
          broker <- Broker.newBroker $ TestAuthenticator authenticatorConfigAllAccess
          t1 <- newEmptyMVar
          t2 <- newEmptyMVar
          t3 <- newEmptyMVar
          t4 <- newEmptyMVar
          t5 <- newEmptyMVar
          let h session _ = do {
              Session.process session (ClientSubscribe (PacketIdentifier 0) [("topic", QoS2)]);
              putMVar t1 ();
              takeMVar t2;
              putMVar t3 =<< (Seq.drop 1 <$> Session.poll session); -- cut off subscribe acknowledge
              void $ takeMVar t4;
              putMVar t5 =<< Session.poll session;
            }
          let w = Broker.withSession broker connectionRequest (const $ pure ()) h
          withAsync w $ \as-> do
            takeMVar t1
            forM_ (take 10 msgs) $ Broker.publishDownstream broker
            putMVar t2 ()
            p <- takeMVar t3
            assertEqual "Expect 10 packets being polld (first time)."
              (Seq.fromList $ zipWith (\i m-> ServerPublish (PacketIdentifier i) (Duplicate False) m) [0..] $ take 10 msgs) p
            forM_ (take 11 msgs) $ Broker.publishDownstream broker
            assertEqual "Expect session handler thread to be killed." "Left thread killed" =<< (show <$> waitCatch as) -}
      ]

    , testGroup "Quality of Service"

      [ testCase "transmit a QoS1 message and process acknowledgement" $ do
          let msg = Message.Message "topic" QoS1 (Retain False) "payload"
              pid = PacketIdentifier 0
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          Broker.withSession broker connectionRequest (const $ pure ()) $ \session _-> do
            pids1 <- Session.getFreePacketIdentifiers session
            Session.enqueue session msg
            queue <- Session.poll session
            pids2 <- Session.getFreePacketIdentifiers session
            Session.process session (ClientPublishAcknowledged pid)
            pids3 <- Session.getFreePacketIdentifiers session
            assertEqual "One packet identifier shall be in use after `poll`." (Seq.drop 1 pids1) pids2
            assertEqual "The packet identifier shall have been returned after the message has been acknowledged." pids1 pids3
            assertEqual "The packet is expected in the output queue." (Seq.fromList [ ServerPublish pid (Duplicate False) msg ]) queue

      , testCase "receive a QoS1 message and send acknowledgement" $ do
          let msg = Message.Message "topic" QoS1 (Retain False) "payload"
              pid = PacketIdentifier 0
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          Broker.withSession broker connectionRequest (const $ pure ()) $ \session _-> do
            Session.process session (ClientSubscribe pid [("topic", QoS0)])
            queue1 <- Session.poll session
            assertEqual "A subscribe acknowledgement shall be in the output queue." (Seq.fromList [ ServerSubscribeAcknowledged pid [Just QoS0] ]) queue1
            Session.process session (ClientPublish pid (Duplicate False) msg)
            queue2 <- Session.poll session
            assertEqual "A publish acknowledgment and the (downgraded) message itself shall be in the output queue." (Seq.fromList [ ServerPublishAcknowledged pid ]) queue2
            queue3 <- Session.poll session
            assertEqual "The downgraded message queue shall be in the output queue." (Seq.fromList [ ServerPublish (PacketIdentifier (-1)) (Duplicate False) msg { msgQoS = QoS0} ]) queue3

      , testCase "transmit a QoS1 message and retransmit after connection failure" $ do
          let req = connectionRequest { requestCleanSession = False }
              msg = Message.Message "topic" QoS1 (Retain False) "payload"
              pid = PacketIdentifier 0
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          Broker.withSession broker req (const $ pure ()) $ \session present-> do
            assertEqual "The session shall not be present." (SessionPresent False) present
            Session.enqueue session msg
            queue <- Session.poll session
            assertEqual "The message shall be in the output queue." (Seq.fromList [ ServerPublish pid (Duplicate False) msg]) queue
          Broker.withSession broker req (const $ pure ()) $ \session present-> do
            assertEqual "The session shall be present." (SessionPresent True) present
            queue <- Session.poll session
            assertEqual "The message shall again be in the output queue, and must not be marked duplicate." (Seq.fromList [ ServerPublish pid (Duplicate True) msg ]) queue

      , testCase "transmit a QoS2 message and process confirmations" $ do
          let msg = Message.Message "topic" QoS2 (Retain False) "payload"
              pid = PacketIdentifier 0
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          Broker.withSession broker connectionRequest (const $ pure ()) $ \session _-> do
            pids1 <- Session.getFreePacketIdentifiers session
            Session.enqueue session msg
            queue2 <- Session.poll session
            pids2 <- Session.getFreePacketIdentifiers session
            assertEqual "One packet identifier shall be in use after `poll`." (Seq.drop 1 pids1) pids2
            assertEqual "A PUB packet is expected in the output queue." (Seq.fromList [ ServerPublish pid (Duplicate False) msg ]) queue2
            Session.process session (ClientPublishReceived pid)
            queue3 <- Session.poll session
            pids3 <- Session.getFreePacketIdentifiers session
            assertEqual "The packet identifier shall still be in use." (Seq.drop 1 pids1) pids3
            assertEqual "A PUBREL packet is expected in the output queue." (Seq.fromList [ ServerPublishRelease pid ]) queue3
            Session.process session (ClientPublishComplete pid)
            pids4 <- Session.getFreePacketIdentifiers session
            assertEqual "The packet identifier shall have been returned after the transaction has been completed." pids1 pids4

      , testCase "transmit a QoS2 message and handle retransmissions on connection failure" $ do
          let req = connectionRequest { requestCleanSession = False }
              msg = Message.Message "topic" QoS2 (Retain False) "payload"
              pid = PacketIdentifier 0
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          Broker.withSession broker req (const $ pure ()) $ \session _-> do
            Session.enqueue session msg
            queue <- Session.poll session
            assertEqual "The message shall be in the output queue." (Seq.fromList [ ServerPublish pid (Duplicate False) msg ]) queue
          Broker.withSession broker req (const $ pure ()) $ \session _-> do
            queue <- Session.poll session
            assertEqual "The message shall again be in the output queue and must be marked duplicate." (Seq.fromList [ ServerPublish pid (Duplicate True) msg ]) queue
            Session.process session (ClientPublishReceived pid)
            queue' <- Session.poll session
            assertEqual "The release command shall be in the output queue." (Seq.fromList [ ServerPublishRelease pid ]) queue'
          Broker.withSession broker req (const $ pure ()) $ \session _-> do
            queue <- Session.poll session
            assertEqual "The release command shall be in the output queue (again)." (Seq.fromList [ ServerPublishRelease pid ]) queue
            Session.process session (ClientPublishComplete pid)
          Broker.withSession broker req (const $ pure ()) $ \session _-> do
            queue <- Session.poll session
            assertEqual "The output queue shall be empty." mempty queue
      ]

    , testGroup "Session Expiration" [

        testCase "create session and trigger removal of expired sessions" $ do
          let req = connectionRequest { requestCleanSession = False }
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          Broker.withSession broker req (\_-> pure ()) (\_ _-> pure ())
          sessions <- Broker.getSessions broker
          case IM.lookup 1 sessions of
            Nothing      -> assertFailure "expected sessions with id 1"
            Just session -> Session.getConnectionState session >>= \case
              Session.Connected {} -> assertFailure "expected session to be disconnected"
              Session.Disconnected {} -> do
                now <- sec <$> getTime Realtime
                Broker.terminateExpiredSessionsAt broker now
                assertEqual "size of session set right now" 1 =<< (IM.size <$> Broker.getSessions broker)
                Broker.terminateExpiredSessionsAt broker (now + 59)
                assertEqual "size of session set 59 seconds in the future" 1 =<< (IM.size <$> Broker.getSessions broker)
                Broker.terminateExpiredSessionsAt broker (now + 60)
                assertEqual "size of session set 60 seconds in the future" 0 =<< (IM.size <$> Broker.getSessions broker)
      ]

    , testGroup "Session Concurrency" [
        testCase "the same principal with the same client identifier shall take over the existing session" $ do
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          let t1 = Broker.withSession broker (connectionRequest { requestClientIdentifier = "a" }) (\_-> pure ()) $ \session (SessionPresent sessionPresent)->
                     if sessionPresent
                        then putMVar m1 Nothing
                        else (threadDelay 1000000 >> putMVar m1 Nothing) `onException` putMVar m1 (Just $ Session.identifier session) -- expect to be killed externally before timeout
          let t2 = Broker.withSession broker (connectionRequest { requestClientIdentifier = "a" }) (\_-> pure ()) $ \session (SessionPresent sessionPresent)->
                     if sessionPresent
                        then putMVar m2 (Just $ Session.identifier session) -- expect to exit regularly
                        else putMVar m2 Nothing
          withAsync t1 $ const $ withAsync (threadDelay 10000 >> t2) $ const $ do
            ms1 <- readMVar m1
            ms2 <- readMVar m2
            assertBool  "isJust ms1" $ isJust ms1
            assertBool  "isJust ms2" $ isJust ms2
            assertBool  "ms1 == ms2" $ ms1 == ms2

      , testCase "the same principal with different client idenfitifier shall get an additional session" $ do
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          let t1 = Broker.withSession broker (connectionRequest { requestClientIdentifier = "a" }) (\_-> pure ()) $ \session (SessionPresent sessionPresent)->
                    if sessionPresent
                      then putMVar m1 Nothing
                      else (threadDelay 20000 >> putMVar m1 (Just $ Session.identifier session)) `onException` putMVar m1 Nothing
          let t2 = Broker.withSession broker (connectionRequest { requestClientIdentifier = "b" }) (\_-> pure ()) $ \session (SessionPresent sessionPresent)->
                    if sessionPresent
                      then putMVar m2 Nothing
                      else (threadDelay 20000 >> putMVar m2 (Just $ Session.identifier session)) `onException` putMVar m2 Nothing
          withAsync t1 $ const $ withAsync (threadDelay 10000 >> t2) $ const $ do
            ms1 <- readMVar m1
            ms2 <- readMVar m2
            assertBool "isJust ms1" $ isJust ms1
            assertBool "isJust ms2" $ isJust ms2
            assertBool "ms1 /= ms2" $ ms1 /= ms2

      , testCase "a principals oldest session shall be terminated when reaching `quotaMaxSessions`" $ do
          broker <- Broker.newBroker (pure $ TestAuthenticator authenticatorConfigAllAccess) def
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          m3 <- newEmptyMVar
          let t1 = Broker.withSession broker (connectionRequest { requestClientIdentifier = "a" }) (\_-> pure ()) $ \session (SessionPresent sessionPresent)->
                    if sessionPresent
                      then putMVar m1 Nothing
                      else (threadDelay 3000000 >> putMVar m1 Nothing) `onException` putMVar m1 (Just $ Session.identifier session) -- expect to be killed externally before timeout
          let t2 = Broker.withSession broker (connectionRequest { requestClientIdentifier = "b" }) (\_-> pure ()) $ \session (SessionPresent sessionPresent)->
                    if sessionPresent
                      then putMVar m2 Nothing
                      else (threadDelay 3000000 >> putMVar m2 (Just $ Session.identifier session)) `onException` putMVar m2 Nothing
          let t3 = Broker.withSession broker (connectionRequest { requestClientIdentifier = "c" }) (\_-> pure ()) $ \session (SessionPresent sessionPresent)->
                    if sessionPresent
                      then putMVar m3 Nothing
                      else putMVar m3 (Just $ Session.identifier session)
          withAsync t1 $ const $ withAsync (threadDelay 1100000 >> t2) $ const $ withAsync (threadDelay 2200000 >> t3) $ const $ do
            ms1 <- readMVar m1
            ms2 <- readMVar m2
            ms3 <- readMVar m3
            assertBool "isJust ms1" $ isJust ms1
            assertBool "isJust ms2" $ isJust ms2
            assertBool "isJust ms3" $ isJust ms3
            assertBool "ms1 /= ms2" $ ms1 /= ms2
            assertBool "ms1 /= ms3" $ ms1 /= ms3
            assertBool "ms2 /= ms3" $ ms2 /= ms3
      ]
    ]

authenticatorConfigNoService :: AuthenticatorConfig TestAuthenticator
authenticatorConfigNoService = TestAuthenticatorConfig
  { cfgAuthenticate           = const $ throwIO TestAuthenticatorException
  , cfgGetPrincipal           = const $ throwIO TestAuthenticatorException
  }

authenticatorConfigNoAccess :: AuthenticatorConfig TestAuthenticator
authenticatorConfigNoAccess  = TestAuthenticatorConfig
  { cfgAuthenticate           = const (pure Nothing)
  , cfgGetPrincipal           = const (pure Nothing)
  }

authenticatorConfigAllAccess :: AuthenticatorConfig TestAuthenticator
authenticatorConfigAllAccess = TestAuthenticatorConfig
  { cfgAuthenticate           = const $ pure (Just uuid)
  , cfgGetPrincipal           = const $ pure (Just pcpl)
  }
  where
    uuid = read "3c7efc50-bff0-4e09-9a9b-0f2bff2db8fc"
    pcpl = Principal {
       principalUsername = Nothing
     , principalQuota = quota
     , principalPublishPermissions = R.singleton "#" ()
     , principalSubscribePermissions = R.singleton "#" ()
     , principalRetainPermissions = R.singleton "#" ()
     }
    quota = Quota {
       quotaMaxSessions          = 2
     , quotaMaxIdleSessionTTL    = 60
     , quotaMaxPacketSize        = 65535
     , quotaMaxPacketIdentifiers = 10
     , quotaMaxQueueSizeQoS0     = 10
     , quotaMaxQueueSizeQoS1     = 10
     , quotaMaxQueueSizeQoS2     = 10
     }

connectionRequest :: ConnectionRequest
connectionRequest  = ConnectionRequest
  { requestClientIdentifier = "mqtt-default"
  , requestCleanSession     = True
  , requestSecure           = False
  , requestCredentials      = Nothing
  , requestHttp             = Nothing
  , requestCertificateChain = Nothing
  , requestRemoteAddress    = Nothing
  , requestWill             = Nothing
  }

