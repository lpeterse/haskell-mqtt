{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
module BrokerTest ( getTestTree ) where

import           Control.Concurrent.MVar
import           Control.Exception
import           Data.Monoid
import qualified Data.Sequence               as Seq
import           Data.Typeable
import           Data.UUID                   (UUID)

import           Network.MQTT.Authentication
import qualified Network.MQTT.Broker         as Broker
import           Network.MQTT.Message
import qualified Network.MQTT.Message        as Message
import qualified Network.MQTT.RoutingTree    as R
import qualified Network.MQTT.Session        as Session

import           Test.Tasty
import           Test.Tasty.HUnit

newtype TestAuthenticator = TestAuthenticator (AuthenticatorConfig TestAuthenticator)

instance Authenticator TestAuthenticator where
  data AuthenticatorConfig TestAuthenticator = TestAuthenticatorConfig
    { cfgAuthenticate           :: ConnectionRequest -> IO (Maybe UUID)
    , cfgGetPrincipal           :: UUID -> IO (Maybe Principal)
    }
  data AuthenticationException TestAuthenticator = TestAuthenticatorException deriving (Typeable, Show)
  newAuthenticator = pure . TestAuthenticator
  authenticate (TestAuthenticator cfg) req = cfgAuthenticate cfg req
  getPrincipal (TestAuthenticator cfg) uuid = cfgGetPrincipal cfg uuid

instance Exception (AuthenticationException TestAuthenticator)

getTestTree :: IO TestTree
getTestTree =
  pure $ testGroup "Broker"
    [ testGroup "Authentication"
      [ testCase "Reject with 'ServerUnavaible' when authentication throws exception" $ do
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          broker <- Broker.new $ TestAuthenticator authenticatorConfigNoService
          let sessionRejectHandler                 = putMVar m1
              sessionAcceptHandler session present = putMVar m2 (session, present)
          Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
          tryReadMVar m1 >>= \x-> Just ServerUnavailable @?= x
          tryReadMVar m2 >>= \x-> Nothing                @?= x
      , testCase "Reject 'NotAuthorized' when authentication returned Nothing" $ do
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          broker <- Broker.new $ TestAuthenticator authenticatorConfigNoAccess
          let sessionRejectHandler                 = putMVar m1
              sessionAcceptHandler session present = putMVar m2 (session, present)
          Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
          tryReadMVar m1 >>= \x-> Just NotAuthorized   @?= x
          tryReadMVar m2 >>= \x-> Nothing              @?= x
      ]
    , testGroup "Subscriptions"

      [ testCase "subscribe the same filter from 2 different sessions" $ do
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              req2 = connectionRequest { requestClientIdentifier = "2" }
              msg  = Message.Message "a/b" QoS0 (Retain False) ""
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _ ->
            Broker.withSession broker req2 (const $ pure ()) $ \session2 _ -> do
              Broker.subscribe broker session1 (PacketIdentifier 42) [("a/b", QoS0)]
              Broker.subscribe broker session2 (PacketIdentifier 47) [("a/b", QoS0)]
              Broker.publishDownstream broker msg
              queue1 <- (<>) <$> Session.dequeue session1 <*> Session.dequeue session1
              queue2 <- (<>) <$> Session.dequeue session2 <*> Session.dequeue session2
              queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged (PacketIdentifier 42) [Just QoS0], ServerPublish (PacketIdentifier (-1)) (Duplicate False) msg]
              queue2 @?= Seq.fromList [ ServerSubscribeAcknowledged (PacketIdentifier 47) [Just QoS0], ServerPublish (PacketIdentifier (-1)) (Duplicate False) msg]

      , testCase "get retained message on subscription (newer overrides older, issue #6)" $ do
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              msg1 = Message.Message "topic" QoS0 (Retain True) "test"
              msg2 = Message.Message "topic" QoS0 (Retain True) "toast"
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _-> do
            Broker.publishDownstream broker msg1
            Broker.publishDownstream broker msg2
            Broker.subscribe broker session1 (PacketIdentifier 23) [("topic", QoS0)]
            queue1 <- (<>) <$> Session.dequeue session1 <*> Session.dequeue session1
            queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged (PacketIdentifier 23) [Just QoS0], ServerPublish (PacketIdentifier (-1)) (Duplicate False) msg2]

      , testCase "delete retained message when body is empty" $ do
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              msg1 = Message.Message "topic" QoS0 (Retain True) "test"
              msg2 = Message.Message "topic" QoS0 (Retain True) ""
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _ -> do
            Broker.publishDownstream broker msg1
            Broker.publishDownstream broker msg2
            Broker.subscribe broker session1 (PacketIdentifier 23) [("topic", QoS0)]
            queue1 <- (<>) <$> Session.dequeue session1 <*> Session.dequeue session1
            queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged (PacketIdentifier 23) [Just QoS0] ]
      ]
    , testGroup "Quality of Service"

      [ testCase "transmit a QoS1 message and process acknowledgement" $ do
          let msg = Message.Message "topic" QoS1 (Retain False) "payload"
              pid = PacketIdentifier 0
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          Broker.withSession broker connectionRequest (const $ pure ()) $ \session _-> do
            pids1 <- Session.getFreePacketIdentifiers session
            Session.enqueueMessage session msg
            queue <- Session.dequeue session
            pids2 <- Session.getFreePacketIdentifiers session
            Session.processPublishAcknowledged session pid
            pids3 <- Session.getFreePacketIdentifiers session
            assertEqual "One packet identifier shall be in use after `dequeue`." (Seq.drop 1 pids1) pids2
            assertEqual "The packet identifier shall have been returned after the message has been acknowledged." pids1 pids3
            assertEqual "The packet is expected in the output queue." (Seq.fromList [ ServerPublish pid (Duplicate False) msg ]) queue

      , testCase "receive a QoS1 message and send acknowledgement" $ do
          let msg = Message.Message "topic" QoS1 (Retain False) "payload"
              pid = PacketIdentifier 0
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          Broker.withSession broker connectionRequest (const $ pure ()) $ \session _-> do
            Broker.subscribe broker session pid [("topic", QoS0)]
            queue1 <- Session.dequeue session
            assertEqual "A subscribe acknowledgement shall be in the output queue." (Seq.fromList [ ServerSubscribeAcknowledged pid [Just QoS0] ]) queue1
            Session.processPublish session pid (Duplicate False) msg (Broker.publishDownstream broker)
            queue2 <- Session.dequeue session
            Session.getSubscriptions session >>= print . R.findMaxBounded "topic"
            assertEqual "A publish acknowledgment and the (downgraded) message itself shall be in the output queue." (Seq.fromList [ ServerPublishAcknowledged pid ]) queue2
            queue3 <- Session.dequeue session
            assertEqual "The downgraded message queue shall be in the output queue." (Seq.fromList [ ServerPublish (PacketIdentifier (-1)) (Duplicate False) msg { msgQoS = QoS0} ]) queue3

      , testCase "transmit a QoS1 message and retransmit after connection failure" $ do
          let req = connectionRequest { requestCleanSession = False }
              msg = Message.Message "topic" QoS1 (Retain False) "payload"
              pid = PacketIdentifier 0
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          Broker.withSession broker req (const $ pure ()) $ \session present-> do
            assertEqual "The session shall not be present." (SessionPresent False) present
            Session.enqueueMessage session msg
            queue <- Session.dequeue session
            assertEqual "The message shall be in the output queue." (Seq.fromList [ ServerPublish pid (Duplicate False) msg]) queue
          Broker.withSession broker req (const $ pure ()) $ \session present-> do
            assertEqual "The session shall be present." (SessionPresent True) present
            queue <- Session.dequeue session
            assertEqual "The message shall again be in the output queue, and must not be marked duplicate." (Seq.fromList [ ServerPublish pid (Duplicate True) msg ]) queue

      , testCase "transmit a QoS2 message and process confirmations" $ do
          let msg = Message.Message "topic" QoS2 (Retain False) "payload"
              pid = PacketIdentifier 0
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          Broker.withSession broker connectionRequest (const $ pure ()) $ \session _-> do
            pids1 <- Session.getFreePacketIdentifiers session
            Session.enqueueMessage session msg
            queue2 <- Session.dequeue session
            pids2 <- Session.getFreePacketIdentifiers session
            assertEqual "One packet identifier shall be in use after `dequeue`." (Seq.drop 1 pids1) pids2
            assertEqual "A PUB packet is expected in the output queue." (Seq.fromList [ ServerPublish pid (Duplicate False) msg ]) queue2
            Session.processPublishReceived session pid
            queue3 <- Session.dequeue session
            pids3 <- Session.getFreePacketIdentifiers session
            assertEqual "The packet identifier shall still be in use." (Seq.drop 1 pids1) pids3
            assertEqual "A PUBREL packet is expected in the output queue." (Seq.fromList [ ServerPublishRelease pid ]) queue3
            Session.processPublishComplete session pid
            pids4 <- Session.getFreePacketIdentifiers session
            assertEqual "The packet identifier shall have been returned after the transaction has been completed." pids1 pids4

      , testCase "transmit a QoS2 message and handle retransmissions on connection failure" $ do
          let req = connectionRequest { requestCleanSession = False }
              msg = Message.Message "topic" QoS2 (Retain False) "payload"
              pid = PacketIdentifier 0
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          Broker.withSession broker req (const $ pure ()) $ \session _-> do
            Session.enqueueMessage session msg
            queue <- Session.dequeue session
            assertEqual "The message shall be in the output queue." (Seq.fromList [ ServerPublish pid (Duplicate False) msg ]) queue
          Broker.withSession broker req (const $ pure ()) $ \session _-> do
            queue <- Session.dequeue session
            assertEqual "The message shall again be in the output queue and must be marked duplicate." (Seq.fromList [ ServerPublish pid (Duplicate True) msg ]) queue
            Session.processPublishReceived session pid
            queue' <- Session.dequeue session
            assertEqual "The release command shall be in the output queue." (Seq.fromList [ ServerPublishRelease pid ]) queue'
          Broker.withSession broker req (const $ pure ()) $ \session _-> do
            queue <- Session.dequeue session
            assertEqual "The release command shall be in the output queue (again)." (Seq.fromList [ ServerPublishRelease pid ]) queue
            Session.processPublishComplete session pid
          Broker.withSession broker req (const $ pure ()) $ \session _-> do
            queue <- Session.dequeue session
            assertEqual "The output queue shall be empty." mempty queue
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
     }
    quota = Quota {
       quotaSessionTTL = 60
     , quotaMaxInflightMessages = 10
     , quotaMaxQueueSizeQoS0 = 10
     , quotaMaxQueueSizeQoS1 = 10
     , quotaMaxQueueSizeQoS2 = 10
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
  }
