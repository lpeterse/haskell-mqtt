{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
module BrokerTest ( getTestTree ) where

import           Control.Concurrent.MVar
import           Control.Exception
import           Data.Monoid
import qualified Data.Sequence               as Seq
import           Data.Typeable

import           Network.MQTT.Authentication
import qualified Network.MQTT.Broker         as Broker
import           Network.MQTT.Message
import qualified Network.MQTT.Message        as Message
import qualified Network.MQTT.Session        as Session
import qualified Network.MQTT.Topic          as Topic
import qualified Network.MQTT.RoutingTree    as R

import           Test.Tasty
import           Test.Tasty.HUnit

newtype TestAuthenticator = TestAuthenticator (AuthenticatorConfig TestAuthenticator)

instance Authenticator TestAuthenticator where
  data Principal TestAuthenticator = TestPrincipal deriving (Eq, Ord, Show)
  data AuthenticatorConfig TestAuthenticator = TestAuthenticatorConfig
    { cfgAuthenticate           :: ConnectionRequest -> IO (Maybe (Principal TestAuthenticator))
    , cfgHasPublishPermission   :: Principal TestAuthenticator -> Topic.Topic  -> IO Bool
    , cfgHasSubscribePermission :: Principal TestAuthenticator -> Topic.Filter -> IO Bool
    }
  data AuthenticationException TestAuthenticator = TestAuthenticatorException deriving (Typeable, Show)
  newAuthenticator = pure . TestAuthenticator
  authenticate (TestAuthenticator cfg) = cfgAuthenticate cfg
  hasPublishPermission (TestAuthenticator cfg) = cfgHasPublishPermission cfg
  hasSubscribePermission (TestAuthenticator cfg) = cfgHasSubscribePermission cfg

instance Exception (AuthenticationException TestAuthenticator)

getTestTree :: IO TestTree
getTestTree =
  pure $ testGroup "Broker"
    [ testGroup "Authentication"
      [ testCase "Reject with 'ServerUnavaible' when authentication throws exception" $ do
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          broker <- Broker.new $ TestAuthenticator $ authenticatorConfig { cfgAuthenticate = const $ throwIO TestAuthenticatorException }
          let sessionRejectHandler                           = putMVar m1
              sessionAcceptHandler session present principal = putMVar m2 (session, present, principal)
          Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
          tryReadMVar m1 >>= \x-> Just ServerUnavailable @?= x
          tryReadMVar m2 >>= \x-> Nothing                @?= x
      , testCase "Reject 'NotAuthorized' when authentication returned Nothing" $ do
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          broker <- Broker.new $ TestAuthenticator $ authenticatorConfig { cfgAuthenticate = const $ pure Nothing }
          let sessionRejectHandler                           = putMVar m1
              sessionAcceptHandler session present principal = putMVar m2 (session, present, principal)
          Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
          tryReadMVar m1 >>= \x-> Just NotAuthorized   @?= x
          tryReadMVar m2 >>= \x-> Nothing              @?= x
      ]
    , testGroup "Subscriptions"

      [ testCase "subscribe the same filter from 2 different sessions" $ do
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              req2 = connectionRequest { requestClientIdentifier = "2" }
              msg  = Message.Message "a/b" "" Qos0 False False
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _ _->
            Broker.withSession broker req2 (const $ pure ()) $ \session2 _ _-> do
              Broker.subscribe broker session1 42 [("a/b", Qos0)]
              Broker.subscribe broker session2 47 [("a/b", Qos0)]
              Broker.publishDownstream broker msg
              queue1 <- (<>) <$> Session.dequeue session1 <*> Session.dequeue session1
              queue2 <- (<>) <$> Session.dequeue session2 <*> Session.dequeue session2
              queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged 42 [Just Qos0], ServerPublish (-1) msg]
              queue2 @?= Seq.fromList [ ServerSubscribeAcknowledged 47 [Just Qos0], ServerPublish (-1) msg]

      , testCase "get retained message on subscription (newer overrides older, issue #6)" $ do
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              msg1 = Message.Message "topic" "test"  Qos0 True False
              msg2 = Message.Message "topic" "toast" Qos0 True False
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _ _-> do
            Broker.publishDownstream broker msg1
            Broker.publishDownstream broker msg2
            Broker.subscribe broker session1 23 [("topic", Qos0)]
            queue1 <- (<>) <$> Session.dequeue session1 <*> Session.dequeue session1
            queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged 23 [Just Qos0], ServerPublish (-1) msg2]

      , testCase "delete retained message when body is empty" $ do
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              msg1 = Message.Message "topic" "test"  Qos0 True False
              msg2 = Message.Message "topic" "" Qos0 True False
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _ _-> do
            Broker.publishDownstream broker msg1
            Broker.publishDownstream broker msg2
            Broker.subscribe broker session1 23 [("topic", Qos0)]
            queue1 <- (<>) <$> Session.dequeue session1 <*> Session.dequeue session1
            queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged 23 [Just Qos0] ]
      ]
    , testGroup "Quality of Service"

      [ testCase "transmit a Qos1 message and process acknowledgement" $ do
          let msg = Message.Message "topic" "body" Qos1 False False
              pid = 0
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          Broker.withSession broker connectionRequest (const $ pure ()) $ \session _ _-> do
            pids1 <- Session.getFreePacketIdentifiers session
            Session.enqueueMessage session msg
            queue <- Session.dequeue session
            pids2 <- Session.getFreePacketIdentifiers session
            Session.processPublishAcknowledged session pid
            pids3 <- Session.getFreePacketIdentifiers session
            assertEqual "One packet identifier shall be in use after `dequeue`." (Seq.drop 1 pids1) pids2
            assertEqual "The packet identifier shall have been returned after the message has been acknowledged." pids1 pids3
            assertEqual "The packet is expected in the output queue." (Seq.fromList [ ServerPublish 0 msg ]) queue

      , testCase "receive a Qos1 message and send acknowledgement" $ do
          let msg = Message.Message "topic" "body" Qos1 False False
              pid = 0
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          Broker.withSession broker connectionRequest (const $ pure ()) $ \session _ _-> do
            Broker.subscribe broker session pid [("topic", Qos0)]
            queue1 <- Session.dequeue session
            assertEqual "A subscribe acknowledgement shall be in the output queue." (Seq.fromList [ ServerSubscribeAcknowledged pid [Just Qos0] ]) queue1
            Session.processPublish session pid msg (Broker.publishDownstream broker)
            queue2 <- Session.dequeue session
            Session.getSubscriptions session >>= print . R.findMaxBounded "topic"
            assertEqual "A publish acknowledgment and the (downgraded) message itself shall be in the output queue." (Seq.fromList [ ServerPublishAcknowledged pid ]) queue2
            queue3 <- Session.dequeue session
            assertEqual "The downgraded message queue shall be in the output queue." (Seq.fromList [ ServerPublish (-1) msg {msgQos = Qos0} ]) queue3

      , testCase "transmit a Qos1 message and retransmit after connection failure" $ do
          let req = connectionRequest { requestCleanSession = False }
              msg = Message.Message "topic" "body" Qos1 False False
              pid = 0
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          Broker.withSession broker req (const $ pure ()) $ \session present _-> do
            assertEqual "The session shall not be present." False present
            Session.enqueueMessage session msg
            queue <- Session.dequeue session
            assertEqual "The message shall be in the output queue." (Seq.fromList [ ServerPublish pid msg]) queue
          Broker.withSession broker req (const $ pure ()) $ \session present _-> do
            assertEqual "The session shall be present." True present
            queue <- Session.dequeue session
            assertEqual "The message shall again be in the output queue, but marked as duplicate." (Seq.fromList [ ServerPublish pid $ msg { msgDuplicate = True }]) queue

      , testCase "transmit a Qos2 message and process confirmations" $ do
          let msg = Message.Message "topic" "body" Qos2 False False
              pid = 0
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          Broker.withSession broker connectionRequest (const $ pure ()) $ \session _ _-> do
            pids1 <- Session.getFreePacketIdentifiers session
            Session.enqueueMessage session msg
            queue2 <- Session.dequeue session
            pids2 <- Session.getFreePacketIdentifiers session
            assertEqual "One packet identifier shall be in use after `dequeue`." (Seq.drop 1 pids1) pids2
            assertEqual "A PUB packet is expected in the output queue." (Seq.fromList [ ServerPublish 0 msg ]) queue2
            Session.processPublishReceived session pid
            queue3 <- Session.dequeue session
            pids3 <- Session.getFreePacketIdentifiers session
            assertEqual "The packet identifier shall still be in use." (Seq.drop 1 pids1) pids3
            assertEqual "A PUBREL packet is expected in the output queue." (Seq.fromList [ ServerPublishRelease 0 ]) queue3
            Session.processPublishComplete session pid
            pids4 <- Session.getFreePacketIdentifiers session
            assertEqual "The packet identifier shall have been returned after the transaction has been completed." pids1 pids4

      , testCase "transmit a Qos2 message and handle retransmissions on connection failure" $ do
          let req = connectionRequest { requestCleanSession = False }
              msg = Message.Message "topic" "body" Qos2 False False
              pid = 0
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          Broker.withSession broker req (const $ pure ()) $ \session _ _-> do
            Session.enqueueMessage session msg
            queue <- Session.dequeue session
            assertEqual "The message shall be in the output queue." (Seq.fromList [ ServerPublish 0 msg ]) queue
          Broker.withSession broker req (const $ pure ()) $ \session _ _-> do
            queue <- Session.dequeue session
            assertEqual "The message shall be in the output queue (again)." (Seq.fromList [ ServerPublish 0 msg ]) queue
            Session.processPublishReceived session pid
            queue' <- Session.dequeue session
            assertEqual "The release command shall be in the output queue." (Seq.fromList [ ServerPublishRelease 0 ]) queue'
          Broker.withSession broker req (const $ pure ()) $ \session _ _-> do
            queue <- Session.dequeue session
            assertEqual "The release command shall be in the output queue (again)." (Seq.fromList [ ServerPublishRelease 0 ]) queue
            Session.processPublishComplete session pid
          Broker.withSession broker req (const $ pure ()) $ \session _ _-> do
            queue <- Session.dequeue session
            assertEqual "The output queue shall be empty." mempty queue
      ]
    ]

authenticatorConfig :: AuthenticatorConfig TestAuthenticator
authenticatorConfig  = TestAuthenticatorConfig
  { cfgAuthenticate           = const (pure Nothing)
  , cfgHasPublishPermission   = \_ _-> pure False
  , cfgHasSubscribePermission = \_ _-> pure False
  }

authenticatorConfigAllAccess :: AuthenticatorConfig TestAuthenticator
authenticatorConfigAllAccess = TestAuthenticatorConfig
  { cfgAuthenticate           = const (pure $ Just TestPrincipal)
  , cfgHasPublishPermission   = \_ _-> pure True
  , cfgHasSubscribePermission = \_ _-> pure True
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
