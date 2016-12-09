{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
module BrokerTest ( getTestTree ) where

import           Control.Exception
import           Control.Concurrent.MVar
import           Data.Typeable

import           Network.MQTT.Message
import           Network.MQTT.Authentication
import qualified Network.MQTT.Broker as Broker
import qualified Network.MQTT.Topic as Topic

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
          let sessionRejectHandler reason                    = putMVar m1 reason
              sessionAcceptHandler session present principal = putMVar m2 (session, present, principal)
          Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
          tryReadMVar m1 >>= \x-> Just ServerUnavailable @?= x
          tryReadMVar m2 >>= \x-> Nothing                @?= x
      , testCase "Reject 'NotAuthorized' when authentication returned Nothing" $ do
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          broker <- Broker.new $ TestAuthenticator $ authenticatorConfig { cfgAuthenticate = const $ pure Nothing }
          let sessionRejectHandler reason                    = putMVar m1 reason
              sessionAcceptHandler session present principal = putMVar m2 (session, present, principal)
          Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
          tryReadMVar m1 >>= \x-> Just NotAuthorized   @?= x
          tryReadMVar m2 >>= \x-> Nothing              @?= x
      -- , testCase "Accept with new session when authentication returned Principal" $ do
      --     m1 <- newEmptyMVar
      --     m2 <- newEmptyMVar
      --     broker <- Broker.new $ TestAuthenticator $ authenticatorConfig { cfgAuthenticate = const $ pure Nothing }
      --     let sessionRejectHandler reason                    = putMVar m1 reason
      --         sessionAcceptHandler session present principal = putMVar m2 (session, present, principal)
      --     Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
      --     tryReadMVar m1 >>= \x-> Nothing              @?= x
      --     tryReadMVar m2 >>= \x-> Nothing              @?= x
      ]
    ]

authenticatorConfig :: AuthenticatorConfig TestAuthenticator
authenticatorConfig  = TestAuthenticatorConfig
  { cfgAuthenticate           = const (pure Nothing)
  , cfgHasPublishPermission   = \_ _-> pure False
  , cfgHasSubscribePermission = \_ _-> pure False
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
