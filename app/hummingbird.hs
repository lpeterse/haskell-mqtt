{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}
module Main where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import qualified Data.Text                   as T
import           Data.Typeable
import           Network.MQTT.Authentication
import           Network.MQTT.Authorization
import qualified Network.MQTT.Broker         as Broker
import qualified Network.MQTT.Server         as Server
import qualified Network.MQTT.ServerStack    as SS
import qualified System.Socket               as S
import qualified System.Socket.Family.Inet   as S
import qualified System.Socket.Protocol.TCP  as S
import qualified System.Socket.Type.Stream   as S

main :: IO ()
main  = do
  broker <- Broker.new FakeAuthenticator
  server <- SS.new (mqttConfig broker) :: IO (SS.Server (Server.MQTT FakeAuthenticator (S.Socket S.Inet S.Stream S.TCP)))
  SS.start server
  forever $ do
    connection <- SS.accept server
    forkIO $ withConnection connection `finally` SS.close connection >> putStrLn "closed"
  where
    socketConfig = SS.SocketServerConfig {
      SS.socketServerConfigBindAddress = S.SocketAddressInet  S.inetLoopback 1883
    , SS.socketServerConfigListenQueueSize = 5
    }
    mqttConfig broker = Server.MqttServerConfig {
      Server.mqttTransportConfig = socketConfig
    , Server.mqttBroker = broker
    }
    withConnection connection = do
      print $ Server.mqttUsername connection
      threadDelay 10000000

data FakeAuthenticator = FakeAuthenticator

instance Exception (AuthenticationException FakeAuthenticator)
instance Exception (AuthorizationException FakeAuthenticator)

instance Authenticator FakeAuthenticator where
  data Principal FakeAuthenticator = FakeAuthenticatorPrinciple T.Text deriving (Eq, Ord, Show)
  data AuthenticationException FakeAuthenticator = FakeAuthenticationException deriving (Eq, Ord, Show, Typeable)
  authenticate _ request = pure $ FakeAuthenticatorPrinciple <$> requestUsername request

instance Authorizer FakeAuthenticator where
  data AuthorizationException FakeAuthenticator = FakeAuthorizationException deriving (Eq, Ord, Show, Typeable)
