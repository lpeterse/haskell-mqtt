{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}
module Main where

import           Control.Concurrent.Async
import           Control.Monad
import qualified Data.ByteString            as BS
import           Network.MQTT.Message
import qualified Network.MQTT.Server        as Server
import qualified Network.MQTT.ServerStack   as SS
import qualified System.Socket              as S
import qualified System.Socket.Family.Inet  as S
import qualified System.Socket.Protocol.TCP as S
import qualified System.Socket.Type.Stream  as S

main :: IO ()
main  = do
  socketServer    <- SS.new sockConfig    :: IO (SS.Server (Server.MQTT (S.Socket S.Inet S.Stream S.TCP)))
  webSocketServer <- SS.new wsConfig      :: IO (SS.Server (Server.MQTT (SS.WebSocket (S.Socket S.Inet S.Stream S.TCP))))
  runServer socketServer `race_` runServer webSocketServer
  where
    runServer :: (SS.ServerStack a, SS.ServerMessage a ~ BS.ByteString) => SS.Server (Server.MQTT a) -> IO ()
    runServer server = do
      SS.start server
      forever $ do
        putStrLn "Waiting for connection..."
        SS.acceptWith server $ \connection-> do
          putStrLn "New connection!"
          SS.receive connection 4096 >>= print
          SS.send connection (ConnectAcknowledgement $ Right False)
          forever $ SS.receive connection 4096 >>= print
    sockConfig = Server.MqttServerConfig {
      Server.mqttTransportConfig = SS.SocketServerConfig {
        SS.socketServerConfigBindAddress = S.SocketAddressInet  S.inetLoopback 1883
      , SS.socketServerConfigListenQueueSize = 5
      }
    }
    wsConfig = Server.MqttServerConfig {
      Server.mqttTransportConfig = SS.WebSocketServerConfig {
        SS.wsTransportConfig = SS.SocketServerConfig {
          SS.socketServerConfigBindAddress = S.SocketAddressInet  S.inetLoopback 1884
        , SS.socketServerConfigListenQueueSize = 5
        }
      }
    }


{-
data FakeAuthenticator = FakeAuthenticator

instance Exception (AuthenticationException FakeAuthenticator)
instance Exception (AuthorizationException FakeAuthenticator)

instance Authenticator FakeAuthenticator where
  data Principal FakeAuthenticator = FakeAuthenticatorPrinciple T.Text deriving (Eq, Ord, Show)
  data AuthenticationException FakeAuthenticator = FakeAuthenticationException deriving (Eq, Ord, Show, Typeable)
  authenticate _ request = pure $ FakeAuthenticatorPrinciple <$> requestUsername request

instance Authorizer FakeAuthenticator where
  data AuthorizationException FakeAuthenticator = FakeAuthorizationException deriving (Eq, Ord, Show, Typeable)
-}