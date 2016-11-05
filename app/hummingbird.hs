{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}
module Main where

import           Control.Concurrent.Async
import           Control.Monad
import           Network.MQTT.Message
import qualified Network.MQTT.Server        as Server
import qualified Network.MQTT.Broker        as Broker
import qualified Network.Stack.Server       as SS
import qualified System.Socket              as S
import qualified System.Socket.Family.Inet  as S
import qualified System.Socket.Protocol.TCP as S
import qualified System.Socket.Type.Stream  as S

main :: IO ()
main  = do
  broker <- Broker.new undefined
  SS.withServer sockConfig (handleServer broker) `race_` SS.withServer wsConfig (handleServer broker)
  where
    handleServer :: (SS.StreamServerStack a, Show (SS.ServerConnectionInfo a)) => Broker.Broker auth -> SS.Server (Server.MQTT a) -> IO ()
    handleServer broker server = forever $ do
      putStrLn "Waiting for connection..."
      SS.withConnection server $ \connection info-> do
        putStrLn "New connection:"
        print info
        Server.handleConnection broker connection info
    sockConfig :: SS.ServerConfig (Server.MQTT (S.Socket S.Inet S.Stream S.TCP))
    sockConfig = Server.MqttServerConfig {
      Server.mqttTransportConfig = SS.SocketServerConfig {
        SS.socketServerConfigBindAddress = S.SocketAddressInet  S.inetLoopback 1883
      , SS.socketServerConfigListenQueueSize = 5
      }
    }
    wsConfig :: SS.ServerConfig (Server.MQTT (SS.WebSocket (S.Socket S.Inet S.Stream S.TCP)))
    wsConfig = Server.MqttServerConfig {
      Server.mqttTransportConfig = SS.WebSocketServerConfig {
        SS.wsTransportConfig = SS.SocketServerConfig {
          SS.socketServerConfigBindAddress = S.SocketAddressInet  S.inetLoopback 1884
        , SS.socketServerConfigListenQueueSize = 5
        }
      }
    }
