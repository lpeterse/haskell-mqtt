{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}
module Main where

import           Control.Concurrent.Async
import           Control.Monad
import           Network.MQTT.Message
import qualified Network.MQTT.Server        as Server
import qualified Network.Stack.Server       as SS
import qualified System.Socket              as S
import qualified System.Socket.Family.Inet  as S
import qualified System.Socket.Protocol.TCP as S
import qualified System.Socket.Type.Stream  as S

main :: IO ()
main  =
  SS.withServer sockConfig handleServer `race_` SS.withServer wsConfig handleServer
  where
    handleServer :: (SS.StreamServerStack a, Show (SS.ServerConnectionInfo a)) => SS.Server (Server.MQTT a) -> IO ()
    handleServer server = forever $ do
        putStrLn "Waiting for connection..."
        SS.withConnection server $ \connection info-> do
          putStrLn "New connection:"
          print info
          SS.receiveMessage connection >>= print
          SS.sendMessage connection (ConnectAcknowledgement $ Right False)
          forever (SS.receiveMessage connection >>= print)
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
