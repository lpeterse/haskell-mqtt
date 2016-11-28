{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
module Main where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Monad
import           Control.Exception
import           Data.Typeable
import           Data.String
import           Network.MQTT.Authentication
import qualified Network.MQTT.Broker        as Broker
import           Network.MQTT.Message
import qualified Network.MQTT.Server        as Server
import qualified Network.Stack.Server       as SS
import qualified System.Clock               as Clock
import qualified System.Socket              as S
import qualified System.Socket.Family.Inet  as S
import qualified System.Socket.Protocol.TCP as S
import qualified System.Socket.Type.Stream  as S

main :: IO ()
main  = do
  broker <- Broker.new SimpleAuthenticator
  void $ async (pingThread broker)
  SS.withServer sockConfig (handleServer broker) `race_` SS.withServer wsConfig (handleServer broker)
  where
    handleServer :: (SS.StreamServerStack a, Show (SS.ServerConnectionInfo a), Server.MqttServerTransportStack a, Authenticator auth) => Broker.Broker auth -> SS.Server (Server.MQTT a) -> IO ()
    handleServer broker server = forever $ do
      putStrLn "Waiting for connection..."
      SS.withConnection server $ \connection info-> do
        putStrLn "New connection:"
        print info
        Server.handleConnection broker connection info
    sockConfig :: SS.ServerConfig (Server.MQTT (S.Socket S.Inet S.Stream S.TCP))
    sockConfig = Server.MqttServerConfig {
      Server.mqttTransportConfig = SS.SocketServerConfig {
        SS.socketServerConfigBindAddress = S.SocketAddressInet  S.inetAny 1883
      , SS.socketServerConfigListenQueueSize = 5
      }
    }
    wsConfig :: SS.ServerConfig (Server.MQTT (SS.WebSocket (S.Socket S.Inet S.Stream S.TCP)))
    wsConfig = Server.MqttServerConfig {
      Server.mqttTransportConfig = SS.WebSocketServerConfig {
        SS.wsTransportConfig = SS.SocketServerConfig {
          SS.socketServerConfigBindAddress = S.SocketAddressInet  S.inetAny 1884
        , SS.socketServerConfigListenQueueSize = 5
        }
      }
    }
    pingThread broker = forM_ [0..] $ \uptime-> do
      threadDelay 1000000
      time <- Clock.sec <$> Clock.getTime Clock.Realtime
      Broker.publishUpstream' broker (uptimeMsg (uptime :: Int))
      Broker.publishUpstream' broker (unixtimeMsg time)
      where
        uptimeMsg uptime = Message "$SYS/uptime" (fromString $ show uptime) Qos0 False False
        unixtimeMsg time = Message "$SYS/unixtime" (fromString $ show time) Qos0 False False

data SimpleAuthenticator = SimpleAuthenticator

instance Authenticator SimpleAuthenticator where
  data Principal SimpleAuthenticator = SimplePrincipal deriving (Eq, Ord, Show)
  data AuthenticationException SimpleAuthenticator = SimpleAuthenticationException deriving (Show, Typeable)
  authenticate _ _ = pure Nothing

instance Exception (AuthenticationException SimpleAuthenticator) where
