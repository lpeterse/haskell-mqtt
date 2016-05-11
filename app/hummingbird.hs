{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Monad
import Control.Concurrent

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import qualified System.Socket as S
import qualified System.Socket.Family.Inet6 as S
import qualified System.Socket.Type.Stream as S
import qualified System.Socket.Protocol.TCP as S

import Network.MQTT
import Network.MQTT.Client
import Network.MQTT.Message

main :: IO ()
main = do
  mqtt <- newMqttClient newConnection
  print "abc"
  connect mqtt
  subscribe mqtt [("$SYS/#", QoS0)]
  ms <- messages mqtt
  forkIO (sendQoS1 mqtt)
  forever $ do
    m <- message ms
    print m

sendQoS1 :: MqttClient -> IO ()
sendQoS1 mqtt = do
  mapM_ (forkIO . f 0) [1..1000]
  where
    f i t = do
      when (mod i 1000 == 0) (putStrLn $ show t ++ ": " ++ show i)
      publish mqtt $ Message QoS1 False (Topic "cabsmcba") "ashdjkahsdhjahd"
      f (succ i) t

newConnection :: IO Connection
newConnection = do
  sock <- S.socket :: IO (S.Socket S.Inet6 S.Stream S.TCP)
  addrInfo:_ <- S.getAddressInfo (Just "localhost") (Just "1883") mempty :: IO [S.AddressInfo S.Inet6 S.Stream S.TCP]
  S.connect sock (S.socketAddress addrInfo)
  pure Connection
    { send    = \bs-> S.sendAll sock (LBS.fromChunks [bs]) S.msgNoSignal
    , receive = S.receive sock 4096 S.msgNoSignal
    , close   = S.close sock
    }
