{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Monad
import Control.Concurrent
import Control.Exception

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import qualified System.Socket as S
import qualified System.Socket.Family.Inet as S
import qualified System.Socket.Type.Stream as S
import qualified System.Socket.Protocol.TCP as S

import Network.MQTT
import Network.MQTT.Client
import Network.MQTT.Message

main :: IO ()
main = do
  undefined
  {-
  mqtt <- newMqttClient newConnection
  print "ABC"
  connect mqtt
  -- threadDelay 1000000
  print "CONNECTED"
  foobar <- BS.readFile "topic.txt"
  forkIO $ (sendQoS1 mqtt foobar) `onException` print "DIED"
  subscribe mqtt [("$SYS/#", QoS0)]
  ms <- messages mqtt
  forever $ do
    m <- message ms
    print m

sendQoS1 :: MqttClient -> BS.ByteString -> IO ()
sendQoS1 mqtt foobar = do
  mapM_ (forkIO . f 0) [1..10]
  where
    f i t = do
      when (mod i 100000 == 0) (putStrLn $ show t ++ ": " ++ show i)
      publish mqtt $ Message QoS0 False (Topic foobar) foobar
      f (succ i) t

newConnection :: IO Connection
newConnection = do
  s <- S.socket :: IO (S.Socket S.Inet S.Stream S.TCP)
  addrInfo:_ <- S.getAddressInfo (Just "environment.dev") (Just "1883") mempty :: IO [S.AddressInfo S.Inet S.Stream S.TCP]
  S.connect s (S.socketAddress addrInfo)
  pure Connection
    { send    = \bs-> S.send s bs S.msgNoSignal >> pure ()
    , receive = S.receive s 4096 S.msgNoSignal
    , close   = S.close s
    , sock    = s
    }
-}
