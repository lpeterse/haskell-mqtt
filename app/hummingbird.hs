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
  mqtt <- new newConnection
  connect mqtt
  subscribe mqtt [("$SYS/#", QoS0)]
  events <- streamEvents mqtt
  forever $ do
    takeEvent events >>= print

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
