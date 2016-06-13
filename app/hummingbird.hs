{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module Main where

import Control.Monad
import Control.Concurrent
import Control.Exception

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import qualified System.Socket as S
import qualified System.Socket.Family.Inet6 as S
import qualified System.Socket.Type.Stream as S
import qualified System.Socket.Protocol.TCP as S

import Network.MQTT
import Network.MQTT.Client as C
import Network.MQTT.Message

main :: IO ()
main = do
  mqtt <- new (TcpSocket <$> S.socket) ("localhost", "1883")
  C.connect mqtt
  threadDelay 1000000
  subscribe mqtt [("$SYS/#", QoS0)]
  subscribe mqtt [("#", QoS0)]
  events <- streamEvents mqtt
  forever $ do
    takeEvent events >>= print

newtype TcpSocket = TcpSocket (S.Socket S.Inet6 S.Stream S.TCP)

instance StreamTransmitter TcpSocket where
  transmit (TcpSocket s) bs = S.sendAll s (LBS.fromChunks [bs]) S.msgNoSignal >> pure ()

instance StreamReceiver TcpSocket where
  receive (TcpSocket s) = S.receive s 8192 S.msgNoSignal

instance Closable TcpSocket where
  close (TcpSocket s)   = S.close s

instance Connectable TcpSocket where
  type ConnectableAddress TcpSocket = (BS.ByteString, BS.ByteString)
  connect (TcpSocket s) (host, port) = do
    as <- S.getAddressInfo (Just host) (Just port)
      (S.aiV4Mapped `mappend` S.aiAll) :: IO [S.AddressInfo S.Inet6 S.Stream S.TCP]
    conn (map S.socketAddress as)
    where
      conn []     = undefined -- will never happen by definition of `getAddressInfo`
      conn [a]    = S.connect s a
      conn (a:as) =
        S.connect s a `catch` (\e-> let _ = e :: S.SocketException in conn as)
