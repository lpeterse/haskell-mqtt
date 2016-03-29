{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Concurrent.Async
import Control.Concurrent.Chan
import Control.Exception ( bracket, catch )
import Control.Monad ( forever )
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Resource

import Data.Conduit
import Data.Function ( fix )
import qualified Data.ByteString as BS

import System.Socket
import System.Socket.Family.Inet6
import System.Socket.Type.Stream
import System.Socket.Protocol.TCP
import System.Socket.Conduit ( toSource, toSink )

import Network.MQTT
import Network.MQTT.SubscriptionTree

main :: IO ()
main = do
  stree <- newSubscriptionTree
  bracket
    ( socket :: IO (Socket Inet6 Stream TCP) )
    ( \s-> do
      close s
      putStrLn "Listening socket closed."
    )
    ( \s-> do
      setSocketOption s (ReuseAddress True)
      setSocketOption s (V6Only False)
      bind s (SocketAddressInet6 inet6Any 1883 0 0)
      listen s 5
      putStrLn "Listening socket ready..."
      forever $ acceptAndHandle stree s `catch` \e-> print (e :: SocketException)
                                        `catch` \e-> print (e :: MQTTException)
    )

acceptAndHandle :: SubscriptionTree Message -> Socket Inet6 Stream TCP -> IO ()
acceptAndHandle stree s = bracket
  ( accept s )
  ( \(sock, addr)-> do
    close sock
    putStrLn $ "Closed connection to " ++ show addr
  )
  ( \(sock, addr)-> do
    out <- newChan :: IO (Chan BS.ByteString)
    let st             = ConnectionState out stree
    let handleIncoming = toSource 4096 msgNoSignal sock $= mqttBroker st $$ sinkChan out
    let handleOutgoing = sourceChan out $$ toSink msgNoSignal sock
    handleIncoming `race_` handleOutgoing
  )
  where
    sourceChan :: Chan BS.ByteString -> Source IO BS.ByteString
    sourceChan chan = forever $
      liftIO (readChan chan) >>= yield
    sinkChan :: Chan BS.ByteString -> Sink BS.ByteString IO ()
    sinkChan chan = fix $ \again->
      await >>= maybe (return ()) ((>> again) . lift . liftIO . writeChan chan)
