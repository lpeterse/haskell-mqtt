{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Exception ( bracket, catch )
import Control.Monad ( forever )

import Data.Conduit

import System.Socket
import System.Socket.Family.Inet6
import System.Socket.Type.Stream
import System.Socket.Protocol.TCP
import System.Socket.Conduit ( toSource, toSink )

import Network.MQTT

main :: IO ()
main = bracket
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
    forever $ acceptAndHandle s `catch` \e-> print (e :: SocketException)
                                `catch` \e-> print (e :: MQTTException)
  )

acceptAndHandle :: Socket Inet6 Stream TCP -> IO ()
acceptAndHandle s = bracket
  ( accept s )
  ( \(sock, addr)-> do
    close sock
    putStrLn $ "Closed connection to " ++ show addr
  )
  ( \(sock, addr)-> do
    x <- toSource 4096 msgNoSignal sock $= mqttBroker $$ toSink msgNoSignal sock
    print x
  )
