{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Concurrent.Async
import Control.Concurrent.Chan
import Control.Exception ( bracket, catch )
import Control.Monad ( forever, void )

import Data.Function ( fix )
import qualified Data.Source as S
import qualified Data.Source.Attoparsec as S
import qualified Data.ByteString as BS

import System.Socket
import System.Socket.Family.Inet6
import System.Socket.Type.Stream
import System.Socket.Protocol.TCP

import Network.MQTT
import Network.MQTT.Message
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
    S.drain $ S.each (void . \x-> send sock x mempty) $ fmap (const "") $ S.each print $ S.parse pMessage $ toSource sock
  )

toSource :: Socket f Stream p -> S.Source IO BS.ByteString BS.ByteString
toSource sock = S.Source $ do
  bs <- receive sock 4096 mempty
  S.pull $ if BS.null bs
    then S.complete id
    else S.prepend bs $ toSource sock
