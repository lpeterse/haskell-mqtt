{-# LANGUAGE OverloadedStrings #-}
module Main ( main, benchmark ) where

import           Criterion.Main
import qualified Data.Binary.Get         as BG
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy    as BSL

import           Network.MQTT.Message

main :: IO ()
main = defaultMain benchmark

benchmark :: [Benchmark]
benchmark =
    [ bgroup "Binary" [
        bgroup "ClientConnect (without will)" $ pb $
          ClientConnect "client-identifier" (CleanSession True) 65298 Nothing credentials
      , bgroup "ClientConnect (with will)" $ pb $
          ClientConnect "client-identifier" (CleanSession True) 65298 (Just message) credentials
      , bgroup "ClientPublish" $ pb $
          ClientPublish (PacketIdentifier 65298) (Duplicate False) message
      , bgroup "ClientSubscribe" $ pb $
          ClientSubscribe (PacketIdentifier 65298) $ replicate 23 (filtr, QoS2)
      , bgroup "ClientUnsubscribe" $ pb $
          ClientUnsubscribe (PacketIdentifier 65298) $ replicate 23 filtr
      ]
    ]
    where
      credentials = Just ("username", Just (Password "password"))
      topic       = "bla/foo/bar/fnord/gnurp"
      filtr       = "bla/foo/+/fnord/gnurp/#"
      payload     = "ashdjahjdahskj hdas dhakjshdas dhakjshdalsjh dashdashd lashdahsad"
      message     = Message {
          msgTopic   = topic
        , msgPayload = payload
        , msgQoS     = QoS1
        , msgRetain  = Retain True
        }
      pb x = [
          -- evaluate the lazy bytestring to strict normal form
          bench "build" (nf (BS.toLazyByteString . clientPacketBuilder) x)
        , env (pure $ BS.toLazyByteString $ clientPacketBuilder x) $ \bs->
            -- evaluate to weak head normal form (the parser must have touched
            -- everything if it could decide whether the input was valid or not)
            bench "parse" (whnf (BG.runGet clientPacketParser) bs)
        ]
