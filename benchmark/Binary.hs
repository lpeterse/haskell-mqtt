{-# LANGUAGE OverloadedStrings #-}
module Main ( main, benchmark ) where

import           Criterion.Main
import qualified Data.Binary.Get         as BG
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy    as BSL

import           Network.MQTT.Topic
import           Network.MQTT.Message

main :: IO ()
main = defaultMain benchmark

benchmark :: [Benchmark]
benchmark =
    [ bgroup "Binary" [
        bgroup "ClientConnect (without will)" $ pb $
          ClientConnect "client-identifier" True 65298 Nothing credentials
      , bgroup "ClientConnect (with will)" $ pb $
          ClientConnect "client-identifier" True 65298 (Just message) credentials
      , bgroup "ClientPublish" $ pb $
          ClientPublish 65298 False message
      , bgroup "ClientSubscribe" $ pb $
          ClientSubscribe 65298 $ replicate 23 (filtr, Qos2)
      , bgroup "ClientUnsubscribe" $ pb $
          ClientUnsubscribe 65298 $ replicate 23 filtr
      ]
    ]
    where
      credentials = Just ("username", Just (Password "password"))
      topic       = "bla/foo/bar/fnord/gnurp"
      filtr       = "bla/foo/+/fnord/gnurp/#"
      payload     = "ashdjahjdahskj hdas dhakjshdas dhakjshdalsjh dashdashd lashdahsad"
      message     = Message {
          msgTopic  = topic
        , msgBody   = payload
        , msgQos    = Qos1
        , msgRetain = True
        }
      pb x = [
          -- evaluate the lazy bytestring to strict normal form
          bench "build" (nf (BS.toLazyByteString . clientMessageBuilder) x)
        , env (pure $ BS.toLazyByteString $ clientMessageBuilder x) $ \bs->
            -- evaluate to weak head normal form (the parser must have touched
            -- everything if it could decide whether the input was valid or not)
            bench "parse" (whnf (BG.runGet clientMessageParser) bs)
        ]
