{-# LANGUAGE OverloadedStrings #-}
module Main ( main, benchmark ) where

import qualified Data.Serialize.Get as SG
import qualified Data.ByteString.Builder as BS

import Criterion.Main

import Network.MQTT.Message

main :: IO ()
main = defaultMain benchmark

benchmark :: [Benchmark]
benchmark =
    [ bgroup "Binary" [
        bgroup "Connect (without will)" $ pb $
          Connect "client-identifier" True 65298 Nothing (Just ("username", Just "password"))
      , bgroup "Connect (with will)" $ pb $
          Connect "client-identifier" True 65298 (Just $ Will topic payload (Just ExactlyOnce) False) (Just ("username", Just "password"))
      , bgroup "ConnectAcknowledgement (rejected)" $ pb $
          ConnectAcknowledgement (Left IdentifierRejected)
      , bgroup "ConnectAcknowledgement (accepted)" $ pb $
          ConnectAcknowledgement (Right False)
      , bgroup "Publish (QoS 0)" $ pb $
          Publish True False "/another/short/topic" PublishQoS0 payload
      , bgroup "Publish (QoS 1)" $ pb $
          Publish True False "/another/short/topic" (PublishQoS1 True $ PacketIdentifier 2342) payload
      , bgroup "PublishAcknowledgement" $ pb $
          PublishAcknowledgement (PacketIdentifier 234)
      , bgroup "Subscribe" $ pb $
          Subscribe (PacketIdentifier 2345) [(topic, QoS1), (topic, QoS0)]
      , bgroup "SubscribeAcknowledgement" $ pb $
          SubscribeAcknowledgement (PacketIdentifier 2345) [Nothing, Just QoS1, Just QoS0, Nothing, Just QoS2]
      , bgroup "Unsubscribe" $ pb $
          Unsubscribe (PacketIdentifier 2345) [topic, topic]
      ]
    ]
    where
      topic   = "bla/foo/bar/fnord/+/gnurp"
      payload = "ashdjahjdahskj hdas dhakjshdas dhakjshdalsjh dashdashd lashdahsad"
      pb x = [
          -- evaluate the lazy bytestring to strict normal form
          bench "build" (nf (BS.toLazyByteString . bRawMessage) x)
        , env (pure $ BS.toLazyByteString $ bRawMessage x) $ \bs->
            -- evaluate to weak head normal form (the parser must have touched
            -- everything if it could decide whether the input was valid or not)
            bench "parse" (whnf (SG.runGetLazy pRawMessage) bs)
        ]
