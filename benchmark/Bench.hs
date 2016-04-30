{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Criterion.Main (bgroup, bench, nf, whnf, defaultMain)

import Data.Monoid
import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Attoparsec.ByteString as A
import qualified Data.Attoparsec.ByteString.Lazy as AL
import qualified Data.Attoparsec.ByteString.Char8 as A

import Network.MQTT.Message

main :: IO ()
main = defaultMain
    [ bgroup "Message-parsing-and-serialisation" [
        bgroup "Connect (without will)" $ pb $
          Connect "client-identifier" True 65298 Nothing (Just ("username", Just "password"))
      , bgroup "Connect (with will)" $ pb $
          Connect "client-identifier" True 65298 (Just $ Will "topic" "message" (Just ExactlyOnce) False) (Just ("username", Just "password"))
      , bgroup "ConnectAcknowledgement (rejected)" $ pb $
          ConnectAcknowledgement (Left IdentifierRejected)
      , bgroup "ConnectAcknowledgement (accepted)" $ pb $
          ConnectAcknowledgement (Right False)
      , bgroup "Publish (QoS 0)" $ pb $
          Publish True False "topic" Nothing "message body"
      , bgroup "Publish (QoS 1)" $ pb $
          Publish True False "topic" (Just (AtLeastOnce, 2342)) "message body"
      , bgroup "PublishAcknowledgement" $ pb $
          PublishAcknowledgement 234
      , bgroup "Subscribe" $ pb $
          Subscribe 2345 [("short", Nothing), ("longer longer longer", Just ExactlyOnce)]
      , bgroup "SubscribeAcknowledgement" $ pb $
          SubscribeAcknowledgement 2345 [Nothing, Just Nothing, Just (Just ExactlyOnce), Nothing]
      , bgroup "Unsubscribe" $ pb $
          Unsubscribe 2345 ["short", "longer longer longer"]
      , bgroup "PingRequest" $ pb
          PingRequest
      , bgroup "PingResponse" $ pb
          PingResponse
      , bgroup "Disconnect" $ pb
          Disconnect
      ]
    ]
    where
      pb x = [
          bench "parse" (whnf (A.parseOnly pMessage) (LBS.toStrict $ BS.toLazyByteString $ bMessage x))
        , bench "build" (whnf (LBS.toStrict . BS.toLazyByteString . bMessage) x)
        ]
