{-# LANGUAGE OverloadedStrings #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT where

import Control.Applicative
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad.Catch (MonadThrow (..))
import Control.Monad

import Data.Monoid
import Data.Bits
import Data.Function (fix)
import qualified Data.Source as S
import qualified Data.Source.ByteString as S
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.Encoding as LT
import Data.Word
import Data.Typeable

import Network.MQTT.Message
import Network.MQTT.Message.RemainingLength
import Network.MQTT.SubscriptionTree

{-
serialize :: Message -> BS.ByteString
serialize p@PUBLISH {} =
let t = T.encodeUtf8 (msgTopic p)
in      LBS.toStrict
$ BS.toLazyByteString
$ BS.word8 (0x30 .|. flagDuplicate .|. flagQoS .|. flagRetain)
<> sRemainingLength ( 2 + BS.length topicBS
+ fromIntegral ( ( ( flagQoS `div` 2 ) .|. flagQoS ) .&. 2 )
+ fromIntegral ( LBS.length (msgBody p) )
)
<> BS.word16BE ( fromIntegral $ BS.length topicBS )
<> BS.byteString topicBS
<> packetid
<> BS.lazyByteString (msgBody p)
where
flagDuplicate, flagRetain, flagQoS :: Word8
flagDuplicate  = if msgDuplicate p then 0x08 else 0
flagRetain     = if msgRetain    p then 0x01 else 0
flagQoS        = case msgQoS p of
AtMostOnce   -> 0x00
AtLeastOnce  -> 0x02
ExactlyOnce  -> 0x04
topicBS        = T.encodeUtf8 $ msgTopic p
packetid       = case msgQoS p of
AtMostOnce   -> mempty
AtLeastOnce  -> BS.word16BE undefined
ExactlyOnce  -> BS.word16BE undefined
type TopicFilter = ([T.Text], QoS)

-}
