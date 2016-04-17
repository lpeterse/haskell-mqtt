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

type Username = T.Text
type Password = BS.ByteString

data Connection
   = Connection
     { connClientId :: T.Text
     , connWillTopic :: Maybe T.Text
     , connWillMessage :: Maybe BS.ByteString
     , connUsername :: Maybe T.Text
     , connPassword :: Maybe BS.ByteString
     , connKeepAlive :: Word16
     , connFlags :: Word8
     } deriving (Eq, Ord, Show)


data ConnectionState
   = ConnectionState
     { csOutgoingMailbox  :: Chan BS.ByteString
     , csSubscriptionTree :: SubscriptionTree Message
     }

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

data SubscriptionAcknowledgement
   = SubscriptionSuccessMaxQoS0
   | SubscriptionSuccessMaxQoS1
   | SubscriptionSuccessMaxQoS2
   | SubscriptionFailure

handleSubscribe :: (MonadThrow m, MonadIO m) => (TopicFilter -> m SubscriptionAcknowledgement) -> MQTT m ()
handleSubscribe subscribe = do
  -- Receive and validate SUBSCRIBE
  header <- maybe (throwM EndOfInput) return =<< C.head
  unless (header == 0x82) $
    throwM $ ProtocolViolation "Malformed SUBSCRIBE header"
  remainingLength <- getRemainingLength
  C.isolate remainingLength C.=$= do
    packetId <- getWord16BeDefault (throwM EndOfInput)
    topics   <- getTopics packetId
    when (null topics) $
      throwM $ ProtocolViolation "No Topics In SUBSCRIBE"
    acks <- lift $ mapM subscribe topics
    C.yield
      $ LBS.toStrict
      $ BS.toLazyByteString
      $ mconcat
      ( BS.word8 0x90
      : BS.word8 (fromIntegral $ 2 + length acks)
      : BS.word16BE packetId
      : map returnCode acks
      )
  where
    returnCode SubscriptionSuccessMaxQoS0 = BS.word8 0x00
    returnCode SubscriptionSuccessMaxQoS1 = BS.word8 0x01
    returnCode SubscriptionSuccessMaxQoS2 = BS.word8 0x02
    returnCode SubscriptionFailure        = BS.word8 0x08
    getTopics :: (MonadThrow m, MonadIO m) => Word16 -> MQTT m [TopicFilter]
    getTopics packetId = fix $ \next-> do
      m <- C.peek
      case m of
        Nothing -> return []
        Just bs -> if BS.null bs
          then return []
          else do
            topic   <- T.splitOn "/" <$> getStringDefault (throwM EndOfInput)
            qosCode <- maybe (throwM EndOfInput) return =<< C.head
            qos     <- case qosCode of
              0 -> return AtMostOnce
              2 -> return $ AtLeastOnce packetId
              4 -> return $ ExactlyOnce packetId
              _ -> throwM $ ProtocolViolation "Invalid QoS Code"
            ((topic, qos):) <$> next

-- | Receives PINGREQ from the client and immediately responds with a PINGRESP.
handlePingRequest :: (MonadThrow m, MonadIO m) => MQTT m ()
handlePingRequest = do
  -- Receive and validate PINGREQ
  w16 <- getWord16BeDefault $ throwM EndOfInput
  unless (w16 == 0xc000) $
    throwM $ ProtocolViolation "Malformed PINGREQ"
  -- Send PINGRESP
  C.yield
      $ LBS.toStrict
      $ BS.toLazyByteString
      $ BS.word16BE 0xd000

-- | Receives a DISCONNECT from the client. Doesn't do anything else.
handleDisconnect :: MonadThrow m => MQTT m ()
handleDisconnect = do
  w16 <- getWord16BeDefault $ throwM EndOfInput
  unless (w16 == 0xe000) $
    throwM $ ProtocolViolation "Malformed DISCONNECT"

getWord16BeDefault :: (Monad m, Num a) => C.ConduitM BS.ByteString o m a -> C.ConduitM BS.ByteString o m a
getWord16BeDefault def = (+)
  <$> ( maybe def (return . (*256) . fromIntegral) =<< C.head )
  <*> ( maybe def (return .          fromIntegral) =<< C.head )

getBlobDefault   :: Monad m => MQTT m BS.ByteString -> MQTT m BS.ByteString
getBlobDefault   def = LBS.toStrict <$> (getWord16BeDefault (def >> return 0) >>= C.take)

getStringDefault :: MonadThrow m => C.ConduitM BS.ByteString o m T.Text -> C.ConduitM BS.ByteString o m T.Text
getStringDefault def = do
  size <- getWord16BeDefault (throwM EndOfInput)
  C.take size >>= parse
 where
   parse = either
     ( const def )
     ( return . LT.toStrict ) . LT.decodeUtf8'

data MQTTException
   = EndOfInput
   | ProtocolViolation String
   | Disconnect
   deriving (Eq, Ord, Show, Typeable)

instance Exception MQTTException
-}
