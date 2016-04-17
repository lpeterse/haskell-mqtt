{-# LANGUAGE TupleSections #-}
module Network.MQTT.Message where

import Control.Applicative
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad.Catch (MonadThrow (..))
import Control.Monad

import qualified Data.Attoparsec.ByteString as A
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

import Prelude

import Network.MQTT.Message.Blob
import Network.MQTT.Message.RemainingLength
import Network.MQTT.Message.Utf8String

type ClientIdentifier = T.Text
type SessionPresent   = Bool
type CleanSession     = Bool
type KeepAlive        = Word16
type Username         = T.Text
type Password         = BS.ByteString
type PacketIdentifier = Word16
type TopicFilter      = T.Text

data QoS
   = AtLeastOnce
   | ExactlyOnce
   deriving (Eq, Ord, Show, Enum)

data ConnectionRefusal
   = UnacceptableProtocolVersion
   | IdentifierRejected
   | ServerUnavailable
   | BadUsernameOrPassword
   | NotAuthorized
   deriving (Eq, Ord, Show, Enum)

data Will
   = Will
     { willTopic   :: T.Text
     , willMessage :: BS.ByteString
     , willQoS     :: Maybe QoS
     , willRetain  :: Bool
     } deriving (Eq, Show)

data Message
   = Connect
     { connectClientIdentifier :: ClientIdentifier
     , connectCleanSession     :: CleanSession
     , connectKeepAlive        :: KeepAlive
     , connectWill             :: Maybe Will
     , connectUsernamePassword :: Maybe (Username, Maybe Password)
     }
   | ConnectAcknowledgement         (Either ConnectionRefusal SessionPresent)
   | Publish
     { publishDuplicate        :: Bool
     , publishRetain           :: Bool
     , publishTopic            :: T.Text
     , publishQoS              :: Maybe (QoS, PacketIdentifier)
     , publishBody             :: LBS.ByteString
     }
   | PublishAcknowledgement       PacketIdentifier
   | PublishReceived              PacketIdentifier
   | PublishRelease               PacketIdentifier
   | PublishComplete              PacketIdentifier
   | Subscribe                    PacketIdentifier [(TopicFilter, Maybe QoS)]
   | SubscribeAcknowledgement     PacketIdentifier [Maybe (Maybe QoS)]
   | Unsubscribe                  PacketIdentifier [TopicFilter]
   | UnsubscribeAcknowledgement   PacketIdentifier
   | PingRequest
   | PingResponse
   | Disconnect
   deriving (Eq, Show)

pMessage :: A.Parser Message
pMessage = do
  h   <- A.anyWord8
  len <- pRemainingLength
  let flags = mod h 0x10
  limit len $ assureEndOfInput $ ($ flags) $ case div h 0x0f of
    0x01 -> pConnect
    0x02 -> pConnectAcknowledgement
    0x03 -> pPublish
    0x04 -> pPublishAcknowledgement
    0x05 -> pPublishReceived
    0x06 -> pPublishRelease
    0x07 -> pPublishComplete
    0x08 -> pSubscribe
    0x09 -> pSubscribeAcknowledgement
    0x10 -> pUnsubscribe
    0x11 -> pUnsubscribeAcknowledgement
    0x0c -> pPingRequest
    0x0d -> pPingResponse
    0x0e -> pDisconnect
    _    -> const $ fail "pMessage: Packet type not implemented."
  where
    assureEndOfInput p = do
      a <- p
      A.endOfInput <|> fail "pMessage: remaining length does not match expectation"
      pure a

pConnect :: Word8 -> A.Parser Message
pConnect hflags
  | hflags /= 0 = fail "pConnect: The header flags are reserved and MUST be set to 0."
  | otherwise   = do
    pProtocolName
    pProtocolLevel
    flags <- pConnectFlags
    keepAlive <- pKeepAlive
    Connect
      <$> pClientIdentifier
      <*> pure (flags .&. 0x02 /= 0)
      <*> pure keepAlive
      <*> pWill flags
      <*> pUsernamePassword flags
  where
    pProtocolName  = A.word8 0x00 >> A.word8 0x04 >> A.word8 0x4d >>
                     A.word8 0x51 >> A.word8 0x54 >> A.word8 0x54 >> pure ()
    pProtocolLevel = A.word8 0x04 >> pure ()
    pConnectFlags  = A.anyWord8
    pKeepAlive     = (\msb lsb-> (fromIntegral msb * 256) + fromIntegral lsb)
                     <$> A.anyWord8 <*> A.anyWord8
    pClientIdentifier = do
      txt <- pUtf8String
      when (T.null txt) $
        fail "pConnect: Client identifier MUST not be empty (in this implementation)."
      return txt
    pWill flags
      | flags .&. 0x04 == 0 = pure Nothing
      | otherwise           = (Just <$>) $  Will
        <$> pUtf8String
        <*> pBlob
        <*> case flags .&. 0x18 of
              0x00 -> pure Nothing
              0x08 -> pure $ Just AtLeastOnce
              0x10 -> pure $ Just ExactlyOnce
              _    -> fail "pConnect: Violation of [MQTT-3.1.2-14]."
        <*> pure (flags .&. 0x20 /= 0)
    pUsernamePassword flags
      | flags .&. 0x80 == 0 = pure Nothing
      | otherwise           = Just <$> ((,) <$> pUtf8String <*> pPassword flags)
    pPassword flags
      | flags .&. 0x40 == 0 = pure Nothing
      | otherwise           = Just <$> pBlob

pConnectAcknowledgement :: Word8 -> A.Parser Message
pConnectAcknowledgement hflags
  | hflags /= 0 = fail "pConnectAcknowledgement: The header flags are reserved and MUST be set to 0."
  | otherwise   = do
    flags <- A.anyWord8
    when (flags .&. 0xfe /= 0) $
      fail "pConnectAcknowledgement: The flags 7-1 are reserved and MUST be set to 0."
    A.anyWord8 >>= f (flags /= 0)
  where
    f sessionPresent returnCode
      | returnCode == 0 = pure $ ConnectAcknowledgement $ Right sessionPresent
      | sessionPresent  = fail "pConnectAcknowledgement: Violation of [MQTT-3.2.2-4]."
      | returnCode <= 5 = pure $ ConnectAcknowledgement $ Left $ toEnum (fromIntegral returnCode - 1)
      | otherwise       = fail "pConnectAcknowledgement: Invalid (reserved) return code."

pPublish :: Word8 -> A.Parser Message
pPublish hflags = Publish
  ( hflags .&. 0x08 /= 0 ) -- duplicate flag
  ( hflags .&. 0x01 /= 0 ) -- retain flag
  <$> pUtf8String
  <*> case hflags .&. 0x6 of
    0x00 -> pure Nothing
    0x02 -> Just . (AtLeastOnce,) <$> pPacketIdentifier
    0x04 -> Just . (ExactlyOnce,) <$> pPacketIdentifier
    _    -> fail "pPublish: Violation of [MQTT-3.3.1-4]."
  <*> A.takeLazyByteString

pPublishAcknowledgement :: Word8 -> A.Parser Message
pPublishAcknowledgement hflags
  | hflags /= 0 = fail "pPubAck: The header flags are reserved and MUST be set to 0."
  | otherwise   = PublishAcknowledgement <$> pPacketIdentifier

pPublishReceived :: Word8 -> A.Parser Message
pPublishReceived hflags
  | hflags /= 0 = fail "pPublishReceived: The header flags are reserved and MUST be set to 0."
  | otherwise   = PublishReceived <$> pPacketIdentifier

pPublishRelease :: Word8 -> A.Parser Message
pPublishRelease hflags
  | hflags /= 2 = fail "pPublishRelease: The header flags are reserved and MUST be set to 2."
  | otherwise   = PublishRelease <$> pPacketIdentifier

pPublishComplete :: Word8 -> A.Parser Message
pPublishComplete hflags
  | hflags /= 0 = fail "pPublishComplete: The header flags are reserved and MUST be set to 0."
  | otherwise   = PublishComplete <$> pPacketIdentifier

pSubscribe :: Word8 -> A.Parser Message
pSubscribe hflags
  | hflags /= 2 = fail "pSubscribe: The header flags are reserved and MUST be set to 2."
  | otherwise = Subscribe
      <$> pPacketIdentifier
      <*> A.many1 pTopicFilter
  where
    pTopicFilter = (,)
      <$> pUtf8String
      <*> ( A.anyWord8 >>= \qos-> case qos of
        0x00 -> pure Nothing
        0x02 -> pure $ Just AtLeastOnce
        0x04 -> pure $ Just ExactlyOnce
        _    -> fail "pSubscribe: Violation of [MQTT-3.8.3-4]." )

pSubscribeAcknowledgement :: Word8 -> A.Parser Message
pSubscribeAcknowledgement hflags
  | hflags /= 0 = fail "pSubscribeAcknowledgement: The header flags are reserved and MUST be set to 0."
  | otherwise   = SubscribeAcknowledgement
      <$> pPacketIdentifier
      <*> A.many1 pReturnCode
  where
    pReturnCode = do
      c <- A.anyWord8
      case c of
        0x00 -> pure $ Just Nothing
        0x01 -> pure $ Just $ Just AtLeastOnce
        0x02 -> pure $ Just $ Just ExactlyOnce
        0x80 -> pure Nothing
        _    -> fail "pSubscribeAcknowledgement: Violation of [MQTT-3.9.3-2]."

pUnsubscribe :: Word8 -> A.Parser Message
pUnsubscribe hflags
  | hflags /= 2 = fail "pUnsubscribe: The header flags are reserved and MUST be set to 2."
  | otherwise   = Unsubscribe <$> pPacketIdentifier <*> A.many1 pUtf8String

pUnsubscribeAcknowledgement :: Word8 -> A.Parser Message
pUnsubscribeAcknowledgement hflags
  | hflags /= 0 = fail "pUnsubscribeAcknowledgement: The header flags are reserved and MUST be set to 0."
  | otherwise   = UnsubscribeAcknowledgement <$> pPacketIdentifier

pPingRequest :: Word8 -> A.Parser Message
pPingRequest hflags
  | hflags /= 0 = fail "pPingRequest: The header flags are reserved and MUST be set to 0."
  | otherwise   = pure PingRequest

pPingResponse :: Word8 -> A.Parser Message
pPingResponse hflags
  | hflags /= 0 = fail "pPingResponse: The header flags are reserved and MUST be set to 0."
  | otherwise   = pure PingResponse

pDisconnect :: Word8 -> A.Parser Message
pDisconnect hflags
  | hflags /= 0 = fail "pDisconnect: The header flags are reserved and MUST be set to 0."
  | otherwise   = pure Disconnect

limit :: Int -> A.Parser a -> A.Parser a
limit  = undefined

pPacketIdentifier :: A.Parser Word16
pPacketIdentifier = do
  msb <- A.anyWord8
  lsb <- A.anyWord8
  pure $  (fromIntegral msb * 256) + fromIntegral lsb
