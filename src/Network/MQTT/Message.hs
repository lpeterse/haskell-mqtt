module Network.MQTT.Message
  ( Message (..)
  , QoS (..)
  , pRemainingLength
  , sRemainingLength
  )
where

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

import Network.MQTT.Message.RemainingLength
import Network.MQTT.Message.Utf8String

data QoS
   = AtMostOnce
   | AtLeastOnce
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
     , willQoS     :: QoS
     , willRetain  :: Bool
     } deriving (Eq, Show)

data Message
   = CONNECT
     { connectClientIdentifier :: T.Text
     , connectCleanSession     :: Bool
     , connectKeepAlive        :: Word16
     , connectWill             :: Maybe Will
     , connectUsernamePassword :: Maybe (T.Text, Maybe BS.ByteString)
     }
   | CONNACK
     { connack                 :: Either ConnectionRefusal Bool
     }
   | PUBLISH
     { msgTopic      :: T.Text
     , msgQoS        :: QoS
     , msgBody       :: LBS.ByteString
     , msgDuplicate  :: Bool
     , msgRetain     :: Bool
     } deriving (Eq, Show)

pMessage :: A.Parser Message
pMessage = do
  h   <- A.anyWord8
  len <- pRemainingLength
  let flags = mod h 0x10
  case div h 0x0f of
    0x01 -> pConnect flags len
    0x02 -> pConnAck flags len
    _    -> fail "pMessage: packet type not implemented"

pConnect :: Word8 -> Int -> A.Parser Message
pConnect hflags len
  | hflags /= 0 = fail "pConnect: The header flags are reserved and MUST be set to 0."
  | otherwise   = do
    pProtocolName
    pProtocolLevel
    flags <- pConnectFlags
    keepAlive <- pKeepAlive
    CONNECT
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
              0x00 -> pure AtMostOnce
              0x08 -> pure AtLeastOnce
              0x10 -> pure ExactlyOnce
              _    -> fail "pConnect: Violation of [MQTT-3.1.2-14]."
        <*> pure (flags .&. 0x20 /= 0)
    pUsernamePassword flags
      | flags .&. 0x80 == 0 = pure Nothing
      | otherwise           = Just <$> ((,) <$> pUtf8String <*> pPassword flags)
    pPassword flags
      | flags .&. 0x40 == 0 = pure Nothing
      | otherwise           = Just <$> pBlob

pConnAck :: Word8 -> Int -> A.Parser Message
pConnAck hflags len
  | len    /= 2 = fail "pConnack: The remaining length field MUST be set to 2."
  | hflags /= 0 = fail "pConnack: The header flags are reserved and MUST be set to 0."
  | otherwise   = do
    flags <- A.anyWord8
    when (flags .&. 0xfe /= 0) $
      fail "pConnack: The flags 7-1 are reserved and MUST be set to 0."
    A.anyWord8 >>= f (flags /= 0)
  where
    f sessionPresent returnCode
      | returnCode == 0 = pure $ CONNACK $ Right sessionPresent
      | sessionPresent  = fail "pConnack: Violation of [MQTT-3.2.2-4]."
      | returnCode <= 5 = pure $ CONNACK $ Left $ toEnum (fromIntegral returnCode - 1)
      | otherwise       = fail "pConnack: Invalid (reserved) return code."

pBlob :: A.Parser BS.ByteString
pBlob = do
  msb <- A.anyWord8
  lsb <- A.anyWord8
  let len = (fromIntegral msb * 256) + fromIntegral lsb :: Int
  A.take len

{-

handlePublish :: (MonadThrow m, MonadIO m) => MQTT m Message
handlePublish = do
  header          <- maybe rejectEof return =<< C.head
  remainingLength <- getRemainingLength
  message         <- C.isolate remainingLength C.=$= PUBLISH
    <$> getStringDefault rejectEof
    <*> case header .&. flagQoS of
          0 -> return AtMostOnce
          2 -> AtLeastOnce <$> getWord16BeDefault (reject "")
          4 -> ExactlyOnce <$> getWord16BeDefault (reject "")
          _ -> reject "Invalid QoS Level"
    -- Read the rest of the _isolated_ message
    <*> fmap LBS.fromChunks C.consume
    <*> return (header .&. flagDuplicate /= 0)
    <*> return (header .&. flagRetain /= 0)
  -- The message has been received. Acknowledge it if required.
  case msgQoS message of
    AtMostOnce             -> return ()
    AtLeastOnce idenfifier -> C.yield
      $ LBS.toStrict
      $ BS.toLazyByteString
      $ BS.word32BE (0x40020000 .|. fromIntegral idenfifier)
    ExactlyOnce identifier -> return () -- FIXME: Send PubRec
  return message
  where
    flagRetain    = 0x01
    flagQoS       = 0x02 + 0x04
    flagDuplicate = 0x08
    reject reason =
      throwM (ProtocolViolation reason)
    rejectEof =
      reject "Unexpected End Of Input"

-}
