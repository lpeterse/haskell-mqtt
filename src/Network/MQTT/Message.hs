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
     } deriving (Eq, Show)

data Message
   = CONNECT
     { connectClientIdentifier :: T.Text
     , connectUsernamePassword :: Maybe (T.Text, Maybe BS.ByteString)
     , connectCleanSession     :: Bool
     , connectKeepAlive        :: Word16
     , connectWill             :: Maybe Will
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
pConnect flags len
  | otherwise = undefined

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

{-
handleConnect :: (MonadIO m, MonadThrow m) => (Maybe Username -> Maybe Password -> m Bool) -> MQTT m Connection
handleConnect authorize = do
  header          <- maybe rejectEof return =<< C.head
  remainingLength <- getRemainingLength
  C.isolate remainingLength C.=$= do
    mapM_ (expect rejectUnacceptableProtocolName) expectedProtocolName
    expect rejectUnacceptableProtocolVersion expectedProtocolVersion
    flags <- maybe rejectEof return =<< C.head
    keepAlive    <- getWord16BeDefault       rejectEof
    clientId     <- getStringDefault         rejectUnacceptableIdentifier
    mWillTopic   <- if flagWill .&. flags /= 0
      then Just <$> getStringDefault        ( reject "Bad Will Topic")
      else return Nothing
    mWillMessage <- if flagWill .&. flags /= 0
      then Just <$> getBlobDefault          ( reject "Bad Will Message")
      else return Nothing
    mUsername    <- if flagUsername .&. flags /= 0
      then Just <$> getStringDefault         rejectUnacceptableUsernamePassword
      else return Nothing
    mPassword    <- if flagPassword .&. flags /= 0
      then Just <$> getBlobDefault           rejectUnacceptableUsernamePassword
      else return Nothing
    isAuthorized <- lift $ authorize mUsername mPassword
    unless isAuthorized rejectUnauthorized
    acceptConnection
    return $ Connection clientId mWillTopic mWillMessage  mUsername mPassword keepAlive flags
  where
    expectedProtocolName    = [0x00, 0x04, 0x4d, 0x51, 0x54, 0x54]
    expectedProtocolVersion = 0x04
    expect handleUnexpected expected =
      maybe handleUnexpected (\actual-> when (actual /= expected) handleUnexpected) =<< C.head
    flagUsername     = 128
    flagPassword     = 64
    flagWillRetain   = 32
    flagWillQoS      = 8 + 16
    flagWill         = 4
    flagCleanSession = 2
    reject reason =
      throwM (ProtocolViolation reason)
    rejectEof =
      reject "Unexpected End Of Input"
    rejectUnacceptableProtocolName =
      reject "Unacceptable Protocol Name"
    rejectUnacceptableProtocolVersion = do
      sendAcknowledgement 0x01
      reject "Unacceptable Protocol Version"
    rejectUnacceptableIdentifier = do
      sendAcknowledgement 0x02
      reject "Unacceptable Identifier"
    rejectUnacceptableUsernamePassword = do
      sendAcknowledgement 0x04
      reject "Unacceptable Username/Password"
    rejectUnauthorized = do
      sendAcknowledgement 0x05
      reject "Unauthorized"
    acceptConnection =
      sendAcknowledgement 0x00
    sendAcknowledgement responseCode =
      C.yield $ LBS.toStrict
              $ BS.toLazyByteString
              $ BS.word32BE (0x20020000 .|. sessionPresent .|. responseCode)
      where
        sessionPresent = 0 -- FIXE: session present flag

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
