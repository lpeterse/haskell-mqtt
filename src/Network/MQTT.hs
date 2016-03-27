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
import Control.Exception
import Control.Monad.Catch (MonadThrow (..))
import Control.Monad
import Control.Monad.Trans.Class (lift)

import Data.Bits ((.&.), (.|.))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Conduit as C
import qualified Data.Conduit.Binary as C
import qualified Data.Text as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.Encoding as LT
import Data.Word
import Data.Typeable

type Username = T.Text
type Password = BS.ByteString

type MQTT m a = C.ConduitM BS.ByteString BS.ByteString m a


newtype Header = Header Word16

data Message = Message

data ControlPacketType
   = Connect
   | ConnectAcknowledgement
   | Publish
   | PublishAcknowledgement
   | PublishReceived
   | PublishRelease
   | PublishComplete
   | Subscribe
   | SubscribeAcknowledgement
   | Unsubscribe
   | UnsubscribeAcknowledgement
   | PingRequest
   | PingResponse
   | Disconnect

receive :: MonadThrow m => MQTT m ()
receive = do
  mctl <- C.head
  case mctl of
    Nothing  -> throwM EndOfInput
    Just ctl -> case ctl `div` 16 of
      1  -> handleConnect
      2  -> handleConnectAcknowledgement
{-    3  -> handlePublish
      4  -> handlePublishAcknowledgement
      5  ->
      6  ->
      7  ->
      8  ->
      9  ->
      10 ->
      11 ->
      12 ->
      13 ->
      14 -> -}
      _  -> undefined
  where
    handleConnectAcknowledgement = undefined

getRemainingLength :: MonadThrow m => MQTT m Int
getRemainingLength = do
  mb0 <- C.head
  case mb0 of
    Nothing -> protocolViolation
    Just b0 -> if b0 < 128
      then return $ fromIntegral b0
      else do
        mb1 <- C.head
        case mb1 of
          Nothing -> protocolViolation
          Just b1 -> if b1 < 128
            then return $ fromIntegral b1 * 128 +
                          fromIntegral b0
            else do
              mb2 <- C.head
              case mb2 of
                Nothing -> protocolViolation
                Just b2 -> if b2 < 128
                  then return $ fromIntegral b2 * 128 * 128 +
                                fromIntegral b1 * 128 +
                                fromIntegral b0
                  else do
                    mb3 <- C.head
                    case mb3 of
                      Nothing -> protocolViolation
                      Just b3 -> if b3 < 128
                        then return $ fromIntegral b3 * 128*128*128 +
                                      fromIntegral b2 * 128*128 +
                                      fromIntegral b1 * 128 +
                                      fromIntegral b0
                        else protocolViolation
  where
    protocolViolation = throwM $ ProtocolViolation "Malformed Remaining Length"



handleConnect :: MonadThrow m => (Maybe Username -> Maybe Password -> m Bool) -> MQTT m ()
handleConnect authorize = do
  C.isolate =<< getRemainingLength
  mapM_ (expect rejectUnacceptableProtocolName) expectedProtocolName
  expect rejectUnacceptableProtocolVersion expectedProtocolVersion
  connectFlags <- maybe handleEof return =<< C.head
  keepAlive    <- getWord16BeDefault      ( reject "Bad Keep Alive")
  clientId     <- getStringDefault         rejectUnacceptableIdentifier
  mWillTopic   <- if flagWill .&. connectFlags /= 0
    then Just <$> getStringDefault        ( reject "Bad Will Topic")
    else return Nothing
  mWillMessage <- if flagWill .&. connectFlags /= 0
    then Just <$> getBlobDefault          ( reject "Bad Will Message")
    else return Nothing
  mUsername    <- if flagUsername .&. connectFlags /= 0
    then Just <$> getStringDefault         rejectUnacceptableUsernamePassword
    else return Nothing
  mPassword    <- if flagPassword .&. connectFlags /= 0
    then Just <$> getBlobDefault           rejectUnacceptableUsernamePassword
    else return Nothing
  isAuthorized <- lift $ authorize mUsername mPassword
  unless isAuthorized rejectUnauthorized
  where
    expectedProtocolName    = [0x00, 0x04, 0xd4, 0x51, 0x54, 0x54]
    expectedProtocolVersion = 0x04
    expect handleUnexpected expected =
      maybe handleEof (\actual-> when (actual /= expected) handleUnexpected) =<< C.head
    flagUsername     = 128
    flagPassword     = 64
    flagWillRetain   = 32
    flagWillQoS      = 8 + 16
    flagWill         = 4
    flagCleanSession = 2
    reject reason =
      throwM (ProtocolViolation reason)
    rejectUnacceptableProtocolName =
      reject "Unacceptable Protocol Name"
    rejectUnacceptableProtocolVersion = do
      sendConnectAcknowledgement 0x01
      reject "Unacceptable Protocol Version"
    rejectUnacceptableIdentifier = do
      sendConnectAcknowledgement 0x02
      reject "Unacceptable Identifier"
    rejectUnacceptableUsernamePassword = do
      sendConnectAcknowledgement 0x04
      reject "Unacceptable Username/Password"
    rejectUnauthorized = do
      sendConnectAcknowledgement 0x05
      reject "Unauthorized"

handleEof :: MonadThrow m => MQTT m a
handleEof =
  throwM $ ProtocolViolation "Unexpected End Of Input"

sendConnectAcknowledgement :: Int -> MQTT m ()
sendConnectAcknowledgement =
  undefined

getWord16BeDefault :: (Monad m, Num a) => MQTT m a -> MQTT m a
getWord16BeDefault def = (+)
  <$> ( maybe def (return . (*256) . fromIntegral) =<< C.head )
  <*> ( maybe def (return .          fromIntegral) =<< C.head )

getBlobDefault   :: Monad m => MQTT m BS.ByteString -> MQTT m BS.ByteString
getBlobDefault   def = LBS.toStrict <$> (getWord16BeDefault (def >> return 0) >>= C.take)

getStringDefault :: Monad m => MQTT m T.Text -> MQTT m T.Text
getStringDefault def = getWord16BeDefault (def >> return 0) >>= C.take >>= parse
 where
   parse = either
     ( const def )
     ( return . LT.toStrict ) . LT.decodeUtf8'

data MQTTException
   = EndOfInput
   | ProtocolViolation String
   deriving (Eq, Ord, Show, Typeable)

instance Exception MQTTException
