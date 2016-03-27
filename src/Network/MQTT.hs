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

import Control.Exception
import Control.Monad.Catch (MonadThrow (..))

import qualified Data.ByteString as BS
import qualified Data.Conduit as C
import qualified Data.Conduit.Binary as C
import Data.Word
import Data.Typeable

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

connect :: MQTT m a
connect = undefined

remainingLength :: MonadThrow m => MQTT m Int
remainingLength = do
  mb0 <- C.head
  case mb0 of
    Nothing -> protocolViolation
    Just b0 -> if b0 < 128
      then return (fromIntegral b0)
      else do
        mb1 <- C.head
        case mb1 of
          Nothing -> protocolViolation
          Just b1 -> if b1 < 128
            then return (fromIntegral b1 * 128 + fromIntegral b0)
            else do
              mb2 <- C.head
              case mb2 of
                Nothing -> protocolViolation
                Just b2 -> if b2 < 128
                  then return (fromIntegral b2 * 128 * 128 + fromIntegral b1 * 128 + fromIntegral b0)
                  else do
                    mb3 <- C.head
                    case mb3 of
                      Nothing -> protocolViolation
                      Just b3 -> if b3 < 128
                        then return (fromIntegral b3 * 128*128*128 + fromIntegral b2 *128*128 + fromIntegral b1 * 128 + fromIntegral b0)
                        else protocolViolation
  where
    protocolViolation = throwM $ ProtocolViolation "Malformed Remaining Length"

data MQTTException
   = ProtocolViolation String
   deriving (Eq, Ord, Show, Typeable)

instance Exception MQTTException
