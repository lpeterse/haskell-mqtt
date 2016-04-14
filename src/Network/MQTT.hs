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

serialize :: Message -> BS.ByteString
serialize p@PUBLISH {} =
  let t = T.encodeUtf8 (msgTopic p)
  in      LBS.toStrict
  $ BS.toLazyByteString
  $ BS.word8 (0x30 .|. flagDuplicate .|. flagQoS .|. flagRetain)
  <> len ( 2 + BS.length topicBS
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
    len :: Int -> BS.Builder
    len i | i < 0x80                = BS.word8    ( fromIntegral i )
          | i < 0x80*0x80           = BS.word16BE ( fromIntegral $ unsafeShiftR ( i .&. 0x7f00     ) 1
                                                                 +              ( i .&. 0x7f       ) )
          | i < 0x80*0x80*0x80      = BS.word16BE ( fromIntegral $ unsafeShiftR ( i .&. 0x7f0000   ) 2
                                                                 + unsafeShiftR ( i .&. 0x7f00     ) 1
                                 ) <> BS.word8    ( fromIntegral                ( i .&. 0x7f       ) )
          | i < 0x80*0x80*0x80*0x80 = BS.word32BE ( fromIntegral $ unsafeShiftR ( i .&. 0x7f000000 ) 3
                                                                 + unsafeShiftR ( i .&. 0x7f0000   ) 2
                                                                 + unsafeShiftR ( i .&. 0x7f00     ) 1
                                                                 +              ( i .&. 0x7f       ) )
          | otherwise     = undefined
    flagQoS        = case msgQoS p of
      AtMostOnce   -> 0x00
      AtLeastOnce  -> 0x02
      ExactlyOnce  -> 0x04
    topicBS        = T.encodeUtf8 $ msgTopic p
    packetid       = case msgQoS p of
      AtMostOnce   -> mempty
      AtLeastOnce  -> BS.word16BE undefined
      ExactlyOnce  -> BS.word16BE undefined

data ConnectionState
   = ConnectionState
     { csOutgoingMailbox  :: Chan BS.ByteString
     , csSubscriptionTree :: SubscriptionTree Message
     }

{-
mqttBroker :: (MonadIO m, MonadThrow m) => ConnectionState -> MQTT m ()
mqttBroker st = fix $ \continue-> do
    mbs <- C.peek
    case mbs of
      Nothing -> throwM EndOfInput
      Just bs -> when (BS.length bs > 0) $ case BS.head bs `div` 16 of
          1  -> do
            connection <- handleConnect (\_ _-> return True )
            liftIO $ print connection
            continue
          3  -> do
            message <- handlePublish
            liftIO $ print message
            liftIO $ print (serialize message)
            liftIO $ writeChan (csOutgoingMailbox st) (serialize message)
            continue
          4  -> liftIO $ print "handlePublishAcknowledgement"
          5  -> liftIO $ print "handlePublishReceived"
          6  -> liftIO $ print "handlePublishRelease"
          7  -> liftIO $ print "handlePublishComplete"
          8  -> do
            handleSubscribe $ \(topic,qos)-> do
              liftIO $ print "SUBSCRIBE"
              _ <- liftIO $ subscribe (csSubscriptionTree st) ( liftIO . (>> print "abc") . writeChan (csOutgoingMailbox st) . serialize ) topic
              return SubscriptionSuccessMaxQoS0
            continue
          9  -> liftIO $ print "handleSubscribeAck"
          10 -> liftIO $ print "handleUnsubscribe"
          11 -> liftIO $ print "handleUnsubscribeAck"
          12 -> handlePingRequest >> continue
          13 -> liftIO $ print "handlePingResponse"
          14 -> do
            handleDisconnect
            liftIO $ print "Graceful DISCONNECT"
          _  -> throwM $ ProtocolViolation "Unacceptable Command"



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
