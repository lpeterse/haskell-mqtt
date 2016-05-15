{-# LANGUAGE TupleSections, GeneralizedNewtypeDeriving #-}
module Network.MQTT.Message
  ( ClientIdentifier (..)
  , SessionPresent
  , CleanSession
  , Retain
  , KeepAlive
  , Username
  , Password
  , TopicFilter
  , Topic (..)
  , Payload
  , PacketIdentifier (..)
  , QoS (..)
  , QualityOfService (..)
  , ConnectionRefusal (..)
  , Will (..)
  , RawMessage (..)
  , bRawMessage
  , pRawMessage ) where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad.Catch (MonadThrow (..))
import Control.Monad

import Data.Monoid
import Data.Bits
import Data.Function (fix)
import Data.String
import Data.Word
import Data.Typeable
import qualified Data.Serialize.Get as SG
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.Encoding as LT

import Network.MQTT.Message.RemainingLength
import Network.MQTT.Message.Utf8String

newtype ClientIdentifier = ClientIdentifier T.Text
  deriving (Eq, Ord, Show, IsString)

type SessionPresent   = Bool
type CleanSession     = Bool
type Retain           = Bool
type KeepAlive        = Word16
type Username         = T.Text
type Password         = BS.ByteString
type TopicFilter      = T.Text
type Payload          = BS.ByteString

newtype Topic            = Topic BS.ByteString
  deriving (Eq, Ord, Show)

newtype PacketIdentifier = PacketIdentifier Int
  deriving (Eq, Show)

data QualityOfService
   = AtLeastOnce
   | ExactlyOnce
   deriving (Eq, Ord, Show, Enum)

data QoS
  = QoS0
  | QoS1
  | QoS2
  deriving (Eq, Ord, Show)

data ConnectionRefusal
   = UnacceptableProtocolVersion
   | IdentifierRejected
   | ServerUnavailable
   | BadUsernameOrPassword
   | NotAuthorized
   deriving (Eq, Ord, Show, Enum, Bounded)

data Will
   = Will
     { willTopic   :: T.Text
     , willMessage :: BS.ByteString
     , willQoS     :: Maybe QualityOfService
     , willRetain  :: Bool
     } deriving (Eq, Show)

data RawMessage
   = Connect
     { connectClientIdentifier :: ClientIdentifier
     , connectCleanSession     :: CleanSession
     , connectKeepAlive        :: KeepAlive
     , connectWill             :: Maybe Will
     , connectUsernamePassword :: Maybe (Username, Maybe Password)
     }
   | ConnectAcknowledgement         (Either ConnectionRefusal SessionPresent)
   | Publish
     { publishDuplicate        :: !Bool
     , publishRetain           :: !Bool
     , publishTopic            :: !Topic
     , publishQoS              :: !(Maybe (QualityOfService, PacketIdentifier))
     , publishBody             :: !BS.ByteString
     }
   | PublishAcknowledgement       PacketIdentifier
   | PublishReceived              PacketIdentifier
   | PublishRelease               PacketIdentifier
   | PublishComplete              PacketIdentifier
   | Subscribe                    PacketIdentifier [(TopicFilter, QoS)]
   | SubscribeAcknowledgement     PacketIdentifier [Maybe QoS]
   | Unsubscribe                  PacketIdentifier [TopicFilter]
   | UnsubscribeAcknowledgement   PacketIdentifier
   | PingRequest
   | PingResponse
   | Disconnect
   deriving (Eq, Show)


bBlob :: BS.ByteString -> BS.Builder
bBlob bs = BS.word16BE (fromIntegral $ BS.length bs) <> BS.byteString bs
{-# INLINE bBlob #-}

pRawMessage :: SG.Get RawMessage
pRawMessage = do
  SG.lookAhead SG.getWord8 >>= \h-> case h .&. 0xf0 of
--    0x10 -> pConnect                     flags
    0x20 -> pConnectAcknowledgement
    0x30 -> pPublish
    0x40 -> pPublishAcknowledgement
    0x50 -> pPublishReceived
--    0x60 -> pPublishRelease
    0x70 -> pPublishComplete
--    0x80 -> pSubscribe
    0x90 -> pSubscribeAcknowledgement
--    0xa0 -> pUnsubscribe
--    0xb0 -> pUnsubscribeAcknowledgement
--    0xc0 -> pPingRequest
--    0xd0 -> pPingResponse
--    0xe0 -> pDisconnect
    _    -> fail "pRawMessage: Packet type not implemented."

{-
pConnect :: A.Parser RawMessage
pConnect = do
    hflags <- (.&. 0x0f) <$> A.anyWord8
    when (hflags /= 0) (fail "pConnect: The header flags are reserved and MUST be set to 0.")
    len    <- pRemainingLength
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
    pClientIdentifier = ClientIdentifier <$> do
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
-}

pConnectAcknowledgement :: SG.Get RawMessage
pConnectAcknowledgement = do
  _ <- SG.getWord32be
  pure $ ConnectAcknowledgement $ Right False

{-
    hflags <- (.&. 0x0f) <$> SG.getWord8
    when (hflags /= 0) (fail "pConnectAcknowledgement: The header flags are reserved and MUST be set to 0.")
    len   <- fromIntegral <$> SG.getWord8
    flags <- SG.getWord8
    when (flags .&. 0xfe /= 0) $
      fail "pConnectAcknowledgement: The flags 7-1 are reserved and MUST be set to 0."
    SG.getWord8 >>= f (flags /= 0)
  where
    f sessionPresent returnCode
      | returnCode == 0 = pure $ ConnectAcknowledgement $ Right sessionPresent
      | sessionPresent  = fail "pConnectAcknowledgement: Violation of [MQTT-3.2.2-4]."
      | returnCode <= 5 = pure $ ConnectAcknowledgement $ Left $ toEnum (fromIntegral returnCode - 1)
      | otherwise       = fail "pConnectAcknowledgement: Invalid (reserved) return code."
-}

pPublish :: SG.Get RawMessage
pPublish = do
  hflags <- (.&. 0x0f) <$> SG.getWord8
  len    <- pRemLen
  tlen   <- fromIntegral <$> SG.getWord16be
  blen   <- pure (len - 2 - tlen)
  Publish
    ( hflags .&. 0x08 /= 0 ) -- duplicate flag
    ( hflags .&. 0x01 /= 0 ) -- retain flag
    <$> ( Topic <$> SG.getByteString tlen )
    <*> case hflags .&. 0x06 of
      0x00 -> pure Nothing
      0x02 -> Just . (AtLeastOnce,) . PacketIdentifier . fromIntegral <$> SG.getWord16be
      0x04 -> Just . (ExactlyOnce,) . PacketIdentifier . fromIntegral <$> SG.getWord16be
      _    -> fail "pPublish: Violation of [MQTT-3.3.1-4]."
    <*> ( SG.getByteString blen )

pRemLen :: SG.Get Int
pRemLen  = do
  b0 <- fromIntegral <$> SG.getWord8
  if b0 < 128
    then pure b0
    else do
      b1 <- fromIntegral <$> SG.getWord8
      pure $ (b1 * 128) + (b0 .|. 127)

pPublishAcknowledgement :: SG.Get RawMessage
pPublishAcknowledgement = do
    w32 <- SG.getWord32be
    pure $ PublishAcknowledgement $ PacketIdentifier $ fromIntegral $ w32 .&. 0xffff

pPublishReceived :: SG.Get RawMessage
pPublishReceived = do
    w32 <- SG.getWord32be
    pure $ PublishReceived $ PacketIdentifier $ fromIntegral $ w32 .&. 0xffff

pPublishRelease :: SG.Get RawMessage
pPublishRelease = do
    w32 <- SG.getWord32be
    pure $ PublishRelease $ PacketIdentifier $ fromIntegral $ w32 .&. 0xffff

pPublishComplete :: SG.Get RawMessage
pPublishComplete = do
    w32 <- SG.getWord32be
    pure $ PublishComplete $ PacketIdentifier $ fromIntegral $ w32 .&. 0xffff

{-
pSubscribe :: Int -> Word8 -> A.Parser RawMessage
pSubscribe len hflags
  | hflags /= 2 = fail "pSubscribe: The header flags are reserved and MUST be set to 2."
  | otherwise = do
      stop <- (+ len) <$> pPosition
      Subscribe
        <$> pPacketIdentifier
        <*> pManyWithLimit (len - 2) pTopicFilter
  where
    pTopicFilter = (,)
      <$> pUtf8String
      <*> ( A.anyWord8 >>= \qos-> case qos of
        0x00 -> pure QoS0
        0x01 -> pure QoS1
        0x02 -> pure QoS2
        _    -> fail $ "pSubscribe: Violation of [MQTT-3.8.3-4]." ++ show qos )
-}

pSubscribeAcknowledgement :: SG.Get RawMessage
pSubscribeAcknowledgement = do
  _   <- SG.getWord8
  len <- fromIntegral <$> SG.getWord8
  pid <- PacketIdentifier . fromIntegral <$> SG.getWord16be
  SubscribeAcknowledgement pid <$> (map f . BS.unpack <$> SG.getBytes (len - 2))
  where
    f 0x00 = Just QoS0
    f 0x01 = Just QoS1
    f 0x02 = Just QoS2
    f    _ = Nothing

{-
pUnsubscribe :: Int -> Word8 -> A.Parser RawMessage
pUnsubscribe len hflags
  | hflags /= 2 = fail "pUnsubscribe: The header flags are reserved and MUST be set to 2."
  | otherwise   = Unsubscribe <$> pPacketIdentifier <*> pManyWithLimit (len - 2) pUtf8String

pUnsubscribeAcknowledgement :: Int -> Word8 -> A.Parser RawMessage
pUnsubscribeAcknowledgement len hflags
  | hflags /= 0 = fail "pUnsubscribeAcknowledgement: The header flags are reserved and MUST be set to 0."
  | otherwise   = UnsubscribeAcknowledgement <$> pPacketIdentifier

pPingRequest :: Int -> Word8 -> A.Parser RawMessage
pPingRequest len hflags
  | hflags /= 0 = fail "pPingRequest: The header flags are reserved and MUST be set to 0."
  | otherwise   = pure PingRequest

pPingResponse :: Int -> Word8 -> A.Parser RawMessage
pPingResponse len hflags
  | hflags /= 0 = fail "pPingResponse: The header flags are reserved and MUST be set to 0."
  | otherwise   = pure PingResponse

pDisconnect :: Int -> Word8 -> A.Parser RawMessage
pDisconnect len hflags
  | hflags /= 0 = fail "pDisconnect: The header flags are reserved and MUST be set to 0."
  | otherwise   = pure Disconnect
-}

bRawMessage :: RawMessage -> BS.Builder
bRawMessage (Connect (ClientIdentifier i) cleanSession keepAlive will credentials) =
  BS.word8 0x10
    <> BS.word8 (fromIntegral len)
    <> BS.word64BE ( 0x00044d5154540400 .|. f1 .|. f2 .|. f3 )
    <> BS.word16BE keepAlive
    <> bUtf8String i
    <> maybe mempty (\(Will t m _ _)-> bUtf8String t <> bBlob m) will
    <> maybe mempty (\(u,mp)-> bUtf8String u <> maybe mempty bBlob mp) credentials
  where
    f1 = case credentials of
      Nothing                                  -> 0x00
      Just (_, Nothing)                        -> 0x80
      Just (_, Just _)                         -> 0xc0
    f2 = case will of
      Nothing                                  -> 0x00
      Just (Will _ _ Nothing False)            -> 0x04
      Just (Will _ _ Nothing True)             -> 0x24
      Just (Will _ _ (Just AtLeastOnce) False) -> 0x0c
      Just (Will _ _ (Just AtLeastOnce) True)  -> 0x2c
      Just (Will _ _ (Just ExactlyOnce) False) -> 0x14
      Just (Will _ _ (Just ExactlyOnce) True)  -> 0x34
    f3 = if cleanSession then 0x02 else 0x00
    len = 12
      + BS.length ( T.encodeUtf8 i )
      + maybe 0 ( \(Will t m _ _)-> 4 + BS.length (T.encodeUtf8 t) + BS.length m ) will
      + maybe 0 ( \(u,mp)->
          2 + BS.length ( T.encodeUtf8 u ) + maybe 0 ( (2 +) . BS.length ) mp
        ) credentials

bRawMessage (ConnectAcknowledgement crs) =
  BS.word32BE $ 0x20020000 .|. case crs of
    Left cr -> fromIntegral $ fromEnum cr + 1
    Right s -> if s then 0x0100 else 0

bRawMessage (Publish d r (Topic t) mqp b) =
  case mqp of
    Nothing ->
      let len = 2 + BS.length t + BS.length b
          h   = fromIntegral $ ( if r then 0x31000000 else 0x30000000 )
                .|. ( len `unsafeShiftL` 16 )
                .|. ( BS.length t )
      in if len < 128
        then BS.word32BE h
          <> BS.byteString t
          <> BS.byteString b
        else BS.word8 ( if r then 0x31 else 0x30 )
          <> bRemainingLength len
          <> BS.word16BE ( fromIntegral $ BS.length t )
          <> BS.byteString t
          <> BS.byteString b
    Just (q, PacketIdentifier p) ->
      let len = 4 + BS.length t + BS.length b
      in BS.word8 ( 0x30
        .|. ( if d then 0x08 else 0 )
        .|. ( if r then 0x01 else 0 )
        .|. case q of
            AtLeastOnce -> 0x02
            ExactlyOnce -> 0x04
        )
      <> bRemainingLength len
      <> BS.word16BE ( fromIntegral $ BS.length t )
      <> BS.byteString t
      <> BS.word16BE (fromIntegral p)
      <> BS.byteString b

bRawMessage (PublishAcknowledgement (PacketIdentifier p)) =
  BS.word32BE $ fromIntegral $ 0x40020000 .|. p
bRawMessage (PublishReceived (PacketIdentifier p)) =
  BS.word32BE $ fromIntegral $ 0x50020000 .|. p
bRawMessage (PublishRelease (PacketIdentifier p)) =
  BS.word32BE $ fromIntegral $ 0x62020000 .|. p
bRawMessage (PublishComplete (PacketIdentifier p)) =
  BS.word32BE $ fromIntegral $ 0x70020000 .|. p
bRawMessage (Subscribe (PacketIdentifier p) tf)  =
  BS.word8 0x82 <> bRemainingLength len <> BS.word16BE (fromIntegral p) <> mconcat ( map f tf )
  where
    f (t, q) = (bUtf8String t <>) $ BS.word8 $ case q of
      QoS0 -> 0x00
      QoS1 -> 0x01
      QoS2 -> 0x02
    len  = 2 + length tf * 3 + sum ( map (BS.length . T.encodeUtf8 . fst) tf )
bRawMessage (SubscribeAcknowledgement (PacketIdentifier p) rcs) =
  BS.word8 0x90 <> bRemainingLength (2 + length rcs)
    <> BS.word16BE (fromIntegral p) <> mconcat ( map ( BS.word8 . f ) rcs )
  where
    f Nothing     = 0x80
    f (Just QoS0) = 0x00
    f (Just QoS1) = 0x01
    f (Just QoS2) = 0x02
bRawMessage (Unsubscribe (PacketIdentifier p) tfs) =
  BS.word8 0xa2 <> bRemainingLength len
    <> BS.word16BE (fromIntegral p) <> mconcat ( map bUtf8String tfs )
  where
    bfs = map T.encodeUtf8 tfs
    len = 2 + sum ( map ( ( + 2 ) . BS.length ) bfs )
bRawMessage (UnsubscribeAcknowledgement (PacketIdentifier p)) =
  BS.word16BE 0xb002 <> BS.word16BE (fromIntegral p)
bRawMessage PingRequest =
  BS.word16BE 0xc000
bRawMessage PingResponse =
  BS.word16BE 0xd000
bRawMessage Disconnect =
  BS.word16BE 0xe000
