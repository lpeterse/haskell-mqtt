{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TupleSections              #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Message
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
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
  , PublishQoS (..)
  , Will (..)
  , RawMessage (..)
  , bRawMessage
  , pRawMessage
  , pRemainingLength
  , bRemainingLength
  , pUtf8String
  , bUtf8String ) where

import           Control.Monad
import           Data.Bits
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Builder as BS
import           Data.Monoid
import qualified Data.Serialize.Get      as SG
import           Data.String
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import           Data.Word

newtype ClientIdentifier = ClientIdentifier T.Text
  deriving (Eq, Ord, Show, IsString)

type SessionPresent   = Bool
type CleanSession     = Bool
type Retain           = Bool
type Duplicate        = Bool
type KeepAlive        = Word16
type Username         = T.Text
type Password         = BS.ByteString
type TopicFilter      = T.Text
type Payload          = BS.ByteString

newtype Topic            = Topic BS.ByteString
  deriving (Eq, Ord, Show, IsString)

newtype PacketIdentifier = PacketIdentifier Int
  deriving (Eq, Ord, Show)

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

data PublishQoS
   = PublishQoS0
   | PublishQoS1 !Duplicate !PacketIdentifier
   | PublishQoS2            !PacketIdentifier
   deriving (Eq, Ord, Show)

data RawMessage
   = Connect
     { connectClientIdentifier :: !ClientIdentifier
     , connectCleanSession     :: !CleanSession
     , connectKeepAlive        :: !KeepAlive
     , connectWill             :: !(Maybe Will)
     , connectCredentials      :: !(Maybe (Username, Maybe Password))
     }
   | ConnectAcknowledgement         (Either ConnectionRefusal SessionPresent)
   | Publish
     { publishDuplicate :: !Bool
     , publishRetain    :: !Bool
     , publishTopic     :: !Topic
     , publishQoS       :: !PublishQoS
     , publishBody      :: !BS.ByteString
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
    0x10 -> pConnect
    0x20 -> pConnectAcknowledgement
    0x30 -> pPublish
    0x40 -> pPublishAcknowledgement
    0x50 -> pPublishReceived
    0x60 -> pPublishRelease
    0x70 -> pPublishComplete
    0x80 -> pSubscribe
    0x90 -> pSubscribeAcknowledgement
    0xa0 -> pUnsubscribe
    0xb0 -> pUnsubscribeAcknowledgement
    0xc0 -> pPingRequest
    0xd0 -> pPingResponse
    0xe0 -> pDisconnect
    _    -> fail "pRawMessage: Packet type not implemented."

pConnect :: SG.Get RawMessage
pConnect = do
  h    <- SG.getWord8
  when (h .&. 0x0f /= 0) $
    fail "pConnect: The header flags are reserved and MUST be set to 0."
  rlen <- pRemainingLength
  y    <- SG.getWord64be
  when (y .&. 0xffffffffffffff00 /= 0x00044d5154540400) $
    fail "pConnect: Unexpected protocol initialization."
  let cleanSession = y .&. 0x02 /= 0
  keepAlive <- SG.getWord16be
  clientId  <- ClientIdentifier <$> pUtf8String
  will      <- if y .&. 0x04 == 0
    then pure Nothing
    else Just <$> ( Will
      <$> pUtf8String
      <*> ( SG.getWord16be >>= SG.getByteString . fromIntegral )
      <*> case y .&. 0x18 of
            0x00 -> pure Nothing
            0x08 -> pure $ Just AtLeastOnce
            0x10 -> pure $ Just ExactlyOnce
            _    -> fail "pConnect: Violation of [MQTT-3.1.2-14]."
      <*> pure ( y .&. 0x20 /= 0 ) )
  userpass  <- if y .&. 0x80 == 0
    then pure Nothing
    else Just <$> ( (,)
      <$> pUtf8String
      <*> if y .&. 0x40 == 0
            then pure Nothing
            else Just <$> (SG.getWord16be >>= SG.getByteString . fromIntegral) )
  pure ( Connect clientId cleanSession keepAlive will userpass )

pConnectAcknowledgement :: SG.Get RawMessage
pConnectAcknowledgement = do
  x <- SG.getWord32be
  ConnectAcknowledgement <$> case x .&. 0xff of
    0 -> pure $ Right (x .&. 0x0100 /= 0)
    1 -> pure $ Left UnacceptableProtocolVersion
    2 -> pure $ Left IdentifierRejected
    3 -> pure $ Left ServerUnavailable
    4 -> pure $ Left BadUsernameOrPassword
    5 -> pure $ Left NotAuthorized
    _ -> fail "pConnectAcknowledgement: Invalid (reserved) return code."

pPublish :: SG.Get RawMessage
pPublish = do
  hflags <- SG.getWord8
  let dup = hflags .&. 0x08 /= 0 -- duplicate flag
  let ret = hflags .&. 0x01 /= 0 -- retain flag
  rlen   <- pRemainingLength
  tlen   <- fromIntegral <$> SG.getWord16be
  t      <- Topic <$> SG.getByteString tlen
  qos    <- case hflags .&. 0x06 of
    0x00 -> pure PublishQoS0
    0x02 -> PublishQoS1 dup . PacketIdentifier . fromIntegral <$> SG.getWord16be
    0x04 -> PublishQoS2     . PacketIdentifier . fromIntegral <$> SG.getWord16be
    _    -> fail "pPublish: Violation of [MQTT-3.3.1-4]."
  p      <- case qos of
    PublishQoS0 -> SG.getByteString ( rlen - tlen - 2 )
    _           -> SG.getByteString ( rlen - tlen - 4 )
  pure $ Publish dup ret t qos p

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

pSubscribe :: SG.Get RawMessage
pSubscribe = do
  _    <- SG.getWord8
  rlen <- pRemainingLength
  pid  <- PacketIdentifier . fromIntegral <$> SG.getWord16be
  Subscribe pid <$> getTopics (rlen - 2) []
  where
    getTopics 0 ts = pure (reverse ts)
    getTopics r ts = do
      len <- fromIntegral <$> SG.getWord16be
      t   <- SG.getByteString len
      qos <- getQoS
      getTopics ( r - 3 - len ) ( ( T.decodeUtf8 t, qos ) : ts )
    getQoS = SG.getWord8 >>= \w-> case w of
        0x00 -> pure QoS0
        0x01 -> pure QoS1
        0x02 -> pure QoS2
        _    -> fail $ "pSubscribe: Violation of [MQTT-3.8.3-4]."

pSubscribeAcknowledgement :: SG.Get RawMessage
pSubscribeAcknowledgement = do
  _    <- SG.getWord8
  rlen <- pRemainingLength
  pid  <- PacketIdentifier . fromIntegral <$> SG.getWord16be
  SubscribeAcknowledgement pid <$> (map f . BS.unpack <$> SG.getBytes (rlen - 2))
  where
    f 0x00 = Just QoS0
    f 0x01 = Just QoS1
    f 0x02 = Just QoS2
    f    _ = Nothing

pUnsubscribe :: SG.Get RawMessage
pUnsubscribe = do
  header <- SG.getWord8
  rlen   <- pRemainingLength
  pid    <- SG.getWord16be
  Unsubscribe (PacketIdentifier $ fromIntegral pid) <$> f (rlen - 2) []
  where
    f 0 ts = pure (reverse ts)
    f r ts = do
      len <- fromIntegral <$> SG.getWord16be
      t   <- SG.getByteString len
      f (r - 2 - len) (T.decodeUtf8 t:ts)

pUnsubscribeAcknowledgement :: SG.Get RawMessage
pUnsubscribeAcknowledgement = do
  x <- fromIntegral <$> SG.getWord32be
  pure $ UnsubscribeAcknowledgement $ PacketIdentifier (x .&. 0xffff)

pPingRequest :: SG.Get RawMessage
pPingRequest = do
  _ <- SG.getWord16be
  pure PingRequest

pPingResponse :: SG.Get RawMessage
pPingResponse = do
  _ <- SG.getWord16be
  pure PingResponse

pDisconnect :: SG.Get RawMessage
pDisconnect = do
  _ <- SG.getWord16be
  pure Disconnect

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
      Nothing           -> 0x00
      Just (_, Nothing) -> 0x80
      Just (_, Just _)  -> 0xc0
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

bRawMessage (Publish d r (Topic t) qos b) =
  case qos of
    PublishQoS0 ->
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
    PublishQoS1 dup (PacketIdentifier pid) ->
      let len = 4 + BS.length t + BS.length b
      in BS.word8 ( 0x30
        .|. ( if dup then 0x08 else 0 )
        .|. ( if r then 0x01 else 0 )
        .|. 0x02 )
      <> bRemainingLength len
      <> BS.word16BE ( fromIntegral $ BS.length t )
      <> BS.byteString t
      <> BS.word16BE (fromIntegral pid)
      <> BS.byteString b
    PublishQoS2 (PacketIdentifier pid) ->
      let len = 4 + BS.length t + BS.length b
      in BS.word8 ( 0x30
        .|. ( if r then 0x01 else 0 )
        .|. 0x04 )
      <> bRemainingLength len
      <> BS.word16BE ( fromIntegral $ BS.length t )
      <> BS.byteString t
      <> BS.word16BE (fromIntegral pid)
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

--------------------------------------------------------------------------------
-- Utils
--------------------------------------------------------------------------------

pUtf8String :: SG.Get T.Text
pUtf8String = do
  str <- SG.getWord16be >>= SG.getByteString . fromIntegral
  when (BS.elem 0x00 str) (fail "pUtf8String: Violation of [MQTT-1.5.3-2].")
  case T.decodeUtf8' str of
    Right txt -> return txt
    _         -> fail "pUtf8String: Violation of [MQTT-1.5.3]."

bUtf8String :: T.Text -> BS.Builder
bUtf8String txt =
  if len > 0xffff
    then error "bUtf8String: Encoded size must be <= 0xffff."
    else BS.word16BE (fromIntegral len) <> BS.byteString bs
  where
    bs  = T.encodeUtf8 txt
    len = BS.length bs

pRemainingLength:: SG.Get Int
pRemainingLength = do
  b0 <- SG.getWord8
  if b0 < 128
    then return $ fromIntegral b0
     else do
       b1 <- SG.getWord8
       if b1 < 128
        then return $ fromIntegral b1 * 128 +
                      fromIntegral (b0 .&. 127)
        else do
          b2 <- SG.getWord8
          if b2 < 128
            then return $ fromIntegral b2 * 128 * 128 +
                          fromIntegral (b1 .&. 127) * 128 +
                          fromIntegral (b0 .&. 127)
            else do
              b3 <- SG.getWord8
              if b3 < 128
                then return $ fromIntegral b3 * 128 * 128 * 128 +
                              fromIntegral (b2 .&. 127) * 128 * 128 +
                              fromIntegral (b1 .&. 127) * 128 +
                              fromIntegral (b0 .&. 127)
                else fail "pRemainingLength: invalid input"
{-# INLINE pRemainingLength #-}

bRemainingLength :: Int -> BS.Builder
bRemainingLength i
  | i < 0x80                = BS.word8    ( fromIntegral i )
  | i < 0x80*0x80           = BS.word16LE $ fromIntegral $ 0x0080 -- continuation bit
                                         .|.              ( i .&. 0x7f      )
                                         .|. unsafeShiftL ( i .&. 0x3f80    )  1
  | i < 0x80*0x80*0x80      = BS.word16LE ( fromIntegral $ 0x8080
                                         .|.              ( i .&. 0x7f      )
                                         .|. unsafeShiftL ( i .&. 0x3f80    )  1
                                          )
                           <> BS.word8    ( fromIntegral
                                          $ unsafeShiftR ( i .&. 0x1fc000   ) 14
                                          )
  | i < 0x80*0x80*0x80*0x80 = BS.word32LE $ fromIntegral $ 0x00808080
                                         .|.              ( i .&. 0x7f      )
                                         .|. unsafeShiftL ( i .&. 0x3f80    )  1
                                         .|. unsafeShiftL ( i .&. 0x1fc000  )  2
                                         .|. unsafeShiftL ( i .&. 0x0ff00000)  3
  | otherwise               = error "sRemainingLength: invalid input"
{-# INLINE bRemainingLength #-}
