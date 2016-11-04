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
  ( ClientIdentifier
  , SessionPresent
  , CleanSession
  , Retain
  , KeepAlive
  , Username
  , Password
  , PacketIdentifier (..)
  , QualityOfService (..)
  , ConnectionRefusal (..)
  , Message (..)
  , UpstreamMessage (..)
  , buildUpstreamMessage
  , parseUpstreamMessage
  , DownstreamMessage (..)
  , buildDownstreamMessage
  , parseDownstreamMessage
  , pRemainingLength
  , bRemainingLength
  , pUtf8String
  , bUtf8String ) where

import           Control.Monad
import qualified Data.Attoparsec.ByteString as A
import           Data.Bits
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Builder    as BS
import qualified Data.ByteString.Lazy       as BSL
import           Data.Monoid
import qualified Data.Serialize.Get         as SG
import qualified Data.Text                  as T
import qualified Data.Text.Encoding         as T
import           Data.Word
import           Network.MQTT.Topic

type SessionPresent   = Bool
type CleanSession     = Bool
type Retain           = Bool
type Duplicate        = Bool
type KeepAlive        = Word16
type Username         = T.Text
type Password         = BS.ByteString
type ClientIdentifier = BS.ByteString

newtype PacketIdentifier = PacketIdentifier Int
  deriving (Eq, Ord, Show)

data QualityOfService
  = Qos0
  | Qos1
  | Qos2
  deriving (Eq, Ord, Show)

data ConnectionRefusal
   = UnacceptableProtocolVersion
   | IdentifierRejected
   | ServerUnavailable
   | BadUsernameOrPassword
   | NotAuthorized
   deriving (Eq, Ord, Show, Enum, Bounded)

data Message
   = Message
   { msgTopic     :: !Topic
   , msgBody      :: !BSL.ByteString
   , msgQos       :: !QualityOfService
   , msgRetain    :: !Retain
   , msgDuplicate :: !Duplicate
   } deriving (Eq, Ord, Show)

data UpstreamMessage
   = Connect
     { connectClientIdentifier :: !ClientIdentifier
     , connectCleanSession     :: !CleanSession
     , connectKeepAlive        :: !KeepAlive
     , connectWill             :: !(Maybe Message)
     , connectCredentials      :: !(Maybe (Username, Maybe Password))
     }
   | Subscribe                    PacketIdentifier [(TopicFilter, QualityOfService)]
   | Unsubscribe                  PacketIdentifier [TopicFilter]
   | PingRequest
   | Disconnect
   deriving (Eq, Show)

data DownstreamMessage
   = ConnectAcknowledgement !(Either ConnectionRefusal SessionPresent)
   | PingResponse
   | Publish                                                      !Message
   | Publish'                    {-# UNPACK #-} !PacketIdentifier !Message
   | PublishAcknowledgement      {-# UNPACK #-} !PacketIdentifier
   | PublishReceived             {-# UNPACK #-} !PacketIdentifier
   | PublishRelease              {-# UNPACK #-} !PacketIdentifier
   | PublishComplete             {-# UNPACK #-} !PacketIdentifier
   | SubscribeAcknowledgement    {-# UNPACK #-} !PacketIdentifier [Maybe QualityOfService]
   | UnsubscribeAcknowledgement  {-# UNPACK #-} !PacketIdentifier
   deriving (Eq, Show)

parseUpstreamMessage :: SG.Get UpstreamMessage
parseUpstreamMessage =
  SG.lookAhead SG.getWord8 >>= \h-> case h .&. 0xf0 of
    0x10 -> pConnect
    0x80 -> undefined --pSubscribe
    0xa0 -> undefined --pUnsubscribe
    0xc0 -> pPingRequest
    0xe0 -> pDisconnect
    _    -> fail "parseUpstreamMessage: Invalid message type."

parseDownstreamMessage :: SG.Get DownstreamMessage
parseDownstreamMessage =
  SG.lookAhead SG.getWord8 >>= \h-> case h .&. 0xf0 of
    0x20 -> pConnectAcknowledgement
    0x30 -> pPublish
    0x40 -> pPublishAcknowledgement
    0x50 -> pPublishReceived
    0x60 -> pPublishRelease
    0x70 -> pPublishComplete
    0x90 -> undefined -- pSubscribeAcknowledgement
    0xb0 -> pUnsubscribeAcknowledgement
    0xd0 -> pPingResponse
    _    -> fail "pDownstreamMessage: Packet type not implemented."

pTopic :: SG.Get Topic
pTopic = do
  topicLen   <- fromIntegral <$> SG.getWord16be
  topicBytes <- SG.getByteString topicLen
  case A.parseOnly parseTopic topicBytes of
    Right t -> pure t
    Left  _ -> fail "pPublish: Failed parsing topic."
{-# INLINE pTopic #-}

pConnect :: SG.Get UpstreamMessage
pConnect = do
  h <- SG.getWord8
  when (h .&. 0x0f /= 0) $
    fail "pConnect: The header flags are reserved and MUST be set to 0."
  void pRemainingLength -- the remaining length is redundant in this packet type
  y <- SG.getWord64be -- get the next 8 bytes all at once and not byte per byte
  when (y .&. 0xffffffffffffff00 /= 0x00044d5154540400) $
    fail "pConnect: Unexpected protocol initialization."
  let cleanSession = y .&. 0x02 /= 0
  keepAlive <- SG.getWord16be
  cidLen    <- SG.getWord16be
  cid       <- SG.getByteString (fromIntegral cidLen)
  will      <- if y .&. 0x04 == 0
    then pure Nothing
    else Just <$> do
      topic      <- pTopic
      bodyLen    <- fromIntegral <$> SG.getWord16be
      body       <- SG.getLazyByteString bodyLen
      qos        <- case y .&. 0x18 of
        0x00 -> pure Qos0
        0x08 -> pure Qos1
        0x10 -> pure Qos2
        _    -> fail "pConnect: Violation of [MQTT-3.1.2-14]."
      pure $ Message topic body qos ( y .&. 0x20 /= 0 ) False
  cred  <- if y .&. 0x80 == 0
    then pure Nothing
    else Just <$> ( (,)
      <$> pUtf8String
      <*> if y .&. 0x40 == 0
            then pure Nothing
            else Just <$> (SG.getWord16be >>= SG.getByteString . fromIntegral) )
  pure ( Connect cid cleanSession keepAlive will cred )

pConnectAcknowledgement :: SG.Get DownstreamMessage
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

pPublish :: SG.Get DownstreamMessage
pPublish = do
  hflags <- SG.getWord8
  let dup = hflags .&. 0x08 /= 0 -- duplicate flag
  let ret = hflags .&. 0x01 /= 0 -- retain flag
  len        <- pRemainingLength
  topicLen   <- fromIntegral <$> SG.getWord16be
  topicBytes <- SG.getByteString topicLen
  topic      <- case A.parseOnly parseTopic topicBytes of
    Right t -> pure t
    Left  _ -> fail "pPublish: Failed parsing topic."
  let qosBits = hflags .&. 0x06
  if qosBits == 0x00
    then do
      body <- SG.getLazyByteString $ fromIntegral ( len - topicLen - 2 )
      pure (Publish $ Message topic body Qos0 dup ret)
    else do
      let qos = if qosBits == 0x02 then Qos1 else Qos2
      pid  <- PacketIdentifier . fromIntegral <$> SG.getWord16be
      body <- SG.getLazyByteString $ fromIntegral ( len - topicLen - 4 )
      pure (Publish' pid $ Message topic body qos dup ret)

pPublishAcknowledgement :: SG.Get DownstreamMessage
pPublishAcknowledgement = do
    w32 <- SG.getWord32be
    pure $ PublishAcknowledgement $ PacketIdentifier $ fromIntegral $ w32 .&. 0xffff

pPublishReceived :: SG.Get DownstreamMessage
pPublishReceived = do
    w32 <- SG.getWord32be
    pure $ PublishReceived $ PacketIdentifier $ fromIntegral $ w32 .&. 0xffff

pPublishRelease :: SG.Get DownstreamMessage
pPublishRelease = do
    w32 <- SG.getWord32be
    pure $ PublishRelease $ PacketIdentifier $ fromIntegral $ w32 .&. 0xffff

pPublishComplete :: SG.Get DownstreamMessage
pPublishComplete = do
    w32 <- SG.getWord32be
    pure $ PublishComplete $ PacketIdentifier $ fromIntegral $ w32 .&. 0xffff

{-
pSubscribe :: SG.Get UpstreamMessage
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

pSubscribeAcknowledgement :: SG.Get DownstreamMessage
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

pUnsubscribe :: SG.Get UpstreamMessage
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
      -}

pUnsubscribeAcknowledgement :: SG.Get DownstreamMessage
pUnsubscribeAcknowledgement = do
  x <- fromIntegral <$> SG.getWord32be
  pure $ UnsubscribeAcknowledgement $ PacketIdentifier (x .&. 0xffff)

pPingRequest :: SG.Get UpstreamMessage
pPingRequest = do
  _ <- SG.getWord16be
  pure PingRequest

pPingResponse :: SG.Get DownstreamMessage
pPingResponse = do
  _ <- SG.getWord16be
  pure PingResponse

pDisconnect :: SG.Get UpstreamMessage
pDisconnect = do
  _ <- SG.getWord16be
  pure Disconnect

buildUpstreamMessage :: UpstreamMessage -> BS.Builder
buildUpstreamMessage (Connect cid cleanSession keepAlive will credentials) =
  BS.word8 0x10
  <> bRemainingLength len
  <> BS.word64BE ( 0x00044d5154540400 .|. willFlag .|. credFlag .|. sessFlag )
  <> BS.word16BE keepAlive
  <> BS.byteString cid
  <> willBuilder <> credBuilder
  where
    len = 12 + cidLen + willLen + credLen + BS.length cid
    cidLen = BS.length cid
    sessFlag = if cleanSession then 0x02 else 0x00
    (willBuilder, willLen, willFlag) = case will of
      Nothing ->
        (mempty, 0, 0x00)
      Just (Message t b q r _)->
       let tlen  = topicLength t
           blen  = fromIntegral (BSL.length b)
           qflag = case q of
             Qos0 -> 0x04
             Qos1 -> 0x0c
             Qos2 -> 0x14
           rflag = if r then 0x20 else 0x00
           x1 = BS.word16BE (fromIntegral tlen)
           x2 = topicBuilder t
           x3 = BS.word16BE (fromIntegral blen)
           x4 = BS.lazyByteString b
       in  (x1 <> x2 <> x3 <> x4, 4 + tlen + blen, qflag .|. rflag)
    (credBuilder, credLen, credFlag) = case credentials of
      Nothing           ->
        (mempty, 0, 0x00)
      Just (ut, Nothing) ->
        let u    = T.encodeUtf8 ut
            ulen = BS.length u
            x1   = BS.word16BE (fromIntegral ulen)
            x2   = BS.byteString u
        in (x1 <> x2, 2 + ulen, 0x80)
      Just (ut, Just p)  ->
        let u    = T.encodeUtf8 ut
            ulen = BS.length u
            plen = BS.length p
            x1   = BS.word16BE (fromIntegral ulen)
            x2   = BS.byteString u
            x3   = BS.word16BE (fromIntegral plen)
            x4   = BS.byteString p
        in (x1 <> x2 <> x3 <> x4, 4 + ulen + plen, 0xc0)
{-
buildUpstreamMessage (Subscribe (PacketIdentifier p) tf)  =
  BS.word8 0x82 <> bRemainingLength len <> BS.word16BE (fromIntegral p) <> mconcat ( map f tf )
  where
    f (t, q) = (bUtf8String t <>) $ BS.word8 $ case q of
      Qos0 -> 0x00
      Qos1 -> 0x01
      Qos2 -> 0x02
    len  = 2 + length tf * 3 + sum ( map (BS.length . T.encodeUtf8 . fst) tf )
buildUpstreamMessage (Unsubscribe (PacketIdentifier p) tfs) =
  BS.word8 0xa2 <> bRemainingLength len
    <> BS.word16BE (fromIntegral p) <> mconcat ( map bUtf8String tfs )
  where
    bfs = map T.encodeUtf8 tfs
    len = 2 + sum ( map ( ( + 2 ) . BS.length ) bfs )
-}
buildUpstreamMessage PingRequest =
  BS.word16BE 0xc000
buildUpstreamMessage Disconnect =
  BS.word16BE 0xe000

buildDownstreamMessage :: DownstreamMessage -> BS.Builder
buildDownstreamMessage (ConnectAcknowledgement crs) =
  BS.word32BE $ 0x20020000 .|. case crs of
    Left cr -> fromIntegral $ fromEnum cr + 1
    Right s -> if s then 0x0100 else 0

{-
buildDownstreamMessage (Publish d r t qos b) =
  case qos of
    PublishQoS0 ->
      let len = 2 + BS.length t + BS.length b
          h   = fromIntegral $ (if r then 0x31000000 else 0x30000000)
                .|. len `unsafeShiftL` 16
                .|. BS.length t
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
      let tLen = topicLength t;
          len = 4 + tLen + BS.length b
      in BS.word8 ( 0x30 .|. ( if r then 0x01 else 0x00 ) .|. 0x04 )
      <> bRemainingLength len
      <> BS.word16BE (fromIntegral tLen)
      <> topicBuilder t
      <> BS.word16BE (fromIntegral pid)
      <> BS.byteString b
-}
buildDownstreamMessage (PublishAcknowledgement (PacketIdentifier p)) =
  BS.word32BE $ fromIntegral $ 0x40020000 .|. p
buildDownstreamMessage (PublishReceived (PacketIdentifier p)) =
  BS.word32BE $ fromIntegral $ 0x50020000 .|. p
buildDownstreamMessage (PublishRelease (PacketIdentifier p)) =
  BS.word32BE $ fromIntegral $ 0x62020000 .|. p
buildDownstreamMessage (PublishComplete (PacketIdentifier p)) =
  BS.word32BE $ fromIntegral $ 0x70020000 .|. p
buildDownstreamMessage (SubscribeAcknowledgement (PacketIdentifier p) rcs) =
  BS.word8 0x90 <> bRemainingLength (2 + length rcs)
    <> BS.word16BE (fromIntegral p) <> mconcat ( map ( BS.word8 . f ) rcs )
  where
    f Nothing     = 0x80
    f (Just Qos0) = 0x00
    f (Just Qos1) = 0x01
    f (Just Qos2) = 0x02

buildDownstreamMessage (UnsubscribeAcknowledgement (PacketIdentifier p)) =
  BS.word16BE 0xb002 <> BS.word16BE (fromIntegral p)
buildDownstreamMessage PingResponse =
  BS.word16BE 0xd000

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
