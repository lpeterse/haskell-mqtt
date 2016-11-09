{-# LANGUAGE TupleSections #-}
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
  , KeepAliveInterval
  , Username
  , Password
  , PacketIdentifier
  , QualityOfService (..)
  , ConnectionRefusal (..)
  , Message (..)
  , ClientMessage (..)
  , clientMessageBuilder
  , clientMessageParser
  , ServerMessage (..)
  , serverMessageBuilder
  , serverMessageParser
  , lengthParser
  , lengthBuilder
  , utf8Parser
  , utf8Builder
  ) where

import           Control.Monad
import qualified Data.Attoparsec.ByteString as A
import           Data.Bits
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Builder    as BS
import qualified Data.ByteString.Lazy       as BSL
import           Data.Maybe
import           Data.Monoid
import qualified Data.Serialize.Get         as SG
import qualified Data.Text                  as T
import qualified Data.Text.Encoding         as T
import           Data.Word
import qualified Network.MQTT.Topic         as TF

type SessionPresent   = Bool
type CleanSession     = Bool
type Retain           = Bool
type Duplicate        = Bool
type KeepAliveInterval = Word16
type Username         = T.Text
type Password         = BS.ByteString
type ClientIdentifier = T.Text
type PacketIdentifier = Int

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
   { msgTopic     :: !TF.Topic
   , msgBody      :: !BSL.ByteString
   , msgQos       :: !QualityOfService
   , msgRetain    :: !Retain
   , msgDuplicate :: !Duplicate
   } deriving (Eq, Ord, Show)

data ClientMessage
   = ClientConnect
     { connectClientIdentifier :: !ClientIdentifier
     , connectCleanSession     :: !CleanSession
     , connectKeepAlive        :: !KeepAliveInterval
     , connectWill             :: !(Maybe Message)
     , connectCredentials      :: !(Maybe (Username, Maybe Password))
     }
   | ClientPublish                                                !Message
   | ClientPublish'              {-# UNPACK #-} !PacketIdentifier !Message
   | ClientPublishAcknowledged   {-# UNPACK #-} !PacketIdentifier
   | ClientPublishReceived       {-# UNPACK #-} !PacketIdentifier
   | ClientPublishRelease        {-# UNPACK #-} !PacketIdentifier
   | ClientPublishComplete       {-# UNPACK #-} !PacketIdentifier
   | ClientSubscribe             {-# UNPACK #-} !PacketIdentifier ![(TF.Filter, QualityOfService)]
   | ClientUnsubscribe           {-# UNPACK #-} !PacketIdentifier ![TF.Filter]
   | ClientPingRequest
   | ClientDisconnect
   deriving (Eq, Show)

data ServerMessage
   = ConnectAck !(Either ConnectionRefusal SessionPresent)
   | ServerPublish                                                         !Message
   | ServerPublish'                       {-# UNPACK #-} !PacketIdentifier !Message
   | ServerPublishAcknowledged            {-# UNPACK #-} !PacketIdentifier
   | ServerPublishReceived                {-# UNPACK #-} !PacketIdentifier
   | ServerPublishRelease                 {-# UNPACK #-} !PacketIdentifier
   | ServerPublishComplete                {-# UNPACK #-} !PacketIdentifier
   | ServerSubscribeAcknowledged          {-# UNPACK #-} !PacketIdentifier ![Maybe QualityOfService]
   | ServerUnsubscribeAcknowledged        {-# UNPACK #-} !PacketIdentifier
   | ServerPingResponse
   deriving (Eq, Show)

clientMessageParser :: SG.Get ClientMessage
clientMessageParser =
  SG.lookAhead SG.getWord8 >>= \h-> case h .&. 0xf0 of
    0x10 -> connectParser
    0x30 -> publishParser      ClientPublish ClientPublish'
    0x40 -> acknowledgedParser ClientPublishAcknowledged
    0x50 -> acknowledgedParser ClientPublishReceived
    0x60 -> acknowledgedParser ClientPublishRelease
    0x70 -> acknowledgedParser ClientPublishComplete
    0x80 -> subscribeParser
    0xa0 -> unsubscribeParser
    0xc0 -> pingRequestParser
    0xe0 -> disconnectParser
    _    -> fail "clientMessageParser: Invalid message type."

serverMessageParser :: SG.Get ServerMessage
serverMessageParser =
  SG.lookAhead SG.getWord8 >>= \h-> case h .&. 0xf0 of
    0x20 -> connectAcknowledgedParser
    0x30 -> publishParser      ServerPublish ServerPublish'
    0x40 -> acknowledgedParser ServerPublishAcknowledged
    0x50 -> acknowledgedParser ServerPublishReceived
    0x60 -> acknowledgedParser ServerPublishRelease
    0x70 -> acknowledgedParser ServerPublishComplete
    0xb0 -> acknowledgedParser ServerUnsubscribeAcknowledged
    0x90 -> subscribeAcknowledgedParser
    0xd0 -> pingResponseParser
    _    -> fail "serverMessageParser: Packet type not implemented."

connectParser :: SG.Get ClientMessage
connectParser = do
  h <- SG.getWord8
  when (h .&. 0x0f /= 0) $
    fail "clientConnectParser: The header flags are reserved and MUST be set to 0."
  void lengthParser -- the remaining length is redundant in this packet type
  y <- SG.getWord64be -- get the next 8 bytes all at once and not byte per byte
  when (y .&. 0xffffffffffffff00 /= 0x00044d5154540400) $
    fail "clientConnectParser: Unexpected protocol initialization."
  let cleanSession = y .&. 0x02 /= 0
  keepAlive <- SG.getWord16be
  cid       <- utf8Parser
  will      <- if y .&. 0x04 == 0
    then pure Nothing
    else Just <$> do
      topic      <- fst <$> topicParser
      bodyLen    <- fromIntegral <$> SG.getWord16be
      body       <- SG.getLazyByteString bodyLen
      qos        <- case y .&. 0x18 of
        0x00 -> pure Qos0
        0x08 -> pure Qos1
        0x10 -> pure Qos2
        _    -> fail "clientConnectParser: Violation of [MQTT-3.1.2-14]."
      pure $ Message topic body qos ( y .&. 0x20 /= 0 ) False
  cred  <- if y .&. 0x80 == 0
    then pure Nothing
    else Just <$> ( (,)
      <$> utf8Parser
      <*> if y .&. 0x40 == 0
            then pure Nothing
            else Just <$> (SG.getWord16be >>= SG.getByteString . fromIntegral) )
  pure ( ClientConnect cid cleanSession keepAlive will cred )

connectAcknowledgedParser :: SG.Get ServerMessage
connectAcknowledgedParser = do
  x <- SG.getWord32be
  ConnectAck <$> case x .&. 0xff of
    0 -> pure $ Right (x .&. 0x0100 /= 0)
    1 -> pure $ Left UnacceptableProtocolVersion
    2 -> pure $ Left IdentifierRejected
    3 -> pure $ Left ServerUnavailable
    4 -> pure $ Left BadUsernameOrPassword
    5 -> pure $ Left NotAuthorized
    _ -> fail "serverConnectAcknowledgedParser: Invalid (reserved) return code."

publishParser :: (Message -> a) -> (PacketIdentifier -> Message -> a) -> SG.Get a
publishParser publish publish' = do
  hflags <- SG.getWord8
  let dup = hflags .&. 0x08 /= 0 -- duplicate flag
  let ret = hflags .&. 0x01 /= 0 -- retain flag
  len <- lengthParser
  (topic, topicLen)  <- topicParser
  let qosBits = hflags .&. 0x06
  if  qosBits == 0x00
    then do
      body <- SG.getLazyByteString $ fromIntegral ( len - topicLen )
      pure (publish $ Message topic body Qos0 dup ret)
    else do
      let qos = if qosBits == 0x02 then Qos1 else Qos2
      pid  <- fromIntegral <$> SG.getWord16be
      body <- SG.getLazyByteString $ fromIntegral ( len - topicLen - 2 )
      pure (publish' pid $ Message topic body qos dup ret)

acknowledgedParser :: (PacketIdentifier -> a) -> SG.Get a
acknowledgedParser f = do
  w32 <- SG.getWord32be
  pure $ f $ fromIntegral $ w32 .&. 0xffff
{-# INLINE acknowledgedParser #-}

subscribeParser :: SG.Get ClientMessage
subscribeParser = do
  void SG.getWord8
  rlen <- lengthParser
  pid  <- fromIntegral <$> SG.getWord16be
  ClientSubscribe pid <$> parseFilters (rlen - 2) []
  where
    parseFilters r accum
      | r <= 0    = pure (reverse accum)
      | otherwise = do
          (filtr,len) <- filterParser
          qos <- getQoS
          parseFilters ( r - 1 - len ) ( ( filtr, qos ) : accum )
    getQoS = SG.getWord8 >>= \w-> case w of
        0x00 -> pure Qos0
        0x01 -> pure Qos1
        0x02 -> pure Qos2
        _    -> fail "clientSubscribeParser: Violation of [MQTT-3.8.3-4]."

unsubscribeParser :: SG.Get ClientMessage
unsubscribeParser = do
  void SG.getWord8
  rlen <- lengthParser
  pid  <- fromIntegral <$> SG.getWord16be
  ClientUnsubscribe pid <$> parseFilters (rlen - 2) []
  where
    parseFilters r accum
      | r <= 0    = pure (reverse accum)
      | otherwise = do
          (filtr,len) <- filterParser
          parseFilters ( r - len ) ( filtr : accum )

subscribeAcknowledgedParser :: SG.Get ServerMessage
subscribeAcknowledgedParser = do
  void SG.getWord8
  rlen <- lengthParser
  pid  <- fromIntegral <$> SG.getWord16be
  ServerSubscribeAcknowledged pid <$> (map f . BS.unpack <$> SG.getBytes (rlen - 2))
  where
    f 0x00 = Just Qos0
    f 0x01 = Just Qos1
    f 0x02 = Just Qos2
    f    _ = Nothing

pingRequestParser :: SG.Get ClientMessage
pingRequestParser = do
  void SG.getWord16be
  pure ClientPingRequest
{-# INLINE pingRequestParser #-}

pingResponseParser :: SG.Get ServerMessage
pingResponseParser = do
  void SG.getWord16be
  pure ServerPingResponse
{-# INLINE pingResponseParser #-}

disconnectParser :: SG.Get ClientMessage
disconnectParser = do
  void SG.getWord16be
  pure ClientDisconnect
{-# INLINE disconnectParser #-}

clientMessageBuilder :: ClientMessage -> BS.Builder
clientMessageBuilder (ClientConnect cid cleanSession keepAlive will credentials) =
  BS.word8 0x10
  <> lengthBuilder ( 10 + cidLen + willLen + credLen )
  <> BS.word64BE ( 0x00044d5154540400 .|. willFlag .|. credFlag .|. sessFlag )
  <> BS.word16BE keepAlive
  <> cidBuilder
  <> willBuilder
  <> credBuilder
  where
    sessFlag = if cleanSession then 0x02 else 0x00
    (cidBuilder, cidLen) = utf8Builder cid
    (willBuilder, willLen, willFlag) = case will of
      Nothing ->
        (mempty, 0, 0x00)
      Just (Message t b q r _)->
       let tlen  = TF.topicLength t
           blen  = fromIntegral (BSL.length b)
           qflag = case q of
             Qos0 -> 0x04
             Qos1 -> 0x0c
             Qos2 -> 0x14
           rflag = if r then 0x20 else 0x00
           x1 = BS.word16BE (fromIntegral tlen)
           x2 = TF.topicBuilder t
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
clientMessageBuilder (ClientPublish msg) =
  publishBuilder Nothing msg
clientMessageBuilder (ClientPublish' pid msg) =
  publishBuilder (Just pid) msg
clientMessageBuilder (ClientPublishAcknowledged p) =
  BS.word32BE $ fromIntegral $ 0x40020000 .|. p
clientMessageBuilder (ClientPublishReceived p) =
  BS.word32BE $ fromIntegral $ 0x50020000 .|. p
clientMessageBuilder (ClientPublishRelease p) =
  BS.word32BE $ fromIntegral $ 0x62020000 .|. p
clientMessageBuilder (ClientPublishComplete p) =
  BS.word32BE $ fromIntegral $ 0x70020000 .|. p
clientMessageBuilder (ClientSubscribe pid filters) =
  BS.word8 0x82 <> lengthBuilder len <> BS.word16BE (fromIntegral pid)
                                     <> mconcat (fmap filterBuilder filters)
  where
    filterBuilder (f, q) = BS.word16BE (fromIntegral fl) <> fb <> qb
      where
        fb = TF.filterBuilder f
        fl = TF.filterLength  f
        qb = BS.word8 $ case q of
          Qos0 -> 0x00
          Qos1 -> 0x01
          Qos2 -> 0x02
    len  = 2  + sum ( map ( (+3) . TF.filterLength . fst ) filters )
clientMessageBuilder (ClientUnsubscribe pid filters) =
  BS.word8 0xa2 <> lengthBuilder len <> BS.word16BE (fromIntegral pid)
                                     <> mconcat ( map filterBuilder filters )
  where
    filterBuilder f = BS.word16BE (fromIntegral fl) <> fb
      where
        fb = TF.filterBuilder f
        fl = TF.filterLength  f
    len  = 2  + sum ( map ( (+2) . TF.filterLength ) filters )
clientMessageBuilder ClientPingRequest =
  BS.word16BE 0xc000
clientMessageBuilder ClientDisconnect =
  BS.word16BE 0xe000

serverMessageBuilder :: ServerMessage -> BS.Builder
serverMessageBuilder (ConnectAck crs) =
  BS.word32BE $ 0x20020000 .|. case crs of
    Left cr -> fromIntegral $ fromEnum cr + 1
    Right s -> if s then 0x0100 else 0
serverMessageBuilder (ServerPublish msg) =
  publishBuilder Nothing msg
serverMessageBuilder (ServerPublish' pid msg) =
  publishBuilder (Just pid) msg
serverMessageBuilder (ServerPublishAcknowledged p) =
  BS.word32BE $ fromIntegral $ 0x40020000 .|. p
serverMessageBuilder (ServerPublishReceived p) =
  BS.word32BE $ fromIntegral $ 0x50020000 .|. p
serverMessageBuilder (ServerPublishRelease p) =
  BS.word32BE $ fromIntegral $ 0x62020000 .|. p
serverMessageBuilder (ServerPublishComplete p) =
  BS.word32BE $ fromIntegral $ 0x70020000 .|. p
serverMessageBuilder (ServerSubscribeAcknowledged p rcs) =
  BS.word8 0x90 <> lengthBuilder (2 + length rcs)
    <> BS.word16BE (fromIntegral p) <> mconcat ( map ( BS.word8 . f ) rcs )
  where
    f Nothing     = 0x80
    f (Just Qos0) = 0x00
    f (Just Qos1) = 0x01
    f (Just Qos2) = 0x02
serverMessageBuilder (ServerUnsubscribeAcknowledged p) =
  BS.word16BE 0xb002 <> BS.word16BE (fromIntegral p)
serverMessageBuilder ServerPingResponse =
  BS.word16BE 0xd000

publishBuilder :: Maybe PacketIdentifier -> Message -> BS.Builder
publishBuilder mpid msg
  | len < 128 =
      BS.word32BE (fromIntegral h)
      <> topicBuilder
      <> BS.lazyByteString (msgBody msg)
  | otherwise =
      BS.word8 ( if msgRetain msg then 0x31 else 0x30 )
      <> lengthBuilder len
      <> BS.word16BE (fromIntegral topicLen)
      <> topicBuilder
      <> fromMaybe mempty (BS.word16BE . fromIntegral <$> mpid)
      <> BS.lazyByteString (msgBody msg)
  where
    topicLen     = TF.topicLength  (msgTopic msg)
    topicBuilder = TF.topicBuilder (msgTopic msg)
    len          = 2 + topicLen + fromIntegral (BSL.length $ msgBody msg)
    h            = (if msgRetain msg then 0x31000000 else 0x30000000)
                .|. len `unsafeShiftL` 16
                .|. topicLen

topicParser :: SG.Get (TF.Topic, Int)
topicParser = do
  topicLen   <- fromIntegral <$> SG.getWord16be
  topicBytes <- SG.getByteString topicLen
  case A.parseOnly TF.topicParser topicBytes of
    Right t -> pure (t, topicLen + 2)
    Left  _ -> fail "topicParser: Invalid topic."
{-# INLINE topicParser #-}

filterParser :: SG.Get (TF.Filter, Int)
filterParser = do
  filterLen   <- fromIntegral <$> SG.getWord16be
  filterBytes <- SG.getByteString filterLen
  case A.parseOnly TF.filterParser filterBytes of
    Right f -> pure (f, filterLen + 2)
    Left  _ -> fail "filterParser: Invalid filter."
{-# INLINE filterParser #-}

utf8Parser :: SG.Get T.Text
utf8Parser = do
  str <- SG.getWord16be >>= SG.getByteString . fromIntegral
  when (BS.elem 0x00 str) (fail "utf8Parser: Violation of [MQTT-1.5.3-2].")
  case T.decodeUtf8' str of
    Right txt -> return txt
    _         -> fail "utf8Parser: Violation of [MQTT-1.5.3]."
{-# INLINE utf8Parser #-}

utf8Builder :: T.Text -> (BS.Builder, Int)
utf8Builder txt =
  if len > 0xffff
    then error "utf8Builder: Encoded size must be <= 0xffff."
    else (BS.word16BE (fromIntegral len) <> BS.byteString bs, len + 2)
  where
    bs  = T.encodeUtf8 txt
    len = BS.length bs
{-# INLINE utf8Builder #-}

lengthParser:: SG.Get Int
lengthParser = do
  b0 <- fromIntegral <$> SG.getWord8
  if b0 < 128
    then pure b0
     else do
       b1 <- fromIntegral <$> SG.getWord8
       if b1 < 128
        then pure $ b1 * 128 + (b0 .&. 127)
        else do
          b2 <- fromIntegral <$> SG.getWord8
          if b2 < 128
            then pure $ b2 * 128 * 128 + (b1 .&. 127) * 128 + (b0 .&. 127)
            else do
              b3 <- fromIntegral <$> SG.getWord8
              if b3 < 128
                then pure $ b3 * 128 * 128 * 128 + (b2 .&. 127) * 128 * 128 + (b1 .&. 127) * 128 + (b0 .&. 127)
                else fail "lengthParser: invalid input"
{-# INLINE lengthParser #-}

lengthBuilder :: Int -> BS.Builder
lengthBuilder i
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
  | otherwise               = error "lengthBuilder: invalid input"
{-# INLINE lengthBuilder #-}
