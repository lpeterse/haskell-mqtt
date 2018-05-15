{-# LANGUAGE OverloadedStrings #-}
module EncodingTest (tests) where

import qualified Data.Binary.Get         as SG
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy    as BSL
import           Prelude                 hiding (head)
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck   as QC

import           Network.MQTT.Message
import qualified Network.MQTT.Message    as Topic

tests :: TestTree
tests = testGroup "Encoding / Decoding"
  [ testClientPacket
  , testServerPacket
  , testLengthEncoding
  ]

testClientPacket :: TestTree
testClientPacket = QC.testProperty "clientPacketBuilder <-> clientPacketParser" $ \msg->
  msg === SG.runGet clientPacketParser (BS.toLazyByteString $ clientPacketBuilder msg)

testServerPacket :: TestTree
testServerPacket = QC.testProperty "serverPacketBuilder <-> serverPacketParser" $ \msg->
  msg === SG.runGet serverPacketParser (BS.toLazyByteString $ serverPacketBuilder msg)

instance Arbitrary ClientPacket where
  arbitrary = oneof
    [ arbitraryConnect
    , arbitraryPublish
    , arbitrarySubscribe
    , arbitraryUnsubscribe
    , ClientPublishAcknowledged <$> arbitrary
    , ClientPublishReceived     <$> arbitrary
    , ClientPublishRelease      <$> arbitrary
    , ClientPublishComplete     <$> arbitrary
    , pure ClientPingRequest
    , pure ClientDisconnect
    ]
    where
      arbitraryConnect = ClientConnect
        <$> elements [ "", "client-identifier" ]
        <*> arbitrary
        <*> arbitrary
        <*> arbitrary
        <*> oneof [ pure Nothing, (Just .) . (,)
          <$> elements [ "", "username" ]
          <*> oneof [ pure Nothing, Just <$> elements [ Password "", Password "password" ] ] ]
      arbitraryPublish = do
        msg <- arbitrary
        dup <- Duplicate <$> if msgQoS msg == QoS0 then pure False else arbitrary
        pid <- PacketIdentifier <$> if msgQoS msg == QoS0 then pure (-1) else choose (0, 65535)
        pure (ClientPublish pid dup msg)
      arbitrarySubscribe = ClientSubscribe
        <$> arbitrary
        <*> listOf1 ((,) <$> arbitrary <*> arbitrary )
      arbitraryUnsubscribe = ClientUnsubscribe
        <$> arbitrary
        <*> listOf1 arbitrary

instance Arbitrary ServerPacket where
  arbitrary = oneof
    [ ServerConnectionAccepted      <$> arbitrary
    , ServerConnectionRejected      <$> arbitrary
    , do
        msg <- arbitrary
        dup <- Duplicate <$> if msgQoS msg == QoS0 then pure False else arbitrary
        pid <- PacketIdentifier <$> if msgQoS msg == QoS0 then pure (-1) else choose (0, 65535)
        pure (ServerPublish pid dup msg)
    , ServerPublishAcknowledged     <$> arbitrary
    , ServerPublishReceived         <$> arbitrary
    , ServerPublishRelease          <$> arbitrary
    , ServerPublishComplete         <$> arbitrary
    , ServerSubscribeAcknowledged   <$> arbitrary <*> listOf1 arbitrary
    , ServerUnsubscribeAcknowledged <$> arbitrary
    , pure ServerPingResponse
    ]

instance Arbitrary Message where
  arbitrary = Message
    <$> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> elements [ "", "shortTopic", Payload $ BSL.replicate 345 23 ]

instance Arbitrary PacketIdentifier where
  arbitrary = PacketIdentifier <$> choose (0, 65535)

instance Arbitrary SessionPresent where
  arbitrary = SessionPresent <$> arbitrary

instance Arbitrary CleanSession where
  arbitrary = CleanSession <$> arbitrary

instance Arbitrary Retain where
  arbitrary = Retain <$> arbitrary

instance Arbitrary KeepAliveInterval where
  arbitrary = KeepAliveInterval <$> arbitrary

instance Arbitrary RejectReason where
  arbitrary = elements
    [ UnacceptableProtocolVersion
    , IdentifierRejected
    , ServerUnavailable
    , BadUsernameOrPassword
    , NotAuthorized
    ]

instance Arbitrary QoS where
  arbitrary = elements [ QoS0, QoS1, QoS2 ]

instance Arbitrary Topic.Topic where
  arbitrary = elements
    [ "a"
    , "/"
    , "//"
    , "a/b/c"
    , "a/nyːnɔʃk/c"
    , "   /"
    , "   /   /"
    , "$SYS/grampfglompf"
    ]

instance Arbitrary Topic.Filter where
  arbitrary = elements
    [ "#"
    , "+/#"
    , "a/a/a/a/a/a/a/a/adasdas//asda/+/+/    /#"
    , "$SYS/#"
    ]

testLengthEncoding :: TestTree
testLengthEncoding =
  testGroup "lengthParser, lengthBuilder"
  [ testCase "p [193,2] == 321" $ assertEqual "" 321
      ( SG.runGet lengthParser $ BSL.pack [193,2] )

  , testCase "p [0x00] == 0" $ assertEqual "" 0
      ( SG.runGet lengthParser (BSL.pack [0x00]) )

  , testCase "p [0x7f] == 127" $ assertEqual "" 127
      ( SG.runGet lengthParser (BSL.pack [0x7f]) )

  , testCase "p [0x80, 0x01] == 128" $ assertEqual "" 128
      ( SG.runGet lengthParser (BSL.pack [0x80, 0x01]) )

  , testCase "p [0xff, 0x7f] == 16383" $ assertEqual "" 16383
      ( SG.runGet lengthParser (BSL.pack [0xff, 0x7f]) )

  , testCase "p [0x80, 0x80, 0x01] == 16384" $ assertEqual "" 16384
      ( SG.runGet lengthParser (BSL.pack [0x80, 0x80, 0x01]) )

  , testCase "p [0xff, 0xff, 0x7f] == 2097151" $ assertEqual "" 2097151
      ( SG.runGet lengthParser (BSL.pack [0xff, 0xff, 0x7f]) )

  , testCase "p [0x80, 0x80, 0x80, 0x01] == 2097152" $ assertEqual "" 2097152
      ( SG.runGet lengthParser (BSL.pack [0x80, 0x80, 0x80, 0x01]) )

  , testCase "p [0xff, 0xff, 0xff, 0x7f] == 268435455" $ assertEqual "" 268435455
      ( SG.runGet lengthParser (BSL.pack [0xff, 0xff, 0xff, 0x7f]) )

  , testCase "p [0xff, 0xff, 0xff, 0xff] == invalid" $ assertEqual ""
      ( Left ("", 4, "lengthParser: invalid input") )
      ( SG.runGetOrFail lengthParser (BSL.pack [0xff, 0xff, 0xff, 0xff]) )

  , QC.testProperty "lengthParser . lengthBuilder == id" $
      \i -> let i' = i `mod` 268435455
            in  i' == SG.runGet lengthParser (BS.toLazyByteString (lengthBuilder i'))
  ]
