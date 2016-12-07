{-# LANGUAGE OverloadedStrings #-}
module EncodingTest (tests) where

import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy    as BSL
import qualified Data.Binary.Get         as SG
import           Network.MQTT.Message
import qualified Network.MQTT.Topic      as Topic
import           Prelude                 hiding (head)
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck   as QC

tests :: TestTree
tests = testGroup "Encoding / Decoding"
  [ testClientMessage
  , testServerMessage
  , testLengthEncoding
  ]

testClientMessage :: TestTree
testClientMessage = QC.testProperty "clientMessageBuilder <-> clientMessageParser" $ \msg->
  msg === SG.runGet clientMessageParser (BS.toLazyByteString $ clientMessageBuilder msg)

testServerMessage :: TestTree
testServerMessage = QC.testProperty "serverMessageBuilder <-> serverMessageParser" $ \msg->
  msg === SG.runGet serverMessageParser (BS.toLazyByteString $ serverMessageBuilder msg)

instance Arbitrary ClientMessage where
  arbitrary = oneof
    [ arbitraryConnect
    , arbitraryPublish
    , arbitrarySubscribe
    , arbitraryUnsubscribe
    , ClientPublishAcknowledged <$> choose (0, 65535)
    , ClientPublishReceived     <$> choose (0, 65535)
    , ClientPublishRelease      <$> choose (0, 65535)
    , ClientPublishComplete     <$> choose (0, 65535)
    , pure ClientPingRequest
    , pure ClientDisconnect
    ]
    where
      arbitraryConnect = ClientConnect
        <$> elements [ "", "client-identifier" ]
        <*> arbitrary
        <*> arbitrary
        <*> oneof [ pure Nothing , Just <$> ( arbitrary >>= \msg-> pure msg { msgDuplicate = False } ) ]
        <*> oneof [ pure Nothing, (Just .) . (,)
          <$> elements [ "", "username" ]
          <*> oneof [ pure Nothing, Just <$> elements [ Password "", Password "password" ] ] ]
      arbitraryPublish = do
        msg <- arbitrary
        pid <- if msgQos msg == Qos0 then pure (-1) else choose (0, 65535)
        pure (ClientPublish pid msg)
      arbitrarySubscribe = ClientSubscribe
        <$> choose (0, 65535)
        <*> listOf1 ((,) <$> arbitrary <*> arbitrary )
      arbitraryUnsubscribe = ClientUnsubscribe
        <$> choose (0, 65535)
        <*> listOf1 arbitrary

instance Arbitrary ServerMessage where
  arbitrary = oneof
    [ ServerConnectionAccepted      <$> arbitrary
    , ServerConnectionRejected      <$> arbitrary
    , do
        msg <- arbitrary
        pid <- if msgQos msg == Qos0 then pure (-1) else choose (0, 65535)
        pure (ServerPublish pid msg)
    , ServerPublishAcknowledged     <$> choose (0, 65535)
    , ServerPublishReceived         <$> choose (0, 65535)
    , ServerPublishRelease          <$> choose (0, 65535)
    , ServerPublishComplete         <$> choose (0, 65535)
    , ServerSubscribeAcknowledged   <$> choose (0, 65535) <*> listOf1 arbitrary
    , ServerUnsubscribeAcknowledged <$> choose (0, 65535)
    , pure ServerPingResponse
    ]

instance Arbitrary Message where
  arbitrary = Message
    <$> arbitrary
    <*> elements [ "", "shortTopic", BSL.replicate 345 23 ]
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary

instance Arbitrary ConnectionRejectReason where
  arbitrary = elements
    [ UnacceptableProtocolVersion
    , IdentifierRejected
    , ServerUnavailable
    , BadUsernameOrPassword
    , NotAuthorized
    ]

instance Arbitrary QualityOfService where
  arbitrary = elements [ Qos0, Qos1, Qos2 ]

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
