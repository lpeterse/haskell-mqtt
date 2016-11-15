{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Control.Exception       (SomeException, try)
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy    as BSL
import           Data.Monoid
import qualified Data.Binary.Get         as SG
import qualified Data.Text               as T
import           Network.MQTT.Message
import           Prelude                 hiding (head)
import qualified RoutingTreeTest
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck   as QC
import qualified TopicTest

main :: IO ()
main  = defaultMain $ testGroup "Network.MQTT"
  [ RoutingTreeTest.tests
  , tgRawMessage
  , TopicTest.tests
  ]

tgRawMessage :: TestTree
tgRawMessage = testGroup "Encoding / Decoding"
  [ testLengthEncoding
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

{-
tgRawMessageAll :: TestTree
tgRawMessageAll = QC.testProperty "pRawMessage . bRawMessage == id" $ \msg->
  Right msg === SG.runGet pRawMessage (BSL.toStrict $ BS.toLazyByteString $ bRawMessage msg)

instance Arbitrary RawMessage where
  arbitrary = oneof
    [ arbitraryConnect
    , arbitraryConnectAcknowledgment
    , arbitraryPublish
    , PublishAcknowledgement <$> arbitrary
    , PublishReceived <$> arbitrary
    , PublishRelease <$> arbitrary
    , PublishComplete <$> arbitrary
    , arbitrarySubscribe
    , arbitrarySubscribeAcknowledgement
    , arbitraryUnsubscribe
    , arbitraryUnsubscribeAcknowledgement
    , pure PingRequest
    , pure PingResponse
    , pure Disconnect
    ]
    where
      arbitraryConnect = Connect
        <$> arbitrary
        <*> elements [ True, False ]
        <*> choose ( 0, 65535 )
        <*> oneof [ pure Nothing , Just <$> arbitrary ]
        <*> oneof [ pure Nothing, (Just .) . (,)
          <$> elements [ "", "username" ]
          <*> oneof [ pure Nothing, Just <$> elements [ "", "password" ] ] ]
      arbitraryConnectAcknowledgment = ConnectAcknowledgement
        <$> oneof [ Left <$> arbitrary, Right <$> arbitrary ]
      arbitraryPublish = Publish
        <$> elements [ True, False ]
        <*> elements [ True, False ]
        <*> elements [ "", "topic", "nyːnɔʃk"]
        <*> oneof [ pure Nothing, (Just .) . (,) <$> arbitrary <*> arbitrary ]
        <*> elements [ "", "small message", BS.replicate 188273 0x34 ]
      arbitrarySubscribe = Subscribe
        <$> arbitrary
        <*> listOf1 ((,)
          <$> elements [ "+", "$SYS", "a/#"]
          <*> arbitrary )
      arbitrarySubscribeAcknowledgement = SubscribeAcknowledgement
        <$> arbitrary
        <*> listOf1 ( oneof [ pure Nothing, Just <$> arbitrary ])
      arbitraryUnsubscribe = Unsubscribe
        <$> arbitrary
        <*> listOf1 (elements [ "", "#", "a/+/b" ])
      arbitraryUnsubscribeAcknowledgement = UnsubscribeAcknowledgement
        <$> arbitrary

instance Arbitrary QualityOfService where
  arbitrary = elements [ AtLeastOnce, ExactlyOnce ]

instance Arbitrary ClientIdentifier where
  arbitrary = elements ["client-identifier"]

instance Arbitrary Will where
  arbitrary = Will
    <$> elements [ "", "nyːnɔʃk"]
    <*> elements [ "", "message body"]
    <*> elements [ Nothing, Just AtLeastOnce, Just ExactlyOnce ]
    <*> elements [ True, False ]

instance Arbitrary PacketIdentifier where
  arbitrary = PacketIdentifier <$> choose (0x0000, 0xffff)

instance Arbitrary ConnectionRefusal where
  arbitrary = elements [ minBound .. maxBound ]

instance Arbitrary QoS where
  arbitrary = elements [ QoS0, QoS1, QoS2 ]

-}
