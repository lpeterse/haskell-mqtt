{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Exception ( try, SomeException )

import Data.Monoid
import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Serialize.Get as SG

import Network.MQTT.Message

import Prelude hiding (head)

import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck as QC

import qualified RoutingTreeTest

main :: IO ()
main  = defaultMain $ testGroup "Network.MQTT"
  [ RoutingTreeTest.tests
  , tgRawMessage
  ]

tgRawMessage :: TestTree
tgRawMessage = testGroup "RawMessage"
  [ tgRawMessageRemainingLength
  , tgRawMessageUtf8String
  , tgRawMessageAll
  ]

tgRawMessageUtf8String :: TestTree
tgRawMessageUtf8String =
  testGroup "Utf8String"
  [ testCase "empty string" $ assertEqual ""
      ( Right "" )
      ( SG.runGet pUtf8String $ BS.pack [0,0] )

  , testCase "string \"abc\"" $ assertEqual ""
      ( Right "abc" )
      ( SG.runGet pUtf8String $ BS.pack [0,3,97,98,99] )

  , testCase "string of size 0x345" $ assertEqual ""
      ( Right $ T.replicate 0x345 "a" )
      ( SG.runGet pUtf8String $ BS.pack $ [0x03,0x45] ++ replicate 0x0345 97 )

  , testCase "U+D800 [MQTT-1.5.3-1]" $ assertEqual ""
      ( Left "Failed reading: pUtf8String: Violation of [MQTT-1.5.3].\nEmpty call stack\n")
      ( SG.runGet pUtf8String $ BS.pack [0,2,0xd8,0x01] )

  , testCase "U+DFFF [MQTT-1.5.3-1]" $ assertEqual ""
      ( Left "Failed reading: pUtf8String: Violation of [MQTT-1.5.3].\nEmpty call stack\n" )
      ( SG.runGet pUtf8String $ BS.pack [0,2,0xdf,0xff] )

  , testCase "U+0000 [MQTT-1.5.3-2]" $ assertEqual ""
      ( Left "Failed reading: pUtf8String: Violation of [MQTT-1.5.3-2].\nEmpty call stack\n" )
      ( SG.runGet pUtf8String $ BS.pack [0,1,0] )

  , testCase "U+FEFF [MQTT-1.5.3-3]" $ assertEqual ""
      ( Right "\65279" )
      ( SG.runGet pUtf8String $ BS.pack [0x00,0x03,0xef,0xbb,0xbf] )

  , testCase "sUtf8String $ T.replicate 65536 \"a\"" $ do
     e <- try $ return $! BS.toLazyByteString $ bUtf8String $ T.replicate 65536 "a"
     case e :: Either SomeException LBS.ByteString of
       Left _  -> return ()
       Right _ -> assertFailure "Expected exception, instead got result."

  , testCase "pUtf8String . sUtf8String == id" $
      let txt = T.replicate 999 "abc"
      in assertEqual ""
      ( Right txt )
      ( SG.runGet pUtf8String (LBS.toStrict $ BS.toLazyByteString (bUtf8String txt)))
  ]

tgRawMessageRemainingLength :: TestTree
tgRawMessageRemainingLength =
  testGroup "pRemainingLength, bRemainingLength"
  [ testCase "p [193,2] == 321" $ assertEqual ""
      ( Right 321 )
      ( SG.runGet pRemainingLength $ BS.pack [193,2] )

  , testCase "p [0x00] == 0" $ assertEqual ""
      ( Right 0 )
      ( SG.runGet pRemainingLength (BS.pack [0x00]) )

  , testCase "p [0x7f] == 127" $ assertEqual ""
      ( Right 127 )
      ( SG.runGet pRemainingLength (BS.pack [0x7f]) )

  , testCase "p [0x80, 0x01] == 128" $ assertEqual ""
      ( Right 128 )
      ( SG.runGet pRemainingLength (BS.pack [0x80, 0x01]) )

  , testCase "p [0xff, 0x7f] == 16383" $ assertEqual ""
      ( Right 16383 )
      ( SG.runGet pRemainingLength (BS.pack [0xff, 0x7f]) )

  , testCase "p [0x80, 0x80, 0x01] == 16384" $ assertEqual ""
      ( Right 16384 )
      ( SG.runGet pRemainingLength (BS.pack [0x80, 0x80, 0x01]) )

  , testCase "p [0xff, 0xff, 0x7f] == 2097151" $ assertEqual ""
      ( Right 2097151 )
      ( SG.runGet pRemainingLength (BS.pack [0xff, 0xff, 0x7f]) )

  , testCase "p [0x80, 0x80, 0x80, 0x01] == 2097152" $ assertEqual ""
      ( Right 2097152 )
      ( SG.runGet pRemainingLength (BS.pack [0x80, 0x80, 0x80, 0x01]) )

  , testCase "p [0xff, 0xff, 0xff, 0x7f] == 268435455" $ assertEqual ""
      ( Right 268435455 )
      ( SG.runGet pRemainingLength (BS.pack [0xff, 0xff, 0xff, 0x7f]) )

  , testCase "p [0xff, 0xff, 0xff, 0xff] == invalid" $ assertEqual ""
      ( Left "Failed reading: pRemainingLength: invalid input\nEmpty call stack\n" )
      ( SG.runGet pRemainingLength (BS.pack [0xff, 0xff, 0xff, 0xff]) )

  , QC.testProperty "pRemainingLength . bRemainingLength == id" $
      \i -> let i' = i `mod` 268435455
            in  Right i' == SG.runGet pRemainingLength
              (LBS.toStrict $ BS.toLazyByteString (bRemainingLength i'))
  ]

tgRawMessageAll :: TestTree
tgRawMessageAll = QC.testProperty "pRawMessage . bRawMessage == id" $ \msg->
  Right msg === SG.runGet pRawMessage (LBS.toStrict $ BS.toLazyByteString $ bRawMessage msg)

instance Arbitrary RawMessage where
  arbitrary = undefined {-oneof
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
        <$> arbitrary -}

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
