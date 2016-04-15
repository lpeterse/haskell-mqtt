{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Exception ( try )

import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Attoparsec.ByteString
import Data.Attoparsec.ByteString.Char8 ( anyChar, char )

import Network.MQTT.Message
import Network.MQTT.Message.Utf8String

import Prelude hiding (head)

import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck as QC

main :: IO ()
main  = defaultMain $ testGroup "Network" [ testGroup "MQTT" [tgMessage]]

tgMessage :: TestTree
tgMessage = testGroup "Message"
  [ tgMessageRemainingLength
  , tgMessageUtf8String
  ]

tgMessageUtf8String :: TestTree
tgMessageUtf8String =
  testGroup "Utf8String"
  [ testCase "empty string" $ assertEqual ""
      ( Right "" )
      ( parseOnly pUtf8String $ BS.pack [0,0] )

  , testCase "string \"abc\"" $ assertEqual ""
      ( Right "abc" )
      ( parseOnly pUtf8String $ BS.pack [0,3,97,98,99] )

  , testCase "string of size 0x345" $ assertEqual ""
      ( Right $ T.replicate 0x345 "a" )
      ( parseOnly pUtf8String $ BS.pack $ [0x03,0x45] ++ replicate 0x0345 97 )

  , testCase "U+D800 [MQTT-1.5.3-1]" $ assertEqual ""
      ( Left "Failed reading: pUtf8String: violation of [MQTT-1.5.3]")
      ( parseOnly pUtf8String $ BS.pack [0,2,0xd8,0x01] )

  , testCase "U+DFFF [MQTT-1.5.3-1]" $ assertEqual ""
      ( Left "Failed reading: pUtf8String: violation of [MQTT-1.5.3]" )
      ( parseOnly pUtf8String $ BS.pack [0,2,0xdf,0xff] )

  , testCase "U+0000 [MQTT-1.5.3-2]" $ assertEqual ""
      ( Left "Failed reading: pUtf8String: violation of [MQTT-1.5.3-2]" )
      ( parseOnly pUtf8String $ BS.pack [0,1,0] )

  , testCase "U+FEFF [MQTT-1.5.3-3]" $ assertEqual ""
      ( Right "\65279" )
      ( parseOnly pUtf8String $ BS.pack [0x00,0x03,0xef,0xbb,0xbf] )

  , testCase "pUtf8String . sUtf8String == id" $
      let txt = T.replicate 999 "abc"
      in assertEqual ""
      ( Right txt )
      ( parseOnly pUtf8String (LBS.toStrict $ BS.toLazyByteString (sUtf8String txt)))
  ]

tgMessageRemainingLength :: TestTree
tgMessageRemainingLength =
  testGroup "pRemainingLength, sRemainingLength"
  [ testCase "p [193,2] == 321" $ assertEqual ""
      ( Right 321 )
      ( parseOnly pRemainingLength $ BS.pack [193,2] )

  , testCase "p [0x00] == 0" $ assertEqual ""
      ( Right 0 )
      ( parseOnly pRemainingLength (BS.pack [0x00]) )

  , testCase "p [0x7f] == 127" $ assertEqual ""
      ( Right 127 )
      ( parseOnly pRemainingLength (BS.pack [0x7f]) )

  , testCase "p [0x80, 0x01] == 128" $ assertEqual ""
      ( Right 128 )
      ( parseOnly pRemainingLength (BS.pack [0x80, 0x01]) )

  , testCase "p [0xff, 0x7f] == 16383" $ assertEqual ""
      ( Right 16383 )
      ( parseOnly pRemainingLength (BS.pack [0xff, 0x7f]) )

  , testCase "p [0x80, 0x80, 0x01] == 16384" $ assertEqual ""
      ( Right 16384 )
      ( parseOnly pRemainingLength (BS.pack [0x80, 0x80, 0x01]) )

  , testCase "p [0xff, 0xff, 0x7f] == 2097151" $ assertEqual ""
      ( Right 2097151 )
      ( parseOnly pRemainingLength (BS.pack [0xff, 0xff, 0x7f]) )

  , testCase "p [0x80, 0x80, 0x80, 0x01] == 2097152" $ assertEqual ""
      ( Right 2097152 )
      ( parseOnly pRemainingLength (BS.pack [0x80, 0x80, 0x80, 0x01]) )

  , testCase "p [0xff, 0xff, 0xff, 0x7f] == 268435455" $ assertEqual ""
      ( Right 268435455 )
      ( parseOnly pRemainingLength (BS.pack [0xff, 0xff, 0xff, 0x7f]) )

  , testCase "p [0xff, 0xff, 0xff, 0xff] == invalid" $ assertEqual ""
      ( Left "Failed reading: pRemainingLength: invalid input" )
      ( parseOnly pRemainingLength (BS.pack [0xff, 0xff, 0xff, 0xff]) )

  , QC.testProperty "pRemainingLength . sRemainingLength == id" $
      \i -> let i' = i `mod` 268435455
            in  Right i' == parseOnly pRemainingLength (LBS.toStrict $ BS.toLazyByteString (sRemainingLength i'))
  ]
