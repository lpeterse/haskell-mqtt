{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Exception ( try )

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Attoparsec.ByteString
import Data.Attoparsec.ByteString.Char8 ( anyChar, char )

import Network.MQTT.Message

import Prelude hiding (head)

import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck as QC

main :: IO ()
main  = defaultMain $ testGroup "Network" [ testGroup "MQTT" [tgMessage]]

tgMessage :: TestTree
tgMessage = testGroup "Message"
  [ tgMessageRemainingLength
  ]

tgMessageRemainingLength :: TestTree
tgMessageRemainingLength=
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
      ( Left "Failed reading: Invalid remaining length." )
      ( parseOnly pRemainingLength (BS.pack [0xff, 0xff, 0xff, 0xff]) )

  , QC.testProperty "pRemainingLength . sRemainingLength == id" $
      \i -> let i' = i `mod` 268435455
            in  Right i' == parseOnly pRemainingLength (LBS.toStrict $ BS.toLazyByteString (sRemainingLength i'))
  ]
