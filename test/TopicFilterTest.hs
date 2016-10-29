{-# LANGUAGE OverloadedStrings #-}
module TopicTopicFilterTest ( tests ) where

import qualified Data.Attoparsec.ByteString as A
import qualified Data.ByteString.Short      as BS
import           Data.List.NonEmpty         (NonEmpty (..))
import           Data.Monoid
import qualified Data.Text                  as T

import           Test.Tasty
import           Test.Tasty.HUnit

import           Network.MQTT.TopicTopicFilter

tests :: TestTree
tests = testGroup "TopicTopicFilter"
  [ testGroup "Topic"
    [ testGroup "show"
      [ testCase "\"/\""       $ assertEqual "" "\"/\""      (show $ Topic $ "":|[""])
      , testCase "\"\x2603\""  $ assertEqual "" "\"\x2603\"" (show $ Topic $ BS.pack [0xE2,0x98,0x83]:|[])
      , testCase "\"a/b/c\""   $ assertEqual "" "\"a/b/c\""  (show $ Topic $ "a":|["b","c"])
      ]
    , testGroup "parseTopic"
      [ testCase "! \"\""      $ assertEqual "" (Left "Failed reading: invalid topic") $ A.parseOnly parseTopic ""
      , testCase "! \"\\NUL\"" $ assertEqual "" (Left "Failed reading: invalid topic") $ A.parseOnly parseTopic "\NUL"
      , testCase "! \"+\""     $ assertEqual "" (Left "Failed reading: invalid topic") $ A.parseOnly parseTopic "+"
      , testCase "! \"#\""     $ assertEqual "" (Left "Failed reading: invalid topic") $ A.parseOnly parseTopic "#"
      , testCase "  \"/\""     $ assertEqual "" (Right $ Topic $ "":|[""])           $ A.parseOnly parseTopic "/"
      , testCase "  \"//\""    $ assertEqual "" (Right $ Topic $ "":|["",""])        $ A.parseOnly parseTopic "//"
      , testCase "  \"/a\""    $ assertEqual "" (Right $ Topic $ "":|["a"])          $ A.parseOnly parseTopic "/a"
      , testCase "  \"a\""     $ assertEqual "" (Right $ Topic $ "a":|[])            $ A.parseOnly parseTopic "a"
      , testCase "  \"a/\""    $ assertEqual "" (Right $ Topic $ "a":|[""])          $ A.parseOnly parseTopic "a/"
      , testCase "  \"a/b123\""   $ assertEqual "" (Right $ Topic $ "a":|["b123"])   $ A.parseOnly parseTopic "a/b123"
      ]
    ]
  , testGroup "TopicFilter"
    [ testGroup "show"
      [ testCase "\"a/b/c\""       $ assertEqual "" "\"a/b/c\"" (show $ TopicFilter $ "a":|["b","c"])
      ]
    , testGroup "parseTopicFilter"
      [ testCase "! \"\""          $ assertEqual "" (Left "Failed reading: invalid filter")  $ A.parseOnly parseTopicFilter ""
      , testCase "! \"\\NUL\""     $ assertEqual "" (Left "Failed reading: invalid filter")  $ A.parseOnly parseTopicFilter "\NUL"
      , testCase "  \"+\""         $ assertEqual "" (Right $ TopicFilter $ "+":|[])             $ A.parseOnly parseTopicFilter "+"
      , testCase "  \"#\""         $ assertEqual "" (Right $ TopicFilter $ "#":|[])             $ A.parseOnly parseTopicFilter "#"
      , testCase "! \"#/\""        $ assertEqual "" (Left "Failed reading: invalid filter")  $ A.parseOnly parseTopicFilter "#/"
      , testCase "  \"/\""         $ assertEqual "" (Right $ TopicFilter $ "":|[""])            $ A.parseOnly parseTopicFilter "/"
      , testCase "  \"//\""        $ assertEqual "" (Right $ TopicFilter $ "":|["",""])         $ A.parseOnly parseTopicFilter "//"
      , testCase "  \"/a\""        $ assertEqual "" (Right $ TopicFilter $ "":|["a"])           $ A.parseOnly parseTopicFilter "/a"
      , testCase "  \"a\""         $ assertEqual "" (Right $ TopicFilter $ "a":|[])             $ A.parseOnly parseTopicFilter "a"
      , testCase "  \"a/\""        $ assertEqual "" (Right $ TopicFilter $ "a":|[""])           $ A.parseOnly parseTopicFilter "a/"
      , testCase "  \"a/b\""       $ assertEqual "" (Right $ TopicFilter $ "a":|["b"])          $ A.parseOnly parseTopicFilter "a/b"
      , testCase "  \"a/+/c123/#\""   $ assertEqual "" (Right $ TopicFilter $ "a":|["+","c123","#"])  $ A.parseOnly parseTopicFilter "a/+/c123/#"
      , testCase "! \"a/+/c123/#/d\"" $ assertEqual "" (Left "Failed reading: invalid filter")     $ A.parseOnly parseTopicFilter "a/+/c123/#/d"
      ]
    ]
  ]
