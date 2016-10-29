{-# LANGUAGE OverloadedStrings #-}
module TopicFilterTest ( tests ) where

import qualified Data.Attoparsec.ByteString as A
import qualified Data.ByteString.Short      as BS
import           Data.List.NonEmpty         (NonEmpty (..))
import           Data.Monoid
import qualified Data.Text                  as T

import           Test.Tasty
import           Test.Tasty.HUnit

import qualified Network.MQTT.TopicFilter   as F

tests :: TestTree
tests = testGroup "TopicFilter"
  [ testGroup "Topic"
    [ testGroup "show"
      [ testCase "\"/\""       $ assertEqual "" "\"/\""      (show $ F.Topic $ "":|[""])
      , testCase "\"\x2603\""  $ assertEqual "" "\"\x2603\"" (show $ F.Topic $ BS.pack [0xE2,0x98,0x83]:|[])
      , testCase "\"a/b/c\""   $ assertEqual "" "\"a/b/c\""  (show $ F.Topic $ "a":|["b","c"])
      ]
    , testGroup "parseTopic"
      [ testCase "! \"\""      $ assertEqual "" (Left "Failed reading: invalid topic") $ A.parseOnly F.parseTopic ""
      , testCase "! \"\\NUL\"" $ assertEqual "" (Left "Failed reading: invalid topic") $ A.parseOnly F.parseTopic "\NUL"
      , testCase "! \"+\""     $ assertEqual "" (Left "Failed reading: invalid topic") $ A.parseOnly F.parseTopic "+"
      , testCase "! \"#\""     $ assertEqual "" (Left "Failed reading: invalid topic") $ A.parseOnly F.parseTopic "#"
      , testCase "  \"/\""     $ assertEqual "" (Right $ F.Topic $ "":|[""])           $ A.parseOnly F.parseTopic "/"
      , testCase "  \"//\""    $ assertEqual "" (Right $ F.Topic $ "":|["",""])        $ A.parseOnly F.parseTopic "//"
      , testCase "  \"/a\""    $ assertEqual "" (Right $ F.Topic $ "":|["a"])          $ A.parseOnly F.parseTopic "/a"
      , testCase "  \"a\""     $ assertEqual "" (Right $ F.Topic $ "a":|[])            $ A.parseOnly F.parseTopic "a"
      , testCase "  \"a/\""    $ assertEqual "" (Right $ F.Topic $ "a":|[""])          $ A.parseOnly F.parseTopic "a/"
      , testCase "  \"a/b123\""   $ assertEqual "" (Right $ F.Topic $ "a":|["b123"])   $ A.parseOnly F.parseTopic "a/b123"
      ]
    ]
  , testGroup "Filter"
    [ testGroup "show"
      [ testCase "\"a/b/c\""       $ assertEqual "" "\"a/b/c\"" (show $ F.Filter $ "a":|["b","c"])
      ]
    , testGroup "parseFilter"
      [ testCase "! \"\""          $ assertEqual "" (Left "Failed reading: invalid filter")  $ A.parseOnly F.parseFilter ""
      , testCase "! \"\\NUL\""     $ assertEqual "" (Left "Failed reading: invalid filter")  $ A.parseOnly F.parseFilter "\NUL"
      , testCase "  \"+\""         $ assertEqual "" (Right $ F.Filter $ "+":|[])             $ A.parseOnly F.parseFilter "+"
      , testCase "  \"#\""         $ assertEqual "" (Right $ F.Filter $ "#":|[])             $ A.parseOnly F.parseFilter "#"
      , testCase "! \"#/\""        $ assertEqual "" (Left "Failed reading: invalid filter")  $ A.parseOnly F.parseFilter "#/"
      , testCase "  \"/\""         $ assertEqual "" (Right $ F.Filter $ "":|[""])            $ A.parseOnly F.parseFilter "/"
      , testCase "  \"//\""        $ assertEqual "" (Right $ F.Filter $ "":|["",""])         $ A.parseOnly F.parseFilter "//"
      , testCase "  \"/a\""        $ assertEqual "" (Right $ F.Filter $ "":|["a"])           $ A.parseOnly F.parseFilter "/a"
      , testCase "  \"a\""         $ assertEqual "" (Right $ F.Filter $ "a":|[])             $ A.parseOnly F.parseFilter "a"
      , testCase "  \"a/\""        $ assertEqual "" (Right $ F.Filter $ "a":|[""])           $ A.parseOnly F.parseFilter "a/"
      , testCase "  \"a/b\""       $ assertEqual "" (Right $ F.Filter $ "a":|["b"])          $ A.parseOnly F.parseFilter "a/b"
      , testCase "  \"a/+/c123/#\""   $ assertEqual "" (Right $ F.Filter $ "a":|["+","c123","#"])  $ A.parseOnly F.parseFilter "a/+/c123/#"
      , testCase "! \"a/+/c123/#/d\"" $ assertEqual "" (Left "Failed reading: invalid filter")     $ A.parseOnly F.parseFilter "a/+/c123/#/d"
      ]
    ]
  ]
