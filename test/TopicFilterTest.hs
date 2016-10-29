{-# LANGUAGE OverloadedStrings #-}
module TopicFilterTest ( tests ) where

import qualified Data.Attoparsec.ByteString as A
import           Data.List.NonEmpty       ( NonEmpty(..) )
import           Data.Monoid
import qualified Data.Text                as T
import           Prelude                  hiding (head)

import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import qualified Network.MQTT.TopicFilter as F

tests :: TestTree
tests = testGroup "TopicFilter"
  [ testGroup "parseTopic"
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
