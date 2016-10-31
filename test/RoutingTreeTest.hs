{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
module RoutingTreeTest ( tests ) where

import           Data.List.NonEmpty       ( NonEmpty(..))
import           Data.Monoid
import qualified Data.Text                as T
import           Prelude                  hiding (head)

import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import qualified Network.MQTT.RoutingTree as R
import           Network.MQTT.Topic

tests :: TestTree
tests = testGroup "RoutingTree"
  [ testGroup "null"
    [ testCase "! null               (singleton \"a\"     ())"   $ assertBool "" $ not $ R.null $ R.singleton "a" ()
    , testCase "! null               (singleton \"a/b\"   ())"   $ assertBool "" $ not $ R.null $ R.singleton "a/b" ()
    ]
  , testGroup "empty"
    [ testCase "  null empty" $ assertBool "not null" $ R.null R.empty
    ]
  , testGroup "singleton"
    [ testCase "  matchTopic \"a\"     (singleton \"a\"     ())"   $ assertBool "" $ R.matchTopic "a" $ R.singleton "a" ()
    , testCase "  matchTopic \"a/b/c\" (singleton \"a/b/c\" ())"   $ assertBool "" $ R.matchTopic "a/b/c" $ R.singleton "a/b/c" ()
    ]
  , testGroup "matchTopic"
    [ testCase "  matchTopic \"a\"     (singleton \"a\"     ())"   $ assertBool ""       $ R.matchTopic "a"     $ R.singleton "a"   ()
    , testCase "  matchTopic \"a\"     (singleton \"#\"     ())"   $ assertBool ""       $ R.matchTopic "a"     $ R.singleton "#"   ()
    , testCase "  matchTopic \"a\"     (singleton \"a/#\"   ())"   $ assertBool ""       $ R.matchTopic "a"     $ R.singleton "a/#" ()
    , testCase "  matchTopic \"a/b\"   (singleton \"a/#\"   ())"   $ assertBool ""       $ R.matchTopic "a/b"   $ R.singleton "a/#" ()
    , testCase "  matchTopic \"a/b/c\" (singleton \"a/#\"   ())"   $ assertBool ""       $ R.matchTopic "a/b/c" $ R.singleton "a/#" ()
    , testCase "! matchTopic \"b/c/d\" (singleton \"a/#\"   ())"   $ assertBool "" $ not $ R.matchTopic "b/c/d" $ R.singleton "a/#" ()
    , testCase "! matchTopic \"a\"     (singleton \"a/+\"   ())"   $ assertBool "" $ not $ R.matchTopic "a"     $ R.singleton "a/+" ()
    , testCase "! matchTopic \"a\"     (singleton \"/a\"    ())"   $ assertBool "" $ not $ R.matchTopic "a"     $ R.singleton "/a"  ()
    , testCase "  matchTopic \"a/b\"   (singleton \"a/b\"   ())"   $ assertBool ""       $ R.matchTopic "a/b"   $ R.singleton "a/b" ()
    , testCase "  matchTopic \"a/b\"   (singleton \"a/+\"   ())"   $ assertBool ""       $ R.matchTopic "a/b"   $ R.singleton "a/+" ()
    , testCase "  matchTopic \"a/b\"   (singleton \"a/#\"   ())"   $ assertBool ""       $ R.matchTopic "a/b"   $ R.singleton "a/#" ()
    , testCase "  matchTopic \"a/b\"   (singleton \"a/b/#\" ())"   $ assertBool ""       $ R.matchTopic "a/b"   $ R.singleton "a/b/#" ()
    ]
  , testGroup "matchFilter"
    [ testCase "  matchFiler \"#\"     (singleton \"#\"     ())"   $ assertBool ""       $ R.matchFilter "#"     $ R.singleton "#"   ()
    , testCase "  matchFiler \"+\"     (singleton \"#\"     ())"   $ assertBool ""       $ R.matchFilter "+"     $ R.singleton "#"   ()
    , testCase "  matchFiler \"a\"     (singleton \"#\"     ())"   $ assertBool ""       $ R.matchFilter "a"     $ R.singleton "#"   ()
    , testCase "! matchFiler \"#\"     (singleton \"+\"     ())"   $ assertBool "" $ not $ R.matchFilter "#"     $ R.singleton "+"   ()
    , testCase "  matchFiler \"+\"     (singleton \"+\"     ())"   $ assertBool ""       $ R.matchFilter "+"     $ R.singleton "+"   ()
    , testCase "  matchFiler \"a\"     (singleton \"+\"     ())"   $ assertBool ""       $ R.matchFilter "a"     $ R.singleton "+"   ()
    , testCase "! matchFiler \"#\"     (singleton \"a\"     ())"   $ assertBool "" $ not $ R.matchFilter "#"     $ R.singleton "a"   ()
    , testCase "! matchFiler \"+\"     (singleton \"a\"     ())"   $ assertBool "" $ not $ R.matchFilter "+"     $ R.singleton "a"   ()
    , testCase "  matchFiler \"a\"     (singleton \"a\"     ())"   $ assertBool ""       $ R.matchFilter "a"     $ R.singleton "a"   ()
    , testCase "  matchFiler \"a\"     (singleton \"a/#\"   ())"   $ assertBool ""       $ R.matchFilter "a"     $ R.singleton "a/#" ()
    , testCase "! matchFiler \"a\"     (singleton \"a/+\"   ())"   $ assertBool "" $ not $ R.matchFilter "a"     $ R.singleton "a/+" ()
    , testCase "  matchFiler \"a/#\"   (singleton \"#\"     ())"   $ assertBool ""       $ R.matchFilter "a/#"   $ R.singleton "#"   ()
    , testCase "  matchFiler \"a/b/#\" (singleton \"#\"     ())"   $ assertBool ""       $ R.matchFilter "a/b/#" $ R.singleton "#"   ()
    ]
  , testGroup "lookupWith" [ ]
  , testGroup "insert" [  ]
  , testGroup "insertWith" [  ]
  , testGroup "map" [  ]
  , testGroup "adjust" [  ]
  , testGroup "delete" [  ]
  , testGroup "unionWith" [  ]
  , testGroup "differenceWith" [  ]
  ]
