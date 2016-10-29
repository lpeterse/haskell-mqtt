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
    [ testCase "not $ null $ singleton \"a\" ()" $ assertBool "null" $ not $ R.null $ R.singleton "a" ()
    , testCase "not $ null $ singleton \"a/b\" ()" $ assertBool "null" $ not $ R.null $ R.singleton "a/b" ()
    ]
  , testGroup "empty"
    [ testCase "null empty" $ assertBool "not null" $ R.null R.empty
    ]
  , testGroup "singleton"
    [ testCase "matchTopic \"a\" (singleton \"a\" ())" $ assertBool "null" $ R.matchTopic "a" $ R.singleton "a" ()
    , testCase "matchTopic \"a/b/c\" (singleton \"a/b/c\" ())" $ assertBool "null" $ R.matchTopic "a/b/c" $ R.singleton "a/b/c" ()
    ]
  , testGroup "matchTopic" [ ]
  , testGroup "matchFilter" [ missingTests ]
  , testGroup "lookupWith" [ missingTests ]
  , testGroup "insert" [ missingTests ]
  , testGroup "insertWith" [ missingTests ]
  , testGroup "map" [ missingTests ]
  , testGroup "adjust" [ missingTests ]
  , testGroup "delete" [ missingTests ]
  , testGroup "unionWith" [ missingTests ]
  , testGroup "differenceWith" [ missingTests ]
  ]
  where
    missingTests = testCase "test exists" $ assertFailure "no tests implemented"
