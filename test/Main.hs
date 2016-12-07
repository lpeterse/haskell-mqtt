module Main where

import           Test.Tasty

import qualified RoutingTreeTest
import qualified TopicTest
import qualified EncodingTest

main :: IO ()
main  = defaultMain $ testGroup "Network.MQTT"
  [ RoutingTreeTest.tests
  , TopicTest.tests
  , EncodingTest.tests
  ]
