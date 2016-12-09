module Main where

import           Test.Tasty

import qualified BrokerTest
import qualified EncodingTest
import qualified RoutingTreeTest
import qualified TopicTest

main :: IO ()
main = do
  brokerTests <- BrokerTest.getTestTree
  defaultMain $ testGroup "Network.MQTT"
    [ RoutingTreeTest.tests
    , TopicTest.tests
    , EncodingTest.tests
    , brokerTests
    ]
