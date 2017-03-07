module Main where

import           Test.Tasty

import qualified BrokerTest
import qualified EncodingTest
import qualified TrieTest
import qualified TopicTest

main :: IO ()
main = do
  brokerTests <- BrokerTest.getTestTree
  defaultMain $ testGroup "Network.MQTT"
    [ TrieTest.tests
    , TopicTest.tests
    , EncodingTest.tests
    , brokerTests
    ]
