module Main where

import           Test.Tasty

import qualified BrokerTest
import qualified EncodingTest
import qualified ProxyTest
import qualified TopicTest
import qualified TrieTest

main :: IO ()
main = do
  brokerTests <- BrokerTest.getTestTree
  defaultMain $ testGroup "Network.MQTT"
    [ TrieTest.tests
    , TopicTest.tests
    , EncodingTest.tests
    , brokerTests
    , ProxyTest.tests
    ]
