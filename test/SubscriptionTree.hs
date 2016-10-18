{-# LANGUAGE OverloadedStrings, FlexibleInstances #-}
module SubscriptionTree ( tests ) where

import Control.Exception ( try, SomeException )

import Data.Monoid
import Prelude hiding (head)

import qualified Data.Text as T
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck as QC

import Network.MQTT.SubscriptionTree

tests :: TestTree
tests = testGroup "SubscriptionTree"
  [ QC.testProperty "" $ \u f t->
      let _ = (1 :: Int) `asTypeOf` u in
      subscribers t (subscribe u f mempty) === subscribers t (subscribe u f mempty)
  ]

instance Arbitrary Topic where
  arbitrary = Topic <$> (listOf1 $ elements ["a", "b", "c"])

instance Arbitrary Filter where
  arbitrary = do
    ls <- listOf1 $ elements ["a", "b", "c", "+"]
    arbitrary >>= \b-> case b of
      True -> pure (Filter ls)
      False -> pure (Filter $ ls ++ ["#"])
