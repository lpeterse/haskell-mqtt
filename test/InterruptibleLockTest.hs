{-# LANGUAGE OverloadedStrings, LambdaCase #-}
module Main where

import           Control.Monad ( void )
import           Control.Exception
import           Control.Concurrent
import           Control.Concurrent.Async
import           Test.Tasty
import           Test.Tasty.HUnit

import qualified Control.Concurrent.InterruptibleLock as InterruptibleLock

main :: IO ()
main = defaultMain $ testGroup "Control.Concurrent.InterruptibleLock"
  [ testCase "2 threads, subsequently" $ do
      sem <- InterruptibleLock.newInterruptibleLock
      m1  <- newEmptyMVar :: IO (MVar Int)
      m2  <- newEmptyMVar :: IO (MVar Int)
      InterruptibleLock.exclusively sem $
        putMVar m1 1
      InterruptibleLock.exclusively sem $
        putMVar m2 2
      assertEqual "m1" 1 =<< takeMVar m1
      assertEqual "m2" 2 =<< takeMVar m2

  , testCase "2 threads, one interrupting the other" $ do
      sem <- InterruptibleLock.newInterruptibleLock
      m1  <- newEmptyMVar :: IO (MVar Int)
      m2  <- newEmptyMVar :: IO (MVar Int)
      m3  <- newEmptyMVar :: IO (MVar Int)
      t1  <- async $
        InterruptibleLock.exclusively sem $ do
          putMVar m1 1
          threadDelay 2000000 `finally` putMVar m2 3
          void $ swapMVar m1 2
          pure (4 :: Int)
      t2  <- async $ do
        threadDelay 1000000
        InterruptibleLock.exclusively sem $ do
          putMVar m3 5
          pure (6 :: Int)
      threadDelay 3000000
      waitCatch t1 >>= \case
        Left  _ -> pure ()
        Right _ -> assertFailure "thread 1 should have been interrupted"
      waitCatch t2 >>= \case
        Left  _ -> assertFailure "thread 2 should not have failed"
        Right x -> assertEqual "t2" 6 x
      assertEqual "m1" 1 =<< takeMVar m1
      assertEqual "m2" 3 =<< takeMVar m2
      assertEqual "m3" 5 =<< takeMVar m3
  ]
