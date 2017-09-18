{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
module ThreadingTest (tests) where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.Threading
import           Control.Exception
import           Control.Monad

import           Test.Tasty
import           Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "Network.Stack.Threading"
  [ test001
  , test002
  , test003
  , test004
  ]

test001 :: TestTree
test001 = testCase "Threads shall be forked when resource becomes available" $ do

  m0 <- newEmptyMVar :: IO (MVar Int)
  m1 <- newEmptyMVar :: IO (MVar Int)
  m2 <- newEmptyMVar :: IO (MVar Int)

  withAsync
      ( acquireAndHandleWithLimitedNumberOfThreads 2
        ( bracket
            (takeMVar m0)
            (putMVar m1)
        )
        ( putMVar m2 )
      )
      ( const $ forM_ [0..10] $ \i-> do
        putMVar m0 i
        assertEqual "takeMVar m1" i =<< takeMVar m1
        assertEqual "takeMVar m2" i =<< takeMVar m2
      )

test002 :: TestTree
test002 = testCase "Thread limit shall be respected even if resource is available" $ do

  m0 <- newEmptyMVar :: IO (MVar Int)

  withAsync
    ( acquireAndHandleWithLimitedNumberOfThreads 2
      ( bracket
          (takeMVar m0)
          (const $ pure ())
      )
      ( const $ threadDelay 100000000 )
    )
    ( const $ do
      putMVar m0 0
      putMVar m0 1
      putMVar m0 2
      threadDelay 1000
      assertEqual "readMVar m0" 2 =<< readMVar m0
    )

test003 :: TestTree
test003 = testCase "Killing main thread shall kill all child threads" $ do

  m0 <- newEmptyMVar :: IO (MVar Int)
  m1 <- newMVar 0 :: IO (MVar Int)

  withAsync
    ( acquireAndHandleWithLimitedNumberOfThreads 4
      ( bracket
          (takeMVar m0)
          (const $ modifyMVar_ m1 $ \i-> pure (i + 1))
      )
      ( const $ threadDelay 100000000 )
    )
    ( \mainThread-> do
      putMVar m0 0
      putMVar m0 1
      putMVar m0 2
      putMVar m0 3
      threadDelay 1000
      assertEqual "readMVar m1" 0 =<< readMVar m1
      cancel mainThread
      threadDelay 1000
      assertEqual "readMVar m1" 4 =<< readMVar m1
    )

test004 :: TestTree
test004 = testCase "Exception in resource acquisition shall be rethrown in the main thread" $ do

  m0 <- newEmptyMVar :: IO (MVar Int)
  m1 <- newMVar 0    :: IO (MVar Int)

  withAsync
    ( acquireAndHandleWithLimitedNumberOfThreads 4
      ( bracket
          ( takeMVar m0 >>= \case
              2 -> throwIO Overflow
              i -> pure i
          )
          ( const $ modifyMVar_ m1 $ \i-> pure (i + 1) )
      )
      ( const $ threadDelay 100000000 )
    )
    ( \mainThread-> do
      putMVar m0 0
      threadDelay 1000
      assertEqual "poll mainThread" "Nothing" =<< show <$> poll mainThread
      putMVar m0 1
      threadDelay 1000
      assertEqual "poll mainThread" "Nothing" =<< show <$> poll mainThread
      putMVar m0 2
      threadDelay 1000
      assertEqual "poll mainThread" "Just (Left arithmetic overflow)" =<< show <$> poll mainThread
      assertEqual "readMVar m1" 2 =<< readMVar m1
    )
