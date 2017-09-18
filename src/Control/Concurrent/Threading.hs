{-# LANGUAGE LambdaCase #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Control.Concurrent.Threading
-- Copyright   :  (c) Lars Petersen 2017
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Control.Concurrent.Threading
  ( acquireAndHandleWithLimitedNumberOfThreads ) where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import qualified Data.Set           as S

acquireAndHandleWithLimitedNumberOfThreads :: Int -> ((r -> IO ()) -> IO ()) -> (r -> IO ()) -> IO ()
acquireAndHandleWithLimitedNumberOfThreads maxThreads withResource handleResource = do
  qsem <- newQSem maxThreads
  withDependentFork $ \fork->
    forever $ do
      blocker <- newEmptyMVar
      fork $
        -- The child threads needs to acquire a unit of the quantity semaphore
        -- first. It eventually blocks until another thread terminates.
        -- The `bracket_` assures that the semaphore never leaks.
        bracket_ (waitQSem qsem) (signalQSem qsem) $
          -- Acquire the resource - eventually block here.
          -- `withResource` is responsible to safely release the resource after
          -- the handler function has returned or thrown.
          handle
            ( void . tryPutMVar blocker . Just :: SomeException -> IO () )
            $ withResource $ \resource-> do
              -- Signal the main thread that the child thread has acquired a
              -- resource.
              putMVar blocker Nothing
              -- Execute the resource handler within the child thread.
              handleResource resource
      -- The main thread waits here until the most recently forked child thread
      -- has acquired a resource. It then continues to fork off the next thread
      -- when `Nothing` has been signaled by the child thread or an exception
      -- is rethrown if the child thread encountered an exception during
      -- resource acquisition.
      takeMVar blocker >>= \case
        Nothing -> pure ()
        Just e  -> throwIO e

withDependentFork :: ((IO () -> IO ()) -> IO ()) -> IO ()
withDependentFork withFork = bracket before after within
  where
    within :: MVar (Maybe (S.Set ThreadId)) -> IO ()
    within  = withFork . forkWithRegistration

    before :: IO (MVar (Maybe (S.Set ThreadId)))
    before  = newMVar (Just mempty)

    after  :: MVar (Maybe (S.Set ThreadId)) -> IO ()
    after mtids = swapMVar mtids Nothing >>= \case
      Nothing -> pure ()
      Just tids -> mapM_ killThread tids

    forkWithRegistration :: MVar (Maybe (S.Set ThreadId)) -> IO () -> IO ()
    forkWithRegistration mtids action = void $ forkIO $ do
      tid <- myThreadId
      bracket_ (before' tid) (after' tid) action
      where
        before' tid =
          modifyMVar_ mtids $ \case
            Nothing   -> throwIO ThreadKilled -- We should have been killed.
            Just tids -> pure $! Just $! S.insert tid tids
        after' tid =
          modifyMVar_ mtids $ \case
            Nothing   -> pure Nothing -- Shouldn't happen, but doesn't do harm.
            Just tids -> pure $! Just $! S.delete tid tids
