{-# LANGUAGE LambdaCase #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Control.Concurrent.InterruptibleLock
-- Copyright   :  (c) Lars Petersen 2017
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Control.Concurrent.InterruptibleLock where

import Control.Monad ( void )
import Control.Exception
import Control.Concurrent

-- | A `InterruptibleLock` protects a critical section in a way that
--   another thread can interrupt an ongoing computation and take over.
newtype InterruptibleLock = InterruptibleLock (MVar (), MVar ThreadId)

newInterruptibleLock :: IO InterruptibleLock
newInterruptibleLock = InterruptibleLock <$> ((,) <$> newMVar () <*> newEmptyMVar)

-- | Enter a critial section with exlusivity and kill any currently running thread.
--
--   - When no other thread is currently in the section, the action is
--     executed immediately.
--   - When a thread wants to enter and another one is already in the section,
--     the one in the section gets a `ThreadKilled` exception. The waiting
--     thread will enter no earlier than before the killed thread has terminated.
--     This assures that the killed thread can execute cleanup handlers while
--     still having exclusivity wrt to the section.
exclusively :: InterruptibleLock -> IO a -> IO a
exclusively (InterruptibleLock (mw, mt)) action =
  bracket acquireLock releaseLock (const action)
  where
    -- Acquiring the Lock is a two step process in order to assure that
    -- only one thread at a time kills the acting thread and waits for its
    -- termination. All other threads will wait in front of `mw`.
    acquireLock = withMVar mw $ const $ do
          -- The thread holds a lock on `mw` here. Find out whether there is
          -- thread within the section and eventually kill it.
          tryReadMVar mt >>= \case
            Nothing -> pure ()
            Just q -> killThread q
          -- The next line will block and unblock as soon as the killed thread
          -- has terminated. In case no thread is running, we can put immediately.
          -- A race between this line and the line before is ruled out by the
          -- the lock on `mw`.
          -- As soon as we succeeded in putting in our thread id, we release the
          -- lock on `mw` which means we too might get killed while executing
          -- our action.
          myThreadId >>= putMVar mt
    -- Releasing the Lock means taking the own thread id back
    -- from the locking MVar. It is logically guaranteed that the MVar
    -- contains the own thread id as a thread will get here only after it has
    -- put his thread id in and no other thread can do this as long as the MVar
    -- is filled.
    releaseLock = const $ void (takeMVar mt)
