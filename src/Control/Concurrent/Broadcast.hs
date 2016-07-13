module Control.Concurrent.Broadcast
  ( Broadcast ()
  , Subscription ()
  , newBroadcast
  , broadcast
  , terminate
  , subscribe
  , receive
  ) where

import Data.Typeable
import Control.Exception
import Control.Concurrent.MVar

data Tail a
   = Tail (MVar (Tail a, Maybe a))

newtype Broadcast a
      = Broadcast (MVar (Tail a))

newtype Subscription a
      = Subscription (MVar (Tail a))

data BroadcastTerminated = BroadcastTerminated
  deriving (Show, Typeable)

instance Exception BroadcastTerminated

newBroadcast :: IO (Broadcast a)
newBroadcast  = Broadcast <$> ( newMVar =<< Tail <$> newEmptyMVar )

broadcast :: Broadcast a -> a -> IO ()
broadcast (Broadcast mt) a = modifyMVar_ mt $ \(Tail mta)-> do
    nextTail <- EventualTail <$> newEmptyMVar -- a new (unresolved) tail
    putMVar currentTail (nextTail, a)         -- resolve the current head
    pure nextTail                             -- new tail replaces current

terminate :: Broadcast a -> IO ()
terminate (Broadcast mt) = modifyMVar_ mt $ \t-> case t of
  EmptyTail ->
    pure ()
  currentTail@(EventualTail mt) -> do
    nextTail <- EventualTail <$> newEmptyMVar -- a new (unresolved) tail
    putMVar currentTail (nextTail, Just a)    -- resolve the current head
    pure nextTail                             -- new tail replaces current

subscribe :: Broadcast a -> IO (Subscription a)
subscribe (Broadcast mt) = Subscription <$> ( newMVar =<< readMVar mt )

receive :: Subscription a -> IO a
receive (Subscription mt) = modifyMVar mt $ \(Tail next)-> readMVar next
