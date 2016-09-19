module Control.Concurrent.Broadcast
  ( Broadcast ()
  , BroadcastListener ()
  , newBroadcast
  , broadcast
  , terminate
  , listen
  , accept
  ) where

import Data.Maybe
import Data.Typeable
import Control.Exception
import Control.Concurrent.MVar

data Tail a
   = Tail (MVar (Maybe (Tail a, a)))

newtype Broadcast a
      = Broadcast (MVar (Tail a))

newtype BroadcastListener a
      = BroadcastListener (MVar (Tail a))

data BroadcastTerminatedException = BroadcastTerminatedException
  deriving (Show, Typeable)

instance Exception BroadcastTerminatedException

newBroadcast :: IO (Broadcast a)
newBroadcast  = Broadcast <$> ( newMVar =<< Tail <$> newEmptyMVar )

broadcast :: Broadcast a -> a -> IO ()
broadcast (Broadcast mt) a = modifyMVar_ mt $ \(Tail currentTail)-> do
    newTail <- Tail <$> newEmptyMVar          -- a new (unresolved) tail
    putMVar currentTail (Just (newTail, a))   -- resolve the current head
    pure newTail                              -- new tail replaces current

terminate :: Broadcast a -> IO ()
terminate (Broadcast mt) = modifyMVar_ mt $ \(Tail currentTail)-> do
    putMVar currentTail Nothing               -- resolve the current tail
    pure (Tail currentTail)                   -- new tail replaces current

listen :: Broadcast a -> IO (BroadcastListener a)
listen (Broadcast mt) = BroadcastListener <$> ( newMVar =<< readMVar mt )

tryAccept ::  BroadcastListener a -> IO (Maybe a)
tryAccept (BroadcastListener mt) = modifyMVar mt $ \(Tail oldTail)-> do
  mat <- readMVar oldTail
  case mat of
    Nothing           -> pure (Tail oldTail, Nothing)
    Just (newTail, a) -> pure (newTail, Just a)

accept :: BroadcastListener a -> IO a
accept bl = do
  ma <- tryAccept bl
  case ma of
    Nothing -> throwIO BroadcastTerminatedException
    Just a  -> pure a

main :: IO ()
main = do
  b <- newBroadcast
  x <- listen b
  y <- listen b
  broadcast b 1
  tryAccept x >>= print
  broadcast b 2
  tryAccept x >>= print
  broadcast b 3
  terminate b
  tryAccept x >>= print
  tryAccept x >>= print
  tryAccept y >>= print
  tryAccept y >>= print
  tryAccept y >>= print
  tryAccept y >>= print
