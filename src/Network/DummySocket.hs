{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
module Network.DummySocket where

import           Control.Concurrent.Async
import           Control.Concurrent.Chan
import           Control.Concurrent.MVar
import qualified Data.ByteString          as BS
import           Network.Proxy
import           Network.Stack.Server

data DummySocket
  = DummySocket
      (Chan BS.ByteString)
      (MVar (Maybe BS.ByteString))
      (Chan BS.ByteString)

instance ServerStack DummySocket where
  data Server DummySocket = DummySocketServer DummySocket
  data ServerConfig DummySocket = DummySocketConfig DummySocket
  data ServerException DummySocket = DummySocketException
  data ServerConnection DummySocket = DummySocketConnection DummySocket
  data ServerConnectionInfo DummySocket = DummySocketConnectionInfo
  withServer (DummySocketConfig s) h = h (DummySocketServer s)
  withConnection (DummySocketServer s) h = async $ h (DummySocketConnection s) DummySocketConnectionInfo

instance StreamServerStack DummySocket where
  sendStream (DummySocketConnection s) = send s
  receiveStream (DummySocketConnection s) = receive s

newPair :: IO (DummySocket, DummySocket)
newPair = do
  c1 <- newChan
  c2 <- newChan
  m1 <- newMVar (Just mempty)
  m2 <- newMVar (Just mempty)
  pure (DummySocket c1 m1 c2, DummySocket c2 m2 c1)

send :: DummySocket -> BS.ByteString -> IO Int
send (DummySocket _ _ c) bs = writeChan c bs >> pure (BS.length bs)

receive :: DummySocket -> Int -> IO BS.ByteString
receive _ 0 = pure mempty
receive (DummySocket c m _) i =
  modifyMVar m $ \case
    Nothing -> pure (Nothing, mempty) -- connection closed by peer
    Just bs
      | BS.null bs       -> readChan c >>= \bs'-> if BS.null bs'
          then pure (Nothing, mempty)
          else pure (Just (BS.drop i bs'), BS.take i bs')
      | BS.length bs > i -> pure (Just (BS.drop i bs), BS.take i bs)
      | otherwise        -> pure (Just mempty, bs)

close :: DummySocket -> IO ()
close (DummySocket _ _ c) = writeChan c mempty


