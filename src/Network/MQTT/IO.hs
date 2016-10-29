{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.IO
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.IO where

import           Control.Concurrent
import           Control.Exception
import qualified Data.ByteString               as BS
import qualified Data.ByteString.Builder.Extra as BS
import qualified Data.ByteString.Unsafe        as BS
import           Data.Word
import           Foreign.Marshal.Alloc
import           Foreign.Ptr
import           Network.MQTT.Message
import           Network.Transceiver

bufferedOutput :: (StreamConnection s, Data s ~ BS.ByteString) => s -> IO RawMessage -> IO (Maybe RawMessage) -> (BS.ByteString -> IO ()) -> IO ()
bufferedOutput transmitter getMessage getMaybeMessage sendByteString =
  bracket ( mallocBytes bufferSize ) free waitForMessage
  where
    bufferSize :: Int
    bufferSize  = 32768
    waitForMessage :: Ptr Word8 -> IO ()
    waitForMessage buffer =
      getMessage >>= sendMessage buffer 0
    pollForMessage :: Ptr Word8 -> Int -> IO ()
    pollForMessage buffer pos = do
      -- The following `yield` is essential for the subsequent call to return a message.
      -- It gives other threads waiting to put into the MVar the chance to do so.
      -- We rather wait 20ms before flushing the buffer rather than flushing a
      -- nearly empty buffer.
      -- Flushing small buffers means more garbage and more system calls.
      getMaybeMessage >>= \m-> case m of
        Nothing  -> do
          yield
          getMaybeMessage >>= \n-> case n of
            Nothing  -> flushBuffer buffer pos >> waitForMessage buffer
            Just msg -> sendMessage buffer pos msg
        Just msg -> sendMessage buffer pos msg
    sendMessage :: Ptr Word8 -> Int -> RawMessage -> IO ()
    sendMessage buffer pos msg = do
      -- print msg
      pos' <- runBuilder >>= \(written, next)-> finishWriter (pos + written) next
      case msg of
        Disconnect -> flushBuffer buffer pos'
        _          -> pollForMessage buffer pos'
      where
        runBuilder =
          BS.runBuilder (bRawMessage msg) (plusPtr buffer pos) (bufferSize - pos)
        finishWriter pos BS.Done =
          pure pos
        finishWriter pos (BS.More _ writer) = do
          flushBuffer buffer pos
          uncurry finishWriter =<< writer buffer bufferSize
        finishWriter' pos (BS.Chunk chunk writer) = do
          flushBuffer buffer pos
          sendByteString chunk
          uncurry finishWriter =<< writer buffer bufferSize
    flushBuffer :: Ptr Word8 -> Int -> IO ()
    flushBuffer buffer pos =
      BS.unsafePackCStringLen (castPtr buffer, pos) >>= \bs-> sendChunk transmitter bs >> pure ()
{-# INLINE bufferedOutput #-}
