{-# LANGUAGE OverloadedStrings, TypeFamilies, BangPatterns #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.IO where

import Data.Int
import Data.Word
import Data.Typeable
import qualified Data.Map as M
import qualified Data.IntMap as IM
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Builder.Extra as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Attoparsec.ByteString as A
import qualified Data.Text as T

import Foreign.Ptr
import Foreign.Marshal.Alloc

import Control.Exception
import Control.Monad
import Control.Concurrent
import Control.Concurrent.MVar

import Network.MQTT
import Network.MQTT.Message

bufferedOutput :: IO RawMessage -> IO (Maybe RawMessage) -> (BS.ByteString -> IO ()) -> IO ()
bufferedOutput getMessage getMaybeMessage sendByteString =
  bracket ( mallocBytes bufferSize ) free waitForMessage
  where
    bufferSize :: Int
    bufferSize  = 4096
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
    flushBuffer buffer pos = do
      print pos
      BS.unsafePackCStringLen (castPtr buffer, pos) >>= sendByteString
{-# INLINE bufferedOutput #-}
