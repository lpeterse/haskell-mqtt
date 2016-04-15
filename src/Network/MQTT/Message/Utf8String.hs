module Network.MQTT.Message.Utf8String where

import Control.Monad

import Data.Monoid
import Data.Word
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Encoding.Error as T
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.Attoparsec.ByteString as A

pUtf8String :: A.Parser T.Text
pUtf8String = do
  msb <- A.anyWord8
  lsb <- A.anyWord8
  let len = (fromIntegral msb * 256) + fromIntegral lsb :: Int
  str <- A.take len
  when (BS.elem 0x00 str) (fail "pUtf8String: U+0000 violates [MQTT-1.5.3-2]")
  case T.decodeUtf8' str of
    Right txt                     -> return txt
    Left (T.DecodeError reason _) -> fail $ "pUtf8String: " ++ reason
    _                             -> fail "pUtf8String: unknown decoding error"
