module Network.MQTT.Message.Blob where

import Data.Monoid

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.Attoparsec.ByteString as A

pBlob :: A.Parser BS.ByteString
pBlob = do
  msb <- A.anyWord8
  lsb <- A.anyWord8
  let len = (fromIntegral msb * 256) + fromIntegral lsb :: Int
  A.take len

bBlob :: BS.ByteString -> BS.Builder
bBlob bs =
  if len > 0xffff
    then error "bBlob: encoded size must be <= 0xffff"
    else BS.word16BE (fromIntegral len) <> BS.byteString bs
  where
    len = BS.length bs
