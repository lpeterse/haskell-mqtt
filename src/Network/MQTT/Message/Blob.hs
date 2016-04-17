module Network.MQTT.Message.Blob where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.Attoparsec.ByteString as A

pBlob :: A.Parser BS.ByteString
pBlob = do
  msb <- A.anyWord8
  lsb <- A.anyWord8
  let len = (fromIntegral msb * 256) + fromIntegral lsb :: Int
  A.take len
