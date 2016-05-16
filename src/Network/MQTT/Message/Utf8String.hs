module Network.MQTT.Message.Utf8String where

import Control.Monad

import Data.Maybe
import Data.Monoid
import Data.Word
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Encoding.Error as T
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.Serialize.Get as SG

pUtf8String :: SG.Get T.Text
pUtf8String = do
  str <- SG.getWord16be >>= SG.getByteString . fromIntegral
  when (BS.elem 0x00 str) (fail "pUtf8String: Violation of [MQTT-1.5.3-2].")
  case T.decodeUtf8' str of
    Right txt -> return txt
    _         -> fail "pUtf8String: Violation of [MQTT-1.5.3]."

bUtf8String :: T.Text -> BS.Builder
bUtf8String txt =
  if len > 0xffff
    then error "bUtf8String: Encoded size must be <= 0xffff."
    else BS.word16BE (fromIntegral len) <> BS.byteString bs
  where
    bs  = T.encodeUtf8 txt
    len = BS.length bs
