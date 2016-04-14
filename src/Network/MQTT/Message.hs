module Network.MQTT.Message where

import Control.Applicative
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad.Catch (MonadThrow (..))
import Control.Monad

import qualified Data.Attoparsec.ByteString as A
import Data.Monoid
import Data.Bits
import Data.Function (fix)
import qualified Data.Source as S
import qualified Data.Source.ByteString as S
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.Encoding as LT
import Data.Word
import Data.Typeable

import Prelude

data QoS
   = AtMostOnce
   | AtLeastOnce
   | ExactlyOnce
   deriving (Eq, Ord, Show, Enum)

data Message
   = PUBLISH
     { msgTopic      :: T.Text
     , msgQoS        :: QoS
     , msgBody       :: LBS.ByteString
     , msgDuplicate  :: Bool
     , msgRetain     :: Bool
     } deriving (Eq, Ord, Show)

pRemainingLength:: Integral a => A.Parser a
pRemainingLength = do
  b0 <- A.anyWord8
  if b0 < 128
    then return $ fromIntegral b0
     else do
       b1 <- A.anyWord8
       if b1 < 128
        then return $ fromIntegral b1 * 128 +
                      fromIntegral (b0 .&. 127)
        else do
          b2 <- A.anyWord8
          if b2 < 128
            then return $ fromIntegral b2 * 128 * 128 +
                          fromIntegral (b1 .&. 127) * 128 +
                          fromIntegral (b0 .&. 127)
            else do
              b3 <- A.anyWord8
              if b3 < 128
                then return $ fromIntegral b3 * 128 * 128 * 128 +
                              fromIntegral (b2 .&. 127) * 128 * 128 +
                              fromIntegral (b1 .&. 127) * 128 +
                              fromIntegral (b0 .&. 127)
                else fail "Invalid remaining length." orig
