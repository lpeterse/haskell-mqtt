module Network.MQTT.Message
  ( Message (..)
  , QoS (..)
  , pRemainingLength
  , sRemainingLength
  )
where

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

import Network.MQTT.Message.RemainingLength

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
