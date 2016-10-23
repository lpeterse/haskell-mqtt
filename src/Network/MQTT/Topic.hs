{-# LANGUAGE OverloadedStrings #-}
module Network.MQTT.Topic
  ( Filter (..)
  , Topic (..)
  , createFilterElement
  , createTopicElement
  , mWord
  ) where

import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Conversion.From as BS
import qualified Data.List.NonEmpty              as NL
import           Data.Maybe                      (fromMaybe)
import qualified Data.Text                       as T
import qualified Data.Text.Encoding              as T
import           Data.Word                       (Word64)

newtype Filter = Filter (NL.NonEmpty FilterElement)
newtype Topic  = Topic (NL.NonEmpty TopicElement)

data FilterElement
  = SmallFilterElement Word64
  | FilterWildcard     FilterWildcard
  | LongFilterElement  BS.ByteString
  deriving (Show)

data FilterWildcard = Hash | Plus

instance Show FilterWildcard where
  show Hash = show "#"
  show Plus = show "+"

createFilterElement :: T.Text -> FilterElement
createFilterElement str
  | str == "#" = FilterWildcard Hash
  | str == "+" = FilterWildcard Plus
  | size <= 8  = SmallFilterElement $ text2Word64 str
  | otherwise  = LongFilterElement $ T.encodeUtf8 str
    where size = BS.length $ T.encodeUtf8 str

data TopicElement
  = SmallTopicElement Word64
  | LongTopicElement  BS.ByteString
    deriving (Show)

createTopicElement :: T.Text -> TopicElement
createTopicElement str
  | size <= 8  = SmallTopicElement $ text2Word64 str
  | otherwise  = LongTopicElement $ T.encodeUtf8 str
    where size = BS.length $ T.encodeUtf8 str

text2Word64 :: T.Text -> Word64
text2Word64 str = fromMaybe 0 $ BS.fromByteString (T.encodeUtf8 str)

mWord :: String -> Maybe Word64
mWord str = BS.fromByteString (T.encodeUtf8 $ T.pack str)