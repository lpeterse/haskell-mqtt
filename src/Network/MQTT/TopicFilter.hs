{-# LANGUAGE OverloadedStrings #-}
module Network.MQTT.TopicFilter
  ( Filter (..)
  , Topic (..)
  ) where

import qualified Data.ByteString.Short           as BS
import qualified Data.ByteString.Short.Internal  as BS
import qualified Data.List.NonEmpty              as NL

newtype Filter = Filter (NL.NonEmpty BS.ShortByteString) deriving (Eq, Ord, Show)
newtype Topic  = Topic (NL.NonEmpty BS.ShortByteString) deriving (Eq, Ord, Show)
