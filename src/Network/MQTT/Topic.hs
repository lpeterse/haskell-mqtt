{-# LANGUAGE OverloadedStrings #-}
module Network.MQTT.Topic
  ( Filter (..)
  , Topic (..)
  , PathElement (..)
  ) where

import qualified Data.ByteString.Short           as BS
import qualified Data.ByteString.Short.Internal  as BS
import qualified Data.List.NonEmpty              as NL

newtype Filter = Filter (NL.NonEmpty PathElement)
newtype Topic  = Topic (NL.NonEmpty PathElement)

data PathElement = PathElement {-# UNPACK #-} !Int {-# UNPACK #-} !Int !BS.ShortByteString
  deriving (Show)

instance Eq PathElement where
  PathElement off len bs == PathElement off' len' bs' =
    off == off' && len == len' && f len
    where
      -- Heuristic: strings share prefixes and are more likely to differ at the end
      f 0 = True
      f l = let offoff = l - 1; i = off + offoff in
        BS.unsafeIndex bs l ==  BS.unsafeIndex bs' l && f offoff

instance Ord PathElement where
  compare (PathElement off len bs) (PathElement off' len' bs') =
    case compare off off' of
      EQ -> case compare len len' of
        EQ -> f len
        y  -> y
      x -> x
    where
      f 0 = EQ
      f l = let offoff = l - 1; i = off + offoff in
        case compare (BS.unsafeIndex bs l) (BS.unsafeIndex bs' l) of
          EQ -> f offoff
          x  -> x
