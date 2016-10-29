{-# LANGUAGE OverloadedStrings #-}
module Network.MQTT.TopicFilter
  ( Filter (..)
  , Topic (..)
  , parseTopic
  , parseFilter
  ) where

import Control.Monad ( void )
import qualified Data.ByteString.Short          as BS
import qualified Data.ByteString.Short.Internal as BS
import qualified Data.Attoparsec.ByteString     as A
import qualified Data.List.NonEmpty             as NL
import           Data.String
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Control.Applicative
import Data.Word

newtype Filter = Filter (NL.NonEmpty BS.ShortByteString) deriving (Eq, Ord, Show)
newtype Topic  = Topic (NL.NonEmpty BS.ShortByteString) deriving (Eq, Ord, Show)

parseTopic :: A.Parser Topic
parseTopic = (<|> fail "invalid topic") $ Topic <$> do
  void A.peekWord8'
  level  <- pLevel
  levels <- A.many' (pSlash >> pLevel)
  A.endOfInput
  pure (level NL.:| levels)
  where
    pSlash      = A.skip (== slash)
    pLevel      = BS.toShort <$> A.takeWhile
                  (\w8-> w8 /= slash && w8 /= zero && w8 /= hash && w8 /= plus)

parseFilter :: A.Parser Filter
parseFilter = (<|> fail "invalid filter") $ Filter <$> do
  void A.peekWord8'
  level  <- pHashOrLevel
  levels <- A.many' (pSlash >> pHashOrLevel)
  A.endOfInput
  pure (level NL.:| levels)
  where
    pSlash       = A.skip (== slash)
    pHashOrLevel = (A.skip (== hash) >> A.endOfInput >> pure hashLevel) <|> pLevel
    pLevel       = BS.toShort <$> A.takeWhile
                   (\w8-> w8 /= slash && w8 /= zero && w8 /= hash)

zero, plus, hash, slash :: Word8
zero  = 0x00
plus  = 0x2b
hash  = 0x23
slash = 0x2f

hashLevel :: BS.ShortByteString
hashLevel  = "#"
