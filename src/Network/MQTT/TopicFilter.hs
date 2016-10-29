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
import Data.List
import Data.List.NonEmpty           (NonEmpty(..))
import           Data.String
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Encoding.Error as T
import Control.Applicative
import Data.Word

newtype Filter = Filter (NonEmpty BS.ShortByteString) deriving (Eq, Ord)
newtype Topic  = Topic (NonEmpty BS.ShortByteString) deriving (Eq, Ord)

instance Show Topic where
  show (Topic xs)       = show (Filter xs)

instance Show Filter where
  show (Filter (x:|xs)) = concat ["\"", intercalate "/" $ f x : map f xs, "\""]
    where
      f = T.unpack . T.decodeUtf8With T.lenientDecode . BS.fromShort

instance IsString Topic where
  fromString s = case A.parseOnly parseTopic (T.encodeUtf8 $ T.pack s) of
    Left e  -> error e
    Right t -> t

instance IsString Filter where
  fromString s = case A.parseOnly parseFilter (T.encodeUtf8 $ T.pack s) of
    Left e  -> error e
    Right t -> t

parseTopic :: A.Parser Topic
parseTopic = (<|> fail "invalid topic") $ Topic <$> do
  void A.peekWord8'
  level  <- pLevel
  levels <- A.many' (pSlash >> pLevel)
  A.endOfInput
  pure (level :| levels)
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
  pure (level :| levels)
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
