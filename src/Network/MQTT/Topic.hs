{-# LANGUAGE OverloadedStrings #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Topic
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Topic
  ( Topic ()
  , TopicFilter ()
  , TopicFilterLevel ()
  , topicLevels
  , topicFilterLevels
  , parseTopic
  , parseTopicFilter
  , parseTopicFilterLevel
  , multiLevelWildcard
  , singleLevelWildcard
  ) where

import           Control.Applicative
import           Control.Monad                  (void)
import qualified Data.Attoparsec.ByteString     as A
import qualified Data.ByteString.Short          as BS
import qualified Data.ByteString.Short.Internal as BS
import           Data.List
import           Data.List.NonEmpty             (NonEmpty (..))
import           Data.String
import qualified Data.Text                      as T
import qualified Data.Text.Encoding             as T
import qualified Data.Text.Encoding.Error       as T
import           Data.Word

-- | According to the MQTT specification a topic
--
--  * may not be empty
--  * may not contain @+@, @#@ or @\\NUL@ characters
newtype Topic            = Topic (NonEmpty TopicFilterLevel)       deriving (Eq, Ord)
newtype TopicFilter      = TopicFilter (NonEmpty TopicFilterLevel) deriving (Eq, Ord)
newtype TopicFilterLevel = TopicFilterLevel BS.ShortByteString     deriving (Eq, Ord)

instance Show Topic where
  show (Topic xs) = show (TopicFilter xs)

instance Show TopicFilter where
  show (TopicFilter (x:|xs)) = concat ["\"", intercalate "/" $ show x : map show xs, "\""]

instance Show TopicFilterLevel where
  show (TopicFilterLevel x) =
    T.unpack $ T.decodeUtf8With T.lenientDecode $ BS.fromShort x

instance IsString Topic where
  fromString s = case A.parseOnly parseTopic (T.encodeUtf8 $ T.pack s) of
    Left e  -> error e
    Right t -> t

instance IsString TopicFilter where
  fromString s = case A.parseOnly parseTopicFilter (T.encodeUtf8 $ T.pack s) of
    Left e  -> error e
    Right t -> t

instance IsString TopicFilterLevel where
  fromString s = case A.parseOnly parseTopicFilterLevel (T.encodeUtf8 $ T.pack s) of
    Left e  -> error e
    Right t -> t

topicLevels :: Topic -> NonEmpty TopicFilterLevel
topicLevels (Topic x) = x
{-# INLINE topicLevels #-}

topicFilterLevels :: TopicFilter -> NonEmpty TopicFilterLevel
topicFilterLevels (TopicFilter x) = x
{-# INLINE topicFilterLevels #-}

parseTopic :: A.Parser Topic
parseTopic = (<|> fail "invalid topic") $ Topic <$> do
  void A.peekWord8'
  level  <- pLevel
  levels <- A.many' (pSlash >> pLevel)
  A.endOfInput
  pure (level :| levels)
  where
    pSlash      = A.skip (== slash)
    pLevel      = TopicFilterLevel . BS.toShort <$> A.takeWhile
                  (\w8-> w8 /= slash && w8 /= zero && w8 /= hash && w8 /= plus)

parseTopicFilter :: A.Parser TopicFilter
parseTopicFilter = (<|> fail "invalid filter") $ TopicFilter <$> do
  void A.peekWord8'
  level  <- pHashOrLevel
  levels <- A.many' (pSlash >> pHashOrLevel)
  A.endOfInput
  pure (level :| levels)
  where
    pSlash       = A.skip (== slash)
    pHashOrLevel = (A.skip (== hash) >> A.endOfInput >> pure multiLevelWildcard) <|> pLevel
    pLevel       = TopicFilterLevel . BS.toShort <$> A.takeWhile
                   (\w8-> w8 /= slash && w8 /= zero && w8 /= hash)

parseTopicFilterLevel :: A.Parser TopicFilterLevel
parseTopicFilterLevel = do
  x <- A.takeWhile (\w8-> w8 /= slash && w8 /= zero)
  A.endOfInput
  pure (TopicFilterLevel $ BS.toShort x)

multiLevelWildcard :: TopicFilterLevel
multiLevelWildcard  = TopicFilterLevel $ BS.pack $ pure hash

singleLevelWildcard :: TopicFilterLevel
singleLevelWildcard  = TopicFilterLevel $ BS.pack $ pure plus

zero, plus, hash, slash :: Word8
zero  = 0x00
plus  = 0x2b
hash  = 0x23
slash = 0x2f
