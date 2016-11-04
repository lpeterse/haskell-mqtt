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
  , topicLength
  , topicBuilder
  , topicFilterLevels
  , parseTopic
  , parseTopicFilter
  , parseTopicFilterLevel
  , multiLevelWildcard
  , singleLevelWildcard
  ) where

import Data.Monoid ((<>))
import           Control.Applicative
import           Control.Monad              (void)
import qualified Data.Attoparsec.ByteString as A
import qualified Data.ByteString.Builder    as BS
import qualified Data.ByteString.Short      as BS
import           Data.List
import           Data.List.NonEmpty         (NonEmpty (..))
import           Data.String
import qualified Data.Text                  as T
import qualified Data.Text.Encoding         as T
import qualified Data.Text.Encoding.Error   as T
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
  show (TopicFilter (x:|xs)) = concat ["\"", intercalate "/" $ f x : map f xs, "\""]
    where
      f (TopicFilterLevel l) = T.unpack $ T.decodeUtf8With T.lenientDecode $ BS.fromShort l

instance Show TopicFilterLevel where
  show (TopicFilterLevel x) =
    concat ["\"", T.unpack $ T.decodeUtf8With T.lenientDecode $ BS.fromShort x, "\""]

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
    pSlash      = void (A.word8 slash)
    pLevel      = TopicFilterLevel . BS.toShort <$> A.takeWhile
                  (\w8-> w8 /= slash && w8 /= zero && w8 /= hash && w8 /= plus)

topicBuilder :: Topic -> BS.Builder
topicBuilder (Topic (TopicFilterLevel x:|xs)) =
  foldl'
    (\acc (TopicFilterLevel l)-> acc <> slashBuilder <> BS.shortByteString l)
    (BS.shortByteString x) xs
{-# INLINE topicBuilder #-}

topicLength :: Topic -> Int
topicLength (Topic (TopicFilterLevel x:|xs)) =
   BS.length x + len' xs 0
   where
    len' []                      acc = acc
    len' (TopicFilterLevel z:zs) acc = len' zs $! acc + 1 + BS.length z
{-# INLINE topicLength #-}

parseTopicFilter :: A.Parser TopicFilter
parseTopicFilter = (<|> fail "invalid filter") $ TopicFilter <$> do
  void A.peekWord8'
  (x:xs) <- pLevels
  pure (x:|xs)
  where
    pSlash = void (A.word8 slash)
    pLevel = TopicFilterLevel . BS.toShort <$> A.takeWhile
      (\w8-> w8 /= slash && w8 /= zero && w8 /= hash && w8 /= plus)
    pLevels
       =  (void (A.word8 hash) >> A.endOfInput >> pure [multiLevelWildcard])
      <|> (void (A.word8 plus) >> ((A.endOfInput >> pure [singleLevelWildcard]) <|>
                       (pSlash >> (:) <$> pure singleLevelWildcard <*> pLevels)))
      <|> (pLevel >>= \x-> (x:) <$> ((A.endOfInput >> pure []) <|> (pSlash >> pLevels)))

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

slashBuilder :: BS.Builder
slashBuilder  = BS.word8 slash
