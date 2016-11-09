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
  , topicLevels
  , topicLength
  , topicParser
  , topicBuilder
  , Filter ()
  , filterLevels
  , filterLength
  , filterParser
  , filterBuilder
  , Level ()
  , levelParser
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
newtype Topic  = Topic  (NonEmpty Level)  deriving (Eq, Ord)
newtype Filter = Filter (NonEmpty Level)  deriving (Eq, Ord)
newtype Level  = Level BS.ShortByteString deriving (Eq, Ord)

instance Show Topic where
  show (Topic xs) = show (Filter xs)

instance Show Filter where
  show (Filter (x:|xs)) = concat ["\"", intercalate "/" $ f x : map f xs, "\""]
    where
      f (Level l) = T.unpack $ T.decodeUtf8With T.lenientDecode $ BS.fromShort l

instance Show Level where
  show (Level x) =
    concat ["\"", T.unpack $ T.decodeUtf8With T.lenientDecode $ BS.fromShort x, "\""]

instance IsString Topic where
  fromString s = case A.parseOnly topicParser (T.encodeUtf8 $ T.pack s) of
    Left e  -> error e
    Right t -> t

instance IsString Filter where
  fromString s = case A.parseOnly filterParser (T.encodeUtf8 $ T.pack s) of
    Left e  -> error e
    Right t -> t

instance IsString Level where
  fromString s = case A.parseOnly levelParser (T.encodeUtf8 $ T.pack s) of
    Left e  -> error e
    Right t -> t

topicLevels :: Topic -> NonEmpty Level
topicLevels (Topic x) = x
{-# INLINE topicLevels #-}

filterLevels :: Filter -> NonEmpty Level
filterLevels (Filter x) = x
{-# INLINE filterLevels #-}

topicParser :: A.Parser Topic
topicParser = (<|> fail "invalid topic") $ Topic <$> do
  void A.peekWord8'
  level  <- pLevel
  levels <- A.many' (pSlash >> pLevel)
  A.endOfInput
  pure (level :| levels)
  where
    pSlash      = void (A.word8 slash)
    pLevel      = Level . BS.toShort <$> A.takeWhile
                  (\w8-> w8 /= slash && w8 /= zero && w8 /= hash && w8 /= plus)

topicBuilder :: Topic -> BS.Builder
topicBuilder (Topic (Level x:|xs)) =
  foldl'
    (\acc (Level l)-> acc <> slashBuilder <> BS.shortByteString l)
    (BS.shortByteString x) xs
{-# INLINE topicBuilder #-}

filterBuilder :: Filter -> BS.Builder
filterBuilder (Filter (Level x:|xs)) =
  foldl'
    (\acc (Level l)-> acc <> slashBuilder <> BS.shortByteString l)
    (BS.shortByteString x) xs
{-# INLINE filterBuilder #-}

topicLength :: Topic -> Int
topicLength (Topic (Level x:|xs)) =
   BS.length x + len' xs 0
   where
    len' []                      acc = acc
    len' (Level z:zs) acc = len' zs $! acc + 1 + BS.length z
{-# INLINE topicLength #-}

filterLength :: Filter -> Int
filterLength (Filter (Level x:|xs)) =
   BS.length x + len' xs 0
   where
    len' []                      acc = acc
    len' (Level z:zs) acc = len' zs $! acc + 1 + BS.length z
{-# INLINE filterLength #-}

filterParser :: A.Parser Filter
filterParser = (<|> fail "invalid filter") $ Filter <$> do
  void A.peekWord8'
  (x:xs) <- pLevels
  pure (x:|xs)
  where
    pSlash = void (A.word8 slash)
    pLevel = Level . BS.toShort <$> A.takeWhile
      (\w8-> w8 /= slash && w8 /= zero && w8 /= hash && w8 /= plus)
    pLevels
       =  (void (A.word8 hash) >> A.endOfInput >> pure [multiLevelWildcard])
      <|> (void (A.word8 plus) >> ((A.endOfInput >> pure [singleLevelWildcard]) <|>
                       (pSlash >> (:) <$> pure singleLevelWildcard <*> pLevels)))
      <|> (pLevel >>= \x-> (x:) <$> ((A.endOfInput >> pure []) <|> (pSlash >> pLevels)))

levelParser :: A.Parser Level
levelParser = do
  x <- A.takeWhile (\w8-> w8 /= slash && w8 /= zero)
  A.endOfInput
  pure (Level $ BS.toShort x)

multiLevelWildcard :: Level
multiLevelWildcard  = Level $ BS.pack $ pure hash

singleLevelWildcard :: Level
singleLevelWildcard  = Level $ BS.pack $ pure plus

zero, plus, hash, slash :: Word8
zero  = 0x00
plus  = 0x2b
hash  = 0x23
slash = 0x2f

slashBuilder :: BS.Builder
slashBuilder  = BS.word8 slash
