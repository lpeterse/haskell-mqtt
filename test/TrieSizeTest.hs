{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Control.Monad            ( foldM, when )
import           Data.IntSet              as IS
import qualified Data.List.NonEmpty       as NL
import qualified Data.Map                 as M
import           System.Random            ( randomIO )

import           Network.MQTT.Message
import           Network.MQTT.Trie as R

-- | This test shall assure that the memory consumption of the subscription
--   tree stays within certain limits.

--   This test suite runs with restricted maximum heap size.
--   The routing tree with 1 million nodes and around 3 subscriptions per node
--   took 83MB when measured.
--   We allow for some deviation, but it should not exceed 100MB heap space.
main :: IO ()
main  = do
  r <- randomTree 6 10 :: IO (Trie IS.IntSet)
  -- We need to to something with `r` or it won't be evaluated.
  when (R.size r == 0) (error "should not be 0")

randomTree :: Int -> Int -> IO (Trie IS.IntSet)
randomTree 0     _         = Trie <$> pure mempty
randomTree depth branching = Trie <$> foldM (\m e->
  flip (M.insert e) m <$> (R.node
  <$> randomTree (depth - 1) branching
  <*> randomSet :: IO (TrieNode IS.IntSet))) M.empty
        (take branching randomTreeElements)
  where
    randomSet :: IO (Maybe IS.IntSet)
    randomSet = Just <$> f 0 IS.empty
      where
        f :: Int -> IS.IntSet -> IO IS.IntSet
        f i accum = do
          p <- randomIO :: IO Double
          if p < 0.25
            then pure accum
            else f (succ i) $! IS.insert i accum

randomTreeElements  :: [Level]
randomTreeElements =
  fmap (NL.head . topicLevels) ["a","b","c","d","e","f","g","h","i","j","k","l","m" :: Topic]
