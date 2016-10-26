{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Monad ( foldM )
import Control.Concurrent
import Data.IntSet as IS
import Network.MQTT.RoutingTree as R
import System.Mem
import System.Random ( randomIO )

import qualified Data.ByteString.Short as BS
import qualified Data.Map as M
import qualified Data.IntSet as IS

main :: IO ()
main  = do
  r <- randomTree 7 10 :: IO (RoutingTree IS.IntSet)
  r `seq` print "Evaluated."
  performGC
  print "Performed GC. See memory consumption now!"
  threadDelay 100000000
  print (R.null r)

randomTree :: Int -> Int -> IO (RoutingTree IS.IntSet)
randomTree 0     branching = RoutingTree <$> pure mempty
randomTree depth branching = RoutingTree <$> foldM (\m e->
  flip (M.insert e) m <$> (nodeFromTreeAndValue
  <$> randomTree (depth - 1) branching
  <*> randomSet :: IO (RoutingTreeNode IS.IntSet))) M.empty
        (take branching randomTreeElements)
  where
    randomSet :: IO IS.IntSet
    randomSet = f 0 IS.empty
      where
        f :: Int -> IS.IntSet -> IO IS.IntSet
        f i accum = do
          p <- randomIO :: IO Double
          if p < 0.25
            then pure accum
            else f (succ i) $! IS.insert i accum

randomTreeElements  :: [BS.ShortByteString]
randomTreeElements =
  [ "a","b","c","d","e","f","g","h","i","j","k","l","m" ]
