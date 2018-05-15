module Main where

import           Criterion.Main

import           Data.Bits
import           Data.String
import           Network.MQTT.Message
import           Network.MQTT.Trie    as R

main :: IO ()
main = filterTree `seq` defaultMain [benchmark]

benchmark :: Benchmark
benchmark  = bench "Matching 512 topics against 20000 permissions." $ whnf (foldr (\topic accum-> accum `xor` R.matchTopic topic filterTree) False) topics

filterTree :: Trie ()
filterTree = foldr (\f t-> R.insert (fromString f) () t) mempty filters

filters :: [String]
filters = [ [x1,x2,x3,'/',x4,x5,x6,'/',x7,x8,x9] | x1<-r,x2<-r,x3<-r,x4<-r,x5<-r,x6<-r,x7<-r,x8<-r,x9<-r]
   where
    r = ['a'..'c']

topics :: [Topic]
topics = fromString <$> f filters
  where
    f [] = []
    f xs = head xs : f (drop 36 xs)
