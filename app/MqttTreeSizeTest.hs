module Main where

import Control.Concurrent
import Control.DeepSeq
import Data.IntSet as IS
import qualified Network.MQTT.RoutingTree as R
import System.Mem

main :: IO ()
main  = do
  r <- R.randomTree 7 10 :: IO (R.RoutingTree IS.IntSet)
  r `seq` print "Evaluated."
  performGC
  print "Performed GC. See memory consumption now!"
  threadDelay 100000000
  print (R.null r)
