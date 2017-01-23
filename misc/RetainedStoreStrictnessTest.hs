{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Control.Concurrent
import           Control.Monad            (forM_)
import           System.Mem

import qualified Network.MQTT.RetainedMessages as Retained
import qualified Network.MQTT.Message as M
import qualified Network.MQTT.QualityOfService as Qos

main :: IO ()
main  = do
  store <- Retained.new
  forM_ [1..1000000] $ \_i-> do
    Retained.store message store
  performGC
  putStrLn "Performed GC. See memory consumption now!"
  threadDelay 100000000
  msg' <- Retained.retrieve "abc/def" store 
  print msg'
  where
    message :: M.Message
    message = M.Message {
        M.msgTopic = "ahsdjkha/def/hij"
      , M.msgBody = "ahsjdkhajskdhaksjdhakjshd"
      , M.msgQos = Qos.Qos1
      , M.msgRetain = True
      }

