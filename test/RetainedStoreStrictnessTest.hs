{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Control.Concurrent
import           Control.Monad            (forM_)
import           System.Mem

import qualified Network.MQTT.RetainedMessages as Retained
import qualified Network.MQTT.Message as M
import qualified Network.MQTT.QualityOfService as Qos

-- | This shall test whether inserting into the RetainedStore
--   leaks memory by building up unevaluated thunks.
--
--   The test is supposed to be run with '+RTS -M4m'.
--   In case of a memory leak the program will crash as
--   the thunks would require around 300MB heap.
main :: IO ()
main  = do
  store <- Retained.new
  forM_ [1..1000000] $ \_i-> do
    Retained.store message store
  where
    message :: M.Message
    message = M.Message {
        M.msgTopic = "ahsdjkha/def/hij"
      , M.msgBody = "ahsjdkhajskdhaksjdhakjshd"
      , M.msgQos = Qos.Qos1
      , M.msgRetain = True
      }

