{-# LANGUAGE OverloadedStrings #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Session where

import           Data.Maybe
import           Control.Concurrent.MVar
import           Control.Concurrent.BoundedChan
import           Control.Exception
import           Control.Monad
import           Data.Functor.Identity
import qualified Data.IntMap                    as IM
import qualified Data.IntSet                    as IS
import           Network.MQTT.Message
import qualified Network.MQTT.RoutingTree       as R
import           Network.MQTT.Topic
import           System.Random

type Identifier = Int

data Session = Session
  { sessionIdentifier    :: Identifier
  , sessionSubscriptions :: !(MVar (R.RoutingTree (Identity QualityOfService)))
  , sessionQueue0        :: !(BoundedChan Message)
  , sessionQueue1        :: !(BoundedChan Message)
  , sessionQueue2        :: !(BoundedChan Message)
  }

publish :: Session -> Message -> IO ()
publish session msg = do
  writeChan (sessionQueue0 session) msg
