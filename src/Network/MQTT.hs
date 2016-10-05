--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
{-# LANGUAGE TypeFamilies #-}
module Network.MQTT where

import Control.Exception

import Data.Typeable
import qualified Data.ByteString as BS

import Network.MQTT.Message

data Message = Message
     { topic     :: Topic
     , qos       :: QoS
     , retained  :: Bool
     , duplicate :: Bool
     , payload   :: Payload
     }
   deriving (Eq, Ord, Show)
