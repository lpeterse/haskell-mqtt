{-# LANGUAGE TypeFamilies #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Message
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Message.QualityOfService where

import Network.MQTT.RoutingTree

-- | The `QualityOfService` defines the guarantees given wrt to message reception.
data QualityOfService
  = Qos0 -- ^ Message delivery is not guaranteed.
  | Qos1 -- ^ Message is guaranteed to be received at least once.
  | Qos2 -- ^ Message is guaranteed to be delivered exactly once.
  deriving (Eq, Ord, Show, Enum, Bounded)

instance RoutingTreeValue QualityOfService where
  data RoutingTreeNode QualityOfService = QosNode {-# UNPACK #-} !Int !(RoutingTree QualityOfService)
  node t (Just Qos0)           = QosNode 0 t
  node t (Just Qos1)           = QosNode 1 t
  node t (Just Qos2)           = QosNode 2 t
  node t Nothing               = QosNode 3 t
  nodeNull                     = const False
  nodeTree  (QosNode _ t)      = t
  nodeValue (QosNode 0 _)      = Just Qos0
  nodeValue (QosNode 1 _)      = Just Qos1
  nodeValue (QosNode 2 _)      = Just Qos2
  nodeValue (QosNode _ _)      = Nothing
