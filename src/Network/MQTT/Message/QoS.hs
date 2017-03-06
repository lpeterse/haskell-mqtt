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
module Network.MQTT.Message.QoS where

import Network.MQTT.RoutingTree

-- | The quality of service defines the guarantees given wrt to message reception.
data QoS
  = QoS0 -- ^ Message delivery is not guaranteed.
  | QoS1 -- ^ Message is guaranteed to be delivered at least once.
  | QoS2 -- ^ Message is guaranteed to be delivered exactly once.
  deriving (Eq, Ord, Show, Enum, Bounded)

instance RoutingTreeValue QoS where
  data RoutingTreeNode QoS = QosNode {-# UNPACK #-} !Int !(RoutingTree QoS)
  node t (Just QoS0)           = QosNode 0 t
  node t (Just QoS1)           = QosNode 1 t
  node t (Just QoS2)           = QosNode 2 t
  node t Nothing               = QosNode 3 t
  nodeNull                     = const False
  nodeTree  (QosNode _ t)      = t
  nodeValue (QosNode 0 _)      = Just QoS0
  nodeValue (QosNode 1 _)      = Just QoS1
  nodeValue (QosNode 2 _)      = Just QoS2
  nodeValue (QosNode _ _)      = Nothing
