{-# LANGUAGE TypeFamilies #-}
module Network.MQTT.QualityOfService where

import Network.MQTT.RoutingTree

data QualityOfService
  = Qos0
  | Qos1
  | Qos2
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
