module Network.MQTT.Authorization where

import Network.MQTT.Authentication
import Network.MQTT.RoutingTree

class Authorizer a where
  authorize :: a -> Principal -> IO Permissions

data Permissions
   = Permissions
     { principalReceiveFilter   :: RoutingTree ()
     , principalPublishFilter   :: RoutingTree ()
     , principalSubscribeFilter :: RoutingTree ()
     }
