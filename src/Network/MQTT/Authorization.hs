{-# LANGUAGE TypeFamilies, FlexibleContexts #-}
module Network.MQTT.Authorization where

import Control.Exception

import Network.MQTT.Authentication
import Network.MQTT.RoutingTree

class Exception (AuthorizationException a) => Authorizer a where
  data AuthorizationException a
  authorize :: a -> Principal -> IO Permissions

data Permissions
   = Permissions
     { principalReceiveFilter   :: RoutingTree ()
     , principalPublishFilter   :: RoutingTree ()
     , principalSubscribeFilter :: RoutingTree ()
     }
