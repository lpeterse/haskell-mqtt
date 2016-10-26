{-# LANGUAGE TypeFamilies, FlexibleContexts #-}
module Network.MQTT.Authorization where

import Control.Exception
import Control.Concurrent.MVar

import Network.MQTT.TopicFilter
import Network.MQTT.Authentication
import Network.MQTT.RoutingTree

-- | An `Authorizer` is able to determine a `Principal`'s permissions.
class (Authenticator a, Exception (AuthorizationException a)) => Authorizer a where
  -- | This `Exception` may be thrown by any operation within this class.
  --   Operations /must/ only throw this type of exception. Other exceptions won't be
  --   catched and may kill the kill.
  data AuthorizationException a
  -- | Determine the set of `Topic`s a `Principal` is allowed to publish on.
  --
  --   Beware that this operation is called on each publication. The implementation
  --   is advised to cache the result (i.e. in an `MVar`) and update it
  --   from time to time.
  getPublishPermissions   :: Principal a -> IO (RoutingTree ())
  getPublishPermissions    = pure mempty
  -- | Determine the set of `Filter`s a `Principal` is allowed to subscribe.
  --   The `Principal` is implicitly allowed to subscribe all filters that
  --   are more specific than those explicitly stated in the `RoutingTree`.
  --
  --   Beware that this operation is called on each subscription. The implementation
  --   is advised to cache the result (i.e. in an `MVar`) and update it
  --   from time to time.
  --
  --   Also note that `Filter`s already subscribed by the client won't be
  --   unsubscribed if they are no longer in the current permission set.
  --   There is no way specified by MQTT to signal this to the client other
  --   than terminating the session.
  getSubscribePermissions :: Principal a -> IO (RoutingTree ())
  getSubscribePermissions  = pure mempty
