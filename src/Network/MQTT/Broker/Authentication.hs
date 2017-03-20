{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker.Authentication
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Broker.Authentication where

import           Control.Exception
import qualified Data.Binary              as B
import qualified Data.ByteString          as BS
import           Data.CaseInsensitive
import           Data.UUID                as UUID
import           Data.Word
import qualified Data.X509                as X509
import           GHC.Generics

import           Network.MQTT.Message
import           Network.MQTT.Trie as R

-- | A peer identity optionally associated with connection/session
--   specific information.
--newtype Principal = Principal T.Text deriving (Eq, Ord, Show)

-- | An `Authenticator` is able to determine a `Principal`'s identity from a
--   `Request`.
class (Exception (AuthenticationException a)) => Authenticator a where
  data AuthenticatorConfig a
  -- | This `Exception` may be thrown by any operation within this class.
  --   Operations /must/ only throw this type of exception. Other exceptions
  --   won't be catched and may kill the broker.
  data AuthenticationException a
  -- | Create a new authenticator instance from configuration.
  newAuthenticator       :: AuthenticatorConfig a -> IO a
  -- | Try to determine a `Principal`'s identity from a connection `Request`.
  --
  --   The operation shall return `Nothing` in case the authentication
  --   mechanism is working, but couldn't associate an identity. It shall
  --   throw an `AuthenticationException` in case of other problems.
  authenticate           :: a -> ConnectionRequest -> IO (Maybe PrincipalIdentifier)
  -- | Gets a principal by principal primary key (UUID).
  --
  --   The operation shall return `Nothing` in case the principal is not / no
  --   longer available. It shall throw an `AuthenticationException` in case
  --   of other problems.
  getPrincipal           :: a -> PrincipalIdentifier -> IO (Maybe Principal)

type PrincipalIdentifier = UUID

data Principal
   = Principal
   { principalUsername             :: Maybe Username
   , principalQuota                :: Quota
   , principalPublishPermissions   :: R.Trie ()
   , principalSubscribePermissions :: R.Trie ()
   , principalRetainPermissions    :: R.Trie ()
   } deriving (Eq, Show, Generic)

data Quota
   = Quota
   { quotaMaxIdleSessionTTL    :: Word64
   , quotaMaxPacketSize        :: Word64
   , quotaMaxPacketIdentifiers :: Word64
   , quotaMaxQueueSizeQoS0     :: Word64
   , quotaMaxQueueSizeQoS1     :: Word64
   , quotaMaxQueueSizeQoS2     :: Word64
   } deriving (Eq, Ord, Show, Generic)

instance B.Binary Quota
instance B.Binary Principal

-- | This class defines how the information gathered from a
--   connection request looks like. An `Authenticator` may use
--   whatever information it finds suitable to authenticate the `Principal`.
data ConnectionRequest
   = ConnectionRequest
   { requestClientIdentifier :: ClientIdentifier,
     requestCleanSession     :: Bool,
     -- | Is this connection secure in terms of
     --  [Transport Layer Security](https://en.wikipedia.org/wiki/Transport_Layer_Security)?
     requestSecure           :: Bool,
     -- | The username and password supplied with the MQTT handshake.
     requestCredentials      :: Maybe (Username, Maybe Password),
     -- | The HTTP request head in case the client connected via
     --   [WebSocket](https://en.wikipedia.org/wiki/WebSocket).
     requestHttp             :: Maybe (BS.ByteString, [(CI BS.ByteString, BS.ByteString)]),
     -- | An [X.509 certificate](https://en.wikipedia.org/wiki/X.509) chain
     --   supplied by the peer.
     --   It can be assumed that the transport layer implementation already
     --   verified that the peer owns the corresponding private key. The validation
     --   of the certificate claims (including certificate chain checking) /must/
     --   be performed by the `Authenticator`.
     requestCertificateChain :: Maybe X509.CertificateChain,
     requestRemoteAddress    :: Maybe BS.ByteString
   } deriving (Show)
