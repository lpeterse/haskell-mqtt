{-# LANGUAGE TypeFamilies, FlexibleContexts #-}
module Network.MQTT.Authentication where

import           Control.Exception

import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.X509 as X509
import           Data.CaseInsensitive ( CI )

import qualified Network.MQTT.RoutingTree as R

-- | An `Authenticator` is able to determine a `Principal`'s identity from a
--   `Request`.
class Exception (AuthenticationException a) => Authenticator a where
  data Principal a
  data AuthenticationException a
  -- | Try to determine a `Principal`'s identity from connection `Request`.
  --
  --   The operation shall return `Nothing` in case the authentication
  --   mechanism is working, but couldn't associate an identity. It shall
  --   throw and `AuthenticationException` in case of other problems.
  authenticate :: Request c => a -> c -> IO (Maybe (Principal a))

-- | This class defines how the information gathered from a
--   connection request looks like. An `Authenticator` may use
--   whatever information it finds suitable to authenticate the `Principal`.
class Request r where
  -- | Is this connection secure in terms of
  --  [Transport Layer Security](https://en.wikipedia.org/wiki/Transport_Layer_Security)?
  requestSecure          :: r -> Bool
  requestSecure           = const False
  -- | The username supplied with the MQTT handshake.
  requestUsername        :: r -> Maybe T.Text
  requestUsername         = const Nothing
  -- | The password supplied with the MQTT handshake.
  requestPassword        :: r -> Maybe BS.ByteString
  requestPassword         = const Nothing
  -- | The HTTP request headers in case the client connected via
  --   [WebSocket](https://en.wikipedia.org/wiki/WebSocket).
  requestHeaders         :: r -> [(CI BS.ByteString, BS.ByteString)]
  requestHeaders          = const []
  -- | An [X.509 certificate](https://en.wikipedia.org/wiki/X.509) chain
  --   supplied by the peer.
  --   It can be assumed that the transport layer implementation already
  --   verified that the peer owns the corresponding private key. The validation
  --   of the certificate claims (including certificate chain checking) /must/
  --   be performed by the `Authenticator`.
  requestCertificateChain :: r -> [X509.Certificate]
  requestCertificateChain  = const []
