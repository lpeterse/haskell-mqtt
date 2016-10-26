module Network.MQTT.Authentication where

import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.X509 as X509
import           Data.CaseInsensitive ( CI )

import qualified Network.MQTT.RoutingTree as R

-- | An instance of `Authenticator` is able to determine a `Principal`s
--   identity from authentication `Credentials`.
class Authenticator a where
  -- | Try to determine a `Principal`s identity from authentication `Credentials`.
  authenticate :: Credentials c => a -> c -> IO (Maybe Principal)

-- | This class defines how credentials that have been gathered from a
--   connection request look like (at least). An `Authenticator` may use
--   whatever information it finds suitable to authenticate the `Principal`.
class Credentials c where
  -- | Is this connection secure in terms of
  --  [Transport Layer Security](https://en.wikipedia.org/wiki/Transport_Layer_Security)?
  credentialsSecure         :: c -> Bool
  credentialsSecure          = const False
  -- | The username supplied with the MQTT handshake.
  credentialsUsername       :: c -> Maybe T.Text
  credentialsUsername        = const Nothing
  -- | The password supplied with the MQTT handshake.
  credentialsPassword       :: c -> Maybe BS.ByteString
  credentialsPassword        = const Nothing
  -- | An [X.509 certificate](https://en.wikipedia.org/wiki/X.509) chain
  --   supplied by the peer.
  --   It can be assumed that the transport layer implementation already
  --   verified that the peer owns the corresponding private key. The validation
  --   of the certificate claims (including certificate chain checking) /must/
  --   be performed by the `Authenticator`.
  credentialsX509CertificateChain    :: c -> [X509.Certificate]
  credentialsX509CertificateChain     = const []
  -- | The HTTP request headers in case the client connected via
  --   [WebSocket](https://en.wikipedia.org/wiki/WebSocket).
  credentialsWebSocketRequestHeaders :: c -> [(CI BS.ByteString, BS.ByteString)]
  credentialsWebSocketRequestHeaders  = const []

-- | The result of a call to `authenticate`.
data Principal
   = Principal
     { principalIdentity        :: BS.ByteString
     }
