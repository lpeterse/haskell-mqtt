module Network.MQTT.Auth where

import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.X509 as X509
import           Data.CaseInsensitive ( CI )

import qualified Network.MQTT.RoutingTree as R

class Credentials c where
  credentialSecure         :: c -> Bool
  credentialUsername       :: c -> Maybe T.Text
  credentialPassword       :: c -> Maybe BS.ByteString
  credentialCertificate    :: c -> Maybe X509.Certificate
  credentialHttpHeaders    :: c -> [(CI BS.ByteString, BS.ByteString)]

class Authenticator a where
  authenticate :: Credentials c => a -> c -> IO (Maybe Principal)

data Principal
   = Principal
     { principalIdentity        :: BS.ByteString
     , principalReceiveFilter   :: R.RoutingTree ()
     , principalPublishFilter   :: R.RoutingTree ()
     , principalSubscribeFilter :: R.RoutingTree ()
     }
