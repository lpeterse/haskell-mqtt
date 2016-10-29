{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.ServerStack
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.ServerStack where

import qualified Data.X509                   as X509
import           Control.Monad
import qualified Data.ByteString             as BS
import qualified Data.ByteString.Lazy        as BSL
import           Network.MQTT.Authentication
import qualified Network.TLS                 as TLS
import qualified Network.WebSockets          as WS
import qualified System.Socket               as S
import qualified System.Socket.Family.Inet   as S
import qualified System.Socket.Protocol.TCP  as S
import qualified System.Socket.Type.Stream   as S

data MqttMessage

data SocketServer
data TlsServer a
data WebSocketServer a
data MqttServer a

class Server a where
  data ServerConnection a
  data ServerConnectionRequest a
  send    :: ServerConnection a -> BS.ByteString -> IO ()
  receive :: ServerConnection a -> IO BS.ByteString

instance Server SocketServer where
  data ServerConnection SocketServer = SocketServerConnection (S.Socket S.Inet S.Stream S.TCP)
  data ServerConnectionRequest SocketServer = SocketServerConnectionRequest (S.SocketAddress S.Inet)
  send (SocketServerConnection s) = sendAll
    where
      sendAll bs = do
        sent <- S.send s bs S.msgNoSignal
        when (sent < BS.length bs) $ sendAll (BS.drop sent bs)
  receive (SocketServerConnection s) = S.receive s 8192 S.msgNoSignal

instance Server (TlsServer a) where
  data ServerConnection (TlsServer a) = TlsServerConnection
    { tlsTransportState   :: ServerConnection a
    , tlsContext          :: TLS.Context
    }
  data ServerConnectionRequest (TlsServer a) = TlsServerConnectionRequest
    { tlsCertificateChain :: X509.CertificateChain
    , tlsTransportConnectionRequest :: ServerConnectionRequest a
    }
  send conn bs = TLS.sendData (tlsContext conn) (BSL.fromStrict bs)
  receive conn = TLS.recvData (tlsContext conn)

instance Server (WebSocketServer a) where
  data ServerConnection (WebSocketServer a) = WebSocketServerConnection
    { wsTransportState    :: ServerConnection a
    , wsConnection        :: WS.Connection
    }
  data ServerConnectionRequest (WebSocketServer a) = WebSocketServerConnectionRequest
    { wsConnectionRequest          :: WS.PendingConnection
    , wsTransportConnectionRequest :: ServerConnectionRequest a
    }
  send conn    = WS.sendBinaryData (wsConnection conn)
  receive conn = WS.receiveData (wsConnection conn)

instance Request (ServerConnectionRequest SocketServer) where

instance (Request (ServerConnectionRequest a)) => Request (ServerConnectionRequest (TlsServer a)) where
  requestSecure             = const True
  requestCertificateChain   = Just . tlsCertificateChain
  requestHeaders a          = requestHeaders (tlsTransportConnectionRequest a)

instance (Request (ServerConnectionRequest a)) => Request (ServerConnectionRequest (WebSocketServer a)) where
  requestSecure conn        = requestSecure (wsTransportConnectionRequest conn)
  requestCertificateChain a = requestCertificateChain (wsTransportConnectionRequest a)
  requestHeaders conn       = WS.requestHeaders $ WS.pendingRequest (wsConnectionRequest conn)
