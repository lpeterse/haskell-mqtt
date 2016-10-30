{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE OverloadedStrings #-}
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
import qualified Network.WebSockets.Stream   as WS
import qualified System.Socket               as S
import qualified System.Socket.Family.Inet   as S
import qualified System.Socket.Protocol.TCP  as S
import qualified System.Socket.Type.Stream   as S

data Socket
data TLS a
data WebSocket a

class ServerStack a where
  data Server a
  data ServerConfig a
  data ServerConnection a
  data ServerConnectionRequest a
  new     :: ServerConfig a -> IO (Server a)
  start   :: Server a -> IO ()
  stop    :: Server a -> IO ()
  accept  :: Server a -> IO (ServerConnection a)
  send    :: ServerConnection a -> BS.ByteString -> IO ()
  receive :: ServerConnection a -> IO BS.ByteString
  close   :: ServerConnection a -> IO ()

instance ServerStack Socket where
  data Server Socket = SocketServer
    { socketServer       :: S.Socket S.Inet S.Stream S.TCP
    , socketServerConfig :: ServerConfig Socket
    }
  data ServerConfig Socket = SocketServerConfig
    { socketServerConfigBindAddress :: S.SocketAddress S.Inet
    , socketServerConfigListenQueueSize :: Int
    }
  data ServerConnection Socket = SocketServerConnection (S.Socket S.Inet S.Stream S.TCP)
  data ServerConnectionRequest Socket = SocketServerConnectionRequest (S.SocketAddress S.Inet)
  new c = SocketServer <$> S.socket <*> pure c
  start server = do
    S.bind (socketServer server) (socketServerConfigBindAddress $ socketServerConfig server)
    S.listen (socketServer server) (socketServerConfigListenQueueSize $ socketServerConfig server)
  stop server =
    S.close (socketServer server)
  accept s = SocketServerConnection . fst <$> S.accept (socketServer s)
  send (SocketServerConnection s) = sendAll
    where
      sendAll bs = do
        sent <- S.send s bs S.msgNoSignal
        when (sent < BS.length bs) $ sendAll (BS.drop sent bs)
  receive (SocketServerConnection s) = S.receive s 8192 S.msgNoSignal
  close (SocketServerConnection s) = S.close s

instance ServerStack a => ServerStack (TLS a) where
  data Server (TLS a) = TlsServer
    {
    }
  data ServerConfig (TLS a) = TlsServerConfig
    { tlsTransportConfig  :: ServerConfig a
    }
  data ServerConnection (TLS a) = TlsServerConnection
    { tlsTransportState   :: ServerConnection a
    , tlsContext          :: TLS.Context
    }
  data ServerConnectionRequest (TLS a) = TlsServerConnectionRequest
    { tlsCertificateChain :: X509.CertificateChain
    , tlsTransportConnectionRequest :: ServerConnectionRequest a
    }
  new          = undefined
  start        = undefined
  stop         = undefined
  accept       = undefined
  send conn bs = TLS.sendData (tlsContext conn) (BSL.fromStrict bs)
  receive conn = TLS.recvData (tlsContext conn)
  close conn   = do
    TLS.bye (tlsContext conn)
    close (tlsTransportState conn)

instance ServerStack a => ServerStack (WebSocket a) where
  data Server (WebSocket a) = WebSocketServer
    { wsTransportServer            :: Server a
    }
  data ServerConfig (WebSocket a) = WebSocketServerConfig
    { wsTransportConfig            :: ServerConfig a
    }
  data ServerConnection (WebSocket a) = WebSocketServerConnection
    { wsConnection                 :: WS.Connection
    , wsTransportConnection        :: ServerConnection a
    }
  data ServerConnectionRequest (WebSocket a) = WebSocketServerConnectionRequest
    { wsConnectionRequest          :: WS.PendingConnection
    , wsTransportConnectionRequest :: ServerConnectionRequest a
    }
  new config = WebSocketServer <$> new (wsTransportConfig config)
  start server = start (wsTransportServer server)
  stop server = stop (wsTransportServer server)
  accept server = do
    transport <- accept (wsTransportServer server)
    let readSocket = (\bs-> if BS.null bs then Nothing else Just bs) <$> receive transport
    let writeSocket Nothing   = undefined
        writeSocket (Just bs) = send transport (BSL.toStrict bs)
    stream <- WS.makeStream readSocket writeSocket
    pendingConnection <- WS.makePendingConnectionFromStream stream (WS.ConnectionOptions $ pure ())
    WebSocketServerConnection <$> WS.acceptRequest pendingConnection <*> pure transport
  send conn    = WS.sendBinaryData (wsConnection conn)
  receive conn = WS.receiveData (wsConnection conn)
  close conn = do
    WS.sendClose (wsConnection conn) ("Thank you for flying with Haskell airlines. Have a nice day!" :: BS.ByteString)
    close (wsTransportConnection conn)

instance Request (ServerConnectionRequest Socket) where

instance (Request (ServerConnectionRequest a)) => Request (ServerConnectionRequest (TLS a)) where
  requestSecure             = const True
  requestCertificateChain   = Just . tlsCertificateChain
  requestHeaders a          = requestHeaders (tlsTransportConnectionRequest a)

instance (Request (ServerConnectionRequest a)) => Request (ServerConnectionRequest (WebSocket a)) where
  requestSecure conn        = requestSecure (wsTransportConnectionRequest conn)
  requestCertificateChain a = requestCertificateChain (wsTransportConnectionRequest a)
  requestHeaders conn       = WS.requestHeaders $ WS.pendingRequest (wsConnectionRequest conn)
