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

data SocketServerStack
data TlsServerStack a
data WebSocketServerStack a

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

instance ServerStack SocketServerStack where
  data Server SocketServerStack = SocketServer
    { socketServer       :: S.Socket S.Inet S.Stream S.TCP
    , socketServerConfig :: ServerConfig SocketServerStack
    }
  data ServerConfig SocketServerStack = SocketServerConfig
    { socketServerConfigBindAddress :: S.SocketAddress S.Inet
    , socketServerConfigListenQueueSize :: Int
    }
  data ServerConnection SocketServerStack = SocketServerConnection (S.Socket S.Inet S.Stream S.TCP)
  data ServerConnectionRequest SocketServerStack = SocketServerConnectionRequest (S.SocketAddress S.Inet)
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

instance ServerStack a => ServerStack (TlsServerStack a) where
  data Server (TlsServerStack a) = TlsServer
    {
    }
  data ServerConfig (TlsServerStack a) = TlsServerConfig
    { tlsTransportConfig  :: ServerConfig a
    }
  data ServerConnection (TlsServerStack a) = TlsServerConnection
    { tlsTransportState   :: ServerConnection a
    , tlsContext          :: TLS.Context
    }
  data ServerConnectionRequest (TlsServerStack a) = TlsServerConnectionRequest
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

instance ServerStack a => ServerStack (WebSocketServerStack a) where
  data Server (WebSocketServerStack a) = WebSocketServer
    { wsTransportServer            :: Server a
    }
  data ServerConfig (WebSocketServerStack a) = WebSocketServerConfig
    { wsTransportConfig            :: ServerConfig a
    }
  data ServerConnection (WebSocketServerStack a) = WebSocketServerConnection
    { wsConnection                 :: WS.Connection
    , wsTransportConnection        :: ServerConnection a
    }
  data ServerConnectionRequest (WebSocketServerStack a) = WebSocketServerConnectionRequest
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

instance Request (ServerConnectionRequest SocketServerStack) where

instance (Request (ServerConnectionRequest a)) => Request (ServerConnectionRequest (TlsServerStack a)) where
  requestSecure             = const True
  requestCertificateChain   = Just . tlsCertificateChain
  requestHeaders a          = requestHeaders (tlsTransportConnectionRequest a)

instance (Request (ServerConnectionRequest a)) => Request (ServerConnectionRequest (WebSocketServerStack a)) where
  requestSecure conn        = requestSecure (wsTransportConnectionRequest conn)
  requestCertificateChain a = requestCertificateChain (wsTransportConnectionRequest a)
  requestHeaders conn       = WS.requestHeaders $ WS.pendingRequest (wsConnectionRequest conn)
