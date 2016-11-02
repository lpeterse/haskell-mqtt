{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
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

import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import qualified Control.Exception           as E
import           Control.Monad
import qualified Data.ByteString             as BS
import qualified Data.ByteString.Lazy        as BSL
import           Data.Typeable
import qualified Data.X509                   as X509
import           Foreign.Storable
import           Network.MQTT.Authentication
import qualified Network.TLS                 as TLS
import qualified Network.WebSockets          as WS
import qualified Network.WebSockets.Stream   as WS
import qualified System.Socket               as S

data TLS a
data WebSocket a

class Typeable a => ServerStack a where
  type ServerMessage a
  data Server a
  data ServerConfig a
  data ServerException a
  data ServerConnection a
  new        :: ServerConfig a -> IO (Server a)
  start      :: Server a -> IO ()
  stop       :: Server a -> IO ()
  acceptWith :: Server a -> (ServerConnection a -> IO b) -> IO (Async b)
  flush      :: ServerConnection a -> IO ()
  send       :: ServerConnection a -> ServerMessage a -> IO ()
  receive    :: ServerConnection a -> Int -> IO (ServerMessage a)
  close      :: ServerConnection a -> IO ()

instance (Storable (S.SocketAddress f), S.Family f, S.Type t, S.Protocol p, Typeable f, Typeable t, Typeable p) => ServerStack (S.Socket f t p) where
  type ServerMessage (S.Socket f t p) = BS.ByteString
  data Server (S.Socket f t p) = SocketServer
    { socketServer       :: !(S.Socket f t p)
    , socketServerConfig :: !(ServerConfig (S.Socket f t p))
    }
  data ServerConfig (S.Socket f t p) = SocketServerConfig
    { socketServerConfigBindAddress :: !(S.SocketAddress f)
    , socketServerConfigListenQueueSize :: Int
    }
  data ServerException (S.Socket f t p) = SocketServerException !S.SocketException
  data ServerConnection (S.Socket f t p) = SocketServerConnection !(S.Socket f t p)
  new c = SocketServer <$> S.socket <*> pure c
  start server = do
    S.bind (socketServer server) (socketServerConfigBindAddress $ socketServerConfig server)
    S.listen (socketServer server) (socketServerConfigListenQueueSize $ socketServerConfig server)
  stop server =
    S.close (socketServer server)
  acceptWith server handle =
    E.bracketOnError (SocketServerConnection . fst <$> S.accept (socketServer server)) close $ \connection->
      async (handle connection `E.finally` close connection)
  flush =
    const (pure ())
  send (SocketServerConnection s) = sendAll
    where
      sendAll bs = do
        sent <- S.send s bs S.msgNoSignal
        when (sent < BS.length bs) $ sendAll (BS.drop sent bs)
  receive (SocketServerConnection s) i = S.receive s i S.msgNoSignal
  close (SocketServerConnection s) = S.close s

instance (ServerStack a, ServerMessage a ~ BS.ByteString) => ServerStack (TLS a) where
  type ServerMessage (TLS a) = ServerMessage a
  data Server (TLS a) = TlsServer
    { tlsTransportServer            :: Server a
    , tlsServerConfig               :: ServerConfig (TLS a)
    }
  data ServerConfig (TLS a) = TlsServerConfig
    { tlsTransportConfig            :: ServerConfig a
    , tlsServerParams               :: TLS.ServerParams
    }
  data ServerException (TLS a) = TlsServerException
  data ServerConnection (TLS a) = TlsServerConnection
    { tlsTransportConnection        :: ServerConnection a
    , tlsContext                    :: TLS.Context
    , tlsCertificateChain           :: Maybe X509.CertificateChain
    }
  new config = do
    transportServer <- new (tlsTransportConfig config)
    pure (TlsServer transportServer config)
  start  server = start (tlsTransportServer server)
  stop   server = stop  (tlsTransportServer server)
  acceptWith server handle =
    acceptWith (tlsTransportServer server) $ \connection-> do
      let backend = TLS.Backend {
          TLS.backendFlush = flush connection
        , TLS.backendClose = close connection
        , TLS.backendSend  = send connection
        , TLS.backendRecv  = receive connection
        }
      mvar <- newEmptyMVar
      context <- TLS.contextNew backend (tlsServerParams $ tlsServerConfig server)
      TLS.contextHookSetCertificateRecv context (putMVar mvar)
      TLS.handshake context
      certificateChain <- tryTakeMVar mvar
      handle (TlsServerConnection connection context certificateChain)
  flush conn     = TLS.contextFlush (tlsContext conn)
  send conn bs   = TLS.sendData     (tlsContext conn) (BSL.fromStrict bs)
  receive conn _ = TLS.recvData     (tlsContext conn)
  close conn     = TLS.bye          (tlsContext conn) `E.finally` close (tlsTransportConnection conn)

instance (ServerStack a, ServerMessage a ~ BS.ByteString) => ServerStack (WebSocket a) where
  type ServerMessage (WebSocket a) = BS.ByteString
  data Server (WebSocket a) = WebSocketServer
    { wsTransportServer            :: Server a
    }
  data ServerConfig (WebSocket a) = WebSocketServerConfig
    { wsTransportConfig            :: ServerConfig a
    }
  data ServerException (WebSocket a) = WebSocketServerException
  data ServerConnection (WebSocket a) = WebSocketServerConnection
    { wsTransportConnection        :: ServerConnection a
    , wsPendingConnection          :: WS.PendingConnection
    , wsConnection                 :: WS.Connection
    }
  new config = WebSocketServer <$> new (wsTransportConfig config)
  start server = start (wsTransportServer server)
  stop server = stop (wsTransportServer server)
  acceptWith server handle =
    acceptWith (wsTransportServer server) $ \connection-> do
      let readSocket = (\bs-> if BS.null bs then Nothing else Just bs) <$> receive connection 4096
      let writeSocket Nothing   = undefined
          writeSocket (Just bs) = send connection (BSL.toStrict bs)
      stream <- WS.makeStream readSocket writeSocket
      pendingConnection <- WS.makePendingConnectionFromStream stream (WS.ConnectionOptions $ pure ())
      handle =<< WebSocketServerConnection
        <$> pure connection
        <*> pure pendingConnection
        <*> WS.acceptRequest pendingConnection
  flush conn     = flush (wsTransportConnection conn)
  send conn      = WS.sendBinaryData (wsConnection conn)
  receive conn _ = WS.receiveData (wsConnection conn)
  close conn     = closeWebSocket `E.finally` closeTransport
    where
      closeWebSocket = WS.sendClose (wsConnection conn)
        ("Thank you for flying Haskell." :: BS.ByteString)
      closeTransport = close (wsTransportConnection conn)

instance Request (ServerConnection (S.Socket f t p)) where

instance (Request (ServerConnection a)) => Request (ServerConnection (TLS a)) where
  requestSecure             = const True
  requestCertificateChain   = tlsCertificateChain
  requestHeaders a          = requestHeaders (tlsTransportConnection a)

instance (Request (ServerConnection a)) => Request (ServerConnection (WebSocket a)) where
  requestSecure conn        = requestSecure (wsTransportConnection conn)
  requestCertificateChain a = requestCertificateChain (wsTransportConnection a)
  requestHeaders conn       = WS.requestHeaders $ WS.pendingRequest (wsPendingConnection conn)
