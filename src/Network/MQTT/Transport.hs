{-# LANGUAGE TypeFamilies, FlexibleInstances, FlexibleContexts #-}
module Network.MQTT.Transport where

import qualified System.Socket as S
import qualified System.Socket.Protocol.TCP as S

class Transport a where

class Transport a => ServerTransport a where
  data Server a
  data ServerConfiguration a
  server           :: ServerConfiguration a -> IO (Server a)
  serverStart      :: Server a -> IO ()
  serverStop       :: Server a -> IO ()

class Transport a => ClientTransport a where
  data Client a
  data ClientConfiguration a
  client           :: ClientConfiguration a -> IO (Client a)
  clientConnect    :: Client a -> IO ()
  clientDisconnect :: Client a -> IO ()

instance (S.Family f, S.Type t) => Transport (S.Socket f t S.TCP) where

instance (S.Family f, S.Type t) => ServerTransport (S.Socket f t S.TCP) where
  data Server (S.Socket f t S.TCP) = TcpSocketServer (S.Socket f t S.TCP)
  data ServerConfiguration (S.Socket f t S.TCP) = TcpSocketServerConfiguration (S.SocketAddress f)
  server config = TcpSocketServer <$> S.socket
  serverStart (TcpSocketServer s) = undefined
  serverStop (TcpSocketServer s) = S.close s
