{-# LANGUAGE TypeFamilies, FlexibleInstances, FlexibleContexts #-}
module Network.MQTT.Transport where

import           Control.Exception

import           Data.Typeable

import qualified System.Socket as S
import qualified System.Socket.Protocol.TCP as S

class Transport a where
  data TransportException a

class Transport a => ServerTransport a where
  data Server a
  data ServerConfig a
  newServerFromConfig :: ServerConfig a -> IO (Server a)
  startServer         :: Server a -> IO ()
  stopServer          :: Server a -> IO ()

class Transport a => ClientTransport a where
  data Client a
  data ClientConfig a
  newClientFromConfig :: ClientConfig a -> IO (Client a)
  connectClient       :: Client a -> IO ()
  disconnecDisconnect :: Client a -> IO ()

instance (S.Family f, S.Type t) => Transport (S.Socket f t S.TCP) where
  data TransportException (S.Socket f t S.TCP) = TcpSocketTransportException S.SocketException

instance Show (TransportException (S.Socket f t S.TCP)) where
  show (TcpSocketTransportException e) = show e
--instance Typeable (TransportException (S.Socket f t S.TCP)) where
--instance Exception (TransportException (S.Socket f t S.TCP)) where

instance (S.Family f, S.Type t) => ServerTransport (S.Socket f t S.TCP) where
  data Server (S.Socket f t S.TCP) = TcpSocketServer (S.Socket f t S.TCP)
  data ServerConfig (S.Socket f t S.TCP) = TcpSocketServerConfiguration (S.SocketAddress f)
  newServerFromConfig config = TcpSocketServer <$> S.socket
  startServer (TcpSocketServer s) = undefined
  stopServer (TcpSocketServer s) = S.close s
