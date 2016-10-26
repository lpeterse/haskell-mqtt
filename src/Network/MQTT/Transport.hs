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
  data ServerConfiguration a
  serverFromConfig :: ServerConfiguration a -> IO (Server a)
  serverStart      :: Server a -> IO ()
  serverStop       :: Server a -> IO ()

class Transport a => ClientTransport a where
  data Client a
  data ClientConfiguration a
  clientFromConfig :: ClientConfiguration a -> IO (Client a)
  clientConnect    :: Client a -> IO ()
  clientDisconnect :: Client a -> IO ()

instance (S.Family f, S.Type t) => Transport (S.Socket f t S.TCP) where
  data TransportException (S.Socket f t S.TCP) = TcpSocketTransportException S.SocketException

instance Show (TransportException (S.Socket f t S.TCP)) where
  show (TcpSocketTransportException e) = show e
--instance Typeable (TransportException (S.Socket f t S.TCP)) where
--instance Exception (TransportException (S.Socket f t S.TCP)) where

instance (S.Family f, S.Type t) => ServerTransport (S.Socket f t S.TCP) where
  data Server (S.Socket f t S.TCP) = TcpSocketServer (S.Socket f t S.TCP)
  data ServerConfiguration (S.Socket f t S.TCP) = TcpSocketServerConfiguration (S.SocketAddress f)
  serverFromConfig config = TcpSocketServer <$> S.socket
  serverStart (TcpSocketServer s) = undefined
  serverStop (TcpSocketServer s) = S.close s
