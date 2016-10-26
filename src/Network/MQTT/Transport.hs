{-# LANGUAGE TypeFamilies, FlexibleInstances, FlexibleContexts #-}
module Network.MQTT.Transport where

import           Control.Exception

import           Data.Typeable
import qualified Data.ByteString as BS
import qualified Data.List.NonEmpty as NL

import qualified System.Socket as S
import qualified System.Socket.Protocol.TCP as S

class Transport a where
  data TransportException a

class Transport a => ServerTransport a where
  data Server a
  data ServerConfig a
  newServer           :: ServerConfig a -> IO (Server a)
  start               :: Server a -> IO ()
  stop                :: Server a -> IO ()

class Transport a => ClientTransport a where
  data Client a
  data ClientConfig a
  newClient           :: ClientConfig a -> IO (Client a)
  connect             :: Client a -> IO ()
  disconnect          :: Client a -> IO ()

class Transport a => AddressTranslationTansport a where
  data AddressInfo a
  data AddressTranslationException a
  getAddressInfo :: BS.ByteString -> BS.ByteString -> IO (NL.NonEmpty (AddressInfo a))

instance (S.Family f, S.Type t) => Transport (S.Socket f t S.TCP) where
  data TransportException (S.Socket f t S.TCP) = TcpSocketTransportException S.SocketException

instance Show (TransportException (S.Socket f t S.TCP)) where
  show (TcpSocketTransportException e) = show e
--instance Typeable (TransportException (S.Socket f t S.TCP)) where
--instance Exception (TransportException (S.Socket f t S.TCP)) where

instance (S.Family f, S.Type t) => ServerTransport (S.Socket f t S.TCP) where
  data Server (S.Socket f t S.TCP) = TcpSocketServer (S.Socket f t S.TCP)
  data ServerConfig (S.Socket f t S.TCP) = TcpSocketServerConfiguration (S.SocketAddress f)
  newServer config = TcpSocketServer <$> S.socket
  start (TcpSocketServer s) = undefined
  stop  (TcpSocketServer s) = S.close s
