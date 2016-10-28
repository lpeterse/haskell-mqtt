{-# LANGUAGE TypeFamilies, FlexibleInstances, FlexibleContexts #-}
module Network.TransportLayer where

import           Control.Exception

import           Data.Typeable
import qualified Data.ByteString as BS
import qualified Data.List.NonEmpty as NL

import qualified System.Socket as S
import qualified System.Socket.Protocol.TCP as S

class Exception (TransportException a) => TransportLayer a where
  data Transport a
  data TransportAddress a
  data TransportConfiguration a
  data TransportException a
  new                 :: TransportConfiguration a -> IO (Transport a)
  close               :: Transport a -> IO ()

class TransportLayer a => BindableTransportLayer a where
  bind                :: Transport a -> TransportAddress a -> IO ()

class TransportLayer a => ConnectionTransportLayer a where
  data Connection a
  data ConnectionRequest a
  disconnect          :: Connection a -> IO ()

class ConnectionTransportLayer a => StreamConnectionTransportLayer a where
  sendChunk           :: Connection a -> BS.ByteString -> IO Int
  receiveChunk        :: Connection a -> Int -> IO BS.ByteString

class ConnectionTransportLayer a => DatagramConnectionTransportLayer a where
  sendDatagram        :: Connection a -> BS.ByteString -> IO Int
  receiveDatagram     :: Connection a -> Int -> IO BS.ByteString

class ConnectionTransportLayer a => AcceptingTransportLayer a where
  listen              :: Transport a -> IO ()
  accept              :: Transport a -> IO (Connection a, TransportAddress a, ConnectionRequest a)

class ConnectionTransportLayer a => ConnectingTransportLayer a where
  connect             :: Transport a -> TransportAddress a -> ConnectionRequest a -> IO (Connection a)

class AddressTranslatingTransport a where
  data TransportName a
  data TransportNameInfo a
  data TransportAddressInfo a
  data TransportAddressTranslationException a
  name                :: TransportNameInfo a    -> TransportName a
  address             :: TransportAddressInfo a -> TransportAddress a
  getNameInfo         :: TransportAddress a     -> IO (NL.NonEmpty (TransportNameInfo a))
  getAddressInfo      :: TransportName a        -> IO (NL.NonEmpty (TransportAddressInfo a))

instance (Typeable f, Typeable t, Typeable p, S.Family f, S.Type t, S.Protocol p) => TransportLayer (S.Socket f t p) where
  data Transport               (S.Socket f t p) = Socket (S.Socket f t p)
  data TransportAddress        (S.Socket f t p) = SocketAddress (S.SocketAddress f)
  data TransportConfiguration  (S.Socket f t p) = SocketConfiguration (S.SocketAddress f)
  data TransportException (S.Socket f t p)      = SocketTransportException S.SocketException deriving (Show, Typeable)
  new config                                    = Socket <$> S.socket
  close (Socket s)                              = S.close s

instance (Typeable f, Typeable t, Typeable p) => Exception (TransportException (S.Socket f t p)) where
