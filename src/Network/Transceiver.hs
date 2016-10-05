{-# LANGUAGE TypeFamilies, FlexibleContexts, FlexibleInstances #-}
module Network.Transceiver where

import Control.Monad
import qualified Control.Exception as E

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL

import qualified System.Socket as S
import qualified System.Socket.Type.Stream as S
import qualified System.Socket.Family.Inet6 as S

import qualified Network.TLS as TLS

class Transceiver a where
  type Data a
  type Exception a
  isTransceiverException    :: a -> Exception a -> Bool
  isTransceiverException _ _ = True

class Closable a where
  close       :: a -> IO ()

class Closable a => Creatable a where
  create      :: IO a

class (Transceiver a, E.Exception (ConnectionException a), Closable a) => Connection a where
  type ConnectionException a
  isConnectedException            :: a -> ConnectionException a -> Bool
  isConnectedException         _ _ = False
  isNotConnectedException         :: a -> ConnectionException a -> Bool
  isNotConnectedException      _ _ = False
  isConnectionAbortedException    :: a -> ConnectionException a -> Bool
  isConnectionAbortedException _ _ = False
  isConnectionRefusedException    :: a -> ConnectionException a -> Bool
  isConnectionRefusedException _ _ = False
  isConnectionTimeoutException    :: a -> ConnectionException a -> Bool
  isConnectionTimeoutException _ _ = False
  isConnectionResetException      :: a -> ConnectionException a -> Bool
  isConnectionResetException   _ _ = False

class (Connection a, Eq (Data a), Monoid (Data a)) => StreamConnection a where
  sendChunk      :: a -> Data a -> IO ()
  sendChunks     :: (Chunks b, Chunk b ~ Data a) => a -> b -> IO ()
  sendChunks a    = mapM__ (sendChunk a)
  receiveChunk   :: a -> IO (Data a)
  receiveChunks  :: (Chunks b, Chunk b ~ Data a) => a -> IO b
  receiveChunks a = singleton <$> receiveChunk a

class Addressable a where
  type Address a

class (Connection a, Addressable a) => Bindable a where
  bind                              :: a -> Address a -> IO ()
  isAddressNotAvailableException    :: a -> ConnectionException a -> Bool
  isAddressNotAvailableException _ _ = False

class (Connection a, Addressable a) => Connectable a where
  connect              :: a -> Address a -> IO ()

class (Bindable a, Addressable a) => Acceptable a where
  listen               :: a -> Int -> IO ()
  accept               :: a -> IO (a, Address a)

class (Connection a) => DatagramConnection a where
  type Datagram a
  sendDatagram    :: a -> Datagram a -> IO ()
  receiveDatagram :: a -> IO (Datagram a)

class Chunks a where
  type Chunk a
  mapM__    :: (Chunk a -> IO ()) -> a -> IO ()
  singleton :: Chunk a -> a

instance Chunks BSL.ByteString where
  type Chunk BSL.ByteString = BS.ByteString
  mapM__ f  = mapM_ f . BSL.toChunks
  singleton = BSL.fromStrict

instance Chunks [a] where
  type Chunk [a] = a
  mapM__ = mapM_
  singleton = pure

instance Transceiver (S.Socket f t p) where
  type Data (S.Socket f t p) = BS.ByteString
  type Exception (S.Socket f t p) = S.SocketException

instance Closable (S.Socket f t p) where
  close = S.close

instance (S.Family f, S.Type t, S.Protocol p) => Creatable (S.Socket f t p) where
  create = S.socket

instance Addressable (S.Socket f t p) where
  type Address (S.Socket f t p)   = S.SocketAddress f

instance Connection (S.Socket f t p) where
  type ConnectionException (S.Socket f t p) = S.SocketException
  isConnectedException         s = (== S.eIsConnected)
  isNotConnectedException      s = (== S.eNotConnected)
  isConnectionTimeoutException s = (== S.eTimedOut)
  isConnectionAbortedException s = (== S.eConnectionAborted)
  isConnectionRefusedException s = (== S.eConnectionRefused)
  isConnectionResetException   s = (== S.eConnectionReset)

instance StreamConnection (S.Socket f S.Stream p) where
  sendChunk s = sendAll
    where
      sendAll bs = do
        sent <- S.send s bs S.msgNoSignal
        when (sent < BS.length bs) $ sendAll (BS.drop sent bs)
  receiveChunk s = S.receive s 4096 S.msgNoSignal

instance Connectable (S.Socket S.Inet6 t p) where
  connect = S.connect

data SecureStreamServer t
   = SecureStreamServer
     { serverParams :: TLS.ServerParams
     , transport    :: t
     , bindAddress  :: Address t
     }

data SecureStreamServerException t
   = TlsException TLS.TLSException
   | TransportException (Exception t)

instance Transceiver t => Transceiver (SecureStreamServer t) where
  type Data (SecureStreamServer t)      = BS.ByteString
  type Exception (SecureStreamServer t) = SecureStreamServerException t

--instance (Addressable t, Bindable t) => Bindable (SecureStreamServer t) where
--  bind t a = bind (transport t) (bindAddress t)
