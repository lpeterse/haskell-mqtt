{-# LANGUAGE TypeFamilies, FlexibleContexts, FlexibleInstances #-}
module Network.Transceiver where

import Control.Monad

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

class (Transceiver a, Closable a) => Connection a where
  isConnectedException            :: a -> Exception a -> Bool
  isConnectedException         _ _ = False
  isNotConnectedException         :: a -> Exception a -> Bool
  isNotConnectedException      _ _ = False
  isConnectionAbortedException    :: a -> Exception a -> Bool
  isConnectionAbortedException _ _ = False
  isConnectionRefusedException    :: a -> Exception a -> Bool
  isConnectionRefusedException _ _ = False
  isConnectionTimeoutException    :: a -> Exception a -> Bool
  isConnectionTimeoutException _ _ = False
  isConnectionResetException      :: a -> Exception a -> Bool
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
  isAddressNotAvailableException    :: a -> Exception a -> Bool
  isAddressNotAvailableException _ _ = False

class (Connection a, Addressable a) => Connectable a where
  connect              :: a -> Address a -> IO ()

class (Connection (AcceptedConnection a), Addressable (AcceptedConnection a)) => Acceptable a where
  type AcceptedConnection a
  listen               :: a -> Int -> IO ()
  accept               :: a -> IO (AcceptedConnection a, Address (AcceptedConnection a))

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

instance Acceptable (S.Socket S.Inet6 t p) where
  type AcceptedConnection (S.Socket S.Inet6 t p) = S.Socket S.Inet6 t p
  listen = S.listen
  accept = S.accept

newtype TlsStream = TlsStream TLS.Context

instance Transceiver TlsStream where
  type Data TlsStream      = BS.ByteString
  type Exception TlsStream = TLS.TLSException

instance Closable TlsStream where
  close (TlsStream s) = TLS.contextClose s

instance Addressable TlsStream where
  type Address TlsStream = ()

instance Connection TlsStream where

instance StreamConnection TlsStream where
  sendChunk (TlsStream t) c  = TLS.sendData t (BSL.fromStrict c)
  receiveChunk (TlsStream t) = TLS.recvData t

data SecureStreamServer t
   = SecureStreamServer
     { serverParams :: TLS.ServerParams
     , transport    :: t
     , bindAddress  :: Address t
     }

data SecureStreamServerException t
   = TlsException TLS.TLSException
   | TlsTransportException (Exception t)

instance Transceiver t => Transceiver (SecureStreamServer t) where
  type Data (SecureStreamServer t)      = BS.ByteString
  type Exception (SecureStreamServer t) = SecureStreamServerException t

instance (Acceptable t, Data (AcceptedConnection t) ~ BS.ByteString, AcceptedConnection t ~ t, StreamConnection t) => Acceptable (SecureStreamServer t) where
  type AcceptedConnection (SecureStreamServer t) = TlsStream
  listen = undefined
  accept server = do
    (connection,_) <- accept (transport server)
    context    <- TLS.contextNew (backend connection) (serverParams server)
    return (TlsStream context, undefined)
    where
      backend c = TLS.Backend {
        TLS.backendFlush = return (),
        TLS.backendClose = close c,
        TLS.backendSend  = sendChunk c,
        TLS.backendRecv  = \_-> receiveChunk c
      }

--instance (Addressable t, Bindable t) => Bindable (SecureStreamServer t) where
--  bind t a = bind (transport t) (bindAddress t)
