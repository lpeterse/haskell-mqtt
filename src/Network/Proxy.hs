{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
module Network.Proxy where

import           Control.Applicative
import           Control.Exception                (Exception, throwIO)
import           Control.Monad
import qualified Data.Attoparsec.ByteString       as A
import qualified Data.Attoparsec.ByteString.Char8 as A
import qualified Data.ByteString                  as BS
import           Data.IORef
import           Data.Typeable                    (Typeable)
import           Data.Word
import           Network.Stack.Server
import qualified System.Socket.Family.Inet        as Inet
import qualified System.Socket.Family.Inet6       as Inet6

data Proxy a
data ProxyHeader
  = TCP4
  { srcAddress :: Inet.SocketAddress Inet.Inet
  , dstAddress :: Inet.SocketAddress Inet.Inet
  }
  | TCP6
  { src6Address :: Inet6.SocketAddress Inet6.Inet6
  , dst6Address :: Inet6.SocketAddress Inet6.Inet6
  } deriving (Eq, Show)

newtype ProxyException = ProxyException String deriving (Eq, Ord, Show, Typeable)

instance Exception ProxyException

instance StreamServerStack a => ServerStack (Proxy a) where
  data Server (Proxy a) = ProxyServer
    { proxyTransportServer :: Server a
    , proxyServerConfig    :: ServerConfig (Proxy a)
    }
  data ServerConfig (Proxy a) = ProxyConfig
    { proxyTrustedOrigins  :: [()]
    , proxyTransportConfig :: ServerConfig a
    }
  data ServerConnection (Proxy a) = ProxyConnection
    { proxyTransportConnection :: ServerConnection a
    , proxyRemainder           :: IORef BS.ByteString
    }
  data ServerConnectionInfo (Proxy a) = ProxyConnectionInfo
    { proxyTransportConnectionInfo :: ServerConnectionInfo a
    , proxyHeader                  :: ProxyHeader
    }
  withServer config handle
    = withServer (proxyTransportConfig config) $ \transport-> handle
        (ProxyServer transport config)
  serveOnce server handler
    = serveOnce (proxyTransportServer server) $ \connection info-> do
        (mproxyhdr, bs) <- receiveProxyHeader connection
        remainder <- newIORef bs
        handler
          (ProxyConnection connection remainder)
          (ProxyConnectionInfo info mproxyhdr)

instance StreamServerStack a => StreamServerStack (Proxy a) where
  sendStream conn = sendStream (proxyTransportConnection conn)
  -- In case the remainder contains an non-empty string
  -- we'll return it instead of actually receiving on the connection.
  -- The remainder will then be reset and the next receive will
  -- actually read from the connection.
  -- It is safe to use IORef here as only one thread per connection is supposed
  -- to execute this code.
  -- FIXME: This might return more bytes than requested for small n.
  receiveStream conn i = do
    bs <- readIORef (proxyRemainder conn)
    if BS.null bs
      then receiveStream (proxyTransportConnection conn) i
      else writeIORef (proxyRemainder conn) mempty >> pure bs

receiveProxyHeader :: forall a. StreamServerStack a => ServerConnection a -> IO (ProxyHeader, BS.ByteString)
receiveProxyHeader connection = do
  bs <- receiveStream connection maxBufSize
  if BS.null bs
    then throwIO (ProxyException "Connection closed by peer without sending any data.")
    else case A.parse parser bs of
      A.Done i r   -> pure (r, i)
      A.Fail {}    -> throwIO (ProxyException "Syntax error!")
      A.Partial {} -> throwIO (ProxyException "Header must be sent at once!")
  where
    maxBufSize = 108
    parser     = parseTCP4 <|> parseTCP6
    space      = A.skip (== 32)
    crlf       = A.skip (== 13) >> A.skip (== 10)

    parseTCP4 :: A.Parser ProxyHeader
    parseTCP4 = do
      void $ A.string "PROXY TCP4 "
      srcAddr <- parseIPv4
      space
      dstAddr <- parseIPv4
      space
      srcPort <- A.decimal :: A.Parser Word16
      space
      dstPort <- A.decimal :: A.Parser Word16
      crlf
      pure $! TCP4
        ( Inet.SocketAddressInet srcAddr (fromIntegral srcPort) )
        ( Inet.SocketAddressInet dstAddr (fromIntegral dstPort) )

    parseTCP6 :: A.Parser ProxyHeader
    parseTCP6 = do
      void $ A.string "PROXY TCP6 "
      src6Addr <- parseIPv6
      space
      dst6Addr <- parseIPv6
      space
      src6Port <- A.decimal :: A.Parser Word16
      space
      dst6Port <- A.decimal :: A.Parser Word16
      crlf
      pure $! TCP6
        ( Inet6.SocketAddressInet6 src6Addr (fromIntegral src6Port) 0 0 )
        ( Inet6.SocketAddressInet6 dst6Addr (fromIntegral dst6Port) 0 0 )

    parseIPv4 :: A.Parser Inet.InetAddress
    parseIPv4 = do
      a <- A.decimal
      A.skip (== 46)
      b <- A.decimal
      A.skip (== 46)
      c <- A.decimal
      A.skip (== 46)
      d <- A.decimal
      pure $! Inet.inetAddressFromTuple (a,b,c,d)

    parseIPv6 :: A.Parser Inet6.Inet6Address
    parseIPv6 = do
      a <- A.hexadecimal
      A.skip (== 58)
      b <- A.hexadecimal
      A.skip (== 58)
      c <- A.hexadecimal
      A.skip (== 58)
      d <- A.hexadecimal
      A.skip (== 58)
      e <- A.hexadecimal
      A.skip (== 58)
      f <- A.hexadecimal
      A.skip (== 58)
      g <- A.hexadecimal
      A.skip (== 58)
      h <- A.hexadecimal
      pure $! Inet6.inet6AddressFromTuple (a,b,c,d,e,f,g,h)
