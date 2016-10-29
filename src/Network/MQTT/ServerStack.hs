{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
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

import           Network.MQTT.Authentication
import qualified Network.TLS                 as TLS
import qualified System.Socket               as S
import qualified System.Socket.Family.Inet   as S
import qualified System.Socket.Protocol.TCP  as S
import qualified System.Socket.Type.Stream   as S

data MqttMessage

data SocketServer
data TlsServer a
data WebsocketServer a
data MqttServer a

class Server a where
  data ServerConnection a

instance Server SocketServer where
  data ServerConnection SocketServer = SocketServerConnection (S.Socket S.Inet S.Stream S.TCP)

instance Server (TlsServer a) where
  data ServerConnection (TlsServer a) = TlsServerConnection
    { tlsTransportState :: ServerConnection a
    , tlsContext        :: TLS.Context
    }

instance Request (ServerConnection SocketServer) where
instance (Request (ServerConnection a)) => Request (ServerConnection (TlsServer a)) where
  requestSecure           = const True
  requestCertificateChain = const [undefined]
  requestHeaders a        = requestHeaders (tlsTransportState a)
instance Request (ServerConnection (WebsocketServer a)) where
