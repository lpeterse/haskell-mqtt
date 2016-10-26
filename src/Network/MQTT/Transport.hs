{-# LANGUAGE TypeFamilies, FlexibleInstances #-}
module Network.MQTT.Transport where

import qualified System.Socket as S
import qualified System.Socket.Protocol.TCP as S

class Transport a where
  data Configuration a
  data Server a
  newServer :: Configuration a -> IO (Server a)

instance (S.Family f, S.Type t) => Transport (S.Socket f t S.TCP) where
  data Configuration (S.Socket f t S.TCP) = TcpSocketConfiguration (S.SocketAddress f)
  data Server (S.Socket f t S.TCP) = TcpSocketServer (Configuration (S.Socket f t S.TCP)) (S.Socket f t S.TCP)
  newServer config = TcpSocketServer config <$> S.socket
