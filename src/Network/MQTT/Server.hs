{-# LANGUAGE TypeFamilies #-}
module Network.MQTT.Server where

import Control.Monad
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.Async
import Control.Exception

import qualified Data.Set as S

import Network.MQTT.Broker ( Broker (..), Session (..) )

class ConnectionSource s where
  type Stream s
  type ConnectionMetaInformation s
  accept :: s -> IO (Stream s, ConnectionMetaInformation s)
  close  :: s -> IO ()

data Server s
   = Server
   { serverConnectionSource :: s
   , serverBroker           :: Broker
   , serverConnections      :: MVar (S.Set (Connection s))
   }

data Connection s
   = Connection
   { connectionStream       :: s
   }

{--
run :: ConnectionSource s => Server s -> IO ()
run server = (forever $ accept (serverConnectionSource server) >>= forkIO . f) `finally` shutdown
  where
    f (stream, meta) = bracket
      ( Connection <$> stream )
      ( \connection-> modifyMVar_
          (serverConnections server)
          (\connections-> pure $! S.delete connection connections)
      )
      ( \connection-> do
          modifyMVar_
            (serverConnections server)
            (\connections-> pure $! S.insert connection connections)
          handle connection
      )
    handle = undefined
--}
