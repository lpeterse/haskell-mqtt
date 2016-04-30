module Network.MQTT.KeepAlive where

import Control.Concurrent ( threadDelay )
import Control.Concurrent.Async

import Data.Int

import System.Clock ( getTime, Clock (Monotonic), TimeSpec (..) )

import Network.MQTT.Message

keepAliveThread :: KeepAlive -> IO Int64  -> IO () -> IO ()
keepAliveThread keepAlive getTimeLastPacketSent ping = do
  timeLastPacketSent <- getTimeLastPacketSent
  TimeSpec now _     <- getTime Monotonic
  let timeIdle        = now - timeLastPacketSent
  if timeIdle + 1 >= fromIntegral keepAlive
    then ping >> sleep keepAlive
    else sleep (fromIntegral keepAlive - timeIdle)
  where
    sleep seconds = threadDelay (fromIntegral seconds * 1000000)
