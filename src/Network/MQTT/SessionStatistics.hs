module Network.MQTT.SessionStatistics where

import Control.Concurrent.MVar
import Data.Word

data SessionStatistics = SessionStatistics
   { published  :: MVar Word64
   , dropped    :: MVar Word64
   }

new :: IO SessionStatistics
new = SessionStatistics
  <$> newMVar 0
  <*> newMVar 0

accountMessagePublished :: SessionStatistics -> IO ()
accountMessagePublished ss =
  modifyMVar_ (published ss) $ \i-> pure $! i + 1

accountMessageDropped   :: SessionStatistics -> IO ()
accountMessageDropped ss =
  modifyMVar_ (dropped ss) $ \i-> pure $! i + 1
