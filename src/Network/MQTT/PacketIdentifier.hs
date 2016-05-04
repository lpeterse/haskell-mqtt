module Network.MQTT.PacketIdentifier
  ( PacketIdentifier (..)
  , PacketIdentifierPool ()
  , newPool
  , takePacketIdentifier
  , returnPacketIdentifier
  , resolvePacketIdentifier
  )
where

import Data.IntMap as M
import Control.Concurrent.MVar

newtype PacketIdentifier = PacketIdentifier Int
  deriving (Eq, Show)

newtype PacketIdentifierPool a = PacketIdentifierPool (MVar (M.IntMap (MVar a)))

newPool :: IO (PacketIdentifierPool a)
newPool  = PacketIdentifierPool <$> newMVar M.empty

takePacketIdentifier :: PacketIdentifierPool a -> IO (Maybe (PacketIdentifier, MVar a))
takePacketIdentifier (PacketIdentifierPool mp) =
  modifyMVar mp (`f` [0x0000 .. 0xffff])
    where
      f          :: M.IntMap (MVar a) -> [Int] -> IO (M.IntMap (MVar a), Maybe (PacketIdentifier, MVar a))
      f p []      = pure  (p, Nothing)
      f p (i:is)  | M.member i p = f p is
                  | otherwise    = do
                      mvar <- newEmptyMVar
                      pure (M.insert i mvar p, Just (PacketIdentifier i, mvar))

returnPacketIdentifier :: PacketIdentifierPool a -> PacketIdentifier -> IO ()
returnPacketIdentifier (PacketIdentifierPool mp) (PacketIdentifier i) =
  modifyMVar_ mp (pure . M.delete i)

resolvePacketIdentifier :: PacketIdentifierPool a -> PacketIdentifier -> a -> IO ()
resolvePacketIdentifier (PacketIdentifierPool mp) (PacketIdentifier i) a =
  modifyMVar_ mp $ \p-> case M.lookup i p of
    Just mvar -> putMVar mvar a >> pure (M.delete i p)
    Nothing   -> pure p
