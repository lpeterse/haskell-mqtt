module Network.MQTT.PacketIdentifier
  ( PacketIdentifier (..)
  , PacketIdentifierPool ()
  , fullPool
  , getPacketIdentifier
  , returnPacketIdentifier
  )
where

import Data.IntSet as S

newtype PacketIdentifier     = PacketIdentifier Int
  deriving (Eq, Show)

newtype PacketIdentifierPool = PacketIdentifierPool S.IntSet

fullPool :: PacketIdentifierPool
fullPool  = PacketIdentifierPool S.empty

getPacketIdentifier :: PacketIdentifierPool -> Maybe (PacketIdentifier, PacketIdentifierPool)
getPacketIdentifier (PacketIdentifierPool p) = f [0x0000 .. 0xffff]
  where
    f []     = Nothing
    f (i:is) | S.member i p = f is
             | otherwise    = Just (PacketIdentifier i, PacketIdentifierPool $ S.insert i p)

returnPacketIdentifier :: PacketIdentifierPool -> PacketIdentifier -> PacketIdentifierPool
returnPacketIdentifier (PacketIdentifierPool p) (PacketIdentifier i) =
  PacketIdentifierPool (S.delete i p)
