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

takePacketIdentifier :: PacketIdentifierPool -> (PacketIdentifierPool, Maybe PacketIdentifier)
takePacketIdentifier (PacketIdentifierPool p) = f [0x0000 .. 0xffff]
  where
    f []     = (PacketIdentifierPool p, Nothing)
    f (i:is) | S.member i p = f is
             | otherwise    = (PacketIdentifierPool $ S.insert i p, Just $ PacketIdentifier i)

returnPacketIdentifier :: PacketIdentifierPool -> PacketIdentifier -> PacketIdentifierPool
returnPacketIdentifier (PacketIdentifierPool p) (PacketIdentifier i) =
  PacketIdentifierPool (S.delete i p)
