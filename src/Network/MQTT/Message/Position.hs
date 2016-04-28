module Network.MQTT.Message.Position where

import Data.Attoparsec.ByteString as A
import Data.Attoparsec.Internal
import Data.Attoparsec.Internal.Types

pPosition :: A.Parser Int
pPosition = Parser $ \state pos more failure success->
  success state pos more (fromPos pos)
