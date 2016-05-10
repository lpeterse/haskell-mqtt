module Network.MQTT.Message.Position where

import Data.Attoparsec.ByteString as A
import Data.Attoparsec.Internal
import Data.Attoparsec.Internal.Types

pPosition :: A.Parser Int
pPosition = Parser $ \state pos more failure success->
  success state pos more (fromPos pos)
{-# INLINE pPosition #-}

pManyWithLimit :: Int -> A.Parser a -> A.Parser [a]
pManyWithLimit len parser
  | len <= 0 = pure []
  | otherwise = do
      begin <- pPosition
      a <- parser
      end <- pPosition
      (a:) <$> pManyWithLimit (len - (end - begin)) parser
{-# INLINE pManyWithLimit #-}
