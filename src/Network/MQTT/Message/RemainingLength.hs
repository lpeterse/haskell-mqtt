module Network.MQTT.Message.RemainingLength where

import qualified Data.Attoparsec.ByteString as A
import Data.Monoid
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import Data.Word

pRemainingLength:: A.Parser Int
pRemainingLength = do
  b0 <- A.anyWord8
  if b0 < 128
    then return $ fromIntegral b0
     else do
       b1 <- A.anyWord8
       if b1 < 128
        then return $ fromIntegral b1 * 128 +
                      fromIntegral (b0 .&. 127)
        else do
          b2 <- A.anyWord8
          if b2 < 128
            then return $ fromIntegral b2 * 128 * 128 +
                          fromIntegral (b1 .&. 127) * 128 +
                          fromIntegral (b0 .&. 127)
            else do
              b3 <- A.anyWord8
              if b3 < 128
                then return $ fromIntegral b3 * 128 * 128 * 128 +
                              fromIntegral (b2 .&. 127) * 128 * 128 +
                              fromIntegral (b1 .&. 127) * 128 +
                              fromIntegral (b0 .&. 127)
                else fail "Invalid remaining length."

sRemainingLength :: Int -> BS.Builder
sRemainingLength i
  | i < 0x80                = BS.word8    ( fromIntegral i )
  | i < 0x80*0x80           = BS.word16LE $ fromIntegral $ 0x0080 -- continuation bit
                                         .|.              ( i .&. 0x7f      )
                                         .|. unsafeShiftL ( i .&. 0x3f80    )  1
  | i < 0x80*0x80*0x80      = BS.word16LE ( fromIntegral $ 0x8080
                                         .|.              ( i .&. 0x7f      )
                                         .|. unsafeShiftL ( i .&. 0x3f80    )  1
                                          )
                           <> BS.word8    ( fromIntegral
                                          $ unsafeShiftR ( i .&. 0x1fc000   ) 14
                                          )
  | i < 0x80*0x80*0x80*0x80 = BS.word32LE $ fromIntegral $ 0x00808080
                                         .|.              ( i .&. 0x7f      )
                                         .|. unsafeShiftL ( i .&. 0x3f80    )  1
                                         .|. unsafeShiftL ( i .&. 0x1fc000  )  2
                                         .|. unsafeShiftL ( i .&. 0x0ff00000)  3
  | otherwise               = error "sRemainingLength: invalid input"
