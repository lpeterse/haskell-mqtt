{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Data.Int
import Data.IntSet as S
import Data.Monoid
import Data.List (inits, tails)
import qualified Data.ByteString.Short as BS

import Control.Concurrent.MVar
import Control.Concurrent.Async
import Control.Monad

import Network.MQTT.SubscriptionTree

main :: IO ()
main = do
  mtree <- newMVar mempty
  subscriberSum <- fst <$> concurrently
    ( sum <$> mapConcurrently (\t->publish mtree t >>= \k-> print ("PUB " ++ show t) >> pure k) topics >>= \x-> print "PUB ALL" >> pure x)
    ( mapConcurrently (\i->subscribeFilters mtree i >> print ("SUB " ++ show i)) [1..100] >> print "SUB ALL")
  print subscriberSum
  where
    topics = [
      Topic ["a"],
      Topic ["a","b"],
      Topic ["a","b","c"],
      Topic ["a","b","c","d"],
      Topic ["a","b","c","d","e"],
      Topic ["a","b","c","d","e","f"],
      Topic ["a","b","c","d","e","f","g"],
      Topic ["a","b","c","d","e","f","g","h"],
      Topic ["a","b","c","d","e","f","g","h","i"],
      Topic ["a","b","c","d","e","f","g","h","i","j"]
     ]
    publish :: MVar (SubscriptionTree) -> Topic -> IO Int
    publish mtree topic = foldM (\i _-> do
        tree <- readMVar mtree
        pure $! i + S.size (subscribers topic tree)
      ) 0 [1..100000]

    filters :: [Filter]
    filters = fmap (Filter . fmap (BS.pack . fmap (fromIntegral . fromEnum)) . fmap pure) $ concatMap (\a->[a,a++"#"]) $ Prelude.filter (not . Prelude.null) $ concatMap tails $ inits ['a'..'z']

    subscribeFilters :: MVar (SubscriptionTree) -> Int -> IO ()
    subscribeFilters mtree i = do
      forM_ filters $ \filtr-> do
        --print $ "SUB " ++ show i
        modifyMVar_ mtree (pure . subscribe i filtr)
      --print $ "DONE " ++ show i

{-
main :: IO ()
main = defaultMain
    [ bgroup "Message-parsing-and-serialisation" [
        bgroup "Connect (without will)" $ pb $
          Connect "client-identifier" True 65298 Nothing (Just ("username", Just "password"))
      , bgroup "Connect (with will)" $ pb $
          Connect "client-identifier" True 65298 (Just $ Will "topic" "message" (Just ExactlyOnce) False) (Just ("username", Just "password"))
      , bgroup "ConnectAcknowledgement (rejected)" $ pb $
          ConnectAcknowledgement (Left IdentifierRejected)
      , bgroup "ConnectAcknowledgement (accepted)" $ pb $
          ConnectAcknowledgement (Right False)
      , bgroup "Publish (QoS 0)" $ pb $
          Publish True False "topic" Nothing "message body"
      , bgroup "Publish (QoS 1)" $ pb $
          Publish True False "topic" (Just (AtLeastOnce, PacketIdentifier 2342)) "message body"
      , bgroup "PublishAcknowledgement" $ pb $
          PublishAcknowledgement (PacketIdentifier 234)
      , bgroup "Subscribe" $ pb $
          Subscribe (PacketIdentifier 2345) [("short", Nothing), ("longer longer longer", Just ExactlyOnce)]
      , bgroup "SubscribeAcknowledgement" $ pb $
          SubscribeAcknowledgement (PacketIdentifier 2345) [Nothing, Just Nothing, Just (Just ExactlyOnce), Nothing]
      , bgroup "Unsubscribe" $ pb $
          Unsubscribe (PacketIdentifier 2345) ["short", "longer longer longer"]
      , bgroup "PingRequest" $ pb
          PingRequest
      , bgroup "PingResponse" $ pb
          PingResponse
      , bgroup "Disconnect" $ pb
          Disconnect
      ]
    ]
    where
      pb x = [
          bench "parse" (whnf (A.parseOnly pRawMessage) (LBS.toStrict $ BS.toLazyByteString $ bRawMessage x))
        , bench "build" (whnf (LBS.toStrict . BS.toLazyByteString . bRawMessage) x)
        ]
-}
