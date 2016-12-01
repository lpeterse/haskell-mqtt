{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Data.Maybe
import Data.List
import Data.List.NonEmpty
import qualified Data.IntSet as S
import qualified Data.ByteString.Short as BS

import Control.Concurrent.MVar
import Control.Concurrent.Async
import Control.Monad

import           Network.MQTT.TopicFilter
import qualified Network.MQTT.RoutingTree as R

main :: IO ()
main = do
  mtree <- newMVar mempty
  subscriberSum <- fst <$> concurrently
    ( sum <$> mapConcurrently (\t->publish mtree t >>= \k-> print ("PUB " ++ show t) >> pure k) topics >>= \x-> putStrLn "PUB ALL" >> pure x)
    ( mapConcurrently (\i->subscribeFilters mtree i >> print ("SUB " ++ show i)) [1..1000] >> putStrLn "SUB ALL")
  print subscriberSum
  where
    topics = [
      Topic $ "a":|["a"],
      Topic $ "a":|["a","b"],
      Topic $ "a":|["a","b","c"],
      Topic $ "a":|["a","b","c","d"],
      Topic $ "a":|["a","b","c","d","e"],
      Topic $ "a":|["a","b","c","d","e","f"],
      Topic $ "a":|["a","b","c","d","e","f","g"],
      Topic $ "a":|["a","b","c","d","e","f","g","h"],
      Topic $ "a":|["a","b","c","d","e","f","g","h","i"],
      Topic $ "a":|["a","b","c","d","e","f","g","h","i","j"]
     ]

    publish :: MVar (R.RoutingTree S.IntSet) -> Topic -> IO Int
    publish mtree topic = foldM (\i _-> do
        tree <- readMVar mtree
        pure $! (+) i $ fromMaybe 0 $ S.size <$> R.lookupWith S.union topic tree
      ) 0 [1..1000000 :: Int]

    filters :: [Filter]
    filters = fmap (Filter . ("a":|) . fmap ((BS.pack . fmap (fromIntegral . fromEnum)) . pure)) $ concatMap (\a->[a,a++"#"]) $ Prelude.filter (not . Prelude.null) $ concatMap Data.List.tails $ Data.List.inits ['a'..'z']

    subscribeFilters :: MVar (R.RoutingTree S.IntSet) -> Int -> IO ()
    subscribeFilters mtree i =
      forM_ filters $ \filtr->
        modifyMVar_ mtree (pure . R.insertWith S.union filtr (S.singleton i))
