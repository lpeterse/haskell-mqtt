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

import qualified Network.MQTT.RoutingTree as R

main :: IO ()
main = do
  mtree <- newMVar mempty
  subscriberSum <- fst <$> concurrently
    ( sum <$> mapConcurrently (\t->publish mtree t >>= \k-> print ("PUB " ++ show t) >> pure k) topics >>= \x-> putStrLn "PUB ALL" >> pure x)
    ( mapConcurrently (\i->subscribeFilters mtree i >> print ("SUB " ++ show i)) [1..100] >> putStrLn "SUB ALL")
  print subscriberSum
  where
    topics = [
      R.Topic $ "a":|["a"],
      R.Topic $ "a":|["a","b"],
      R.Topic $ "a":|["a","b","c"],
      R.Topic $ "a":|["a","b","c","d"],
      R.Topic $ "a":|["a","b","c","d","e"],
      R.Topic $ "a":|["a","b","c","d","e","f"],
      R.Topic $ "a":|["a","b","c","d","e","f","g"],
      R.Topic $ "a":|["a","b","c","d","e","f","g","h"],
      R.Topic $ "a":|["a","b","c","d","e","f","g","h","i"],
      R.Topic $ "a":|["a","b","c","d","e","f","g","h","i","j"]
     ]

    publish :: MVar (R.RoutingTree S.IntSet) -> R.Topic -> IO Int
    publish mtree topic = foldM (\i _-> do
        tree <- readMVar mtree
        pure $! (+) i $ fromMaybe 0 $ S.size <$> R.lookupWith S.union topic tree
      ) 0 [1..100000 :: Int]

    filters :: [R.Filter]
    filters = fmap (R.Filter . ("a":|) . fmap ((BS.pack . fmap (fromIntegral . fromEnum)) . pure)) $ concatMap (\a->[a,a++"#"]) $ Prelude.filter (not . Prelude.null) $ concatMap Data.List.tails $ Data.List.inits ['a'..'z']

    subscribeFilters :: MVar (R.RoutingTree S.IntSet) -> Int -> IO ()
    subscribeFilters mtree i =
      forM_ filters $ \filtr->
        modifyMVar_ mtree (pure . R.insertWith S.union filtr (S.singleton i))
