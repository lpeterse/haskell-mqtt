{-# LANGUAGE OverloadedStrings #-}
module Network.MQTT.SubscriptionTree
  ( SubscriptionTree
  , newSubscriptionTree
  , subscribe
  , unsubscribe
  , publish ) where

import Control.Concurrent.MVar
import Data.Unique

import qualified Data.Map as M
import qualified Data.Text as T

type TopicLevel  = T.Text

data Tree a        = Tree (Subscribers a) (Subtree a)
type Subtree a     = M.Map TopicLevel (Tree a)
type Subscribers a = M.Map Unique (a -> IO ())

newtype SubscriptionTree a = SubscriptionTree (MVar (Tree a))

newSubscriptionTree :: IO (SubscriptionTree a)
newSubscriptionTree =
  SubscriptionTree <$> newMVar (Tree M.empty M.empty)

subscribe :: SubscriptionTree a -> (a -> IO ()) -> [TopicLevel] -> IO Unique
subscribe (SubscriptionTree mtree) handler ts = do
  unique <- newUnique
  modifyMVar_ mtree $ return . subscribe' unique handler ts
  return unique
  where
    subscribe' unique handler [] (Tree subscribers subtrees) =
      Tree (M.insert unique handler subscribers) subtrees
    subscribe' unique handler (t:ts) (Tree subscribers subtrees) =
      Tree subscribers $ M.alter ( Just . maybe
        ( subscribe' unique handler ts $ Tree M.empty M.empty )
        ( subscribe' unique handler ts ) ) t subtrees

unsubscribe :: SubscriptionTree a -> Unique -> [TopicLevel] -> IO ()
unsubscribe (SubscriptionTree mtree) unique ts =
  modifyMVar_ mtree $ return . unsubscribe' unique ts
  where
    unsubscribe' unique [] (Tree subscribers subtrees) =
      Tree (M.delete unique subscribers) subtrees
    unsubscribe' unique (t:ts) (Tree subscribers subtrees) =
      Tree subscribers $ M.alter
        ( maybe Nothing $ clean . unsubscribe' unique ts ) t subtrees
      where
        clean t@(Tree x y) | M.null x && M.null y = Nothing
                           | otherwise            = Just t

publish :: SubscriptionTree a -> [TopicLevel] -> a -> IO ()
publish (SubscriptionTree mtree) ts msg = do
  tree <- readMVar mtree
  publish' tree ts
  where
    publish' (Tree subscribers _) [] =
      mapM_ ($ msg) subscribers
    publish' (Tree subscribers subtrees) (t:ts) = do
      case M.lookup "#" subtrees of
        Nothing      -> return ()
        Just subtree -> mapM_ ($ msg) subscribers
      case M.lookup "+" subtrees of
        Nothing      -> return ()
        Just subtree -> publish' subtree ts
      case M.lookup  t  subtrees of
        Nothing      -> return ()
        Just subtree -> publish' subtree ts
