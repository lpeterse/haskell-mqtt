module Network.MQTT.TopicTree where

import Control.Concurrent.MVar
import Data.Unique

import qualified Data.Map as M
import qualified Data.Text as T

data Message
type TopicLevel = T.Text

type Subtree = M.Map TopicLevel Tree

data Tree
   = Tree Subscribers Subtree

type Subscribers
   = M.Map Unique (Message -> IO ())

subscribe :: Unique -> (Message -> IO ()) -> [TopicLevel] -> Tree -> Tree
subscribe unique handler [] (Tree subscribers subtrees) =
  Tree (M.insert unique handler subscribers) subtrees
subscribe unique handler (t:ts) (Tree subscribers subtrees) =
  Tree subscribers $ M.alter ( Just . maybe
    ( subscribe unique handler ts $ Tree M.empty M.empty )
    ( subscribe unique handler ts ) ) t subtrees

unsubscribe :: Unique -> [TopicLevel] -> Tree -> Tree
unsubscribe unique [] (Tree subscribers subtrees) =
  Tree (M.delete unique subscribers) subtrees
unsubscribe unique (t:ts) (Tree subscribers subtrees) =
  Tree subscribers $ M.alter ( maybe Nothing ( clean . unsubscribe unique ts ) ) t subtrees
  where
    clean t@(Tree x y) | M.null x && M.null y = Nothing
                       | otherwise            = Just t
