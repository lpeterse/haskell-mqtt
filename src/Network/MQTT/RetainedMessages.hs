{-# LANGUAGE OverloadedStrings #-}
module Network.MQTT.RetainedMessages where

import           Control.Applicative  hiding (empty)
import           Data.List.NonEmpty   (NonEmpty (..))
import qualified Data.Map             as M
import           Data.Maybe
import qualified Data.Set             as S
import qualified Network.MQTT.Message as Message
import qualified Network.MQTT.Topic   as Topic

newtype RetainedTree = RetainedTree (M.Map Topic.Level RetainedNode)
data    RetainedNode = RetainedNode RetainedTree (Maybe Message.Message)

empty  :: RetainedTree
empty   = RetainedTree mempty

singleton :: Message.Message -> RetainedTree
singleton msg =
  let l :| ls = Topic.topicLevels (Message.msgTopic msg)
  in RetainedTree (M.singleton l $ node ls)
  where
    node []     = RetainedNode empty (Just msg)
    node (x:xs) = RetainedNode (RetainedTree $ M.singleton x $ node xs) Nothing

-- FIXME: probably not correct.
insert :: Message.Message -> RetainedTree-> RetainedTree
insert msg (RetainedTree m) =
  let l :| ls = Topic.topicLevels (Message.msgTopic msg)
  in RetainedTree $ M.insertWith' merge l (node ls) m
  where
    node []     = RetainedNode empty (Just msg)
    node (x:xs) = RetainedNode (RetainedTree $ M.singleton x $ node xs) Nothing
    merge (RetainedNode (RetainedTree tree1) msg1) (RetainedNode (RetainedTree tree2) msg2) =
      RetainedNode (RetainedTree $ M.unionWith merge tree1 tree2) (msg2 <|> msg1)

lookupFilter :: Topic.Filter -> RetainedTree -> S.Set Message.Message
lookupFilter filtr t =
  let l :| ls = Topic.filterLevels filtr in collect l ls t
  where
    collect l ls tree@(RetainedTree m) = case l of
      "#" -> allTree tree
      "+" -> M.foldl (\s node-> s `S.union` pathNode ls node) S.empty m
      _   -> fromMaybe S.empty $ pathNode ls <$> M.lookup l m
    allTree (RetainedTree branches) =
      M.foldl (\s node-> s `S.union` allNode node) S.empty branches
    allNode (RetainedNode subtree mmsg) =
      case mmsg of
        Nothing  -> allTree subtree
        Just msg -> S.insert msg (allTree subtree)
    pathNode [] (RetainedNode _ mmsg) =
      fromMaybe S.empty $ S.singleton <$> mmsg
    pathNode (x:xs) (RetainedNode subtree mmsg) =
      case x of
        "#"-> fromMaybe id (S.insert <$> mmsg) (collect x xs subtree)
        _  -> collect x xs subtree
