{-# LANGUAGE OverloadedStrings #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker.RetainedMessages
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Broker.RetainedMessages where

import           Control.Applicative        hiding (empty)
import           Control.Concurrent.MVar
import qualified Data.ByteString.Lazy       as BSL
import           Data.List.NonEmpty         (NonEmpty (..))
import qualified Data.Map.Strict            as M
import           Data.Maybe
import qualified Data.Set                   as S
import           Prelude                    hiding (null)

import qualified Network.MQTT.Message       as Message
import qualified Network.MQTT.Message.Topic as Topic

newtype RetainedStore = RetainedStore { unstore :: MVar RetainedTree }
newtype RetainedTree  = RetainedTree  { untree :: M.Map Topic.Level RetainedNode }
data    RetainedNode  = RetainedNode !RetainedTree !(Maybe Message.Message)

new    :: IO RetainedStore
new     = RetainedStore <$> newMVar empty

store  :: Message.Message -> RetainedStore -> IO ()
store msg (RetainedStore mvar)
  | retain = modifyMVar_ mvar $ \tree->
      -- The seq ($!) is important for not leaking memory!
      pure $! if BSL.null body
        then delete msg tree
        else insert msg tree
  | otherwise = pure ()
  where
    Message.Payload body = Message.msgPayload msg
    Message.Retain retain = Message.msgRetain msg

retrieve :: Topic.Filter -> RetainedStore -> IO (S.Set Message.Message)
retrieve filtr (RetainedStore mvar) =
  lookupFilter filtr <$> readMVar mvar

empty  :: RetainedTree
empty   = RetainedTree mempty

null   :: RetainedTree -> Bool
null    = M.null . untree

insert :: Message.Message -> RetainedTree -> RetainedTree
insert msg = union (singleton msg)

delete :: Message.Message -> RetainedTree -> RetainedTree
delete msg = flip difference (singleton msg)

singleton :: Message.Message -> RetainedTree
singleton msg =
  let l :| ls = Topic.topicLevels (Message.msgTopic msg)
  in RetainedTree (M.singleton l $ node ls)
  where
    node []     = RetainedNode empty $! Just $! msg
    node (x:xs) = RetainedNode (RetainedTree $ M.singleton x $ node xs) Nothing

-- | The expression `union t1 t2` takes the left-biased union of `t1` and `t2`.
union :: RetainedTree -> RetainedTree -> RetainedTree
union (RetainedTree m1) (RetainedTree m2) =
  RetainedTree $ M.unionWith merge m1 m2
    where
      merge (RetainedNode t1 mm1) (RetainedNode t2 mm2) =
        RetainedNode (t1 `union` t2) $! case mm1 <|> mm2 of
          Nothing -> Nothing
          Just mm -> Just $! mm

difference :: RetainedTree -> RetainedTree -> RetainedTree
difference (RetainedTree m1) (RetainedTree m2) =
  RetainedTree $ M.differenceWith diff m1 m2
    where
      diff (RetainedNode t1 mm1) (RetainedNode t2 mm2)
        | null t3 && isNothing mm3 = Nothing
        | otherwise                = Just (RetainedNode t3 mm3)
        where
          t3 = difference t1 t2
          mm3 = case mm2 of
            Just _  -> Nothing
            Nothing -> mm1

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
        _   -> collect x xs subtree
