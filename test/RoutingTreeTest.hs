{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
module RoutingTreeTest ( tests ) where

import qualified Data.IntSet              as IS
import           Data.List.NonEmpty       (NonEmpty (..))
import           Data.Monoid
import qualified Data.Text                as T
import           Prelude                  hiding (head)

import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import qualified Network.MQTT.RoutingTree as R
import           Network.MQTT.Topic

tests :: TestTree
tests = testGroup "RoutingTree"
  [ testGroup "null"
    [ testCase "! null                  $ singleton \"a\"      ()"   $ assertBool "" $ not $ R.null $ R.singleton "a" ()
    , testCase "! null                  $ singleton \"a/b\"    ()"   $ assertBool "" $ not $ R.null $ R.singleton "a/b" ()
    ]
  , testGroup "empty"
    [ testCase "  null empty" $ assertBool "not null" $ R.null R.empty
    ]
  , testGroup "size"
    [ testCase "size empty       == 0"                               $ R.size (R.empty :: R.RoutingTree ()) @?= 0
    , testCase "size intsetTree1 == 11"                              $ R.size intsetTree1                   @?= 11
    ]
  , testGroup "singleton"
    [ testCase "  matchTopic \"a\"      $ singleton \"a\"      ()"   $ assertBool ""       $ R.matchTopic "a"     $ R.singleton "a" ()
    , testCase "  matchTopic \"a/b/c\"  $ singleton \"a/b/c\"  ()"   $ assertBool ""       $ R.matchTopic "a/b/c" $ R.singleton "a/b/c" ()
    ]
  , testGroup "matchTopic"
    [ testCase "  matchTopic \"a\"      $ singleton \"a\"      ()"   $ assertBool ""       $ R.matchTopic "a"      $ R.singleton "a"   ()
    , testCase "  matchTopic \"a\"      $ singleton \"#\"      ()"   $ assertBool ""       $ R.matchTopic "a"      $ R.singleton "#"   ()
    , testCase "  matchTopic \"a\"      $ singleton \"a/#\"    ()"   $ assertBool ""       $ R.matchTopic "a"      $ R.singleton "a/#" ()
    , testCase "  matchTopic \"a/b\"    $ singleton \"a/#\"    ()"   $ assertBool ""       $ R.matchTopic "a/b"    $ R.singleton "a/#" ()
    , testCase "  matchTopic \"a/b/c\"  $ singleton \"a/#\"    ()"   $ assertBool ""       $ R.matchTopic "a/b/c"  $ R.singleton "a/#" ()
    , testCase "! matchTopic \"b/c/d\"  $ singleton \"a/#\"    ()"   $ assertBool "" $ not $ R.matchTopic "b/c/d"  $ R.singleton "a/#" ()
    , testCase "! matchTopic \"a\"      $ singleton \"a/+\"    ()"   $ assertBool "" $ not $ R.matchTopic "a"      $ R.singleton "a/+" ()
    , testCase "! matchTopic \"a\"      $ singleton \"/a\"     ()"   $ assertBool "" $ not $ R.matchTopic "a"      $ R.singleton "/a"  ()
    , testCase "  matchTopic \"a/b\"    $ singleton \"a/b\"    ()"   $ assertBool ""       $ R.matchTopic "a/b"    $ R.singleton "a/b" ()
    , testCase "  matchTopic \"a/b\"    $ singleton \"a/+\"    ()"   $ assertBool ""       $ R.matchTopic "a/b"    $ R.singleton "a/+" ()
    , testCase "  matchTopic \"a/b\"    $ singleton \"a/#\"    ()"   $ assertBool ""       $ R.matchTopic "a/b"    $ R.singleton "a/#" ()
    , testCase "  matchTopic \"a/b\"    $ singleton \"a/b/#\"  ()"   $ assertBool ""       $ R.matchTopic "a/b"    $ R.singleton "a/b/#" ()
    , testCase "! matchTopic \"$SYS\"   $ singleton \"#\"      ()"   $ assertBool "" $ not $ R.matchTopic "$SYS"   $ R.singleton "#"  ()
    , testCase "! matchTopic \"$SYS/a\" $ singleton \"#\"      ()"   $ assertBool "" $ not $ R.matchTopic "$SYS/a" $ R.singleton "#"  ()
    , testCase "  matchTopic \"$SYS\"   $ singleton \"$SYS/#\" ()"   $ assertBool ""       $ R.matchTopic "$SYS"   $ R.singleton "$SYS/#" ()
    , testCase "  matchTopic \"$SYS/a\" $ singleton \"$SYS/#\" ()"   $ assertBool ""       $ R.matchTopic "$SYS/a" $ R.singleton "$SYS/#" ()
    , testCase "! matchTopic \"$SYS\"   $ singleton \"+\"      ()"   $ assertBool "" $ not $ R.matchTopic "$SYS"   $ R.singleton "+"  ()
    , testCase "! matchTopic \"$SYS/a\" $ singleton \"+\"      ()"   $ assertBool "" $ not $ R.matchTopic "$SYS/a" $ R.singleton "+"  ()
    , testCase "! matchTopic \"$SYS\"   $ singleton \"$SYS/+\" ()"   $ assertBool "" $ not $ R.matchTopic "$SYS"   $ R.singleton "$SYS/+" ()
    , testCase "  matchTopic \"$SYS/a\" $ singleton \"$SYS/+\" ()"   $ assertBool ""       $ R.matchTopic "$SYS/a" $ R.singleton "$SYS/+" ()
    ]
  , testGroup "matchFilter"
    [ testCase "  matchFiler \"#\"      $ singleton \"#\"      ()"   $ assertBool ""       $ R.matchFilter "#"     $ R.singleton "#"   ()
    , testCase "  matchFiler \"+\"      $ singleton \"#\"      ()"   $ assertBool ""       $ R.matchFilter "+"     $ R.singleton "#"   ()
    , testCase "  matchFiler \"a\"      $ singleton \"#\"      ()"   $ assertBool ""       $ R.matchFilter "a"     $ R.singleton "#"   ()
    , testCase "! matchFiler \"#\"      $ singleton \"+\"      ()"   $ assertBool "" $ not $ R.matchFilter "#"     $ R.singleton "+"   ()
    , testCase "  matchFiler \"+\"      $ singleton \"+\"      ()"   $ assertBool ""       $ R.matchFilter "+"     $ R.singleton "+"   ()
    , testCase "  matchFiler \"a\"      $ singleton \"+\"      ()"   $ assertBool ""       $ R.matchFilter "a"     $ R.singleton "+"   ()
    , testCase "! matchFiler \"#\"      $ singleton \"a\"      ()"   $ assertBool "" $ not $ R.matchFilter "#"     $ R.singleton "a"   ()
    , testCase "! matchFiler \"+\"      $ singleton \"a\"      ()"   $ assertBool "" $ not $ R.matchFilter "+"     $ R.singleton "a"   ()
    , testCase "  matchFiler \"a\"      $ singleton \"a\"      ()"   $ assertBool ""       $ R.matchFilter "a"     $ R.singleton "a"   ()
    , testCase "  matchFiler \"a\"      $ singleton \"a/#\"    ()"   $ assertBool ""       $ R.matchFilter "a"     $ R.singleton "a/#" ()
    , testCase "! matchFiler \"a\"      $ singleton \"a/+\"    ()"   $ assertBool "" $ not $ R.matchFilter "a"     $ R.singleton "a/+" ()
    , testCase "  matchFiler \"a/#\"    $ singleton \"#\"      ()"   $ assertBool ""       $ R.matchFilter "a/#"   $ R.singleton "#"   ()
    , testCase "  matchFiler \"a/b/#\"  $ singleton \"#\"      ()"   $ assertBool ""       $ R.matchFilter "a/b/#" $ R.singleton "#"   ()
    , testCase "! matchFiler \"a/+/c\"  $ singleton \"a/b/c\"  ()"   $ assertBool "" $ not $ R.matchFilter "a/+/c" $ R.singleton "a/b/c" ()
    ]
  , testGroup "lookupWith"
    [ testCase "lookupWith (IS.union) \"a\"        intsetTree1 == Just [0,1,2]" $ R.lookupWith IS.union "a"        intsetTree1 @?= Just (IS.fromList [0,1,2,4])
    , testCase "lookupWith (IS.union) \"b\"        intsetTree1 == Just [0,1,2]" $ R.lookupWith IS.union "b"        intsetTree1 @?= Just (IS.fromList [0,1])
    , testCase "lookupWith (IS.union) \"a/a\"      intsetTree1 == Just [0,1,2]" $ R.lookupWith IS.union "a/a"      intsetTree1 @?= Just (IS.fromList [0,3,4])
    , testCase "lookupWith (IS.union) \"a/a/a\"    intsetTree1 == Just [0,1,2]" $ R.lookupWith IS.union "a/a/a"    intsetTree1 @?= Just (IS.fromList [0,4])
    , testCase "lookupWith (IS.union) \"$SYS\"     intsetTree1 == Just [0,1,2]" $ R.lookupWith IS.union "$SYS"     intsetTree1 @?= Nothing
    , testCase "lookupWith (IS.union) \"$SYS/a\"   intsetTree1 == Just [0,1,2]" $ R.lookupWith IS.union "$SYS/a"   intsetTree1 @?= Just (IS.fromList [5,6,7])
    , testCase "lookupWith (IS.union) \"$SYS/a/a\" intsetTree1 == Just [0,1,2]" $ R.lookupWith IS.union "$SYS/a/a" intsetTree1 @?= Just (IS.fromList [5,7,8])
    ]
  , testGroup "insert"
    [ testCase "size tree2 == 3"                     $ R.size tree2 @?= 3
    , testCase "lookupWith IS.union \"a/b\"   tree2" $ R.lookupWith IS.union "a/b"   tree2 @?= Just (IS.fromList [3])
    , testCase "lookupWith IS.union \"a/b/c\" tree2" $ R.lookupWith IS.union "a/b/c" tree2 @?= Just (IS.fromList [2])
    ]
  , testGroup "insertWith" [  ]
  , testGroup "map" [  ]
  , testGroup "adjust" [  ]
  , testGroup "delete" [  ]
  , testGroup "unionWith" [  ]
  , testGroup "differenceWith" [  ]
  ]

intsetTree1 :: R.RoutingTree IS.IntSet
intsetTree1
  = R.insertWith IS.union "#"        (IS.singleton 0)
  $ R.insertWith IS.union "+"        (IS.singleton 1)
  $ R.insertWith IS.union "a"        (IS.singleton 2)
  $ R.insertWith IS.union "a/+"      (IS.singleton 3)
  $ R.insertWith IS.union "a/#"      (IS.singleton 4)
  $ R.insertWith IS.union "$SYS/#"   (IS.singleton 5)
  $ R.insertWith IS.union "$SYS/+"   (IS.singleton 6)
  $ R.insertWith IS.union "$SYS/a/#" (IS.singleton 7)
  $ R.insertWith IS.union "$SYS/+/a" (IS.singleton 8)
  $! R.empty

tree2 :: R.RoutingTree IS.IntSet
tree2
  = R.insert "a/b"    (IS.singleton 3)
  $ R.insert "a/b/c"  (IS.singleton 2)
  $ R.insert "a/b/c"  (IS.singleton 1)
  $! R.empty
