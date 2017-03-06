{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
module RoutingTreeTest ( tests ) where

import qualified Data.IntSet              as IS
import           Prelude                  hiding (head)

import           Test.Tasty
import           Test.Tasty.HUnit

import qualified Network.MQTT.RoutingTree as R

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
    [ testCase "size empty ==  0"                                    $ R.size (R.empty :: R.RoutingTree ()) @?=  0
    , testCase "size tree1 == 11"                                    $ R.size tree1                         @?= 11
    , testCase "size tree2 ==  3"                                    $ R.size tree2                         @?=  3
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
  , testGroup "lookup"
    [ testCase "lookup \"a\"        tree1 == [0,1,2,4]" $ R.lookup "a"        tree1 @?= IS.fromList [0,1,2,4]
    , testCase "lookup \"b\"        tree1 == [0,1]"     $ R.lookup "b"        tree1 @?= IS.fromList [0,1]
    , testCase "lookup \"a/a\"      tree1 == [0,3,4]"   $ R.lookup "a/a"      tree1 @?= IS.fromList [0,3,4]
    , testCase "lookup \"a/a/a\"    tree1 == [0,4]"     $ R.lookup "a/a/a"    tree1 @?= IS.fromList [0,4]
    , testCase "lookup \"$SYS\"     tree1 == []"        $ R.lookup "$SYS"     tree1 @?= IS.fromList []
    , testCase "lookup \"$SYS/a\"   tree1 == [5,6,7]"   $ R.lookup "$SYS/a"   tree1 @?= IS.fromList [5,6,7]
    , testCase "lookup \"$SYS/a/a\" tree1 == [5,7,8]"   $ R.lookup "$SYS/a/a" tree1 @?= IS.fromList [5,7,8]
    ]
  , testGroup "insert"
    [ testCase "size tree2                == 3"         $ R.size tree2              @?= 3
    , testCase "lookup \"a/b\"      tree2 == [3]"       $ R.lookup "a/b"      tree2 @?= IS.fromList [3]
    , testCase "lookup \"a/b/c\"    tree2 == [2]"       $ R.lookup "a/b/c"    tree2 @?= IS.fromList [2]
    ]
  , testGroup "insertWith" [
      testCase "parameter order (new, old)" $ ( R.lookup "a" $ R.insertWith const "a" (IS.singleton 2) $ R.singleton "a" (IS.singleton 1) ) @?= IS.fromList [2]
    ]
  , testGroup "map" [ ]
  , testGroup "mapMaybe" [ ]
  , testGroup "adjust" [ ]
  , testGroup "delete" [ ]
  , testGroup "union" [
      testCase "structurally distinct trees with shared prefix"
        $ (R.singleton "a/b/y" $ IS.singleton 1) `R.union` (R.singleton "a/b/x" $ IS.singleton 2)
        @?= R.insertFoldable [("a/b/y", IS.singleton 1), ("a/b/x", IS.singleton 2)] R.empty
    , testCase "structurally equal trees with distinct values"
        $ (R.singleton "a/b/x" $ IS.singleton 1) `R.union` (R.singleton "a/b/x" $ IS.singleton 2)
        @?= R.singleton "a/b/x" (IS.fromList [1,2])
    ]
  , testGroup "unionWith" [
      testCase "structurally distinct trees with shared prefix"
        $ R.unionWith IS.union (R.singleton "a/b/y" $ IS.singleton 1) (R.singleton "a/b/x" $ IS.singleton 2)
        @?= R.insertFoldable [("a/b/y", IS.singleton 1), ("a/b/x", IS.singleton 2)] R.empty
    , testCase "structurally equal trees with distinct values"
        $ R.unionWith IS.union (R.singleton "a/b/x" $ IS.singleton 1) (R.singleton "a/b/x" $ IS.singleton 2)
        @?= R.singleton "a/b/x" (IS.fromList [1,2])
    ]
  , testGroup "differenceWith" [  ]
  ]

tree1 :: R.RoutingTree IS.IntSet
tree1
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
