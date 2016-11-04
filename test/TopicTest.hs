{-# LANGUAGE OverloadedStrings #-}
module TopicTest ( tests ) where

import qualified Data.Attoparsec.ByteString as A
import qualified Data.ByteString.Short      as BS
import           Data.List.NonEmpty         (NonEmpty (..))
import           Data.Monoid
import qualified Data.Text                  as T
import           Network.MQTT.Topic
import           Test.Tasty
import           Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "Topic / Filter / Level"
  [ testGroup "Topic"
    [ testGroup "show / fromString"
      [ testCase "\"/\""          $ assertEqual "" "\"/\""      $ show ("/"      :: Topic)
      , testCase "\"\x2603\""     $ assertEqual "" "\"\x2603\"" $ show ("\x2603" :: Topic)
      , testCase "\"a/b/c\""      $ assertEqual "" "\"a/b/c\""  $ show ("a/b/c"  :: Topic)
      ]
    , testGroup "topicParser"
      [ testCase "! \"\""         $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly topicParser ""
      , testCase "! \"\\NUL\""    $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly topicParser "\NUL"
      , testCase "! \"+\""        $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly topicParser "+"
      , testCase "! \"#\""        $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly topicParser "#"
      , testCase "  \"/\""        $ assertEqual "" (Right $ "":|[""])                     $ topicLevels <$> A.parseOnly topicParser "/"
      , testCase "  \"//\""       $ assertEqual "" (Right $ "":|["",""])                  $ topicLevels <$> A.parseOnly topicParser "//"
      , testCase "  \"/a\""       $ assertEqual "" (Right $ "":|["a"])                    $ topicLevels <$> A.parseOnly topicParser "/a"
      , testCase "  \"a\""        $ assertEqual "" (Right $ "a":|[])                      $ topicLevels <$> A.parseOnly topicParser "a"
      , testCase "  \"a/\""       $ assertEqual "" (Right $ "a":|[""])                    $ topicLevels <$> A.parseOnly topicParser "a/"
      , testCase "  \"a/bcd\""    $ assertEqual "" (Right $ "a":|["bcd"])                 $ topicLevels <$> A.parseOnly topicParser "a/bcd"
      , testCase "! \"a/b+d\""    $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly topicParser "a/b+d"
      , testCase "! \"a/b#d\""    $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly topicParser "a/b#d"
      ]
    ]
  , testGroup "Filter"
    [ testGroup "show / fromString"
      [ testCase "\"/\""          $ assertEqual "" "\"/\""      $ show ("/"      :: Filter)
      , testCase "\"\x2603\""     $ assertEqual "" "\"\x2603\"" $ show ("\x2603" :: Filter)
      , testCase "\"a/b/c\""      $ assertEqual "" "\"a/b/c\""  $ show ("a/b/c"  :: Filter)
      , testCase "\"#\""          $ assertEqual "" "\"#\""      $ show ("#"      :: Filter)
      , testCase "\"+\""          $ assertEqual "" "\"+\""      $ show ("+"      :: Filter)
      , testCase "\"a/#\""        $ assertEqual "" "\"a/#\""    $ show ("a/#"    :: Filter)
      , testCase "\"+/+/#\""      $ assertEqual "" "\"+/+/#\""  $ show ("+/+/#"  :: Filter)
      , testCase "\"/#\""         $ assertEqual "" "\"/#\""     $ show ("/#"     :: Filter)
      ]
    , testGroup "filterParser"
      [ testCase "! \"\""             $ assertEqual "" (Left "Failed reading: invalid filter")  $ filterLevels <$> A.parseOnly filterParser ""
      , testCase "! \"\\NUL\""        $ assertEqual "" (Left "Failed reading: invalid filter")  $ filterLevels <$> A.parseOnly filterParser "\NUL"
      , testCase "  \"+\""            $ assertEqual "" (Right $ "+":|[])                        $ filterLevels <$> A.parseOnly filterParser "+"
      , testCase "  \"#\""            $ assertEqual "" (Right $ "#":|[])                        $ filterLevels <$> A.parseOnly filterParser "#"
      , testCase "! \"#/\""           $ assertEqual "" (Left "Failed reading: invalid filter")  $ filterLevels <$> A.parseOnly filterParser "#/"
      , testCase "! \"a/a+b\""        $ assertEqual "" (Left "Failed reading: invalid filter")  $ filterLevels <$> A.parseOnly filterParser "a/a+b"
      , testCase "! \"a/a#b\""        $ assertEqual "" (Left "Failed reading: invalid filter")  $ filterLevels <$> A.parseOnly filterParser "a/a#b"
      , testCase "! \"#a\""           $ assertEqual "" (Left "Failed reading: invalid filter")  $ filterLevels <$> A.parseOnly filterParser "#a"
      , testCase "  \"/\""            $ assertEqual "" (Right $ "":|[""])                       $ filterLevels <$> A.parseOnly filterParser "/"
      , testCase "  \"//\""           $ assertEqual "" (Right $ "":|["",""])                    $ filterLevels <$> A.parseOnly filterParser "//"
      , testCase "  \"/a\""           $ assertEqual "" (Right $ "":|["a"])                      $ filterLevels <$> A.parseOnly filterParser "/a"
      , testCase "  \"a\""            $ assertEqual "" (Right $ "a":|[])                        $ filterLevels <$> A.parseOnly filterParser "a"
      , testCase "  \"a/\""           $ assertEqual "" (Right $ "a":|[""])                      $ filterLevels <$> A.parseOnly filterParser "a/"
      , testCase "  \"a/b\""          $ assertEqual "" (Right $ "a":|["b"])                     $ filterLevels <$> A.parseOnly filterParser "a/b"
      , testCase "  \"a/+/c123/#\""   $ assertEqual "" (Right $ "a":|["+","c123","#"])          $ filterLevels <$> A.parseOnly filterParser "a/+/c123/#"
      , testCase "! \"a/+/c123/#/d\"" $ assertEqual "" (Left "Failed reading: invalid filter")  $ filterLevels <$> A.parseOnly filterParser "a/+/c123/#/d"
      ]
    ]
  ]
