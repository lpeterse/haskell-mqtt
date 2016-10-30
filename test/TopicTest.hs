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
tests = testGroup "TopicFilter"
  [ testGroup "Topic"
    [ testGroup "show / fromString"
      [ testCase "\"/\""          $ assertEqual "" "\"/\""      $ show ("/"      :: Topic)
      , testCase "\"\x2603\""     $ assertEqual "" "\"\x2603\"" $ show ("\x2603" :: Topic)
      , testCase "\"a/b/c\""      $ assertEqual "" "\"a/b/c\""  $ show ("a/b/c"  :: Topic)
      ]
    , testGroup "parseTopic"
      [ testCase "! \"\""         $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly parseTopic ""
      , testCase "! \"\\NUL\""    $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly parseTopic "\NUL"
      , testCase "! \"+\""        $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly parseTopic "+"
      , testCase "! \"#\""        $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly parseTopic "#"
      , testCase "  \"/\""        $ assertEqual "" (Right $ "":|[""])                     $ topicLevels <$> A.parseOnly parseTopic "/"
      , testCase "  \"//\""       $ assertEqual "" (Right $ "":|["",""])                  $ topicLevels <$> A.parseOnly parseTopic "//"
      , testCase "  \"/a\""       $ assertEqual "" (Right $ "":|["a"])                    $ topicLevels <$> A.parseOnly parseTopic "/a"
      , testCase "  \"a\""        $ assertEqual "" (Right $ "a":|[])                      $ topicLevels <$> A.parseOnly parseTopic "a"
      , testCase "  \"a/\""       $ assertEqual "" (Right $ "a":|[""])                    $ topicLevels <$> A.parseOnly parseTopic "a/"
      , testCase "  \"a/bcd\""    $ assertEqual "" (Right $ "a":|["bcd"])                 $ topicLevels <$> A.parseOnly parseTopic "a/bcd"
      , testCase "! \"a/b+d\""    $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly parseTopic "a/b+d"
      , testCase "! \"a/b#d\""    $ assertEqual "" (Left "Failed reading: invalid topic") $ topicLevels <$> A.parseOnly parseTopic "a/b#d"
      ]
    ]
  , testGroup "TopicFilter"
    [ testGroup "show / fromString"
      [ testCase "\"/\""          $ assertEqual "" "\"/\""      $ show ("/"      :: TopicFilter)
      , testCase "\"\x2603\""     $ assertEqual "" "\"\x2603\"" $ show ("\x2603" :: TopicFilter)
      , testCase "\"a/b/c\""      $ assertEqual "" "\"a/b/c\""  $ show ("a/b/c"  :: TopicFilter)
      , testCase "\"#\""          $ assertEqual "" "\"#\""      $ show ("#"      :: TopicFilter)
      , testCase "\"+\""          $ assertEqual "" "\"+\""      $ show ("+"      :: TopicFilter)
      , testCase "\"a/#\""        $ assertEqual "" "\"a/#\""    $ show ("a/#"    :: TopicFilter)
      , testCase "\"+/+/#\""      $ assertEqual "" "\"+/+/#\""  $ show ("+/+/#"  :: TopicFilter)
      , testCase "\"/#\""         $ assertEqual "" "\"/#\""     $ show ("/#"     :: TopicFilter)
      ]
    , testGroup "parseTopicFilter"
      [ testCase "! \"\""             $ assertEqual "" (Left "Failed reading: invalid filter")  $ topicFilterLevels <$> A.parseOnly parseTopicFilter ""
      , testCase "! \"\\NUL\""        $ assertEqual "" (Left "Failed reading: invalid filter")  $ topicFilterLevels <$> A.parseOnly parseTopicFilter "\NUL"
      , testCase "  \"+\""            $ assertEqual "" (Right $ "+":|[])                        $ topicFilterLevels <$> A.parseOnly parseTopicFilter "+"
      , testCase "  \"#\""            $ assertEqual "" (Right $ "#":|[])                        $ topicFilterLevels <$> A.parseOnly parseTopicFilter "#"
      , testCase "! \"#/\""           $ assertEqual "" (Left "Failed reading: invalid filter")  $ topicFilterLevels <$> A.parseOnly parseTopicFilter "#/"
      , testCase "! \"a/a+b\""        $ assertEqual "" (Left "Failed reading: invalid filter")  $ topicFilterLevels <$> A.parseOnly parseTopicFilter "a/a+b"
      , testCase "! \"a/a#b\""        $ assertEqual "" (Left "Failed reading: invalid filter")  $ topicFilterLevels <$> A.parseOnly parseTopicFilter "a/a#b"
      , testCase "! \"#a\""           $ assertEqual "" (Left "Failed reading: invalid filter")  $ topicFilterLevels <$> A.parseOnly parseTopicFilter "#a"
      , testCase "  \"/\""            $ assertEqual "" (Right $ "":|[""])                       $ topicFilterLevels <$> A.parseOnly parseTopicFilter "/"
      , testCase "  \"//\""           $ assertEqual "" (Right $ "":|["",""])                    $ topicFilterLevels <$> A.parseOnly parseTopicFilter "//"
      , testCase "  \"/a\""           $ assertEqual "" (Right $ "":|["a"])                      $ topicFilterLevels <$> A.parseOnly parseTopicFilter "/a"
      , testCase "  \"a\""            $ assertEqual "" (Right $ "a":|[])                        $ topicFilterLevels <$> A.parseOnly parseTopicFilter "a"
      , testCase "  \"a/\""           $ assertEqual "" (Right $ "a":|[""])                      $ topicFilterLevels <$> A.parseOnly parseTopicFilter "a/"
      , testCase "  \"a/b\""          $ assertEqual "" (Right $ "a":|["b"])                     $ topicFilterLevels <$> A.parseOnly parseTopicFilter "a/b"
      , testCase "  \"a/+/c123/#\""   $ assertEqual "" (Right $ "a":|["+","c123","#"])          $ topicFilterLevels <$> A.parseOnly parseTopicFilter "a/+/c123/#"
      , testCase "! \"a/+/c123/#/d\"" $ assertEqual "" (Left "Failed reading: invalid filter")  $ topicFilterLevels <$> A.parseOnly parseTopicFilter "a/+/c123/#/d"
      ]
    ]
  ]
