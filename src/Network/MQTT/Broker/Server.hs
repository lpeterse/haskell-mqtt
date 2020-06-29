{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NumericUnderscores  #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeFamilies        #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Network.MQTT.Broker.Server
-- Copyright   :  (c) Lars Petersen 2016
-- License     :  MIT
--
-- Maintainer  :  info@lars-petersen.net
-- Stability   :  experimental
--------------------------------------------------------------------------------
module Network.MQTT.Broker.Server
  ( serveConnection
  , MqttServerTransportStack (..)
  ) where

import           Control.Concurrent
import           Control.Concurrent.Async
import qualified Control.Exception                     as E
import           Control.Monad
import qualified Data.Binary.Get                       as SG
import qualified Data.ByteString                       as BS
import           Data.Int
import           Data.IORef
import qualified Data.Sequence                         as Seq
import           Data.Typeable
import qualified Networking                            as SS

import qualified Network.MQTT.Broker                   as Broker
import           Network.MQTT.Broker.Authentication
import qualified Network.MQTT.Broker.Internal          as Session
import qualified Network.MQTT.Broker.Session           as Session
import qualified Network.MQTT.Broker.Session.Statistic as Session
import           Network.MQTT.Message

data MqttServerException
  = ProtocolViolation String
  | MessageTooLong
  | ConnectionRejected RejectReason
  | KeepAliveTimeoutException
  deriving (Eq, Ord, Show, Typeable)

instance E.Exception MqttServerException

class MqttServerTransportStack a where
  getConnectionRequest :: SS.ConnectionInfo a -> IO ConnectionRequest

sendMessage  :: SS.StreamOriented t => SS.Connection t -> ServerPacket -> IO Int64
sendMessage connection = do
  SS.sendChunkBuilder connection 8192 . serverPacketBuilder

sendMessages :: SS.StreamOriented t => SS.Connection t -> Seq.Seq ServerPacket -> IO Int64
sendMessages connection msgs = do
  SS.sendChunkBuilder connection 8192 $ foldl (\b m-> b `mappend` serverPacketBuilder m) mempty msgs

receiveMessage :: SS.StreamOriented t => SS.Connection t -> Int64 -> MVar BS.ByteString -> IO ClientPacket
receiveMessage connection maxMsgSize leftover =
  modifyMVar leftover (execute 0 . SG.pushChunk decode)
  where
    fetch  = SS.receiveChunk connection 4096
    decode = SG.runGetIncremental clientPacketParser
    execute received result
      | received > maxMsgSize = E.throwIO MessageTooLong
      | otherwise = case result of
          SG.Partial continuation -> do
            bs <- fetch
            if BS.null bs
              then execute received (continuation Nothing)
              else execute (received + fromIntegral (BS.length bs)) (continuation $ Just bs)
          SG.Fail _ _ failure ->
            E.throwIO (ProtocolViolation failure)
          SG.Done leftover' _ msg ->
            pure (leftover', msg)

consumeMessages :: SS.StreamOriented t => SS.Connection t -> Int64 -> MVar BS.ByteString -> (ClientPacket -> IO Bool) -> IO ()
consumeMessages connection maxMsgSize leftover consume =
  modifyMVar_ leftover (execute 0 . SG.pushChunk decode)
  where
    fetch  = SS.receiveChunk connection 4096
    decode = SG.runGetIncremental clientPacketParser
    execute received result
      | received > maxMsgSize = E.throwIO MessageTooLong
      | otherwise = case result of
          SG.Partial continuation -> do
            bs <- fetch
            if BS.null bs
              then execute received (continuation Nothing)
              else execute (received + fromIntegral (BS.length bs)) (continuation $ Just bs)
          SG.Fail _ _ failure ->
            E.throwIO (ProtocolViolation failure)
          SG.Done leftover' _ msg -> do
            done <- consume msg
            if done
              then pure leftover'
              else execute 0 (SG.pushChunk decode leftover')

serveConnection :: forall transport auth. (SS.StreamOriented transport, MqttServerTransportStack transport, Authenticator auth) => Broker.Broker auth -> SS.Connection transport -> SS.ConnectionInfo transport -> IO ()
serveConnection broker conn info = do
  leftover       <- newMVar mempty
  recentActivity <- newIORef True
  req            <- getConnectionRequest info
  msg            <- withInitialPacketTimeout $ receiveMessage conn maxInitialPacketSize leftover
  case msg of
    ClientConnectUnsupported -> do
      Broker.onConnectionRejected cbs req UnacceptableProtocolVersion
      void $ sendMessage conn (ServerConnectionRejected UnacceptableProtocolVersion)
      -- Communication ends here gracefully. The caller shall close the connection.
    ClientConnect {} -> do
      let -- | This one is called when the authenticator decided to reject the request.
          sessionRejectHandler reason = do
            Broker.onConnectionRejected cbs request reason
            void $ sendMessage conn (ServerConnectionRejected reason)
            -- Communication ends here gracefully. The caller shall close the connection.

          -- | This part is where the threads for a connection are created
          --   (one for input, one for output and one watchdog thread).
          sessionAcceptHandler session sessionPresent = do
            Broker.onConnectionAccepted cbs request session
            void $ sendMessage conn (ServerConnectionAccepted sessionPresent)
            foldl1 race_
              [ handleInput recentActivity leftover session
              , handleOutput session
              , keepAlive recentActivity (connectKeepAlive msg)
              ] `E.catch` (\e-> do
                Broker.onConnectionFailed cbs session e
                E.throwIO e
              )
            Broker.onConnectionClosed cbs session
          -- Extend the request object with information gathered from the connect packet.
          request = req {
              requestClientIdentifier = connectClientIdentifier msg
            , requestCleanSession     = cleanSession
            , requestCredentials      = connectCredentials msg
            , requestWill             = connectWill msg
            }
            where
              CleanSession cleanSession = connectCleanSession msg
      Broker.withSession broker request sessionRejectHandler sessionAcceptHandler
    _ -> pure () -- TODO: Don't parse not-CONN packets in the first place!
  where
    cbs = Session.brokerCallbacks broker

    -- The size of the initial CONN packet shall somewhat be limited to a moderate size.
    -- This value is assumed to make no problems while still protecting
    -- the servers resources against exaustion attacks.
    maxInitialPacketSize :: Int64
    maxInitialPacketSize = 65_535

    -- The time to wait for the first packet to arrive.
    withInitialPacketTimeout :: IO a -> IO a
    withInitialPacketTimeout action = race timer action >>= \case
      Left _ -> E.throwIO KeepAliveTimeoutException
      Right a -> pure a
      where
        timer = threadDelay 10_000_000

    -- The keep alive thread wakes up every `keepAlive/2` seconds.
    -- When it detects no recent activity, it sleeps one more full `keepAlive`
    -- interval and checks again. When it still finds no recent activity, it
    -- throws an exception.
    -- That way a timeout will be detected between 1.5 and 2 `keep alive`
    -- intervals after the last actual client activity.
    keepAlive :: IORef Bool -> KeepAliveInterval -> IO ()
    keepAlive recentActivity (KeepAliveInterval interval) = forever $ do
      writeIORef recentActivity False
      threadDelay regularInterval
      activity <- readIORef recentActivity
      unless activity $ do
        threadDelay regularInterval
        activity' <- readIORef recentActivity
        unless activity' $ do
          threadDelay regularInterval
          activity'' <- readIORef recentActivity
          unless activity'' $ E.throwIO KeepAliveTimeoutException
      where
        regularInterval = fromIntegral interval *  500_000

    -- | This thread is responsible for continuously processing input.
    --   It blocks on reading the input stream until input becomes available.
    --   Input is consumed no faster than it can be processed.
    --
    --   * Read packet from the input stream.
    --   * Note that there was activity (TODO: a timeout may occur when a packet
    --     takes too long to transmit due to its size).
    --   * Process and dispatch the message internally.
    --   * Repeat and eventually wait again.
    --   * Eventually throws `ServerException`s.
    handleInput :: IORef Bool -> MVar BS.ByteString -> Session.Session auth -> IO ()
    handleInput recentActivity leftover session = do
      maxPacketSize <- fromIntegral . quotaMaxPacketSize . principalQuota <$> Session.getPrincipal session
      consumeMessages conn maxPacketSize leftover $ \packet-> do
        writeIORef recentActivity True
        Session.accountPacketsReceived (Session.sessionStatistic session) 1
        case packet of
          ClientDisconnect ->
            pure True
          ClientConnect {} ->
            E.throwIO $ ProtocolViolation "Unexpected CONN packet."
          ClientConnectUnsupported ->
            E.throwIO $ ProtocolViolation "Unexpected CONN packet (of unsupported protocol version)."
          _ -> Session.process session packet >> pure False

    -- | This thread is responsible for continuously transmitting to the client
    --   and reading from the output queue.
    --
    --   * It blocks on Session.waitPending until output gets available.
    --   * When output is available, it fetches a whole sequence of messages
    --     from the output queue.
    --   * It then uses the optimized SS.sendMessages operation which fills
    --     a whole chunk with as many messages as possible and sends the chunks
    --     each with a single system call. This is _very_ important for high
    --     throughput.
    --   * Afterwards, it repeats and eventually waits again.
    handleOutput :: Session.Session auth -> IO ()
    handleOutput session = forever $ do
      -- The `waitPending` operation is blocking until messages get available.
      Session.wait session
      msgs <- Session.poll session
      void $ sendMessages conn msgs
      Session.accountPacketsSent (Session.sessionStatistic session) (fromIntegral $ Seq.length msgs)
