{-# Language BangPatterns, OverloadedStrings #-}

module Fault where

import Control.Applicative ((<$>))
import Control.Monad (forM_, forM)
import Data.Monoid ((<>))
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import qualified Data.Vector.Unboxed as U
import qualified Database.Cassandra.CQL as DB
import Prelude
import qualified Prelude as PL
import System.Random.MWC (uniformR, withSystemRandom)

import Generate
import Reference
import Storage

type Percent = Float
type TimePeriod = (Day, Day)
type Samples = U.Vector (Float, Float)

data Fault
  = Instant ModuleID UTCTime (Percent, Percent) -- instant loss in power output
  | Degradation ModuleID TimePeriod Percent     -- percentual increase in series resistance (NOT IMPLEMENTED)
  deriving (Eq, Show)

applyFault :: Int -> SystemID -> Int -> Fault -> IO ()
applyFault faultID system systemSize fault = getPool >>= \p -> DB.runCas p $ do
  let lastDay = fromGregorian 2014 12 31

  let (firstDay, days) = case fault of
        Instant _ time' _             -> (utctDay time', drop 1 [utctDay time'..lastDay])
        Degradation _ (first', last') _ -> (first', drop 1 [first'..last'])
 
  processFirstDay firstDay >> forM_ days processOtherDay

  DB.executeWrite DB.ONE
    (DB.query $ "insert into "
                <> _tableName faultDescTable
                <> " (fault_id, system, sys_size, module, date, u_factor, i_factor) values (?, ?, ?, ?, ?, ?, ?)")
    (faultID, system, systemSize, addr, UTCTime firstDay 0, voltChange, currChange)

  where
  processFirstDay day = processDay day $ \current voltage -> (processVec current currChange, processVec voltage voltChange)
    where
    processVec vec factor = (U.++) skip (U.map (*factor) process)
      where
      (skip, process) = (U.take skipCount vec, U.drop skipCount vec)

    skipCount      = fromInteger $ quot ((quot secondsDiff 60) - 4*60) interval
    secondsDiff    = round $ utcTimeToPOSIXSeconds time - utcTimeToPOSIXSeconds (UTCTime (utctDay time) 0)

  processOtherDay day = processDay day $ \current voltage -> (U.map (*currChange) current, U.map (*voltChange) voltage)

  processDay day f = do
    let timestamp = UTCTime day 0
    Just (current, voltage) <- DB.executeRow DB.ONE
      (DB.query $ "select current,voltage from "
                  <> _tableName simulationTable
                  <> " where system=? and module=? and date=?")
      (system, addr, timestamp)

    let (current', voltage') = f (U.fromList current) (U.fromList voltage)

    DB.executeWrite DB.ONE
      (DB.query $ "insert into "
                  <> _tableName faultDataTable
                  <> " (fault_id, date, current, voltage) values (?, ?, ?, ?)")
      (faultID, timestamp, U.toList current', U.toList voltage')

  Instant addr time (currChange, voltChange) = fault

generateFaults :: Int -> Integer -> Int -> Int -> IO [Fault]
generateFaults systems year firstFault lastFault = withSystemRandom $ \gen -> do
  let grab lower upper = uniformR (lower, upper) gen
  forM [firstFault..lastFault] $ \faultID -> do
    system <- grab 1 systems

    systemSize <- grab 16 24
    addr <- grab 1 systemSize

    month <- grab 1 12
    day <- grab 1 28
    hour <- toInteger <$> grab 3 (22 :: Int)
    minute <- toInteger . (*5) <$> grab 0 (12 :: Int)

    let timestamp = UTCTime (fromGregorian year month day) $ secondsToDiffTime $ hour*3600 + minute*60

    voltDegrad <- grab 0.5 0.9
    currDegrad <- grab 0.5 0.9
    let fault = Instant addr timestamp (currDegrad, voltDegrad)

    putStrLn $ "Applying fault: " <> show fault

    applyFault faultID system systemSize fault
    return fault
