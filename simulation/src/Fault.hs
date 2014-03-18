{-# Language BangPatterns, OverloadedStrings #-}

module Fault where

import Control.Applicative ((<$>))
import Control.Monad (forM_)
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

applyFault :: SystemID -> Fault -> IO ()
applyFault system fault = getPool >>= \p -> DB.runCas p $ do
  let lastDay = fromGregorian 2014 12 31

  -- TODO: cleanup code
  let (firstDay, days) = case fault of
        Instant _ time' _             -> (utctDay time', drop 1 [utctDay time'..lastDay])
        Degradation _ (first', last') _ -> (first', drop 1 [first'..last'])
 
  processFirstDay firstDay >> forM_ days processOtherDay
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
                  <> _tableName simulationTable
                  <> " (system, module, date, current, voltage) values (?, ?, ?, ?, ?)")
      (system, addr, timestamp, U.toList current', U.toList voltage')

  Instant addr time (currChange, voltChange) = fault

generateFaults :: SystemID -> Integer -> Int -> IO ()
generateFaults system year faultCount = withSystemRandom $ \gen -> do
  let grab lower upper = uniformR (lower, upper) gen
  forM_ [1..faultCount] $ \_ -> do
    addr <- grab 1 24
    month <- grab 1 12
    day  <- grab 1 28
    hour <- toInteger <$> grab 9 (16 :: Int) -- 3 and 22 previously
    minute <- toInteger . (*5) <$> grab 0 (12 :: Int)
    let timestamp = UTCTime (fromGregorian year month day) $ secondsToDiffTime $ hour*3600 + minute*60

    voltDegrad <- grab 0.5 0.9
    currDegrad <- grab 0.5 0.9
    let fault = Instant addr timestamp (currDegrad, voltDegrad)

    putStrLn $ "Applying fault: " <> show fault

    applyFault system fault
