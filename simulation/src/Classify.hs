{-# Language OverloadedStrings #-}

module Classify where

import Control.Arrow(first, (***))
import Control.Monad (forM_, forM, foldM)
import Control.Monad.State (get, put, runState)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Monoid ((<>))
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Format (parseTime)
import Data.Text hiding (filter, length, map, take, break, foldl')
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import qualified Database.Cassandra.CQL as DB
import Data.Vector.Generic (convert, fromList, toList, (!), (!?))
import Prelude
import qualified Prelude as PL
import System.Environment (getArgs)
import System.IO (hPutStr, hPutStrLn, IOMode(..), withFile)
import System.Locale (defaultTimeLocale)

import Reference
import Storage

type Fault = Int                             -- ^ TODO
type Series = U.Vector Float                 -- ^ Vector of measurements.
type Window = U.Vector Float                 -- ^ Indexed by panel, single measurement.
type SystemSerie = V.Vector (Series, Series) -- ^ Indexed by panel, vectors for voltage and current.
type SystemSSerie = V.Vector Series          -- ^ Indexed by panel, vectors for a single measurement.


-- TODO LIST
-- (1) verify extractScaledModule
-- (2) study other routines
-- (3) extend code with classification rates

-- | Limit in Watt for all samples surveyed
powerLimit = 20.0

-- | Thresholds in consecutive voltage and current samples that imply faults
voltageThreshold, currentThreshold :: Float
-- (voltageThreshold, currentThreshold) = (0.01, 0.01)
(voltageThreshold, currentThreshold) = (0.3, 0.6)

-- for a given module, define a function which builds the day-vector containing scaled measurements
extractScaledModule :: SystemSSerie -> ModuleID -> Series
extractScaledModule daily panel = U.generate numSamples buildSample
 where
 numSamples = U.length $ daily ! 0

 buildSample idx = ((getSample $ panel - 1) - mean) / std
   where
   getSample addr = daily ! addr ! idx
   mean           = U.sum otherSamples / otherLength
   std            = sqrt $ (1/otherLength) * (U.sum $ U.map (`subtract` mean) otherSamples)
   otherLength    = realToFrac $ U.length otherSamples
   otherSamples   = U.map getSample $ U.fromList $ [0..panel-2] <> [panel..V.length daily-1]

-- Function which generates a smoothed data series from samples
applyMovingAverage :: Series -> Series
applyMovingAverage vec = U.generate (U.length vec) average
  where
  average idx = (1 / windowSize') * (realToFrac $ U.sum $ U.slice idx windowSize padded)

  padded = U.replicate windowSize 0.0 <> vec
  windowSize = 16
  windowSize' = realToFrac windowSize

twice f = (***) f f

-- Forward a series of samples until the predicate is satisfied (predicate over power)
forwardUntil :: SystemSerie -> (Float -> Bool) -> SystemSerie
forwardUntil series pred = V.map (twice $ U.drop ignoreCount) series
  where
  ignoreCount = go 0

  go idx
    | idx == seriesLength    = idx
    | pred folded            = idx
    | otherwise              = go (idx + 1)
    where
    seriesLength = U.length . fst $ V.head series
    folded = V.foldl' (\p (u,i) -> p + (u ! idx) * (i ! idx)) 0.0 series

-- * Functionality for retrieving an initial reference window
--   -> simply traverse system values and save first average with >= powerLimit W sum
--   note that the current implementation does not perform smoothing.
getReferenceWindow :: SystemSerie -> ((Series, Series) -> Series) -> Maybe Window
getReferenceWindow series pred = fmap convert . V.sequence . V.map getFirst $ forwardUntil series minPredicate
  where
  minPredicate               = (>= powerLimit)
  getFirst                   = (!? 0) . pred

-- * Processing of a single day given a reference
--   -> traverse all windows and compare first non-negligible with the given reference

type FaultDescription = (ModuleID, Int)

data FaultType = VoltageFault | CurrentFault deriving (Eq, Show)
type LongFaultDescription a = (FaultType, ModuleID, a)

checkDayBoth :: SystemSerie -> (Window, Window) -> ([LongFaultDescription Int], (Window, Window))
checkDayBoth day (voltWindow, currWindow) = (faults, (voltWindow', currWindow'))
  where
  faults = tag VoltageFault voltFaults <> tag CurrentFault currFaults
  (voltFaults, voltWindow') = checkDay power smoothedVolt len voltWindow voltageThreshold
  (currFaults, currWindow') = checkDay power smoothedCurr len currWindow currentThreshold
  tag kind = map $ \(addr, idx) -> (kind, addr, idx)

  -- FF all small samples
  day' = forwardUntil day (>= powerLimit)

  -- each sample in the result corresponds to one time slot
  power = U.map (\idx -> V.sum $ V.map (\(v1, v2) -> (v1 ! idx) * (v2 ! idx)) day') $ U.enumFromN 0 len
  len = U.length . fst $ V.head day'
  numPanels = V.length day'

  smoothedVolt = smooth fst
  smoothedCurr = smooth snd

  smooth f = V.map applyMovingAverage $ V.map (processPanel f) (V.enumFromTo 1 numPanels)
  processPanel f panel = extractScaledModule (V.map f day') panel

checkDay :: Series -> SystemSSerie -> Int -> Window -> Float -> ([FaultDescription], Window)
checkDay power smoothed len window threshold = flip runState window . fmap PL.concat . forM [0.. len-1] $ \idx -> do
  window <- get
  let window' = toList $ V.map (! idx) smoothed
      indicators = map (\(val, prev) -> abs (val - prev) >= threshold) $ PL.zip window' (toList window)

      checkFault (addr, True) = Just (addr, idx)
      checkFault (_, False)   = Nothing
      faults = catMaybes . map checkFault $ PL.zip [1..] indicators 

  case power ! idx >= powerThreshold of
    True -> do
      put $ fromList window'
      return faults
    False -> return []

  where
  powerThreshold = powerLimit

-- Wrapper iterating over all days and returns date/module of fault
checkTimePeriod :: Integer -> Maybe Int -> IO [LongFaultDescription (Day, Int)]
checkTimePeriod year daysCount = do
  pool <- getPool

  -- note: this might not suffice if the first day does not have significant irradiance
  firstValues <- retrieveDayValues pool system modules firstDay
  let Just voltWindow = getReferenceWindow firstValues fst
      Just currWindow = getReferenceWindow firstValues snd

  let processDay (prevFaults, windows) day = do
      putStrLn $ "Checking day: " <> show day
      daily <- retrieveDayValues pool system modules day
      let (newFaults, windows') = checkDayBoth daily windows
          taggedNew = map (\(kind, addr, idx) -> (kind, addr, (day, idx))) newFaults
      return $ (taggedNew <> prevFaults, windows')

  fmap fst $ foldM processDay ([], (voltWindow, currWindow)) days

  where
  system     = 1
  modules    = 24
  firstDay   = fromGregorian year 1 1
  days       = PL.take daysCount' $ [firstDay..]
  daysCount' = fromMaybe 365 daysCount

retrieveDayValues :: DB.Pool -> SystemID -> Int -> Day -> IO SystemSerie
retrieveDayValues pool system modules day = do
  rows <- DB.runCas pool $ DB.executeRows DB.ALL fetchRows (system, UTCTime day 0, modules)
  return . V.fromList $ PL.map (\(l, m) -> (U.fromList l, U.fromList m)) rows
  where
  fetchRows = DB.query $
    "select voltage,current from "
    <> _tableName simulationTable
    <> " where system=? and date=? and module <= ?"
