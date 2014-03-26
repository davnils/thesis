{-# Language OverloadedStrings #-}

module Classify where

import Control.Applicative ((<$>))
import Control.Arrow(first, (***))
import Control.Monad (forM_, forM, foldM, liftM2, when)
import Control.Monad.State (get, put, runState)
import Data.Maybe (mapMaybe, fromMaybe)
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
import System.IO.Unsafe (unsafePerformIO)
import System.Locale (defaultTimeLocale)

import Reference
import Storage

type Series = U.Vector Float                 -- ^ Vector of measurements.
type Window = U.Vector Float                 -- ^ Indexed by panel, single measurement.
type SystemSerie = V.Vector (Series, Series) -- ^ Indexed by panel, vectors for voltage and current.
type SystemSSerie = V.Vector Series          -- ^ Indexed by panel, vectors for a single measurement.

-- | Limit in Watt for all samples surveyed
powerLimit = 1.0

-- | Thresholds in consecutive voltage and current samples that imply faults
voltageThreshold, currentThreshold :: Float
(voltageThreshold, currentThreshold) = (0.3, 0.6)

-- for a given module, define a function which builds the day-vector containing scaled measurements
extractScaledModule :: SystemSSerie -> ModuleID -> Series
extractScaledModule daily panel = U.generate numSamples buildSample
 where
 numSamples = U.length $ daily ! 0

 buildSample idx = normalize $ getSample (panel - 1) - mean
   where
   getSample addr = daily ! addr ! idx
   mean           = U.sum otherSamples / otherLength
   normalize s    = if std /= 0.0 then s / std else s
   std            = sqrt $ (1/otherLength) * U.sum (U.map (\s -> (s - mean)^2) otherSamples)
   otherLength    = realToFrac $ U.length otherSamples
   otherSamples   = U.map getSample $ fromList $ [0..panel-2] <> [panel..V.length daily-1]

-- Function which generates a smoothed data series from samples
applyMovingAverage :: Series -> Series
applyMovingAverage vec = U.generate (U.length vec) average
  where
  average idx = (1 / windowSize') * realToFrac (U.sum $ U.slice idx windowSize padded)

  padded      = U.replicate (windowSize - 1) 0.0 <> vec
  windowSize  = 16
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

type FaultDescription = (ModuleID, Int)

data FaultType
  = VoltageFault
  | CurrentFault
  deriving (Eq, Show)

type LongFaultDescription a = (FaultType, ModuleID, a)

joinM :: Monad m => (m a1, m a2) -> m (a1, a2)
joinM = uncurry $ liftM2 (,)

checkDayBoth :: SystemSerie -> Maybe (Window, Window) -> ([LongFaultDescription Int], Maybe (Window, Window))
checkDayBoth day windows = (faults, windows')
  where
  faults                    = tag VoltageFault voltFaults <> tag CurrentFault currFaults
  windows'                  = joinM (voltWindow', currWindow')
  (voltFaults, voltWindow') = checkDay power smoothedVolt len (fmap fst windows) voltageThreshold
  (currFaults, currWindow') = checkDay power smoothedCurr len (fmap snd windows) currentThreshold
  tag kind                  = map $ \(addr, idx) -> (kind, addr, idx)

  -- FF all small samples
  day'         = forwardUntil day (>= powerLimit)

  -- each sample in the result corresponds to one time slot
  power        = U.map (\idx -> V.sum $ V.map (\(v1, v2) -> (v1 ! idx) * (v2 ! idx)) day') $ U.enumFromN 0 len
  len          = U.length . fst $ V.head day'
  numPanels    = V.length day'

  smoothedVolt = smooth fst
  smoothedCurr = smooth snd

  smooth f       = V.map applyMovingAverage $ V.map (processPanel f) (V.enumFromTo 1 numPanels)
  processPanel f = extractScaledModule (V.map f day')

debug action val = unsafePerformIO $ action >> return val

-- | Returns with ([], Nothing) if no suitable window is found nor provided
checkDay :: Series -> SystemSSerie -> Int -> Maybe Window -> Float -> ([FaultDescription], Maybe Window)
checkDay power smoothed len window threshold = flip runState window . fmap PL.concat . forM [0.. len-1] $ \idx -> do
  window <- get

  -- update window if there is some power available
  let window' = toList $ V.map (! idx) smoothed
  let enoughPower = power ! idx >= powerLimit
  when enoughPower $ put (Just $ fromList window')

  -- classify the current window if there is some power available
  case (enoughPower, window) of
    (True, Just window_) -> do
      let indicators = map (\(val, prev) -> abs (val - prev) >= threshold) $ PL.zip window' (toList window_)
          checkFault (addr, True) = Just (addr, idx)
          checkFault (_, False)   = Nothing
          faults = mapMaybe checkFault $ PL.zip [1..] indicators
      return faults
    _                    -> return []

-- Wrapper iterating over all days and returns date/module of fault
checkTimePeriod :: Integer -> Maybe Int -> IO [LongFaultDescription (Day, Int)]
checkTimePeriod year daysCount = do
  pool <- getPool

  let processDay (prevFaults, windows) day = do
      putStrLn $ "Checking day: " <> show day
      print ">>>>>>>>>>> First windows ever"
      print windows
      daily <- retrieveDayValues pool system modules day
      let (newFaults, windows') = checkDayBoth daily Nothing -- windows
          taggedNew = map (\(kind, addr, idx) -> (kind, addr, (day, idx))) newFaults
      print ">>>>>>>>>>> Last windows ever"
      print windows'
      return (taggedNew <> prevFaults, windows')

  fst <$> foldM processDay ([], Nothing) days

  where
  system     = 1
  modules    = 24
  firstDay   = fromGregorian year 1 1
  days       = PL.take daysCount' [firstDay..]
  daysCount' = fromMaybe 365 daysCount

retrieveDayValues :: DB.Pool -> SystemID -> Int -> Day -> IO SystemSerie
retrieveDayValues pool system modules day = do
  rows <- DB.runCas pool $ DB.executeRows DB.ALL fetchRows (system, UTCTime day 0, modules)
  return . V.fromList $ PL.map (fromList *** fromList) rows
  where
  fetchRows = DB.query $
    "select voltage,current from "
    <> _tableName simulationTable
    <> " where system=? and date=? and module <= ?"
