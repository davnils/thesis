{-# Language OverloadedStrings #-}

module Classify where

import Control.Applicative ((<$>))
import Control.Arrow(first, (***))
import Control.Monad (forM_, forM, foldM, liftM2, when)
import Control.Monad.State (get, modify, put, runState)
import Data.Maybe (mapMaybe, fromMaybe)
import Data.Monoid ((<>))
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Format (parseTime)
import Data.Text hiding (filter, length, map, take, break, foldl')
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Unboxed.Mutable as UM
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

-- | Window size used by sliding average
windowSize  = 16

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

average vec = (1 / (realToFrac windowSize)) * U.sum vec

-- Function which generates a smoothed data series from samples
{-applyMovingAverage :: Series -> Series
applyMovingAverage vec = U.generate (U.length vec) average
  where
  average idx = (1 / windowSize') * realToFrac (U.sum $ U.slice idx windowSize padded)

  padded      = U.replicate (windowSize - 1) 0.0 <> vec
  windowSize' = realToFrac windowSize-}

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

  smooth f       = V.map (processPanel f) (V.enumFromTo 1 numPanels)
  processPanel f = extractScaledModule (V.map f day')

mean :: U.Vector Float -> Float
mean vec = U.sum vec / (realToFrac $ U.length vec)

-- this won't work since power isn't defined over an isolated vector
-- i.e. the predicate needs to have access to the corresponding power vector
consec :: U.Unbox a => (Int -> Bool) -> V.Vector (U.Vector a) -> [V.Vector (U.Vector a)]
consec pred vec = go $ U.enumFromN 0 (U.length $ vec ! 0)
  where
  go indices
    | U.null indices || U.null this = []
    | otherwise                     = V.map (U.slice idx (U.length this)) vec : go rest
    where
    idx          = this ! 0
    (this, rest) = U.span pred ff
    (_, ff)      = U.break pred indices

-- | Returns with ([], Nothing) if no suitable window is found nor provided
checkDay :: Series -> SystemSSerie -> Int -> Maybe Window -> Float -> ([FaultDescription], Maybe Window)
checkDay power samples len window threshold = flip runState window $ do
  get >>= \w -> when (w == Nothing) initReference

  -- process each segment
  fmap PL.concat . forM grouped $ \segment ->
    -- process each panel
    fmap (PL.concat . V.toList) . V.forM (V.imap (,) segment) $ \(panel, panelSegment) -> do
      let (indices, panelSegment') = U.unzip panelSegment
      -- process every sample for this continous sequence of values
      fmap PL.concat . forM [0..U.length panelSegment'] $ \idx -> do
        Just ref <- get

        let upperIndex = min (idx + windowSize - 1) (len - 1)
            lowerIndex = max (idx - windowSize) 0
            extract l u = average $ U.slice l u panelSegment'

            sample      = extract idx        (upperIndex - idx + 1)
            ref'        = extract lowerIndex (idx - lowerIndex)

        if (abs (ref ! panel - sample) <= threshold) then do
              modify $ \(Just v) -> Just $ U.modify (\v' -> UM.write v' panel ref') v
              return [(panel, indices ! idx)]
            else
              return []

  where
  grouped       = consec (\idx -> power ! idx >= powerLimit) (V.map (U.imap (,)) samples)
  initReference = when (not $ PL.null grouped) $ put (Just . convert . V.map (snd . U.head) . PL.head $ grouped)

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
