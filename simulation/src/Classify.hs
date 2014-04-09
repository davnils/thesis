{-# Language DeriveGeneric, OverloadedStrings, ScopedTypeVariables #-}

module Classify where

import Control.Applicative ((<$>))
import Control.Arrow(first, (***))
import Control.DeepSeq.Generics (force, NFData)
import Control.Monad (forM_, forM, foldM, liftM2, when)
import Control.Monad.State.Strict(get, modify, put, runState)
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
import GHC.Generics
import Prelude
import qualified Data.List as PL
import qualified Prelude as PL
import System.Environment (getArgs)
import System.IO (hPutStr, hPutStrLn, IOMode(..), withFile)
import System.IO.Unsafe (unsafePerformIO)
import System.Locale (defaultTimeLocale)

import Fault
import Reference
import Storage

type Series = U.Vector Float                 -- ^ Vector of measurements.
type Window = U.Vector Float                 -- ^ Indexed by panel, single measurement.
type SystemSerie = V.Vector (Series, Series) -- ^ Indexed by panel, vectors for voltage and current.
type SystemSSerie = V.Vector Series          -- ^ Indexed by panel, vectors for a single measurement.

-- | Limit in Watt for all samples surveyed
powerLimit = 1.0

-- | Window size used by sliding average
windowSize = 16

-- | Thresholds in consecutive voltage and current samples that imply faults
voltageThreshold, currentThreshold :: Float
(voltageThreshold, currentThreshold) = (0.3, 0.8)

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

average vec = (1 / (realToFrac $ U.length vec)) * U.sum vec

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
  deriving (Eq, Generic, Show)

instance NFData FaultType

type LongFaultDescription a = (FaultType, ModuleID, a)

joinM :: Monad m => (m a1, m a2) -> m (a1, a2)
joinM = uncurry $ liftM2 (,)

checkDayBoth :: SystemSerie -> Maybe (Window, Window) -> ([LongFaultDescription Int], Maybe (Window, Window))
checkDayBoth day windows = force (faults, windows')
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
    idx          = unsafePerformIO $ {-(putStrLn $ "(this, rest)" <> show ((U.length this, U.length rest)) <> " samples") >>-} return (this ! 0)
    (this, rest) = unsafePerformIO $ {-(putStrLn $ "(tmp, ff)" <> show ((U.length tmp, U.length ff)) <> " samples") >>-} return (U.span pred ff)
    (tmp, ff)    = U.break pred indices

-- | Returns with ([], Nothing) if no suitable window is found nor provided
checkDay :: Series -> SystemSSerie -> Int -> Maybe Window -> Float -> ([FaultDescription], Maybe Window)
checkDay power samples len window threshold = force . flip runState window $ do
  get >>= \w -> when (w == Nothing) initReference

  -- (TODO: REMOVE) print all values for a specific module in some way..
  -- unsafePerformIO $ (print $ samples ! 19) >> return (get >>= \w -> when (w == Nothing) (put undefined))

  -- process each segment
  fmap PL.concat . forM grouped $ \segment ->
    -- process each panel
    fmap (PL.concat . V.toList) . V.forM (V.imap (,) segment) $ \(panel, panelSegment) -> do
      let (indices, panelSegment') = U.unzip panelSegment
      -- process every sample for this continous sequence of values
      let len' = U.length panelSegment'
      fmap PL.concat . forM [(quot windowSize 2 - 1) .. (len' - 1 - quot windowSize 2)] $ \idx -> do
        Just ref <- get

        -- handle start and end in a special way
        -- the absolute first sample (ever) should be average, i.e. update initReference
        -- the second set of samples should not be considered for fault detection, i.e. only iterate until len'-1 - windowSize/2 or w/e

        let upperIndex  = min (idx + windowSize - 1) (len' - 1)
            lowerIndex  = max (idx - windowSize + 1) 0
            extract l u = average $ U.slice l (u - l + 1) panelSegment'

            sample      = extract idx        upperIndex
            ref'        = extract lowerIndex idx

        let threshold'  = threshold {- unsafePerformIO $
                            (when (panel == 19 && threshold == voltageThreshold) $ putStrLn $ "(ref, sample, ref'): " <> show (ref ! panel) <> "," <> show sample <> "," <> show ref') >> return threshold -}

        if (abs ((ref ! panel) - sample) <= threshold') then do
              modify $ \(Just v) -> Just $ U.modify (\v' -> UM.write v' panel ref') v
              return []
            else
              return {-$ unsafePerformIO $ putStrLn "found fault" >> return -} [(panel, indices ! idx)]

  where
  grouped       = consec (\idx -> power ! idx >= powerLimit) (V.map (U.imap (,)) samples)
  initReference = when (not $ PL.null grouped) $ put (Just . convert . V.map (average . U.map snd . U.take windowSize) . PL.head $ grouped)

-- Wrapper iterating over all days and returns date/module of fault
checkTimePeriod :: DB.Pool -> Int -> Int -> Integer -> Int -> Int -> Day -> IO [LongFaultDescription (Day, Int)]
checkTimePeriod pool system systemSize year faultID panel firstFaultyDay = do
  let processDay (prevFaults, windows) day = do
      daily <- retrieveDayValues pool system systemSize faultID panel firstFaultyDay day
      let (newFaults, windows') = checkDayBoth daily windows
          taggedNew             = map (\(kind, addr, idx) -> (kind, addr, (day, idx))) newFaults
      return $ force (taggedNew <> prevFaults, windows')

  fst <$> foldM processDay ([], Nothing) days
  where
  firstDay   = fromGregorian year 1 1
  days       = PL.take 365 [firstDay..]

retrieveDayValues :: DB.Pool -> SystemID -> Int -> Int -> Int -> Day -> Day -> IO SystemSerie
retrieveDayValues pool system systemSize faultID panel firstFaultDay day = do
  splitted <- fmap PL.concat . DB.runCas pool $ mapM id [fetchBefore, fetchFaulty, fetchAfter]
  let rows = PL.unzip splitted
  return . V.map (twice fromList) . uncurry V.zip $ twice fromList rows
  where
  coreQuery arg = DB.executeRows DB.ALL
      (DB.query $ "select voltage,current from " <> _tableName simulationTable <> " where system=? and date=? " <> arg)
      (system, UTCTime day 0, panel)

  fetchBefore   = coreQuery " and module < ?"
  fetchAfter    = coreQuery " and module > ?"
  faultyQuery   = DB.executeRows DB.ALL
      (DB.query $ "select voltage,current from " <> _tableName faultDataTable <> " where fault_id=? and date=?")
      (faultID, UTCTime day 0)

  fetchFaulty
    | day < firstFaultDay = coreQuery " and module=?"
    | otherwise           = faultyQuery

classify :: Int -> Int -> IO ()
classify firstFault lastFault = do
  pool <- getPool
  forM_ [firstFault..lastFault] $ \faultID -> do
    putStr $ "#" <> show faultID <> " "
    checkFault pool faultID

  where
  getFaultDesc faultID = DB.executeRow DB.ONE
      (DB.query $ "select system, sys_size, module, date, u_factor, i_factor from "
                  <> _tableName faultDescTable
                  <> " where fault_id=?")
      (faultID)

  checkFault pool faultID = do
    Just (system, sysSize, panel, timestamp, uFactor, iFactor) <- DB.runCas pool $ getFaultDesc faultID

    putStr $ "(" <> show system <> " " <> show sysSize <> " " <> show panel <> " " <> show timestamp
                 <> " " <> show (uFactor :: Float) <> " " <> show (iFactor :: Float) <> "): "

    let realFaultDay = utctDay timestamp
        (year, _, _) = toGregorian realFaultDay
    descs <- fmap force $ checkTimePeriod pool system sysSize year faultID panel realFaultDay
    let firstFaultDay = PL.foldl' (\s (_,_,(day,_)) -> min s day) (fromGregorian year 12 31) descs

    if firstFaultDay < realFaultDay then
        putStrLn $ "false-positive at day " <> show firstFaultDay
      else
        -- TODO: Check if firstFaultDay is the first one with non-zero power
        putStrLn $ "possibly good at day " <> show firstFaultDay
