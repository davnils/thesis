{-# Language DeriveGeneric, OverloadedStrings, ScopedTypeVariables #-}

module Classify where

import Control.Applicative ((<$>))
import Control.Arrow(first, (***))
import Control.DeepSeq.Generics (force, NFData)
import Control.Monad (forM_, forM, foldM, liftM2, when)
import Control.Monad.Trans (liftIO)
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
import System.IO (hFlush, hPutStr, hPutStrLn, IOMode(..), stderr, stdout, withFile)
import System.IO.Unsafe (unsafePerformIO)
import System.Locale (defaultTimeLocale)

import Fault
import Reference
import Retrieve
import Storage

type Series = U.Vector Float                 -- ^ Vector of measurements.
type Window = U.Vector Float                 -- ^ Indexed by panel, single measurement.
type SystemSerie = V.Vector (Series, Series) -- ^ Indexed by panel, vectors for voltage and current.
type SystemSSerie = V.Vector Series          -- ^ Indexed by panel, vectors for a single measurement.

head' str []   = error $ "Invalid head on empty list (" <> str <> ")"
head' _ (x:_) = x

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

checkDayBoth :: SystemSerie -> Maybe (Window, Window) -> FaultType -> ([LongFaultDescription Int], Maybe (Window, Window))
checkDayBoth day windows faultType = force (faults, windows')
  where
  faults                    = case faultType of
                                VoltageFault -> voltFaults'
                                CurrentFault -> currFaults'
                                _ -> voltFaults' <> currFaults'

  currFaults'               = tag CurrentFault currFaults
  voltFaults'               = tag VoltageFault voltFaults
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
    (tmp, ff)    = U.break pred indices

-- | Returns with ([], Nothing) if no suitable window is found nor provided
checkDay :: Series -> SystemSSerie -> Int -> Maybe Window -> Float -> ([FaultDescription], Maybe Window)
checkDay power samples len window threshold = force . flip runState window $ do
  get >>= \w -> when (w == Nothing) initReference

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
        let upperIndex  = min (idx + windowSize - 1) (len' - 1)
            lowerIndex  = max (idx - windowSize + 1) 0
            extract l u = average $ U.slice l (u - l + 1) panelSegment'

            sample      = extract idx        upperIndex
            ref'        = extract lowerIndex idx

        if (abs ((ref ! panel) - sample) <= threshold) then do
              modify $ \(Just v) -> Just $ U.modify (\v' -> UM.write v' panel ref') v
              return []
            else
              return [(panel, indices ! idx)]

  where
  grouped       = consec (\idx -> power ! idx >= powerLimit) (V.map (U.imap (,)) samples)
  initReference = when (not $ PL.null grouped) $ put (Just . convert . V.map (average . U.map snd . U.take windowSize) . head' "initRef" $ grouped)

-- Wrapper iterating over all days and returns date/module of fault
checkTimePeriod :: DB.Pool -> Int -> Int -> Integer -> Int -> Int -> Day -> FaultType -> IO [LongFaultDescription (Day, Int)]
checkTimePeriod pool system systemSize year faultID panel firstFaultyDay faultType = do
  let processDay (prevFaults, windows) day = do
      daily <- retrieveDayValues pool system systemSize faultID panel firstFaultyDay day
      let (newFaults, windows') = checkDayBoth daily windows faultType
          taggedNew             = map (\(kind, addr, idx) -> (kind, addr, (day, idx))) newFaults
      return $ force (taggedNew <> prevFaults, windows')

  fst <$> foldM processDay ([], Nothing) days
  where
  firstDay   = fromGregorian year 1 1
  days       = PL.take 365 [firstDay..]

retrieveDayValues :: DB.Pool -> SystemID -> Int -> Int -> Int -> Day -> Day -> IO SystemSerie
retrieveDayValues pool system systemSize faultID panel firstFaultDay day = do
  splitted <- fmap PL.concat . DB.runCas pool $ sequence [fetchBefore, fetchFaulty, fetchAfter]
  let rows = PL.unzip splitted
  return . V.map (twice fromList) . uncurry V.zip $ twice fromList rows
  where
  fetchBefore             = coreQuery " and module < ?"
  fetchAfter              = coreQuery " and module > ?"
  fetchFaulty
    | day < firstFaultDay = coreQuery " and module=?"
    | otherwise           = faultyQuery

  coreQuery arg           = DB.executeRows DB.ALL
      (DB.query $ "select voltage,current from " <> _tableName simulationTable <> " where system=? and date=? " <> arg)
      (system, UTCTime day 0, panel)

  faultyQuery             = DB.executeRows DB.ALL
      (DB.query $ "select voltage,current from " <> _tableName faultDataTable <> " where fault_id=? and date=?")
      (faultID, UTCTime day 0)

classify :: Int -> Int -> IO ()
classify firstFault lastFault = do
  pool <- getPool
  forM_ [firstFault..lastFault] $ \faultID -> do
    mapM_ (checkFault pool faultID) [VoltageFault, CurrentFault]
    mapM hFlush [stdout, stderr]

getFaultDesc faultID = DB.executeRow DB.ONE
    (DB.query $ "select system, sys_size, module, date, u_factor, i_factor from "
                <> _tableName faultDescTable
                <> " where fault_id=?")
    (faultID)

checkFault pool faultID faultType = do
  Just (system, sysSize, panel, timestamp, uFactor, iFactor) <- DB.runCas pool $ getFaultDesc faultID

  let val VoltageFault = uFactor :: Float
      val CurrentFault = iFactor :: Float

  putStr $ "(" <> show faultID <> " " <> show faultType <> " " <> show system <> " " <> show sysSize
           <> " " <> show panel <> " " <> show timestamp <> " " <> show (val faultType) <> "): "

  let realFaultDay = utctDay timestamp
      (year, _, _) = toGregorian realFaultDay
  descs <- fmap force $ checkTimePeriod pool system sysSize year faultID panel realFaultDay faultType
  let firstFaultDay = PL.foldl' (\s (_,_,(day,_)) -> min s day) (fromGregorian year 12 31) descs

  case (length descs, compare firstFaultDay realFaultDay) of
    (_, LT) -> putStrLn $ "false-positive at day " <> show firstFaultDay
    (0, _)  -> putStrLn "no faults found"
    _  -> do
      let days                = [succ realFaultDay .. pred firstFaultDay]
          classifierAllowance = 14 -- At most 14 days allowed between occurence and detection

      indicators <- forM days $ \day -> do
        daily <- retrieveDay' pool system sysSize day
        let timeLen = U.length . (\(_,_,t) -> t) . V.head $ daily
        return $ flip PL.any [0..timeLen-1] $ sufficient daily

      let zipped = PL.zip indicators days
      if PL.length days > classifierAllowance && PL.any id indicators then
          putStrLn  $ "false-positive at day "
                   <> show firstFaultDay
                   <> ", should have classified "
                   <> show (PL.snd . PL.last $ PL.takeWhile fst zipped)
        else
          putStrLn $ "valid at day " <> show firstFaultDay

sufficient daily idx = power >= powerLimit
  where
  power   = V.sum $ V.map (uncurry (*)) samples
  samples = V.map (\(v1,v2,_) -> (v1 ! idx, v2 ! idx)) daily

-- TODO: Need to handle continous classification even after finding an error
--       For example that module could be ignored for x samples or something
checkExternal :: (String, String) -> FaultType -> IO [LongFaultDescription (Int)]
checkExternal (currPath, voltPath) faultType = do
  rawCurr <- buildMatrix <$> readFile currPath
  rawVolt <- buildMatrix <$> readFile voltPath

  let currs  = PL.map (\i -> U.fromList $ rawCurr !! i) [0..num-1]
      volts  = PL.map (\i -> U.fromList $ rawVolt !! i) [0..num-1]
      rawVec = V.fromList $ PL.zip currs volts

  let processDay (prevFaults, windows) (off1, off2) = do
      let daily                 = V.map (twice $ U.slice (off1 - 1) ((off2 - 1) - off1 + 1)) rawVec
          (newFaults, windows') = checkDayBoth daily windows faultType
          taggedNew             = map (\(kind, addr, idx) -> (kind, addr, (idx))) newFaults
      return $ force (taggedNew <> prevFaults, windows')

  fst <$> foldM processDay ([], Nothing) chunks
  where
  chunks      = let v = [1, 183, 381, 575, 766, 968] in PL.zip v (PL.tail v)
  num         = 16
  buildMatrix = PL.transpose . PL.map (PL.map PL.read . PL.words) . PL.lines 

evaluateExternal = do
  let check = checkExternal ("logs/kth/out_curr", "logs/kth/out_volt")

  curr <- check CurrentFault
  volt <- check VoltageFault
  print $ curr
  print $ volt
