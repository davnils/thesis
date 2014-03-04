{-# Language BangPatterns, OverloadedStrings #-}

module Reference where

import Control.Monad (liftM2)
import Control.Monad.Primitive (PrimState)
import Data.Char (isDigit)
import qualified Data.HashMap.Strict as H
import Data.Int (Int64)
import Data.List (foldl')
import qualified Data.List as L
import Data.Monoid ((<>))
import Data.Ord (comparing)
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import Data.Time.Format (parseTime)
import Data.Text hiding (filter, length, map, take, break, foldl')
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Algorithms.Intro as V
import GHC.Float (double2Float)
import Lens.Family.State.Strict
import Pipes
import Pipes.Parse
import Pipes.Text hiding (filter, length, unpack, map, take)
import Pipes.Text.IO
import Pipes.Vector
import Prelude hiding (readFile, concat)
import qualified Prelude as PL
import System.Locale (defaultTimeLocale)
import System.IO (Handle, hPutStrLn)
import System.Random.MWC (Gen, asGenIO, withSystemRandom)
import System.Random.MWC.Distributions (normal, standard)
import System.IO.Unsafe
import Text.Read (readMaybe)

data Row
 = Row UTCTime Float
 deriving (Eq, Ord, Show)

type TripleDay = Integer
type ValueEntry = (Int64, Float)
type ValueTable = (Day, Day, H.HashMap TripleDay (U.Vector ValueEntry))

-- parse a single row on the format "2012-11-06 13:53:38	28.178"
parseRow :: (Functor m, Monad m) => Parser Text m (Maybe Row)
parseRow = do
  row <- zoom line $ do
    time <- parseTimeStamp
    _ <- parseToken
    val <- parseDouble
    return $ liftM2 Row time val

  _ <- drawChar
  return row

  where
  parseToken = fmap (strip . concat) $ zoom word drawAll
  parseTimeStamp = do
    date <- parseToken
    time <- parseToken
    return $ parseTime defaultTimeLocale "%F%T" (unpack $ date <> time)

  parseDouble = fmap (readMaybe . unpack) parseToken

exhaustParser parser producer = do
  (r, producer') <- runStateT parser producer
  yield r
  exhaustParser parser producer'

dieOnFailure :: Monad m => Pipe (Maybe a) a m ()
dieOnFailure = do
  val <- await
  case val of
    Just !v -> yield v >> dieOnFailure
    Nothing -> return ()

instance MonadIO m => MonadIO (ToVector v e m) where
  liftIO = lift . liftIO

-- read and pad power table
readPowerTable :: Handle -> IO ValueTable
readPowerTable handle = do
  rows <- runToVector . runEffect $ exhaustParser parseRow (fromHandle handle) >-> dieOnFailure >-> toVector
  rowsRef <- V.thaw rows
  V.sort rowsRef
  rows' <- V.freeze rowsRef

  putStrLn $ "Read " <> show (V.length rows') <> " rows from handle" 

  let getTimestamp (Row t _) = t
      comp f   = utctDay . f $ V.map getTimestamp rows'
      firstDay = comp V.minimum
      lastDay  = comp V.maximum

  return (firstDay, lastDay, snd $ foldl' buildDay (rows', H.empty) [firstDay..lastDay])
  where
  buildDay !(!rows, !table) !day = (left, table')
    where
    table' = H.insert (toModifiedJulianDay day) (V.convert todaySamples') table
    todaySamples' = V.map (\(Row t v) -> (round $ utcTimeToPOSIXSeconds t, v)) todaySamples
    todaySamples = V.snoc (V.cons (Row firstEntry 0.0) current) (Row lastEntry 0.0)
    (current, left) = V.span (\(Row t _) -> utctDay t == day) rows

    firstEntry = time "03:00:00"
    lastEntry  = time "23:00:00"
    time = (\(UTCTime _ t) -> UTCTime day t) . (\(Just t) -> t) . parseTime defaultTimeLocale "%T"

-- write every hashtable entry in order
writePowerTable :: Handle -> ValueTable -> IO ()
writePowerTable handle (firstDay, lastDay, table) = do
  output "raw = ["
  mapM_ printDay [firstDay..lastDay]
  output "];"
  where
  printDay day = do
    let entries = (H.!) table (toModifiedJulianDay day)
    U.forM_ entries $ \(time, val) ->
      output $ formatTime time <> " " <> show val
  --formatTime = show
  formatTime = PL.takeWhile isDigit . show
  output = hPutStrLn handle

normalizePowerTable :: ValueTable -> ValueTable 
normalizePowerTable (f, l, table) = (f, l, H.map (U.map norm) table)
  where
  norm (t, v) = (t, v / maxPower)
  maxPower = H.foldl' (\acc v -> PL.max acc (maximumOfVec v)) 0.0 table
  maximumOfVec = snd . U.maximumBy (comparing snd)

type PowerRating = Float
type ModuleID = Int
type SamplingInterval = Int
type Noise = U.Vector Float

data SolarModule
 = SM PowerRating ModuleID Double
  
-- Generate power curve for a single module during a single day.
-- Input data points are interpolated using a random walk.
-- Outputs (Voltage, Current) tuples.
generatePowerCurve :: ValueTable -> SamplingInterval -> Day -> SolarModule -> U.Vector (Float, Float)
generatePowerCurve (first, _, table) interval inputDay (SM maxPower _ _) = U.unfoldrN steps step iterStart
  where
  day' = toModifiedJulianDay $ fromGregorian yRef mIn dIn
  (yRef, _, _) = toGregorian first
  (_, mIn, dIn) = toGregorian inputDay

  steps = fromIntegral $ 1 + quot (convertTime (U.last daily) - convertTime (U.head daily)) timeStep
  timeStep = fromIntegral $ 60 * interval
  timeStep' = fromIntegral timeStep
  iterStart = (U.zip daily (U.drop 1 daily), fst $ U.head daily, 0.0)
  daily = (H.!) table day'

  step (ref, time, prevDay) = Just ((voltage, current), (ref', time + timeStep', today'))
    where
    (refToday', ref') = discardSamples time ref
    (current, voltage) = calculateComponents maxPower today'
    today' = extrapolate (time - timeStep') prevDay (convertTime refToday') (snd refToday') time

applyVoltageNoise :: Double -> Gen (PrimState IO) -> U.Vector (Float, Float) -> IO (U.Vector (Float, Float))
applyVoltageNoise stdDev gen vec = do
  noise <- U.replicateM (U.length vec) $ normal 1.0 stdDev gen
  return . U.map applyNoise $ U.zip vec noise
  where
  applyNoise ((voltage, current), noise)
    | voltage > 0.0 = (voltage', voltage*current / voltage')
    | otherwise    = (voltage, current)
    where
    voltage' = voltage * double2Float noise

convertTime :: Num b => (Int64, a) -> b
convertTime = fromIntegral . fst

-- Derive voltage and current from power rating and normalized irradiance
calculateComponents :: PowerRating -> Float -> (Float, Float)
calculateComponents maxPower irradiance
  | irradiance <= 0.0 = (0.0, 0.0)
  | otherwise         = (u_opt, current)
  where
  openCircuitRange = (snd openCircuitRange - 3, 0.16 * maxPower)  -- at most 40V at 250Wp
  current          = (irradiance * maxPower) / u_opt              -- simply derive current from voltage
  u_opt            = 0.76 * u_oc                                  -- well-known ratio for MPP
  u_oc             = fst openCircuitRange + u_oc_coeff * (snd openCircuitRange - fst openCircuitRange)
  u_oc_coeff       = (exp irradiance) / exp 1

discardSamples :: Int64 -> U.Vector (ValueEntry, ValueEntry) -> (ValueEntry, U.Vector (ValueEntry, ValueEntry))
discardSamples time input 
  | convertTime (fst first) == time = (fst first, discard)
  | otherwise = (snd first, discard)
  where
  first = U.head discard
  discard = U.dropWhile (\(_, tuple) -> convertTime tuple < time) input

extrapolate :: Int64 -> Float -> Int64 -> Float -> Int64 -> Float
extrapolate x1 y1 x2 y2 x = k*x' + m
  where
  k = (y1 - y2)/(x1' - x2')
  m = y2 - k*x2'
  [x1', x2', x'] = map realToFrac [x1, x2, x]
