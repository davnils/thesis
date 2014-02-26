{-# Language BangPatterns, OverloadedStrings #-}

import Control.Monad (liftM2, liftM)
import Control.Monad.Trans.Free
import Data.Char (isDigit)
import qualified Data.HashMap.Strict as H
import Data.HashMap.Strict ((!))
import Data.List (minimumBy, maximumBy, foldl')
import Data.Monoid ((<>))
import Data.Ord (comparing)
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX (POSIXTime, utcTimeToPOSIXSeconds)
import Data.Time.Format (parseTime)
import Data.Text hiding (filter, length, putStrLn, map, take, break, insert, foldl')
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Algorithms.Intro as V
import Lens.Family
import Lens.Family.State.Strict
import Pipes
import qualified Pipes.Group as G
import Pipes.Parse
import qualified Pipes.Prelude as P
import Pipes.Safe
import Pipes.Text hiding (filter, length, unpack, map, take)
import Pipes.Text.IO
import Pipes.Vector
import Prelude hiding (readFile, concat)
import qualified Prelude as PL
import System.Locale (defaultTimeLocale)
import System.IO (Handle(..), IOMode(..),withFile, hPutStrLn)
import System.IO.Unsafe -- TODO: REMOVE
import Text.Read (readMaybe)

data Row
 = Row UTCTime Float
 deriving (Eq, Ord, Show)

type TripleDay = Integer
type ValueTable = (Day, Day, H.HashMap TripleDay (V.Vector (UTCTime, Float)))

-- parse a single row on the format "2012-11-06 13:53:38	28.178"
parseRow :: (Functor m, Monad m) => Parser Text m (Maybe Row)
parseRow = do
  row <- zoom line $ do
    time <- parseTimeStamp
    parseToken
    val <- parseDouble
    return $ liftM2 Row time val

  drawChar
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

  let getTimestamp = \(Row t _) -> t
      comp f   = utctDay . f $ V.map getTimestamp rows'
      firstDay = comp V.minimum
      lastDay  = comp V.maximum

  return (firstDay, lastDay, snd $ foldl' buildDay (rows', H.empty) [firstDay..lastDay])
  where
  buildDay !(!rows, !table) !day = (left, table')
    where
    table' = H.insert (toModifiedJulianDay day) todaySamples' table
    todaySamples' = V.map (\(Row t v) -> (t, v)) todaySamples
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
    let entries = table ! (toModifiedJulianDay day)
    V.forM_ entries $ \(time, val) ->
      output $ formatTime time <> " " <> show val
  --formatTime = show
  formatTime = PL.takeWhile isDigit . show . utcTimeToPOSIXSeconds 
  output = hPutStrLn handle

normalizePowerTable :: ValueTable -> ValueTable 
normalizePowerTable (f, l, table) = (f, l, H.map (V.map norm) table)
  where
  norm (t, v) = (t, v / maxPower)
  maxPower = H.foldl' (\acc v -> PL.max acc (maximumOfVec v)) 0.0 table
  maximumOfVec = snd . V.maximumBy (comparing snd)

type PowerRating = Int
type ModuleID = Int
type SamplingInterval = Int

data SolarModule
 = SM PowerRating ModuleID

gen = do
  table <- withFile "logs/work" ReadMode readPowerTable
  return $ generateExample table

generateExample table = generatePowerCurve table 5 (toModifiedJulianDay $ utctDay day) (SM undefined undefined)
  where
  Just day = parseTime defaultTimeLocale "%F %T" "2014-01-26 12:00:00"
  
-- Generate power curve for a single module during a single day.
-- Input data points are interpolated using a random walk.
-- Outputs (Voltage, Current) tuples.
generatePowerCurve :: ValueTable -> SamplingInterval -> TripleDay -> SolarModule -> U.Vector (Float, Float)
generatePowerCurve (_, _, table) interval day (SM maxPower moduleID) = U.unfoldrN steps step iterStart
  where
  steps = 1 + quot (round (convertTime (V.last daily) - convertTime (V.head daily))) timeStep
  timeStep = 60*5 :: Int
  timeStep' = fromIntegral timeStep
  iterStart = (V.zip daily (V.drop 1 daily), convertTime $ V.head daily, 0.0)
  daily = table ! day

  step (ref, time, prevDay) = Just ((voltage, current), (ref', time + timeStep', today))
    where
    (refToday, ref') = discardSamples time ref
    current = today
    voltage = today
    today = extrapolate (time - timeStep') prevDay (convertTime $ refToday) (snd refToday) time

convertTime = utcTimeToPOSIXSeconds . fst

discardSamples :: POSIXTime -> V.Vector ((UTCTime, Float), (UTCTime, Float)) ->
                  (((UTCTime, Float)), V.Vector ((UTCTime, Float), (UTCTime, Float)))
discardSamples time input 
  | convertTime (fst first) == time = (fst first, discard)
  | otherwise = (snd first, discard)
  where
  first = V.head discard
  discard = V.dropWhile (\(_, tuple) -> convertTime tuple < time) input

extrapolate :: POSIXTime -> Float -> POSIXTime -> Float -> POSIXTime -> Float
extrapolate x1 y1 x2 y2 x = k*x' + m
  where
  k = (y1 - y2)/(x1' - x2')
  m = y2 - k*x2'
  [x1', x2', x'] = map realToFrac [x1, x2, x]

main :: IO ()
main =
  withFile "logs/tmp_work" ReadMode $ \input ->
    withFile "logs/proc.m" WriteMode $ \output ->
      readPowerTable input >>= writePowerTable output . normalizePowerTable
