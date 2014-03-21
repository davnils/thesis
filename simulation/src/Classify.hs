{-# Language OverloadedStrings #-}

-- Implement the following:
--   Global avg-window size

type Fault =
type Series = 
type Window = 

-- 
-- * Function which generates a smoothed data series from samples
-- a point is given by (1-a)*prev + a*new and the window should be prefixed with 0
-- the resulting vector should be of the same length
applyMovingAverage :: Series -> Series
applyMovingAverage = undefined
  where
  windowSize = 16

-- * Functionality for retrieving an initial reference window
--   -> simply traverse system values and save first average with >= 50 W sum

getReferenceWindow :: (Series, Series) -> Maybe Window
getReferenceWindow = undefined
  where
  minPredicate volt curr = volt * curr >= 50

-- * Processing of a single day given a reference
--   -> traverse all windows and compare first non-negligible with the given reference

checkDay :: (Series, Series) -> Window -> (Window, [Fault])
checkDay = undefined

-- * Wrapper which iterates over all days and returns date/module of fault
--   -> ?

checkTimePeriod :: (Series, Series) -> [Fault]
checkTimePeriod = undefined

-- * General wrapper which generates a sequence of faults and
--   calls the classifier in order to check classification performance

module Classify where

import Control.Monad (forM_)
import Data.Monoid ((<>))
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Format (parseTime)
import Data.Text hiding (filter, length, map, take, break, foldl')
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import qualified Database.Cassandra.CQL as DB
import Prelude
import qualified Prelude as PL
import System.Environment (getArgs)
import System.IO (hPutStr, hPutStrLn, IOMode(..), withFile)
import System.Locale (defaultTimeLocale)

import Storage

type Result = V.Vector (U.Vector Float, U.Vector Float, U.Vector Float)

retrieveYear :: SystemID -> Int -> Integer -> IO Result
retrieveYear system modules year = do
  pool <- getPool 
  fmap flatten $ PL.mapM (retrieveDay' pool system modules) days
  where
   days = PL.take 365 $ [(fromGregorian year 1 1)..]
   flatten res = V.fromList $ PL.map conv [0..23]
     where
     conv addr = (U.concat volt, U.concat curr, U.concat temp)
       where
       this = PL.map (\v -> (V.!) v addr) res
       (volt, curr, temp) = PL.unzip3 this

retrieveYearSep :: SystemID -> Int -> Integer -> IO ()
retrieveYearSep system modules year = do
  pool <- getPool 
  forM_ (PL.zip [1..] days) $ \(count, day) -> retrieveDay' pool system modules day >>= outputMatlab ("output_" <> show count)
  where
   days = PL.take 365 $ [(fromGregorian year 1 1)..]

retrieveDay :: SystemID -> Int -> Day -> IO Result
retrieveDay system modules day = getPool >>= (\p -> retrieveDay' p system modules day)

retrieveDay' :: DB.Pool -> SystemID -> Int -> Day -> IO Result
retrieveDay' pool system modules day = do
  putStrLn $ "Retrieving day: " <> show day
  rows <- DB.runCas pool $ DB.executeRows DB.ALL fetchRows (system, UTCTime day 0, modules)
  return . V.fromList $ PL.map (\(l, m, r) -> (U.fromList l, U.fromList m, U.fromList r)) rows
  where
  fetchRows = DB.query $
    "select voltage,current,temperature from "
    <> _tableName simulationTable
    <> " where system=? and date=? and module <= ?"

outputMatlab :: String -> Result -> IO ()
outputMatlab file raw = withFile (file <> ".m") WriteMode $ \handle -> do
  hPutStrLn handle $ (PL.takeWhile (/= '_') file) <> "_ = ["
  V.mapM_ (\(v, c, t) -> outputVec handle v >> outputVec handle c >> outputVec handle t) raw
  hPutStrLn handle "];"
  where
  outputVec h vec = U.mapM_ (hPutStr h . (' ':) . show) vec >> hPutStrLn h ""
