{-# Language BangPatterns, OverloadedStrings #-}

module Main where

import Control.Monad (liftM2, liftM)
import qualified Control.Monad as M
import Control.Monad.Trans.Free
import Data.Char (isDigit)
import qualified Data.HashMap.Strict as H
import Data.List (minimumBy, maximumBy, foldl')
import qualified Data.List as L
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
import qualified Database.Cassandra.CQL as DB
import GHC.Float (double2Float, float2Double)
import Prelude hiding (readFile, concat)
import qualified Prelude as PL
import System.Locale (defaultTimeLocale)
import System.IO (Handle(..), IOMode(..),withFile, hPutStrLn)
import System.Random.MWC (asGenIO, withSystemRandom)
import System.Random.MWC.Distributions (normal, standard)
import Text.Read (readMaybe)

import Reference
import Storage

type SystemID = Int

-- integrate Cas monad into IO stack - where should the RNG eval take place?
generateYear :: ValueTable -> [SolarModule] -> Int -> SystemID -> IO ()
generateYear table modules year system = withSystemRandom . asGenIO $ \gen -> do
  M.forM_ modules $ \sm@(SM maxPower _) -> do
    noise <- U.replicateM (24*12*(utctDay lastDay-utctDay firstDay))
                          (fmap double2Float $ normal (float2Double maxPower) 0.01 gen)

    runCas pool $ M.forM_ [utctDay firstDay..utctDay lastDay] $ \day -> do
      let daily = generatePowerCurve table 5 (toModifiedJulianDay day) (U.slice undefined noise) sm
          (voltage, current) = U.unzip daily
      write day sm voltage current
  where
  write day sm voltage current = DB.executeWrite ONE
    (query "insert into " <> undefined <> " () values (?,?,?,?,?)")
    (system, day, sm, U.toList voltage, U.toList current)
    -- no vector support in cassy?

  Just firstDay = parseTime defaultTimeLocale "%F" $ show year <> "-02-07"
  Just lastDay  = parseTime defaultTimeLocale "%F" $ show (year+1) <> "-02-06"

generateYears :: Int -> Int -> Int -> IO ()
generateYears firstYear lastYear sys = do
  table <- fmap normalizePowerTable $ withFile "logs/work" ReadMode readPowerTable
  pool <- DB.newPool [(cassandraHost, cassandraPort)] "thesis"
  M.forM_ [firstYear..lastYear] $ \year ->
    generateYear table modules year sys
  where
  modules = []

main :: IO ()
main = generateYears 1990 2000 1
