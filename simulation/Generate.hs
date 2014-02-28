{-# Language OverloadedStrings #-}

module Generate where

import qualified Control.Monad as M
import Control.Monad.Trans
import Data.Monoid ((<>))
import Data.Time.Calendar
import Data.Time.Clock
import Data.Text hiding (filter, length, map, take, break, foldl')
import qualified Data.Vector.Unboxed as U
import qualified Database.Cassandra.CQL as DB
import GHC.Float (double2Float, float2Double)
import Prelude
import qualified Prelude as PL
import System.IO (IOMode(..),withFile)
import System.Random.MWC (create, uniformR, asGenIO, withSystemRandom)
import System.Random.MWC.Distributions (normal)

import Reference
import Storage

generateYear :: DB.Pool -> ValueTable -> [SolarModule] -> Integer -> SystemID -> IO ()
generateYear pool table modules year system = withSystemRandom . asGenIO $ \gen -> do
  putStrLn $ "Generating year: " <> show year <> " with " <> show (PL.length modules) <> " modules"
  M.forM_ modules $ \sm@(SM maxPower addr stdDev) -> do
    DB.runCas pool $ M.forM_ [firstDay..lastDay] $ \day -> do
      noise <- liftIO $ U.replicateM
        samplesPerDay (fmap double2Float $ normal (float2Double maxPower) stdDev gen)

      liftIO . putStrLn $ "Generating power curve for day " <> show day <> " (module: " <> show addr <> ")"

      let daily = generatePowerCurve table 5 day noise sm
          (voltage, current) = U.unzip daily
      write (UTCTime day 0) sm voltage current

  where
  samplesPerDay = 24 * 12
  write day (SM _ addr _) voltage current = DB.executeWrite DB.ONE
    (DB.query $ "insert into "
                <> _tableName simulationTable
                <> " "
                <> tableFieldsStr simulationTable
                <> " values (?,?,?,?,?)")
    (system, addr, day, U.toList voltage, U.toList current)

  firstDay  = fromGregorian year 0 1
  lastDay   = fromGregorian year 12 31

generateYears :: Integer -> Integer -> Int -> IO ()
generateYears firstYear lastYear sys = do
  moduleGen <- create
  modules <- mapM (buildModule moduleGen) [1..24]

  table <- fmap normalizePowerTable $ withFile "logs/work" ReadMode readPowerTable
  pool <- getPool

  M.forM_ [firstYear..lastYear] $ \year ->
    generateYear pool table modules year sys
  where
  buildModule gen addr = do
    power <- uniformR (180.0, 250.0) gen
    return $ SM power addr 0.1 -- TODO: Replace stddev
