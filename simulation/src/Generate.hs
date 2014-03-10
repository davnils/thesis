{-# Language BangPatterns, OverloadedStrings #-}

module Generate where

import qualified Control.Concurrent.Async as C
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
generateYear pool table modules year system = do
  putStrLn $ "Generating year: " <> show year <> " with " <> show (PL.length modules) <> " modules"
  M.void $ (`C.mapConcurrently` modules) $ \sm@(SM maxPower addr stdDev) -> do
    withSystemRandom . asGenIO $ \gen -> do
      liftIO . putStrLn $ "Generating power curve (module: " <> show addr <> ")"
      DB.runCas pool $ M.forM_ [firstDay..lastDay] $ \day -> do
        let curve = generatePowerCurve table 5 day sm
        daily <- liftIO $ applyVoltageNoise stdDev gen curve
        let (voltage, current) = U.unzip daily
        write (UTCTime day 0) sm voltage current

  where
  interval = 5
  samplesPerDay = 24 * (quot 60 interval)
  write day (SM addr _ _) voltage current = DB.executeWrite DB.ONE
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
  modules <- M.forM [1..24] $ buildModule moduleGen

  table <- fmap normalizePowerTable $ withFile "logs/work" ReadMode readPowerTable
  pool <- getPool

  M.forM_ [firstYear..lastYear] $ \year ->
    generateYear pool table modules year sys

  where
  buildModule gen addr = do
    let grab coeff lower upper = fmap (* coeff) $ uniformR (lower, upper) gen
    !cells <- fmap ([60, 72] !!) $ uniformR (0, 1) gen
    !i_sc  <- grab 1.0e+01 0.9 1.1
    !i_sat <- grab 1.0e-10 1.8 2.2
    !r_ser <- grab 1.0e-02 0.8 1.2
    !r_par <- grab 1.0e+02 1.8 2.2
    return $ SM addr (cells, i_sc, i_sat, r_ser, r_par) 0.01
