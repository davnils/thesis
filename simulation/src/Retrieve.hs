{-# Language OverloadedStrings #-}

module Retrieve where

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

type Result = V.Vector (U.Vector Float, U.Vector Float)

retrieveYear :: SystemID -> Int -> Integer -> IO Result
retrieveYear system modules year = do
  pool <- getPool 
  fmap flatten $ PL.mapM (retrieveDay' pool system modules) days
  where
   days = PL.take 365 $ [(fromGregorian year 1 1)..]
   flatten res = V.fromList $ PL.map conv [0..23]
     where
     conv addr = (U.concat volt, U.concat curr)
       where
       this = PL.map (\v -> (V.!) v addr) res
       (volt, curr) = PL.unzip this

retrieveDay :: SystemID -> Int -> Day -> IO Result
retrieveDay system modules day = getPool >>= (\p -> retrieveDay' p system modules day)

retrieveDay' :: DB.Pool -> SystemID -> Int -> Day -> IO Result
retrieveDay' pool system modules day = do
  putStrLn $ "Retrieving day: " <> show day
  rows <- DB.runCas pool $ DB.executeRows DB.ALL fetchRows (system, UTCTime day 0, modules)
  return . V.fromList $ PL.map (\(l, r) -> (U.fromList l, U.fromList r)) rows
  where
  fetchRows = DB.query $
    "select voltage,current from "
    <> _tableName simulationTable
    <> " where system=? and date=? and module <= ?"

outputMatlab :: String -> Result -> IO ()
outputMatlab file raw = withFile (file <> ".m") WriteMode $ \handle -> do
  hPutStrLn handle $ file <> "_ = ["
  V.mapM_ (\(v, c) -> outputVec handle v >> outputVec handle c) raw
  hPutStrLn handle "]"
  where
  outputVec h vec = U.mapM_ (hPutStr h . (' ':) . show) vec >> hPutStrLn h ""
