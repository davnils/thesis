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

retrieveDay :: SystemID -> Int -> Day -> IO Result
retrieveDay system modules day = do
  pool <- DB.newPool [(cassandraHost, cassandraPort)] "thesis"
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
