module Fault where

import qualified Data.Vector.Unboxed as U
import System.Random.MWC (create, uniformR, asGenIO, withSystemRandom)
import System.Random.MWC.Distributions (normal)

import Storage

type Percent = Float
type TimePeriod = (Day, Day)
type Samples = U.Vector (Float, Float)

data Fault
  | Instant ModuleID POSIXTime (Parameters -> Parameters) -- instant loss in power output
  | Degradation ModuleID TimePeriod Percent               -- percentual increase in series resistance

applyFault :: SystemID -> ModuleID -> Fault -> IO ()
applyFault system addr fault = do
  pool <- getPool

  -- instant: apply change from timestamp to final sample
  -- degrad : apply change from given day until final sample

  let interval = case fault of
    Instant _ time _              -> [utctDay time, utctDay time]
    Degradation _ (first, last) _ -> [first..last]
 
  runCas . forM interval $ \day -> do
    let timestamp = UTCTime day 0
    Just (voltage,current) <- DB.executeRow DB.ONE
      (DB.query $ "select voltage,current from "
                  <> _tableName simulationTable
                  <> " where system=? and module=? and date=?")
      (system, addr, timestamp)

    -- TODO: Process
    let (voltage', current') = undefined

    DB.executeWrite DB.ONE
      (DB.query $ "insert into "
                  <> _tableName simulationTable
                  <> " (voltage,current) values (?, ?) "
                  <> " where system=? and module=? and date=?")
      (voltage', current', system, addr, timestamp)
