module Fault where

import qualified Data.Vector.Unboxed as U
import System.Random.MWC (create, uniformR, asGenIO, withSystemRandom)
import System.Random.MWC.Distributions (normal)

type Percent = Float
type TimePeriod = (Day, Day)
type Samples = U.Vector (Float, Float)

data Fault
  | Instant POSIXTime (Percent, Percent)      -- instant loss in voltage and current
  | Degradation TimePeriod (Percent, Percent) -- percentual degradation over interval

injectFaults :: [Fault] -> Samples -> IO Samples
injectFaults faults vec = undefined
