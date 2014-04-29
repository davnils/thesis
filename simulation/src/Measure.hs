{-# Language DeriveGeneric, OverloadedStrings, ScopedTypeVariables #-}

module Measure where

import Control.Applicative ((<$>))
import Control.Arrow(first, (***))
import Control.DeepSeq.Generics (force, NFData)
import Control.Monad (forM_, forM, foldM, liftM2, when)
import Control.Monad.State.Strict(get, modify, put, runState)
import Data.Foldable (fold)
import Data.Maybe (mapMaybe, fromMaybe)
import Data.Monoid ((<>))
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Format (parseTime)
import Data.Text hiding (filter, length, map, take, break, foldl')
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Unboxed.Mutable as UM
import qualified Database.Cassandra.CQL as DB
import Data.Vector.Generic (convert, fromList, toList, (!), (!?))
import GHC.Generics
import Prelude
import qualified Data.List as PL
import qualified Prelude as PL
import System.Environment (getArgs)
import System.IO (hFlush, hPutStr, hPutStrLn, IOMode(..), stderr, stdout, withFile)
import System.IO.Unsafe (unsafePerformIO)
import System.Locale (defaultTimeLocale)

import Fault
import Reference
import Retrieve
import Storage

measureTimePeriod :: Day -> Day -> IO (Maybe (Int, (Int, [Double])))
measureTimePeriod firstWindow secondWindow = do
  [first, second] <- mapM fetchMonth [firstWindow, secondWindow]
  let maxSamples = undefined
  res <- fmap PL.concat . forM [0..maxSamples-1] $ \idx -> do
    indicators <- forM [idx..maxSamples-1] $ \idx' -> do
      let grabVec f vec step = V.map (U.map $ (\entry -> entry ! step) . f) vec
          vecDiffs f         = V.map (\(x,y) -> abs $ x - y) $ V.zip (grabVec f first idx) (grabVec f second idx')
          classify           = V.map (<= epsilon) . vecDiffs
          (vec1, vec2, vec3) = (classify (\(x,_,_) -> x), classify (\(_,y,_) -> y), classify (\(_,_,z) -> z))
          moduleSum          = sum $ map (\m -> if (vec1 ! m) + (vec2 ! m) + (vec3 ! m) == 3 then 1 else 0) [0..modules-1]

      if moduleSum >= modules - allowedFailures then
        return [(idx', V.toList $ vecDiffs (\(x,_,_) -> x), V.toList $ vecDiffs (\(_,y,_) -> y))]
        else return []

    return . map (\pair -> (idx, pair)) $ PL.concat indicators 

  case res of
    []    -> return $ Nothing
    (e:_) -> return $ Just e

  where
  system = 1
  modules = 24
  allowedFailures = 1
  epsilon = 1e-2
  fetchMonth :: Day -> IO (V.Vector (U.Vector Double, U.Vector Double, U.Vector Double))
  fetchMonth day = fmap fold $ mapM (retrieveDay system modules) $ take 30 [day ..]
