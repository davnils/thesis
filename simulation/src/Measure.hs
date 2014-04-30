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

import Classify
import Fault
import Reference
import Retrieve
import qualified Storage as DB

type Triple = (U.Vector Float, U.Vector Float, U.Vector Float)


-- | Provides the capability to locate a valid sample point, given two months to be compared.
--   The returned indices refer to the sample position in the data series, from the perspective
--   of a flattened vector containing the entire time series.
--
measureTimePeriod :: DB.Pool -> Day -> Day -> Int -> Day -> IO (Maybe (Int, (Int, [Float], [Float])))
measureTimePeriod pool firstWindow secondWindow faultID firstFaultDay = do
  [first, second] <- mapM (fetchMonth pool) [firstWindow, secondWindow]
  let maxSamples = undefined :: Int
  res <- fmap PL.concat . forM [0..maxSamples-1] $ \idx -> do
    indicators <- forM [idx..maxSamples-1] $ \idx' -> do
      let grabVec f vec step = V.map ((!step) . f) $ vec
          vecDiffs f g       = V.map g $ V.zip (grabVec f first idx) (grabVec f second idx')
          classify f         = V.map (\v -> if v <= epsilon then 1 else 0) $ vecDiffs f (\(x,y) -> abs $ x - y)
          (vec1, vec2, vec3) = (classify (\(x,_,_) -> x), classify (\(_,y,_) -> y), classify (\(_,_,z) -> z))
          moduleSum          = sum $ map (\m -> if (vec1 ! m) + (vec2 ! m) + (vec3 ! m) == 3 then 1 else 0) [0..modules-1]

      let outputFunction (x, y) = y / x
      if moduleSum >= modules - allowedFailures then
        return [(idx', V.toList $ vecDiffs (\(x,_,_) -> x) outputFunction, V.toList $ vecDiffs (\(_,y,_) -> y) outputFunction)]
        else return []

    return . map (\pair -> (idx, pair)) $ PL.concat indicators 

  case res of
    []    -> return $ Nothing
    (e:_) -> return $ Just e

  where
  system = 1
  modules = 24
  allowedFailures = 1
  epsilon = 0.2

  -- fetch month (voltage, current, temp) based on the first day
  fetchMonth :: DB.Pool -> Day -> IO (V.Vector Triple)

  -- ... -> V.Vector (U, U) -> V.Vector U -> V.Vector (U, U, U)
  fetchMonth pool day     = do
    (v1, v2) <- fmap V.unzip (fetchOthers pool day)
    v3 <- getTempVectors pool day
    return $ V.zip3 v1 v2 v3

  fetchOthers pool day    = fmap fold $ mapM (retrieveDayValues pool system modules faultID 0 firstFaultDay) $ take 30 [day ..] 
  getTempVectors pool day = fmap (V.map (\(_,_,t) -> t) . fold) $ mapM (retrieveDay system modules) $ take 30 [day ..]

checkDegrads :: Int -> Int -> IO ()
checkDegrads firstFault lastFault = mapM_ checkDegrad [firstFault..lastFault]

checkDegrad :: Int -> IO ()
checkDegrad faultID = do
  pool <- DB.getPool
  Just (system, sysSize, addr :: Int, timestamp, uFactor :: Float, iFactor :: Float) <- DB.runCas pool $ getFaultDesc faultID

  let firstFaultDay            = utctDay timestamp
      (faultY, faultM, faultD) = toGregorian firstFaultDay
      makeDate y m             = if faultD == 1 then adjust y m else fromGregorian y  m 1
      adjust y m               = (\(y',m',_) -> fromGregorian y' m' 1) . toGregorian $ pred (fromGregorian y m 1)
      firstDate                = makeDate faultY faultM
      secondDate               = makeDate (faultY+1) faultM

  putStr $ "("    <> show faultID
           <> " " <> show (system :: Int)
           <> " " <> show (sysSize :: Int)
           <> " " <> show timestamp
           <> " " <> show firstDate
           <> " " <> show secondDate
           <> "): "

  res <- measureTimePeriod pool firstDate secondDate faultID firstFaultDay

  case res of
    Nothing -> putStrLn $ "nothing"
    Just (first, (second, v1, v2)) -> do
      putStrLn $ show (v1 !! addr - 1) <> " "
              <> show (v2 !! addr - 1)
