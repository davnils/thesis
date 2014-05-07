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
measureTimePeriod :: DB.Pool -> Int -> Day -> Day -> Int -> Day -> IO (Maybe [(Float, Float)])
measureTimePeriod pool addr firstWindow secondWindow faultID firstFaultDay = do
  [first, second] <- fmap force $ mapM (fetchMonth pool) [firstWindow, secondWindow]
  let maxSamples = U.length ((\(x,_,_) -> x) (first ! 0))

  res <- fmap PL.concat . forM [0..maxSamples-1] $ \idx -> do
    indicators <- fmap force $ forM [idx..maxSamples-1] $ \idx' -> do
      let grabVec f vec step = V.map ((!step) . f) $ vec
          vecDiffs f g       = V.map g $ V.zip (grabVec f first idx) (grabVec f second idx')
          classify f (lim, eps) = V.map (\(v, ref) -> if v <= eps && ref >= lim then 1 else 0) $ vecDiffs f (\(x,y) -> (abs $ x - y, min x y))
          (vec1, vec2, vec3) = (classify (\(x,_,_) -> x) uParams, classify (\(_,y,_) -> y) iParams, classify (\(_,_,z) -> z) tParams)
          moduleSum          = sum $ map (\m -> if (vec1 ! m) + (vec2 ! m) + (vec3 ! m) == 3 then 1 else 0) [0..modules-1]

      let outputFunction (x, y) = y / x
      if moduleSum >= modules - allowedFailures then do
        return $ force $ [(idx', V.toList $ vecDiffs (\(x,_,_) -> x) outputFunction, V.toList $ vecDiffs (\(_,y,_) -> y) outputFunction)]
        else return []

    return . force . map (\pair -> (idx, pair)) $ PL.concat indicators 

  putStrLn $ show (length res) <> " samples"

  -- Produce degradation numbers for all solar panels, as averages of the saved values (if any)
  case convert res of
    []    -> return $ Nothing
    xs    -> return $ Just (map (\(v1, v2) -> (average v1, average v2)) xs)

  where
  system = 1
  modules = 24
  allowedFailures = 1

  uParams = (20, 0.45)
  iParams = (0.5, 0.2)
  tParams = (-300, 1)

  convert :: [(x, (y, [b], [c]))] -> [([b], [c])]
  convert [(_, (_, volt, curr))]      = PL.map (\(u, i) -> ([u], [i])) $ PL.zip volt curr
  convert ((_, (_, volt, curr)) : xs) = map (\(u, i, (u', i')) -> (u : u', i : i')) $ PL.zip3 volt curr rest
    where
    rest = convert xs

  average :: [Float] -> Float
  average list = (1 / (realToFrac $ length list)) * sum list

  -- fetch month (voltage, current, temp) based on the first day
  fetchMonth :: DB.Pool -> Day -> IO (V.Vector Triple)

  -- ... -> V.Vector (U, U) -> V.Vector U -> V.Vector (U, U, U)
  fetchMonth pool day     = do
    (v1, v2) <- fmap V.unzip (fetchOthers pool day)
    v3 <- getTempVectors pool day
    return $ V.zip3 v1 v2 v3

  fetchOthers pool day    = fmap merge $ mapM (retrieveDayValues pool system modules faultID addr firstFaultDay) (take 28 [day ..])
  getTempVectors pool day = fmap (mergeTemp . PL.map (V.map (\(_,_,t) -> t))) $ mapM (retrieveDay system modules) (take 28 [day ..])

  -- need something like [V.Vec (U.Vec a)] -> V.Vec (U.Vec a)
  -- take each segment, append the subresult
  --
  mergeTemp [x]    = x
  mergeTemp (x:xs) = V.map (\(v1, v2) -> v1 <> v2) $ V.zip subResult x
    where
    subResult = mergeTemp xs


-- implement [V.Vec (U, U, ,U)] -> V.Vec (U, U, ,U) while maintaining constant outer dimension
merge :: [V.Vector (U.Vector Float, U.Vector Float)] -> V.Vector (U.Vector Float, U.Vector Float)
merge [vec]    = vec
merge (vec:xs) = V.zip (build v1 t1) (build v2 t2)
  where
  (v1, v2) = V.unzip vec
  (t1, t2) = V.unzip $ merge xs
  build v t = V.map (\(x,xs) -> x <> xs) $ V.zip v t

checkDegrads :: Int -> Int -> IO ()
checkDegrads firstFault lastFault = do
  pool <- DB.getPool
  mapM_ (checkDegrad pool) [firstFault..lastFault]

checkDegrad :: DB.Pool -> Int -> IO ()
checkDegrad pool faultID = do
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
           <> " " <> show uFactor
           <> " " <> show iFactor
           <> " " <> show addr
           <> "): "

  res <- measureTimePeriod pool addr firstDate secondDate faultID firstFaultDay

  case res of
    Nothing -> putStrLn $ "nothing"
    Just res -> putStrLn $ "averages: " <> (PL.unwords $ PL.map (\(x,y) -> show x <> " " <> show y) res)
  mapM_ hFlush [stderr, stdout]
