{-# Language BangPatterns, OverloadedStrings #-}

import Control.Monad (liftM2, liftM)
import Control.Monad.Trans.Free
import Data.Char (isDigit)
import qualified Data.HashMap.Strict as H
import Data.HashMap.Strict ((!))
import Data.List (minimumBy, maximumBy, foldl')
import Data.Monoid ((<>))
import Data.Ord (comparing)
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import Data.Time.Format (parseTime)
import Data.Text hiding (filter, length, putStrLn, map, take, break, insert, foldl')
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Intro as V
import Lens.Family
import Lens.Family.State.Strict
import Pipes
import qualified Pipes.Group as G
import Pipes.Parse
import qualified Pipes.Prelude as P
import Pipes.Safe
import Pipes.Text hiding (filter, length, unpack, map, take)
import Pipes.Text.IO
import Pipes.Vector
import Prelude hiding (readFile, concat)
import qualified Prelude as PL
import System.Locale (defaultTimeLocale)
import System.IO (Handle(..), IOMode(..),withFile, hPutStrLn)
import Text.Read (readMaybe)

data MeasurementType
 = Voltage
 | Current
 | Power
 deriving (Eq, Ord, Show)

data Row
 = Row UTCTime Double
 deriving (Eq, Ord, Show)

type TripleDay = Integer
type ValueTable = (Day, Day, H.HashMap TripleDay (V.Vector (UTCTime, Double)))

-- parse a single row on the format "2012-11-06 13:53:38	28.178"
parseRow :: (Functor m, Monad m) => Parser Text m (Maybe Row)
parseRow = do
  row <- zoom line $ do
    time <- parseTimeStamp
    parseToken
    val <- parseDouble
    return $ liftM2 Row time val

  drawChar
  return row

  where
  parseToken = fmap (strip . concat) $ zoom word drawAll
  parseTimeStamp = do
    date <- parseToken
    time <- parseToken
    return $ parseTime defaultTimeLocale "%F%T" (unpack $ date <> time)

  parseAddress = parseToken >> return ()

  parseDouble = fmap (readMaybe . unpack) parseToken

exhaustParser parser producer = do
  (r, producer') <- runStateT parser producer
  yield r
  exhaustParser parser producer'

dieOnFailure :: Monad m => Pipe (Maybe a) a m ()
dieOnFailure = do
  val <- await
  case val of
    Just !v -> yield v >> dieOnFailure
    Nothing -> return ()

instance MonadIO m => MonadIO (ToVector v e m) where
  liftIO = lift . liftIO

-- read and pad power table
readPowerTable :: Handle -> IO ValueTable
readPowerTable handle = do
  rows <- runToVector . runEffect $ exhaustParser parseRow (fromHandle handle) >-> dieOnFailure >-> toVector
  rowsRef <- V.thaw rows
  V.sort rowsRef
  rows' <- V.freeze rowsRef

  putStrLn $ "Read " <> show (V.length rows') <> " rows from handle" 

  let getTimestamp = \(Row t _) -> t
      comp f   = utctDay . f $ V.map getTimestamp rows'
      firstDay = comp V.minimum
      lastDay  = comp V.maximum

  return (firstDay, lastDay, snd $ foldl' buildDay (rows', H.empty) [firstDay..lastDay])
  where
  buildDay !(!rows, !table) !day = (left, table')
    where
    table' = H.insert (toModifiedJulianDay day) todaySamples' table
    todaySamples' = V.map (\(Row t v) -> (t, v)) todaySamples
    todaySamples = V.snoc (V.cons (Row firstEntry 0.0) current) (Row lastEntry 0.0)
    (current, left) = V.span (\(Row t _) -> utctDay t == day) rows

    firstEntry = time "03:00:00"
    lastEntry  = time "23:00:00"
    time = (\(UTCTime _ t) -> UTCTime day t) . (\(Just t) -> t) . parseTime defaultTimeLocale "%T"

-- write every hashtable entry in order
writePowerTable :: Handle -> ValueTable -> IO ()
writePowerTable handle (firstDay, lastDay, table) = do
  -- output "raw = ["
  mapM_ printDay [firstDay..lastDay]
  -- output "];"
  where
  printDay day = do
    let entries = table ! (toModifiedJulianDay day)
    V.forM_ entries $ \(time, val) ->
      output $ formatTime time <> " " <> show val
  formatTime = show
  --formatTime = PL.takeWhile isDigit . show . utcTimeToPOSIXSeconds 
  output = hPutStrLn handle

main :: IO ()
main =
  withFile "logs/work" ReadMode $ \input ->
  withFile "logs/proc" WriteMode $ \output ->
    readPowerTable input >>= writePowerTable output
