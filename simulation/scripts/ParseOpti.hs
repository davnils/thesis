{-# LANGUAGE BangPatterns, OverloadedStrings #-}

module Main where

import           Control.DeepSeq (force, NFData)
import           Control.Monad (liftM3)
import qualified Data.List as L
import           Data.Monoid ((<>))
import           Data.Time.Clock
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Time.Format (parseTime)
import qualified Data.Text as T
import           Lens.Family.State.Strict
import           Pipes
import           Pipes.Parse
import           Prelude
import qualified Pipes.Prelude as P
import           Pipes.Text hiding (filter, length, unpack, map, take)
import           Pipes.Text.IO hiding (stdin)
import qualified Prelude as PL
import           System.IO (hClose, hPutStrLn, openFile, IOMode(ReadMode, WriteMode), stdin, withFile)
import           System.Locale (defaultTimeLocale)
import           Text.Read (readMaybe)

data Sample
  = Voltage !Float
  | Current !Float
  deriving (Eq, Ord, Show)

data Row
  = Row !UTCTime !Char !Sample
  deriving (Eq, Ord, Show)

instance NFData Row

parseRow :: (Functor m, Monad m) => Parser T.Text m (Maybe Row)
parseRow = do
  row <- zoom line $ do
    time <- parseTimeStamp
    addr <- fmap validateAddress parseToken

    kind <- parseToken
    val <- parseDouble
    let val' = case kind of
          "VOLTAGE" -> fmap Voltage val
          "CURRENT" -> fmap Current val
          _         -> error "Invalid measurement type"
    return $ liftM3 Row time addr val'

  _ <- drawChar
  return row

  where
  parseToken = fmap (T.strip . T.concat) $ zoom word drawAll
  parseTimeStamp = do
    date <- parseToken
    time <- parseToken
    return $ parseTime defaultTimeLocale "%F%T" (T.unpack $ date <> time)

  parseDouble = fmap (readMaybe . T.unpack) parseToken

  validateAddress str
    | str == "ma"                                       = Just 'x'
    | elem (T.head str) ['a'..'z'] && T.length str == 1 = Just $ T.head str
    | otherwise                                        = Nothing

exhaustParser parser producer = do
  (!r, producer') <- runStateT parser producer
  yield r
  exhaustParser parser producer'

dieOnFailure :: Monad m => Pipe (Maybe a) a m ()
dieOnFailure = do
  val <- await
  case val of
    Just !v -> yield v >> dieOnFailure
    Nothing -> return ()

process handle = fmap force $ runEffect . P.toListM $ exhaustParser parseRow (fromHandle handle) >-> dieOnFailure

grouped raw = map (L.sortBy $ \(Row _ a1 _) (Row _ a2 _) -> compare a1 a2) $ L.groupBy (\(Row t1 _ _) (Row t2 _ _) -> t1 == t2) raw

refLength = 32

filtered = filter (\l -> length l == refLength)

outputs = map $ map (\(Row _ _ val) -> val)

-- create output for matlab
writeOutput h pred list = hPutStrLn h "pow_=[" >> go h list >> hPutStrLn h "];"
  where
  go _ [] = return ()
  go h (x:xs) = do
    let row = PL.unwords $ map (PL.show . unbox) $ filter pred x
    hPutStrLn h row
    go h xs
  unbox (Current f) = f
  unbox (Voltage f) = f

-- discard samples outside of [03, 23]
-- then call 'checkDayBoth' with appropiate params over all days
-- study what happens when there are repeated errors <- (not supported!)

main :: IO ()
main = do
  input <- openFile "logs/kth/kth_new" ReadMode
  result <- fmap (outputs . filtered . grouped) (process input)

  withFile "logs/kth/out_volt.m" WriteMode $ \h -> do
    writeOutput h grabVoltage result

  withFile "logs/kth/out_curr.m" WriteMode $ \h -> do
    writeOutput h grabCurrent result

  hClose input

  where
  grabVoltage s = case s of
    Voltage _ -> True
    Current _ -> False

  grabCurrent s = case s of
    Current _ -> True
    Voltage _ -> False
