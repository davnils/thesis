{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import Control.Applicative
import Data.List.Split (splitOn)
import Data.Monoid ((<>))
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Format
import System.IO
import System.Locale

main :: IO ()
main = do
  offsets <- (map read . lines) <$> readFile "kth_offsets"
  times <- (map words . lines) <$> readFile "kth_indices"
  solar <- (map (splitOn ",") . lines) <$> readFile "kth_solar"

  -- lookup each offset in times, find the corresponding timestamp
  let timestamps = map (\off -> ((times !! off) !! 0, (times !! off) !! 1)) offsets
  mapM_ (putStrLn . show)  timestamps

  solarParsed <- mapM parseSolar solar

  -- lookup each timestamp in the solar position table (2013, yes), take first sample sat. >=
  let coords = map (getCoords solarParsed) timestamps
  mapM_ (\(x,y) -> putStrLn $ show x <> " " <> show y) coords
  where
  parseSolar (dateStr:timeStr:zenithStr:azimuthStr:_) = do
      -- print dateStr
      return $ (parseTimestamp dateStr timeStr, read azimuthStr :: Double, 90 - read zenithStr :: Double)
  parseTimestamp dateStr timeStr = let Just t = parseTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" (dayStr <> " " <> timeStr') in (t :: UTCTime)
    where
    timeStr' = if (timeStr !! 1 == ':') then '0':timeStr else timeStr
    dayStr = formatTime defaultTimeLocale "%Y-%m-%d" day
    day = let [m,d,y] = splitOn "/" dateStr in fromGregorian (read y) (read m) (read d)

  getCoords coords (dayStr, timeStr) = (\(_, x, y) -> (x, y)) . head $ dropWhile (\(t, _, _) -> t < toCompare) coords
    where
    Just toCompare = parseTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" (dayStr <> " " <> timeStr)
