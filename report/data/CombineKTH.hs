module Main where

import Control.Applicative
import System.IO

main IO ()
main = do
  offsets <- (read . lines) <$> readFile "kth_offsets"
  times <- (map words . lines) <$> readFile "kth_indices"
  solar <- (map words . lines) <$> readFile "kth_isolar"

  -- lookup each offset in times, find the corresponding timestamp

  -- lookup each timestamp in the solar position table (2013, yes)
