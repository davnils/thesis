{-# Language OverloadedStrings #-}

module Main where

import Control.Monad (void)
import Data.Maybe (fromMaybe)
import Data.Time.Clock
import Data.Time.Format (parseTime)
import Prelude
import qualified Prelude as PL
import System.Environment (getArgs)
import System.Locale (defaultTimeLocale)
import Text.Read (readMaybe)

import Classify
import Fault
import Generate
import Retrieve
import Measure

parseCmd :: [String] -> IO ()

parseCmd ["get_year", systemStr, moduleStr, yearStr] = retrieveYear system modules year >>= outputMatlab "output"
  where
  year      = read yearStr
  modules   = read moduleStr 
  system    = read systemStr

parseCmd ["get_year_sep", systemStr, moduleStr, yearStr] = retrieveYearSep system modules year
  where
  year      = read yearStr
  modules   = read moduleStr 
  system    = read systemStr

parseCmd ["get", systemStr, moduleStr, dateStr] = retrieveDay system modules (utctDay date) >>= outputMatlab "output"
  where
  Just date = parseTime defaultTimeLocale "%F" dateStr
  modules   = read moduleStr 
  system    = read systemStr

parseCmd ["gen", systemStr, startYearStr, endYearStr] = generateYears startYear endYear system
  where
  system    = read systemStr
  startYear = read startYearStr
  endYear   = fromMaybe startYear $ readMaybe endYearStr

parseCmd ("fault":systemsStr:yearStr:firstStr:lastStr:finalYearStr) =
  void $ generateFaults systems year finalYear firstFault lastFault
  where
  systems    = read systemsStr
  year       = read yearStr
  firstFault = read firstStr
  lastFault  = read lastStr
  finalYear  = if null finalYearStr then Nothing else Just (read $ head finalYearStr)

parseCmd ["classify", firstStr, lastStr] = classify firstFault lastFault
  where
  firstFault = read firstStr
  lastFault  = read lastStr

parseCmd ["degrad", firstStr, lastStr] = checkDegrads firstFault lastFault
  where
  firstFault = read firstStr
  lastFault  = read lastStr

parseCmd _ = mapM_ putStrLn commands
  where
  commands = ["Invalid command, use:",
              "./driver get system #modules date",
              "./driver get_year system #modules year",
              "./driver gen system start_year end_year",
              "./driver fault #systems year first last [final_year]",
              "./driver classify first last"]

main :: IO ()
main = getArgs >>= parseCmd
