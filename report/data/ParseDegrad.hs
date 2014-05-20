module Main where

import Control.DeepSeq
import Data.List (transpose)
import Data.List.Split (chunksOf)
import Data.Monoid ((<>))
import System.Environment
import System.IO

main :: IO ()
main = do
  [kind] <- getArgs
  input <- fmap (map words . lines) $ readFile "combined_degrad"

  let pairs = head . transpose . chunksOf 2 $ zip input (tail input)
  -- pairs' <-  mapM (\p -> (putStrLn $ "processing " <> show p) >> hFlush stdout >> (return $!! ( convertEntry kind p))) pairs
  let pairs' = map (convertEntry kind) pairs

  putStrLn "Processed all rows"

  -- get ratios for all non-faulty (as one long vector)
  -- get error Deltas for all faulty modules, save into one vector

  let ratios = concat $ map getRatios pairs':: [Double]
      errors = map getError pairs' :: [Double]

  let printVec = unwords . map show
  writeFile (kind <> "_ratio") (printVec ratios)
  writeFile (kind <> "_error") (printVec errors)

  where
  convertEntry mode (line1, line2) =
    (read $ if mode == "voltage" then line1 !! 8 else line1 !! 9,
     read $ takeWhile (/= ')') $ line1 !! 10,
     if mode == "voltage" then both !! 0 else both !! 1)
    where
    both = transpose . chunksOf 2 $ map read (drop 1 line2)

  getRatios (degrad, addr, vals) = take (addr - 1) vals <> drop addr vals
  getError (degrad, addr, vals) = degrad - (vals !! (addr - 1))
