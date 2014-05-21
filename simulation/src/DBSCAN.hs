module DBSCAN where

import           Control.Arrow (first)
import           Control.DeepSeq (force)
import           Control.Lens.Tuple
import           Control.Monad (when, unless)
import           Control.Monad.Trans (lift)
import qualified Control.Monad.State.Strict as S
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import           Data.List ((\\), foldl', maximumBy, union)
import           Data.Monoid ((<>))

testX=[49.4963,55.1227,105.7601,115.1374,143.7381,127.3278,170.4634,179.8407,153.5842,170.4634,214.5366,215.4744,87.4744,120.2949,127.7967,117.4817,71.0641,111.3864,112.7930,117.4817,170.4634,170.4634,162.4927,167.1813,175.6209,170.9322,171.8700,85.1300,111.8553,135.2985]
testY=[125.5732,115.0366,86.5488,57.6707,57.6707,23.3293,62.7439,97.8659,123.6220,126.7439,128.6951,110.3537,65.8659,78.3537,75.6220,74.0610,129.8659,100.5976,96.3049,92.0122,77.1829,83.4268,78.7439,90.4512,88.1098,92.7927,33.8659,29.1829,5.3780,1.8659]

tests = zip testX testY

type Node = Int

data Cluster
  = Cluster Int
  | Noise
  deriving (Eq, Ord, Show)

type ScanState = (HS.HashSet Node, Int, HM.HashMap Node Cluster)

printClusters :: [((Double, Double), Cluster)] -> IO ()
printClusters res = mapM_ (printGroup . Cluster) [1..maxCluster] >> printGroup Noise
  where
  (_, Cluster maxCluster) = maximumBy (\(_, Cluster idx1) (_, Cluster idx2) -> compare idx1 idx2) $
                              filter (\(_, c) -> c /= Noise) res

  printGroup idx = do
    putStrLn $ "c" <> (if idx == Noise then "n" else let Cluster c = idx in show c) <> " = ["
    let entries = filter (\(_, c) -> c == idx) res
    mapM_ (\((p1, p2), _) -> putStrLn $ unwords [show p1, show p2]) entries
    putStrLn "];"

dbscan inputs eps minP = fmap select $ S.execStateT allClusters (HS.fromList allIDs, 1, HM.empty)
  where
  select (_, _, clusters) = map (first (inputs !!)) $ HM.toList clusters
  allClusters             = mapM findCluster allIDs
  allIDs                  = [0 .. length inputs - 1]

  findCluster node = do
    (u, _, _) <- S.get
    when (HS.member node u) $ considerCluster node

  considerCluster node
    | sufficientRegion node = assign node >> expandCluster (region node) >> S.modify (\(u, c, m) -> (u, c + 1, m))
    | otherwise             = mark node >> S.modify (\(u, c, m) -> (u, c, HM.insert node Noise m))

  sufficientRegion node     = length (region node) >= minP

  expandCluster []         = return ()
  expandCluster (p:others) = do
    (u, _, m) <- S.get
    unless (HM.member p m) $ assign p
    let newCluster = (region p `union` others) \\ (HM.keys m `union` [p])
    when (HS.member p u) $ mark p >> when (sufficientRegion p) (expandCluster newCluster)

  mark node   = S.modify $ \(u, c, m) -> (HS.delete node u, c, m)
  assign node = S.modify $ \(u, c, m) -> (u, c, HM.insert node (Cluster c) m)

  region idx' = [idx | (idx, (px, py)) <- zip [0..] inputs, sqrt ((x - px)^2 + (y - py)^2) <= eps]
    where
    (x, y) = inputs !! idx'
