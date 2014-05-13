module DBSCAN where

import           Control.DeepSeq (force)
import           Control.Monad (when)
import           Control.Monad.Trans (lift)
import qualified Control.Monad.State.Strict as S
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import           Data.List ((\\), foldl', union)
import           Data.Monoid ((<>))
import qualified Data.PKTree as PK

type Node = Int

data Cluster
  = Cluster Int
  | Noise
  deriving (Eq, Ord, Show)

type ScanState = (HS.HashSet Node, Int, HM.HashMap Node Cluster)

dbscan inputs eps minP = fmap select $ S.execStateT allClusters (HS.fromList allIDs, 1, HM.empty)
  where
  select (_, _, clusters) = map (\(x,y) -> (inputs !! x, y)) $ HM.toList clusters
  allClusters = mapM findCluster allIDs
  allIDs      = [0 .. (length inputs) - 1]

  findCluster node = do
    (u, _, _) <- S.get
    when (HS.member node u) $ {-mark node >>-} considerCluster node

  considerCluster node
    | sufficientRegion node = lift (putStrLn $ "sufficient node=" <> show node) >> assign node >> expandCluster (region node) >> S.modify (\(u, c, m) -> (u, c + 1, m))
    | otherwise             = lift (putStrLn $ "not sufficient node=" <> show node) >> mark node >> (S.modify $ \(u, c, m) -> (u, c, HM.insert node Noise m))

  sufficientRegion node = length (region node) >= minP

  -- expand the current point set into the current cluster
  expandCluster []         = return ()
  expandCluster (p:others) = do
    lift . putStrLn $ "expandCluster called with p=" <> show p <> " and others=" <> show others <> ", region follows:"
    lift . print $ region p
    (u, _, m) <- S.get
    when (not $ HM.member p m) $ assign p
    let newCluster = (region p `union` others) \\ (HM.keys m `union` [p])
    lift . putStrLn $ "newCluster=" <> show newCluster
    when (HS.member p u) $ mark p >> when (sufficientRegion p) (lift (putStrLn "recursing") >> expandCluster newCluster)

  mark node   = S.modify $ \(u, c, m) -> (HS.delete node u, c, m)
  assign node = S.modify $ \(u, c, m) -> (u, c, HM.insert node (Cluster c) m)

  region idx' = [idx | (idx, (px, py)) <- (zip [0..] inputs), sqrt ((x - px)^2 + (y - py)^2) <= eps]
    where
    (x, y) = inputs !! idx'
