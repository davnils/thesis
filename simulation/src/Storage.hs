{-# Language
  OverloadedStrings #-}

--------------------------------------------------------------------
-- |
-- Module: .
--
-- Module defining all settings related to Cassandra and all schemas.

module Storage where

import qualified Data.Text                       as T
import           Data.Monoid                     ((<>))
import qualified Database.Cassandra.CQL          as DB
import           Network                         (HostName)
import           Network.Socket                  (ServiceName)

type SystemID = Int

getPool = DB.newPool [(cassandraHost, cassandraPort)] "thesis"

-- | Field in Cassandra table.
type Field = T.Text

-- | Type of field in Cassandra table.
type CassandraType = T.Text

-- | Cassandra table in the market keyspace.
data CassandraTable
 = CassandraTable                         -- ^ Describe a table.
 {
   _tableName :: T.Text,                  -- ^ Name of table.
   _schema :: [(Field, CassandraType)],   -- ^ Schema as value-type tuples.
   _schemaProperties :: T.Text            -- ^ Additional suffix used by create table.
 }

-- | Retrieve all fields stored in a table.
cassandraTableFields :: CassandraTable -> [Field]
cassandraTableFields = filter (/= "primary key") . map fst . _schema

-- | Retrieve all fields in a table as a string wrapped with parentheses.
tableFieldsStr :: CassandraTable -> T.Text
tableFieldsStr = flatten . cassandraTableFields
  where
  flatten str = "(" <> T.intercalate ", " str <> ")"

-- | Build a schema string, as used by the create table command, from a table description.
buildCassandraSchema :: CassandraTable -> T.Text
buildCassandraSchema s = "(" <> T.intercalate ", " schemaStr <> ")" <> suffix (_schemaProperties s)
  where
  schemaStr = map (\(field, fieldType) -> field <> " " <> fieldType) $ _schema s
  suffix "" = ""
  suffix str = " " <> str

-- | Cassandra host to be used.
cassandraHost :: HostName
cassandraHost = "localhost"

-- | Cassandra port to be used.
cassandraPort :: ServiceName
cassandraPort = "9042"

-- | Cassandra keyspace to be used.
marketKeyspace :: DB.Keyspace
marketKeyspace = "market_data"

-- | Table storing order all simulation data.
simulationTable :: CassandraTable
simulationTable = CassandraTable
  "simulation"
  [("system",  "int"),
   ("module",  "int"),
   ("date",    "timestamp"),
   ("voltage", "list<float>"),
   ("current", "list<float>"),
   ("primary key", "((system, date), module)")]
   ""
