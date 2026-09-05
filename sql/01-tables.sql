-- Run inside the Flink SQL client:  docker compose run sql-client
-- The quickstart image pre-creates faker-backed source tables in the default_catalog
-- (source_order, source_customer, source_nation) so you have a live stream to ingest.

-- 1) Fluss catalog. The lake (Iceberg/Nessie) config is inherited from the Fluss servers;
--    the s3 creds here let the SQL client read $lake tables directly.
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123',
  'iceberg.s3.access-key-id' = 'admin',
  'iceberg.s3.secret-access-key' = 'password'
);

USE CATALOG fluss_catalog;

-- 2) The hot tier. PK tables give upserts + point lookups (and are the lookup-join build
--    sides); fluss_order is a log table — see the note below.
CREATE TABLE fluss_customer (
  `cust_key`   INT NOT NULL,
  `name`       STRING,
  `phone`      STRING,
  `nation_key` INT NOT NULL,
  `acctbal`    DECIMAL(15, 2),
  `mktsegment` STRING,
  PRIMARY KEY (`cust_key`) NOT ENFORCED
);

CREATE TABLE fluss_nation (
  `nation_key` INT NOT NULL,
  `name`       STRING,
  PRIMARY KEY (`nation_key`) NOT ENFORCED
);

-- fluss_order is a LOG table (no PK). Reading a PK table in streaming mode yields a changelog
-- (-U/+U), and the append-only sink below cannot consume that:
--   "Table sink ... doesn't support consuming update and delete changes"
-- It is only the probe side of the lookup joins, so it loses nothing by being append-only.
-- The lookup-join build sides (fluss_customer, fluss_nation) stay PK tables — that is where
-- the point lookups actually happen.
CREATE TABLE fluss_order (
  `order_key`      BIGINT,
  `cust_key`       INT NOT NULL,
  `total_price`    DECIMAL(15, 2),
  `order_date`     DATE,
  `order_priority` STRING,
  `clerk`          STRING,
  `ptime` AS PROCTIME()
);

-- 3) The tiered table — THIS is the streamhouse table. datalake.enabled turns on tiering;
--    freshness controls how often the tiering job flushes hot -> cold.
--
-- NO PRIMARY KEY, deliberately. Union read (querying the bare table = hot ∪ cold) merges the
-- lake snapshot with the Fluss log; on a PK table that merge is a sort-merge, so it needs the
-- lake reader to implement Fluss's SortedRecordReader. fluss-lake-iceberg 0.9.1 does not, and
-- the read dies with "lake records must instance of sorted view". Log tables concatenate
-- instead of merging, so they union-read fine. See README, Scenario 2.
CREATE TABLE datalake_enriched_orders (
  `order_key`        BIGINT,
  `cust_key`         INT NOT NULL,
  `total_price`      DECIMAL(15, 2),
  `order_date`       DATE,
  `order_priority`   STRING,
  `clerk`            STRING,
  `cust_name`        STRING,
  `cust_phone`       STRING,
  `cust_acctbal`     DECIMAL(15, 2),
  `cust_mktsegment`  STRING,
  `nation_name`      STRING
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);