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

-- 2) Primary-key tables (the hot tier: upserts, point lookups, lookup joins)
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

CREATE TABLE fluss_order (
  `order_key`      BIGINT,
  `cust_key`       INT NOT NULL,
  `total_price`    DECIMAL(15, 2),
  `order_date`     DATE,
  `order_priority` STRING,
  `clerk`          STRING,
  `ptime` AS PROCTIME(),
  PRIMARY KEY (`order_key`) NOT ENFORCED
);

-- 3) The tiered table — THIS is the streamhouse table. datalake.enabled turns on tiering;
--    freshness controls how often the tiering job flushes hot -> cold.
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
  `nation_name`      STRING,
  PRIMARY KEY (`order_key`) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);