-- Phase 4 setup: one bulk stream, two homes — a Fluss PK table and a Kafka topic.
-- Run this in `make sql` and LEAVE IT RUNNING, then `make bench` in another shell.
--
-- Both sides get their own faker scan, so the individual rows differ. That is fine: the
-- benchmark measures what it COSTS to answer "where is order N", not row identity. Same
-- volume, same key space, two storage engines.

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123',
  'iceberg.s3.access-key-id' = 'admin',
  'iceberg.s3.secret-access-key' = 'password'
);

SET 'execution.runtime-mode' = 'streaming';

-- 2M rows over the same 2M key space, fast enough that the topic gets big in ~2 minutes.
CREATE TEMPORARY TABLE bench_source (
  `order_key`   BIGINT,
  `cust_key`    INT,
  `total_price` DECIMAL(15, 2)
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '20000',
  'number-of-rows' = '2000000',
  'fields.order_key.expression'   = '#{number.numberBetween ''1'',''2000000''}',
  'fields.cust_key.expression'    = '#{number.numberBetween ''1'',''50000''}',
  'fields.total_price.expression' = '#{number.randomDouble ''2'',''1'',''10000''}'
);

-- Kafka lives in the default catalog (the Fluss catalog only holds Fluss tables).
-- scan.bounded.mode is what makes this readable in batch at all — and it is the whole point:
-- to answer one point query, Kafka hands you every offset from earliest to latest.
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`kafka_order` (
  `order_key`   BIGINT,
  `cust_key`    INT,
  `total_price` DECIMAL(15, 2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'bench_orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset'
);

USE CATALOG fluss_catalog;

-- The Fluss side: a PK table. datalake.enabled so the same rows also land in Iceberg,
-- giving the third leg of the benchmark (cold scan) for free off the existing tiering job.
CREATE TABLE bench_order (
  `order_key`   BIGINT,
  `cust_key`    INT,
  `total_price` DECIMAL(15, 2),
  PRIMARY KEY (`order_key`) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO bench_order SELECT * FROM `default_catalog`.`default_database`.bench_source;
  INSERT INTO `default_catalog`.`default_database`.kafka_order
    SELECT * FROM `default_catalog`.`default_database`.bench_source;
END;
