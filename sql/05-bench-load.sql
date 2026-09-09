-- Tutorial 4 setup: one bulk stream, two homes — a Fluss PK table and a Kafka topic.
-- Run this in `make sql` and LEAVE IT RUNNING, then `make bench` in another shell —
-- repeatedly, while it is still loading.
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

-- 20M rows at 20k/s ≈ 17 minutes of load. Bounded on purpose (no runaway disk), but long enough
-- to run `make bench` several times WHILE it loads — which is the whole point: the Kafka scan
-- grows with the topic while the Fluss point lookup stays flat. Bench a drained topic and both
-- numbers just sit still.
-- Fully qualified on purpose: an unqualified CREATE lands in whatever catalog is current, so
-- pasting this after sql/01 (which ends in USE CATALOG fluss_catalog) would put it in the wrong
-- place and the statement set below would not find it.
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`bench_source` (
  `order_key`   BIGINT,
  `cust_key`    INT,
  `total_price` DECIMAL(15, 2)
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '20000',
  'number-of-rows' = '20000000',
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

-- The Fluss side: a PK table, NOT tiered. We want to price a pure hot-tier point lookup; with
-- datalake.enabled the bare table becomes a union read, which Iceberg cannot do on a PK table
-- (see sql/01). Tutorial 2 already prices the cold tier.
CREATE TABLE bench_order (
  `order_key`   BIGINT,
  `cust_key`    INT,
  `total_price` DECIMAL(15, 2),
  PRIMARY KEY (`order_key`) NOT ENFORCED
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO bench_order SELECT * FROM `default_catalog`.`default_database`.bench_source;
  INSERT INTO `default_catalog`.`default_database`.kafka_order
    SELECT * FROM `default_catalog`.`default_database`.bench_source;
END;
