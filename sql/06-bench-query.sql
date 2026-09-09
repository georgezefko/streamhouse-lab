-- Tutorial 4: the same question asked of two engines.
--   "What is order 424242?"
-- Fluss answers it with a primary-key point lookup. Kafka has no index, so Flink reads the
-- topic from earliest to latest offset.
-- Timings come from the Flink job durations — see scripts/bench.sh.

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123',
  'iceberg.s3.access-key-id' = 'admin',
  'iceberg.s3.secret-access-key' = 'password'
);

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

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

-- Job 1 — Fluss: full PK predicate, so this is a point lookup. Cost is flat in table size.
SELECT count(*) AS hits_fluss FROM bench_order WHERE order_key = 424242;

-- Job 2 — Kafka: no index. Every record between earliest and latest offset is deserialized
-- and filtered. Cost is linear in retention.
SELECT count(*) AS hits_kafka
  FROM `default_catalog`.`default_database`.kafka_order WHERE order_key = 424242;
