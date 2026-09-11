-- Experiment 2: the same question asked of two engines.
--   "What is reading 424242?"
-- Fluss answers it with a primary-key point lookup. Kafka has no index, so Flink reads the
-- topic from earliest to latest offset.
-- Timings come from the Flink job durations — see scripts/bench.sh, which also prepends
-- sql/common/catalog.sql.

-- scan.bounded.mode is what makes the topic readable in batch at all — and it is the whole
-- point: to answer one point query, Kafka hands you every offset from earliest to latest.
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`kafka_telemetry` (
  `reading_id`      BIGINT,
  `device_id`       STRING,
  `event_time`      TIMESTAMP(3),
  `energy_usage`    DOUBLE,
  `temperature`     DOUBLE,
  `vibration`       DOUBLE,
  `signal_strength` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'bench-telemetry',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset'
);

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

-- The two counts differ on purpose: Fluss keeps one row per reading_id (later copies upsert
-- over it), the topic keeps every copy. Same question, two storage models.

-- Job 1 — Fluss: full PK predicate, so this is a point lookup. Cost is flat in table size.
SELECT count(*) AS hits_fluss FROM bench_telemetry WHERE reading_id = 424242;

-- Job 2 — Kafka: no index. Every record between earliest and latest offset is deserialized
-- and filtered. Cost is linear in retention.
SELECT count(*) AS hits_kafka
  FROM `default_catalog`.`default_database`.kafka_telemetry WHERE reading_id = 424242;
