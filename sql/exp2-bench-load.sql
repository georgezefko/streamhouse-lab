-- Experiment 2 setup: one bulk telemetry topic, mirrored into a Fluss PK table.
--
--   bench-producer (scripts/iot_producer.py) ──▶ kafka: bench-telemetry ──▶ bench_telemetry (Fluss, PK)
--
-- `make bench-load` starts the producer and submits this as a detached job, then returns.
-- Run `make bench` a few times WHILE it is still loading — that is the whole point: the Kafka
-- scan grows with the topic while the Fluss point lookup stays flat. Bench a drained topic and
-- both numbers just sit still.
--
-- 20M readings over a key space of 2M (ID_MAX), so reading_id 424242 definitely exists and is
-- hit repeatedly. Prepended with sql/common/catalog.sql by the Makefile.

SET 'execution.runtime-mode' = 'streaming';

-- Kafka lives in the default catalog (the Fluss catalog only holds Fluss tables), so this is
-- fully qualified — an unqualified CREATE would land in whatever catalog is current.
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`bench_source` (
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
  'properties.group.id' = 'streamhouse-bench-load',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

-- The Fluss side: a PK table, NOT tiered. We are pricing a pure hot-tier point lookup; with
-- datalake.enabled the bare table becomes a union read, which Iceberg cannot do on a PK table
-- at all (see docs/EXPLANATION.md). Experiment 1 already prices the cold tier.
CREATE TABLE bench_telemetry (
  `reading_id`      BIGINT NOT NULL,
  `device_id`       STRING,
  `event_time`      TIMESTAMP(3),
  `energy_usage`    DOUBLE,
  `temperature`     DOUBLE,
  `vibration`       DOUBLE,
  `signal_strength` INT,
  PRIMARY KEY (`reading_id`) NOT ENFORCED
);

INSERT INTO bench_telemetry SELECT * FROM `default_catalog`.`default_database`.bench_source;
