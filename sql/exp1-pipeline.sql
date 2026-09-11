-- Experiment 1, part B: the pipeline. Kafka -> Fluss (hot) -> Iceberg on MinIO (cold).
--
--   kafka: iot-telemetry ─┐
--                         ├─▶ iot_telemetry (log) ──lookup join dim_device──▶ enriched
--   kafka: iot-events   ──┴─▶ iot_events (log, tiered)                          │
--                                                                               ├─▶ datalake_device_telemetry   (per reading)
--                                                                               └─▶ datalake_device_health_1min (1-min window)
--
-- Requires the topics to exist — start the producer first: `make produce`
-- (scripts/iot_producer.py), or point your own producer at iot-telemetry / iot-events with
-- the same field names and ISO-8601 timestamps.
--
-- Paste into an interactive session (`make sql`), AFTER sql/common/catalog.sql.

-- ─────────────────────────────────────────────────────────────────────────────
-- 1) The device dimension. A PK table: this is the lookup-join build side, where
--    the point lookups actually happen. Not tiered — it is 11 rows and it is hot.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE dim_device (
  `device_id`      STRING NOT NULL,
  `temp_threshold` DOUBLE,
  `location_id`    STRING,
  `model`          STRING,
  `status`         STRING,
  PRIMARY KEY (`device_id`) NOT ENFORCED
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 2) The hot landing tables. Both LOG tables (no PK).
--    Reading a PK table in streaming mode emits -U/+U, and an append-only sink
--    rejects that. Everything downstream here is append-only, so these must be too.
--    See docs/EXPLANATION.md.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE iot_telemetry (
  `reading_id`      BIGINT,
  `device_id`       STRING NOT NULL,
  `event_time`      TIMESTAMP(3),
  `energy_usage`    DOUBLE,
  `temperature`     DOUBLE,
  `vibration`       DOUBLE,
  `signal_strength` INT,
  `ptime` AS PROCTIME()
);

-- Tiered as well, so StarRocks can read events from the cold tier in part D.
-- No PK, for the same union-read reason as the datalake_* tables below.
-- The type-specific columns are sparse: only the ones belonging to a row's
-- event_type are populated, exactly as they arrive on the topic.
CREATE TABLE iot_events (
  `device_id`   STRING NOT NULL,
  `event_time`  TIMESTAMP(3),
  `event_type`  STRING,
  `severity`    STRING,
  `error_code`  STRING,
  `component`   STRING,
  `root_cause`  STRING,
  `technician`  STRING,
  `duration_min` INT,
  `parts_replaced` STRING,
  `status`      STRING,
  `next_inspection_days` INT
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 3) The tiered tables — THESE are the streamhouse tables.
--
--    NO PRIMARY KEY, deliberately. Union read (querying the bare table = hot ∪ cold)
--    merges the lake snapshot with the Fluss log; on a PK table that merge is a
--    sort-merge, which needs the lake reader to implement Fluss's SortedRecordReader.
--    fluss-lake-iceberg 0.9.1 does not, and the read dies with
--    "lake records must instance of sorted view". Log tables concatenate instead.
-- ─────────────────────────────────────────────────────────────────────────────

-- Per-reading, enriched with the device's own threshold. Rows appear immediately,
-- which is what makes the hot-vs-cold contrast visible within seconds.
--
-- In the reference lambda pipeline this is the point where anomalies were published
-- back onto a THIRD Kafka topic and Routine-Loaded into StarRocks — a second copy,
-- kept in sync by hand. Here the row is queryable the instant it lands and tiers
-- itself into Iceberg. That is the whole argument; see docs/EXPLANATION.md.
CREATE TABLE datalake_device_telemetry (
  `reading_id`      BIGINT,
  `device_id`       STRING NOT NULL,
  `event_time`      TIMESTAMP(3),
  `ingest_time`     TIMESTAMP(3),
  `energy_usage`    DOUBLE,
  `temperature`     DOUBLE,
  `vibration`       DOUBLE,
  `signal_strength` INT,
  `temp_threshold`  DOUBLE,
  `anomaly_flag`    BOOLEAN,
  `vibration_spike` BOOLEAN,
  `location_id`     STRING,
  `model`           STRING
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- The analytical fact table — the reference pipeline's fact_telemetry_5min, at
-- experiment time-scale. Dropped vs the original: cnt_events / events_* (needs a
-- stream-stream join) and the incomplete_by_* flags (need event time + watermarks).
-- ponytail: no event counts here; join iot_events at read time instead (sql/exp1-starrocks.sql).
--
-- cnt_anomalies is what carries the signal. anomaly_flag is the original's
-- max()>threshold rule, kept for parity, but over a full minute of readings the max
-- almost always clears the threshold — so it is TRUE for nearly every window and
-- ranks nothing. Rank on the anomaly RATE (cnt_anomalies / cnt_points) instead.
CREATE TABLE datalake_device_health_1min (
  `device_id`        STRING NOT NULL,
  `window_start`     TIMESTAMP(3),
  `window_end`       TIMESTAMP(3),
  `cnt_points`       BIGINT,
  `cnt_anomalies`    BIGINT,
  `cnt_vib_spikes`   BIGINT,
  `avg_temperature`  DOUBLE,
  `min_temperature`  DOUBLE,
  `max_temperature`  DOUBLE,
  `avg_energy_usage` DOUBLE,
  `max_vibration`    DOUBLE,
  `min_signal`       INT,
  `threshold_used`   DOUBLE,
  `anomaly_flag`     BOOLEAN,
  `location_id`      STRING,
  `model`            STRING
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4) Seed the dimension. dml-sync makes this block until it FINISHES, so the
--    lookup joins below never start against an empty dimension.
--
--    Thresholds are spread 24-29 °C across the 11 devices. The producer draws
--    temperature uniformly from 18-30 °C, so device_1 (24.0) sits over its
--    threshold about half the time and device_11 (29.0) about a twelfth — which is
--    what makes the part-D ranking come out ordered by threshold.
-- ─────────────────────────────────────────────────────────────────────────────
SET 'table.dml-sync' = 'true';

INSERT INTO dim_device VALUES
  ('device_1',  24.0, 'plant_a', 'Model-A', 'active'),
  ('device_2',  24.5, 'plant_b', 'Model-A', 'active'),
  ('device_3',  25.0, 'plant_c', 'Model-B', 'active'),
  ('device_4',  25.5, 'plant_a', 'Model-B', 'active'),
  ('device_5',  26.0, 'plant_b', 'Model-A', 'active'),
  ('device_6',  26.5, 'plant_c', 'Model-C', 'active'),
  ('device_7',  27.0, 'plant_a', 'Model-C', 'active'),
  ('device_8',  27.5, 'plant_b', 'Model-D', 'active'),
  ('device_9',  28.0, 'plant_c', 'Model-D', 'active'),
  ('device_10', 28.5, 'plant_a', 'Model-B', 'active'),
  ('device_11', 29.0, 'plant_b', 'Model-A', 'active');

-- Back to detached: everything below should submit and return immediately.
SET 'table.dml-sync' = 'false';

-- ─────────────────────────────────────────────────────────────────────────────
-- 5) The Kafka sources. This is the seam: Fluss sits BEHIND the broker you already
--    have, it does not replace it.
--
--    'earliest-offset' so re-running this picks up everything already produced.
--    'json.timestamp-format.standard' = 'ISO-8601' must match the producer — see the
--    note in scripts/iot_producer.py. 'json.ignore-parse-errors' keeps one malformed message from
--    killing the job, which is what you want against a real topic.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`src_telemetry` (
  `reading_id`      BIGINT,
  `device_id`       STRING,
  `event_time`      TIMESTAMP(3),
  `energy_usage`    DOUBLE,
  `temperature`     DOUBLE,
  `vibration`       DOUBLE,
  `signal_strength` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot-telemetry',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'streamhouse-telemetry',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`src_events` (
  `device_id`   STRING,
  `event_time`  TIMESTAMP(3),
  `event_type`  STRING,
  `severity`    STRING,
  `error_code`  STRING,
  `component`   STRING,
  `root_cause`  STRING,
  `technician`  STRING,
  `duration_min` INT,
  `parts_replaced` STRING,
  `status`      STRING,
  `next_inspection_days` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot-events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'streamhouse-events',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 6) Land the topics in Fluss. ONE detached job with two sinks — an EXECUTE STATEMENT SET
--    compiles into a single JobGraph, which is why the Flink UI shows both sink names on one row.
--    From here on the data is indexed and queryable — which it was not on the topic.
-- ─────────────────────────────────────────────────────────────────────────────
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO iot_telemetry
  SELECT reading_id, device_id, event_time, energy_usage, temperature, vibration, signal_strength
  FROM `default_catalog`.`default_database`.src_telemetry;

  INSERT INTO iot_events
  SELECT * FROM `default_catalog`.`default_database`.src_events;
END;

-- ─────────────────────────────────────────────────────────────────────────────
-- 7) Enrich once, consume twice. The lookup join is the point-lookup workload:
--    one PK read against dim_device per incoming reading.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`enriched` AS
SELECT t.reading_id,
       t.device_id,
       t.event_time,
       t.energy_usage,
       t.temperature,
       t.vibration,
       t.signal_strength,
       t.ptime,
       d.temp_threshold,
       d.location_id,
       d.`model`
FROM fluss_catalog.fluss.iot_telemetry t
LEFT JOIN fluss_catalog.fluss.dim_device FOR SYSTEM_TIME AS OF t.ptime AS d
  ON t.device_id = d.device_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- 8) Derive both tiered tables. One more detached job, again two sinks off one enriched stream:
--    the per-reading table and the windowed aggregate read the same view, not each other.
--
--    The window is PROCESSING time. A proctime tumble needs no watermark and emits
--    append-only, which is exactly what a log-table sink can consume. The reference
--    pipeline used event time because it simulated late data and outages; those are
--    out of scope here.
--    ponytail: proctime window; switch to event-time + WATERMARK when late data matters.
--
--    1 minute, not the original's 5 — so the fact table produces rows inside an experiment.
-- ─────────────────────────────────────────────────────────────────────────────
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO datalake_device_telemetry
  SELECT reading_id,
         device_id,
         event_time,
         CURRENT_TIMESTAMP,
         energy_usage,
         temperature,
         vibration,
         signal_strength,
         temp_threshold,
         temperature > temp_threshold,
         vibration > 3.0,
         location_id,
         `model`
  FROM `default_catalog`.`default_database`.enriched;

  INSERT INTO datalake_device_health_1min
  SELECT device_id,
         window_start,
         window_end,
         count(*),
         sum(CASE WHEN temperature > temp_threshold THEN 1 ELSE 0 END),
         sum(CASE WHEN vibration > 3.0 THEN 1 ELSE 0 END),
         avg(temperature),
         min(temperature),
         max(temperature),
         avg(energy_usage),
         max(vibration),
         min(signal_strength),
         max(temp_threshold),
         max(temperature) > max(temp_threshold),
         max(location_id),
         max(`model`)
  FROM TABLE(
    TUMBLE(TABLE `default_catalog`.`default_database`.enriched, DESCRIPTOR(ptime), INTERVAL '1' MINUTE)
  )
  GROUP BY device_id, window_start, window_end;
END;

-- Next:  make tiering     (start moving hot -> cold)
-- Then:  sql/exp1-live.sql in this session, or `make demo` in another shell.
