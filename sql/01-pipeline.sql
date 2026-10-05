-- Step 2: the pipeline. Kafka -> Fluss (hot) -> Iceberg on MinIO (cold).
--
--   kafka: iot-telemetry ─┐
--                         ├─▶ iot_telemetry (log, tiered) ─lookup join dim_device─▶ enriched
--   kafka: iot-events   ──┴─▶ iot_events     (log, tiered)                      │
--                                                                               ├─▶ datalake_device_telemetry   (per reading)
--                                                                               └─▶ datalake_device_health_1min (1-min window)
--
-- Needs the topics to exist — `make produce` first. Paste into an interactive session
-- (`make sql`), AFTER sql/catalog.sql.

-- 1) The device dimension. The only PK table here (lookup-join build side) and the only
--    untiered one — in 0.9.1 a tiered PK table cannot be batch-read by its bare name.
--    See docs/NOTES.md.
CREATE TABLE dim_device (
  `device_id`      STRING NOT NULL,
  `temp_threshold` DOUBLE,
  `location_id`    STRING,
  `model`          STRING,
  `status`         STRING,
  PRIMARY KEY (`device_id`) NOT ENFORCED
);

-- 2) BRONZE — the topics as landed, nothing derived. Log tables (no PK), both tiered.
--    Without 'table.datalake.enabled' there is no Iceberg twin and the log ages out at
--    'table.log.ttl' (7 days). `ptime` is virtual — it never reaches the Iceberg schema.
CREATE TABLE iot_telemetry (
  `reading_id`      BIGINT,
  `device_id`       STRING NOT NULL,
  `event_time`      TIMESTAMP(3),
  `energy_usage`    DOUBLE,
  `temperature`     DOUBLE,
  `vibration`       DOUBLE,
  `signal_strength` INT,
  `ptime` AS PROCTIME()
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- The type-specific columns are sparse: only the ones belonging to a row's event_type
-- are populated, exactly as they arrive on the topic.
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

-- 3) The derived tables. No PRIMARY KEY, deliberately: a batch union read sort-merges a
--    PK table and fluss-lake-iceberg 0.9.1 has no sorted reader. docs/NOTES.md.

-- SILVER — every reading, enriched with its device's own threshold and flagged. The flag is
-- there as soon as the reading is, so this is the table to query during an incident.
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

-- GOLD — one row per device per minute; the table a dashboard reads. It trails the readings
-- by up to a minute, because a window has to close before Flink can write it.
-- cnt_anomalies carries the signal —
-- anomaly_flag (max temp > threshold) is TRUE for nearly every window and ranks nothing,
-- so rank on cnt_anomalies / cnt_points instead.
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

-- 4) Seed the dimension. dml-sync blocks until it FINISHES, so the lookup joins below
--    never start against an empty dimension.
--
--    Thresholds spread 24-29 °C over 11 devices; the producer draws temperature uniformly
--    from 18-30 °C. So device_1 (24.0) sits over its threshold about half the time and
--    device_11 (29.0) about a twelfth — that spread is what the rankings pick up.
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

-- 5) The Kafka sources. Fluss sits behind the broker you already have.
--
--    'ISO-8601' must match the producer or every timestamp arrives NULL with no error.
--    'earliest-offset' so a re-run picks up what is already on the topic.
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

-- 6) The LANDING JOB: both topics into bronze, unchanged. ONE detached job with two sinks —
--    a statement set compiles into a single JobGraph, which is why the Flink UI shows both
--    sink names on one row.
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO iot_telemetry
  SELECT reading_id, device_id, event_time, energy_usage, temperature, vibration, signal_strength
  FROM `default_catalog`.`default_database`.src_telemetry;

  INSERT INTO iot_events
  SELECT * FROM `default_catalog`.`default_database`.src_events;
END;

-- 7) The ENRICHMENT JOB, part one: the lookup join. For every reading Flink asks Fluss for
--    one row — the device with that device_id — which a PK table answers from its key index,
--    so Flink keeps no copy of the dimension. `t.ptime` is when the reading arrived, so each
--    reading gets the threshold in the table at that moment.
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

-- 8) The ENRICHMENT JOB, part two: silver and gold, two sinks off the one enriched view.
--    Gold is computed from the enriched stream inside the job, not read back from silver.
--
--    PROCESSING-time window: a proctime tumble needs no watermark and emits append-only,
--    which is what a log-table sink can consume. So a reading is counted in the minute it
--    ARRIVED, not the minute on its own event_time — enough here, because readings arrive
--    within a second. 1 minute, not 5, so rows appear while you watch. To group by the
--    reading's own timestamp, see docs/NOTES.md "Switching to event-time windows".
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

-- Next:  make tiering     (step 3 — start copying hot -> cold)
-- Then:  sql/02-live.sql in this session, or `make demo` in another shell.
