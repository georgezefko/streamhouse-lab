-- Tutorial 1: a real-time IoT analytics pipeline on the streamhouse.
--
-- Sensors -> Fluss (hot) -> two tiered tables -> Iceberg on MinIO (cold).
-- Paste this whole file into an interactive session:  make sql
--
-- Shape of the pipeline:
--   source_telemetry (faker) ─┐
--                             ├─▶ iot_telemetry (log) ──lookup join dim_device──▶ enriched
--   source_events    (faker) ─┴─▶ iot_events (log, tiered)                          │
--                                                                                   ├─▶ datalake_device_telemetry   (per reading)
--                                                                                   └─▶ datalake_device_health_1min (1-min window)
--
-- The two datalake_* tables are what the tiering job (`make tiering`) moves into Iceberg.

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123',
  'iceberg.s3.access-key-id' = 'admin',
  'iceberg.s3.secret-access-key' = 'password'
);

USE CATALOG fluss_catalog;

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
-- 2) The raw streams. Both LOG tables (no PK).
--    Reading a PK table in streaming mode emits -U/+U, and an append-only sink
--    rejects that. Everything downstream here is append-only, so the sources
--    must be too. See docs/EXPLANATION.md.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE iot_telemetry (
  `reading_id`  BIGINT,
  `device_id`   STRING NOT NULL,
  `temperature` DOUBLE,
  `ptime` AS PROCTIME()
);

-- Tiered as well, so StarRocks can read events from the cold tier in Tutorial 3.
-- No PK, for the same union-read reason as the datalake_* tables below.
CREATE TABLE iot_events (
  `event_id`   STRING,
  `device_id`  STRING NOT NULL,
  `event_type` STRING,
  `severity`   STRING,
  `event_time` TIMESTAMP(3)
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
CREATE TABLE datalake_device_telemetry (
  `reading_id`     BIGINT,
  `device_id`      STRING NOT NULL,
  `ingest_time`    TIMESTAMP(3),
  `temperature`    DOUBLE,
  `temp_threshold` DOUBLE,
  `anomaly_flag`   BOOLEAN,
  `location_id`    STRING,
  `model`          STRING
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- The analytical fact table — kappa's fact_telemetry_5min, at tutorial time-scale.
-- Dropped vs kappa: cnt_events / events_* (needs a stream-stream join) and the
-- incomplete_by_* flags (need event time + watermarks).
-- ponytail: no event counts here; join iot_events at read time instead (sql/04).
--
-- cnt_anomalies is what carries the signal. anomaly_flag is kappa's max()>threshold rule, kept
-- for parity, but over a full minute of uniform readings the max almost always clears the
-- threshold — so it is TRUE for nearly every window and ranks nothing. Rank on the anomaly
-- RATE (cnt_anomalies / cnt_points) instead; that tracks each device's threshold cleanly.
CREATE TABLE datalake_device_health_1min (
  `device_id`       STRING NOT NULL,
  `window_start`    TIMESTAMP(3),
  `window_end`      TIMESTAMP(3),
  `cnt_points`      BIGINT,
  `cnt_anomalies`   BIGINT,
  `avg_temperature` DOUBLE,
  `min_temperature` DOUBLE,
  `max_temperature` DOUBLE,
  `threshold_used`  DOUBLE,
  `anomaly_flag`    BOOLEAN,
  `location_id`     STRING,
  `model`           STRING
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4) Seed the dimension. dml-sync makes this block until it FINISHES, so the
--    lookup joins below never start against an empty dimension.
--    Thresholds/locations/models are the same 11 devices as the kappa pipeline.
-- ─────────────────────────────────────────────────────────────────────────────
SET 'table.dml-sync' = 'true';

INSERT INTO dim_device VALUES
  ('device_1',  35.0, 'plant_a', 'Model-A', 'active'),
  ('device_2',  34.0, 'plant_b', 'Model-A', 'active'),
  ('device_3',  36.0, 'plant_c', 'Model-B', 'active'),
  ('device_4',  33.0, 'plant_a', 'Model-B', 'active'),
  ('device_5',  35.0, 'plant_b', 'Model-A', 'active'),
  ('device_6',  37.0, 'plant_c', 'Model-C', 'active'),
  ('device_7',  32.0, 'plant_a', 'Model-C', 'active'),
  ('device_8',  38.0, 'plant_b', 'Model-D', 'active'),
  ('device_9',  35.0, 'plant_c', 'Model-D', 'active'),
  ('device_10', 34.5, 'plant_a', 'Model-B', 'active'),
  ('device_11', 36.5, 'plant_b', 'Model-A', 'active');

-- Back to detached: everything below should submit and return immediately.
SET 'table.dml-sync' = 'false';

-- ─────────────────────────────────────────────────────────────────────────────
-- 5) The sensors. flink-faker, straight into Fluss — no broker in this path.
--
--    Fully qualified on purpose: an unqualified CREATE lands in whatever catalog
--    is current, and we are in fluss_catalog. Same reason as sql/05.
--
--    200k readings at 50/s ≈ 66 minutes. Bounded on purpose, but long enough that
--    the source does not drain mid-tutorial — every contrast in this repo only
--    exists while data is still arriving.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`source_telemetry` (
  `reading_id`  BIGINT,
  `device_id`   STRING,
  `temperature` DOUBLE
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '50',
  'number-of-rows' = '200000',
  'fields.reading_id.expression'  = '#{number.numberBetween ''1'',''100000000''}',
  'fields.device_id.expression'   = 'device_#{number.numberBetween ''1'',''12''}',
  'fields.temperature.expression' = '#{number.randomDouble ''1'',''15'',''45''}'
);

-- 15-45 °C against thresholds of 32-38 °C produces anomalies on its own — no need
-- to force a "hot device" the way the kappa generator does.

CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`source_events` (
  `event_id`   STRING,
  `device_id`  STRING,
  `event_type` STRING,
  `severity`   STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '5',
  'number-of-rows' = '20000',
  'fields.event_id.expression'   = '#{Internet.uuid}',
  'fields.device_id.expression'  = 'device_#{number.numberBetween ''1'',''12''}',
  'fields.event_type.expression' = '#{options.option ''failure'',''maintenance'',''inspection''}',
  'fields.severity.expression'   = '#{options.option ''low'',''medium'',''high''}',
  'fields.event_time.expression' = '#{date.past ''15'',''SECONDS''}'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 6) Ingest. Two detached Flink jobs.
-- ─────────────────────────────────────────────────────────────────────────────
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO iot_telemetry SELECT * FROM `default_catalog`.`default_database`.source_telemetry;
  INSERT INTO iot_events    SELECT * FROM `default_catalog`.`default_database`.source_events;
END;

-- ─────────────────────────────────────────────────────────────────────────────
-- 7) Enrich once, consume twice. The lookup join is the point-lookup workload:
--    one PK read against dim_device per incoming reading.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`enriched` AS
SELECT t.reading_id,
       t.device_id,
       t.temperature,
       t.ptime,
       d.temp_threshold,
       d.location_id,
       d.`model`
FROM fluss_catalog.fluss.iot_telemetry t
LEFT JOIN fluss_catalog.fluss.dim_device FOR SYSTEM_TIME AS OF t.ptime AS d
  ON t.device_id = d.device_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- 8) Derive both tiered tables. Two more detached jobs.
--
--    The window is PROCESSING time. A proctime tumble needs no watermark and emits
--    append-only, which is exactly what a log-table sink can consume. kappa used
--    event time because it simulated late data and outages; those are out of scope.
--    ponytail: proctime window; switch to event-time + WATERMARK when late data matters.
--
--    1 minute, not kappa's 5 — so the fact table produces rows inside a tutorial.
-- ─────────────────────────────────────────────────────────────────────────────
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO datalake_device_telemetry
  SELECT reading_id,
         device_id,
         CURRENT_TIMESTAMP,
         temperature,
         temp_threshold,
         temperature > temp_threshold,
         location_id,
         `model`
  FROM `default_catalog`.`default_database`.enriched;

  INSERT INTO datalake_device_health_1min
  SELECT device_id,
         window_start,
         window_end,
         count(*),
         sum(CASE WHEN temperature > temp_threshold THEN 1 ELSE 0 END),
         avg(temperature),
         min(temperature),
         max(temperature),
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
-- Then:  sql/09-iot-live.sql in this session, or `make demo` in another shell.
