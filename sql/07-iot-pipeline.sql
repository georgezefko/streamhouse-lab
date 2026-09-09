-- Tutorial 1: a real-time IoT analytics pipeline on the streamhouse.
--
--   sensors ─┬─▶ iot_telemetry (log) ──lookup join dim_device──▶ enriched ─┬─▶ datalake_device_telemetry   (per reading)
--            └─▶ iot_events (log, tiered)                                  └─▶ datalake_device_health_1min (1-min window)
--
-- There is no broker in this path, on purpose. iot_telemetry is a Fluss LOG table: an
-- append-only stream, partitioned and replicated, which is the job a Kafka topic would
-- normally do here — except you can also query it, join it, and tier it. Tutorial 4 prices
-- exactly that difference.
--
-- Paste this whole file into an interactive session:  make sql

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
-- 2) The hot landing tables. Both LOG tables (no PK).
--
--    A Fluss log table is an append-only, partitioned, replicated stream — the same
--    shape as a Kafka topic, and it is deliberately the only "queue" in this pipeline.
--    The difference is that this one has a schema, joins, and a SELECT.
--
--    No PK for a second reason too: reading a PK table in streaming mode emits -U/+U,
--    and an append-only sink rejects that. Everything downstream here is append-only,
--    so these must be too. See docs/EXPLANATION.md.
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

-- Tiered as well, so StarRocks can read events from the cold tier in Tutorial 3.
-- No PK, for the same union-read reason as the datalake_* tables below.
-- The type-specific columns are a sparse union: only the ones belonging to a row's
-- event_type are populated, the rest are NULL.
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
-- onto a second Kafka topic and Routine-Loaded into StarRocks — another copy, kept in
-- sync by hand. Here the row is queryable the instant it lands and tiers itself into
-- Iceberg. That is the whole argument; see docs/EXPLANATION.md.
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
-- tutorial time-scale. Dropped vs the original: cnt_events / events_* (needs a
-- stream-stream join) and the incomplete_by_* flags (need event time + watermarks).
-- ponytail: no event counts here; join iot_events at read time instead (sql/04).
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
--    Thresholds are spread 24-29 °C across the 11 devices. The generator draws
--    temperature uniformly from 18-30 °C, so device_1 (24.0) sits over its
--    threshold about half the time and device_11 (29.0) about a twelfth — which is
--    what makes the Tutorial 3 ranking come out ordered by threshold.
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
-- 5) The sensors. flink-faker, straight into Fluss.
--
--    Fully qualified on purpose: an unqualified CREATE lands in whatever catalog is
--    current, and we are in fluss_catalog. Same reason as sql/05.
--
--    200k readings at 50/s ≈ 66 minutes. Bounded on purpose, but long enough that the
--    source does not drain mid-tutorial — every contrast in this repo only exists while
--    data is still arriving.
--
--    Field ranges follow a real device fleet: temperature 18-30 °C, vibration 0.1-2.0
--    with occasional spikes, energy 0.5-5.0, signal 70-100.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`gen_telemetry` (
  `reading_id`      BIGINT,
  `device_id`       STRING,
  `event_time`      TIMESTAMP(3),
  `energy_usage`    DOUBLE,
  `temperature`     DOUBLE,
  `vibration_base`  DOUBLE,
  `spike_roll`      DOUBLE,
  `signal_strength` INT
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '50',
  'number-of-rows' = '200000',
  'fields.reading_id.expression'      = '#{number.numberBetween ''1'',''100000000''}',
  'fields.device_id.expression'       = 'device_#{number.numberBetween ''1'',''12''}',
  'fields.event_time.expression'      = '#{date.past ''5'',''SECONDS''}',
  'fields.energy_usage.expression'    = '#{number.randomDouble ''2'',''0'',''5''}',
  'fields.temperature.expression'     = '#{number.randomDouble ''1'',''18'',''30''}',
  'fields.vibration_base.expression'  = '#{number.randomDouble ''1'',''0'',''2''}',
  'fields.spike_roll.expression'      = '#{number.randomDouble ''2'',''0'',''1''}',
  'fields.signal_strength.expression' = '#{number.numberBetween ''70'',''100''}'
);

CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`gen_events` (
  `device_id`   STRING,
  `event_time`  TIMESTAMP(3),
  `type_roll`   DOUBLE,
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
  'connector' = 'faker',
  'rows-per-second' = '5',
  'number-of-rows' = '20000',
  'fields.device_id.expression'    = 'device_#{number.numberBetween ''1'',''12''}',
  'fields.event_time.expression'   = '#{date.past ''5'',''SECONDS''}',
  'fields.type_roll.expression'    = '#{number.randomDouble ''3'',''0'',''1''}',
  'fields.severity.expression'     = '#{options.option ''low'',''medium'',''high''}',
  'fields.error_code.expression'   = 'ERR#{number.numberBetween ''1000'',''1999''}',
  'fields.component.expression'    = '#{options.option ''motor'',''bearing'',''sensor'',''battery''}',
  'fields.root_cause.expression'   = '#{options.option ''overheating'',''wear'',''power_surge'',''unknown''}',
  'fields.technician.expression'   = 'tech-#{number.numberBetween ''1'',''20''}',
  'fields.duration_min.expression' = '#{number.numberBetween ''15'',''240''}',
  'fields.parts_replaced.expression' = '#{options.option ''bearing'',''filter'',''battery''}',
  'fields.status.expression'       = '#{options.option ''passed'',''passed'',''failed''}',
  'fields.next_inspection_days.expression' = '#{number.numberBetween ''7'',''30''}'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 6) Ingest. Two detached jobs. From here the data is indexed and queryable.
-- ─────────────────────────────────────────────────────────────────────────────
EXECUTE STATEMENT SET
BEGIN
  -- Vibration spikes ~5% of the time.
  INSERT INTO iot_telemetry
  SELECT reading_id, device_id, event_time, energy_usage, temperature,
         round(vibration_base + CASE WHEN spike_roll > 0.95 THEN 3.0 ELSE 0.0 END, 1),
         signal_strength
  FROM `default_catalog`.`default_database`.gen_telemetry;

  -- 10% failure / 30% maintenance / 60% inspection, and only the columns that belong
  -- to each type are populated — the rest stay NULL.
  INSERT INTO iot_events
  SELECT device_id,
         event_time,
         CASE WHEN type_roll < 0.10 THEN 'failure'
              WHEN type_roll < 0.40 THEN 'maintenance'
              ELSE 'inspection' END,
         severity,
         CASE WHEN type_roll < 0.10 THEN error_code  END,
         CASE WHEN type_roll < 0.10 THEN component   END,
         CASE WHEN type_roll < 0.10 THEN root_cause  END,
         CASE WHEN type_roll >= 0.10 AND type_roll < 0.40 THEN technician     END,
         CASE WHEN type_roll >= 0.10 AND type_roll < 0.40 THEN duration_min   END,
         CASE WHEN type_roll >= 0.10 AND type_roll < 0.40 THEN parts_replaced END,
         CASE WHEN type_roll >= 0.40 THEN status               END,
         CASE WHEN type_roll >= 0.40 THEN next_inspection_days END
  FROM `default_catalog`.`default_database`.gen_events;
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
-- 8) Derive both tiered tables. Two more detached jobs.
--
--    The window is PROCESSING time. A proctime tumble needs no watermark and emits
--    append-only, which is exactly what a log-table sink can consume. The reference
--    pipeline used event time because it simulated late data and outages; those are
--    out of scope here.
--    ponytail: proctime window; switch to event-time + WATERMARK when late data matters.
--
--    1 minute, not the original's 5 — so the fact table produces rows inside a tutorial.
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
-- Then:  sql/09-iot-live.sql in this session, or `make demo` in another shell.
