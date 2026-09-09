-- Tutorial 3: query the COLD tier from StarRocks.
-- Connect:  mysql -h 127.0.0.1 -P 9030 -u root
-- StarRocks reads the tiered Iceberg tables via Nessie's REST catalog — not Fluss directly.
--
-- "Location does not exist: s3://warehouse/..." means StarRocks is serving cached Iceberg
-- metadata for files a reset deleted. `make down` now tears StarRocks down with everything
-- else; if you reset some other way, refresh the table instead:
--   REFRESH EXTERNAL TABLE iceberg_nessie.fluss.datalake_device_telemetry;

CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_nessie
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "http://nessie:19120/iceberg/main",
  "iceberg.catalog.warehouse" = "warehouse",
  "aws.s3.endpoint" = "http://minio:9000",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.access_key" = "admin",
  "aws.s3.secret_key" = "password",
  "aws.s3.region" = "us-east-1"
);

SHOW DATABASES FROM iceberg_nessie;
-- SET CATALOG iceberg_nessie;
-- USE <database_shown_above>;
-- SELECT sum(total_price) FROM datalake_enriched_orders;

-- ═════════════════════════════════════════════════════════════════════════════
-- Tutorial 3 — the IoT cold tier. StarRocks reads Iceberg. Fluss is not in this path.
-- ═════════════════════════════════════════════════════════════════════════════
SET CATALOG iceberg_nessie;
USE fluss;

-- 0) The point of the tutorial: this is BELOW the union-read count from sql/09 §B1.
--    StarRocks sees only what the tiering job has flushed.
SELECT count(*) AS cold_only_readings FROM datalake_device_telemetry;

-- 1) Temperature vs each device's own threshold (kappa panel 1).
SELECT device_id,
       location_id,
       model,
       round(avg(avg_temperature), 1) AS avg_c,
       round(max(max_temperature), 1) AS max_c,
       max(threshold_used)            AS threshold,
       round(max(max_temperature) - max(threshold_used), 1) AS delta_over
FROM datalake_device_health_1min
GROUP BY device_id, location_id, model
ORDER BY delta_over DESC;

-- 2) Events by type and severity. Also tiered, also cold.
SELECT event_type, severity, count(*) AS n
FROM iot_events
GROUP BY event_type, severity
ORDER BY event_type, severity;

-- 2b) The sparse columns survive the whole trip: producer -> Kafka JSON -> Fluss ->
--     Iceberg -> StarRocks. Only failure rows carry a root_cause.
SELECT root_cause, component, count(*) AS failures
FROM iot_events
WHERE event_type = 'failure'
GROUP BY root_cause, component
ORDER BY failures DESC
LIMIT 10;

-- 3) Devices ranked by how much time they spend over their own threshold (kappa panel 3).
--    Rate, not anomaly_flag: over a full minute the max reading almost always clears the
--    threshold, so the flag is TRUE for nearly every window and ranks nothing. The rate
--    tracks each device's threshold — device_7 (32.0 C) should top this, device_8 (38.0) sit last.
SELECT device_id,
       max(threshold_used)                       AS threshold,
       count(*)                                  AS windows,
       sum(cnt_anomalies)                        AS anomalous_readings,
       round(100.0 * sum(cnt_anomalies) / sum(cnt_points), 1) AS pct_over_threshold,
       sum(cnt_vib_spikes)                       AS vibration_spikes
FROM datalake_device_health_1min
GROUP BY device_id
ORDER BY pct_over_threshold DESC;

-- Add a time filter once the tables are big:
--   WHERE window_start >= NOW() - INTERVAL 120 MINUTE
