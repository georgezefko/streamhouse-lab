-- Step 5: query the COLD tier from StarRocks — it is plain Iceberg.
-- Connect:  mysql -h 127.0.0.1 -P 9030 -u root
-- StarRocks reads the tiered Iceberg tables via Nessie's REST catalog — not Fluss directly.
--
-- "Location does not exist: s3://warehouse/..." means StarRocks is serving cached Iceberg
-- metadata for files a reset deleted. `make down` tears StarRocks down with everything else;
-- if you reset some other way, refresh the table instead:
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

SHOW DATABASES FROM iceberg_nessie;   -- the tiered tables live in `fluss`

SET CATALOG iceberg_nessie;
USE fluss;

-- 1) The point of this step: this count is BELOW the union read from sql/02-live.sql §B1.
--    StarRocks sees only what the tiering job has flushed, and Fluss is not in this path.
SELECT count(*) AS cold_only_readings FROM datalake_device_telemetry;

-- 2) Devices ranked by how much time they spend over their own threshold.
--    Rate, not anomaly_flag: over a full minute the max reading almost always clears the
--    threshold, so the flag is TRUE for nearly every window and ranks nothing. The rate
--    tracks each device's threshold — device_1 (24.0 C) tops this, device_11 (29.0) sits last.
SELECT device_id,
       max(threshold_used)                                    AS threshold,
       count(*)                                               AS windows,
       sum(cnt_anomalies)                                     AS anomalous_readings,
       round(100.0 * sum(cnt_anomalies) / sum(cnt_points), 1) AS pct_over_threshold
FROM datalake_device_health_1min
GROUP BY device_id
ORDER BY pct_over_threshold DESC;

-- 3) Events are tiered too, with no derived stage of their own.
SELECT event_type, severity, count(*) AS n
FROM iot_events
GROUP BY event_type, severity
ORDER BY event_type, severity;

-- 4) ...so they are joined to the readings at QUERY time instead of in a job: what was each
--    machine doing in the minute a failure was reported? Both sides are cold Iceberg.
SELECT e.device_id,
       e.component,
       e.root_cause,
       h.window_start,
       h.cnt_anomalies,
       round(h.max_temperature, 1) AS max_c,
       h.threshold_used
FROM iot_events e
JOIN datalake_device_health_1min h
  ON  e.device_id  = h.device_id
  AND e.event_time >= h.window_start
  AND e.event_time <  h.window_end
WHERE e.event_type = 'failure'
ORDER BY h.window_start DESC
LIMIT 20;

-- Add a time filter once the tables are big:
--   WHERE window_start >= NOW() - INTERVAL 120 MINUTE
