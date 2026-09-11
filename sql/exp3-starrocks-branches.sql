-- Experiment 3, seen from the consumer: the same table name on two Nessie refs, side by side.
-- Run in StarRocks:  make sr-sql
--
-- This is where WAP becomes visible to whoever actually reads the data. StarRocks never touches
-- Fluss and never runs the pipeline — it reads Iceberg through Nessie's REST endpoint, and that
-- endpoint is per-ref:  /iceberg/main  vs  /iceberg/audit.
--
-- Run it after `make wap-break`: main holds the last publish that passed, audit holds the
-- candidate that failed. Same catalog code, one word different.

CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_audit
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "http://nessie:19120/iceberg/audit",   -- the branch, not main
  "iceberg.catalog.warehouse" = "warehouse",
  "aws.s3.endpoint" = "http://minio:9000",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.access_key" = "admin",
  "aws.s3.secret_key" = "password",
  "aws.s3.region" = "us-east-1"
);

-- 1) What production sees. Only ever a publish that passed its audit.
SELECT count(*) AS rows_on_main,
       sum(CASE WHEN cnt_anomalies > cnt_points THEN 1 ELSE 0 END) AS impossible_rows
FROM iceberg_nessie.curated.device_health_published;

-- 2) What the candidate looks like. After `make wap-break` this has the corrupt row —
--    quarantined on the branch, readable, never merged.
SELECT count(*) AS rows_on_audit,
       sum(CASE WHEN cnt_anomalies > cnt_points THEN 1 ELSE 0 END) AS impossible_rows
FROM iceberg_audit.curated.device_health_published;

-- 3) Name the offending rows, on the branch, without touching production.
SELECT device_id, cnt_points, cnt_anomalies, avg_temperature, model
FROM iceberg_audit.curated.device_health_published
WHERE cnt_anomalies > cnt_points OR model IS NULL;

-- If a REFRESH is needed after a merge, StarRocks is caching Iceberg metadata:
--   REFRESH EXTERNAL TABLE iceberg_nessie.curated.device_health_published;
