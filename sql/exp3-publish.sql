-- Experiment 3, W of write-audit-publish: WRITE the curated table onto a Nessie branch.
--
--   fluss.datalake_device_health_1min (hot ∪ cold)  ──▶  curated.device_health_published @ audit
--
-- Nothing on `main` changes here. The tiering job keeps committing to main throughout — Nessie
-- merges per table, so a branch that only touches `curated.*` never conflicts with it.
--
-- Run by scripts/wap.py, which creates the branch first and prepends sql/common/catalog.sql.

-- The same Iceberg catalog the tiering job writes, pointed at a different ref. That one word is
-- the whole trick: 'ref' = 'audit'.
CREATE CATALOG IF NOT EXISTS ice_audit WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog',
  'uri' = 'http://nessie:19120/api/v2',
  'ref' = 'audit',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = 'password',
  's3.path-style-access' = 'true',
  'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS ice_audit.curated;

CREATE TABLE IF NOT EXISTS ice_audit.curated.device_health_published (
  `device_id`        STRING,
  `window_start`     TIMESTAMP(3),
  `cnt_points`       BIGINT,
  `cnt_anomalies`    BIGINT,
  `avg_temperature`  DOUBLE,
  `max_temperature`  DOUBLE,
  `threshold_used`   DOUBLE,
  `location_id`      STRING,
  `model`            STRING
);

-- OVERWRITE, not INSERT: a full refresh is idempotent, so re-running the demo does not stack
-- duplicate copies of the same windows.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

INSERT OVERWRITE ice_audit.curated.device_health_published
SELECT device_id, window_start, cnt_points, cnt_anomalies,
       avg_temperature, max_temperature, threshold_used, location_id, `model`
FROM fluss_catalog.fluss.datalake_device_health_1min;
