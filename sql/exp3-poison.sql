-- Experiment 3, the deliberate defect. `make wap-break` runs this between the write and the
-- audit: one row that could not come from the pipeline — more anomalies than readings, a
-- temperature no sensor reports, and an unresolved device (NULL model/location).
--
-- The point is what happens next: the audit fails, the branch is left alone, and `main` never
-- sees any of it. A quality gate that does not stop ingestion.

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

SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

INSERT INTO ice_audit.curated.device_health_published
VALUES ('device_99', TIMESTAMP '2026-01-01 00:00:00',
        CAST(10 AS BIGINT), CAST(9999 AS BIGINT),
        CAST(812.5 AS DOUBLE), CAST(999.9 AS DOUBLE), CAST(24.0 AS DOUBLE),
        CAST(NULL AS STRING), CAST(NULL AS STRING));
