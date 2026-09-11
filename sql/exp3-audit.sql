-- Experiment 3, A of write-audit-publish: AUDIT the branch before anyone sees it.
--
-- One row out, with a verdict token that scripts/wap.py greps for: WAP_PASS or WAP_FAIL.
-- The checks live here, in SQL, because they are the part a data team actually argues about;
-- the script only decides what to do with the answer.

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

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

SELECT count(*)                                                            AS rows_published,
       sum(CASE WHEN device_id IS NULL OR window_start IS NULL
                THEN 1 ELSE 0 END)                                         AS null_keys,
       sum(CASE WHEN cnt_anomalies > cnt_points THEN 1 ELSE 0 END)         AS impossible_counts,
       sum(CASE WHEN avg_temperature IS NULL
                  OR avg_temperature NOT BETWEEN 0 AND 100
                THEN 1 ELSE 0 END)                                         AS temp_out_of_range,
       sum(CASE WHEN `model` IS NULL OR location_id IS NULL
                THEN 1 ELSE 0 END)                                         AS unenriched,
       CASE WHEN count(*) > 0
             AND sum(CASE WHEN device_id IS NULL OR window_start IS NULL THEN 1 ELSE 0 END) = 0
             AND sum(CASE WHEN cnt_anomalies > cnt_points THEN 1 ELSE 0 END) = 0
             AND sum(CASE WHEN avg_temperature IS NULL
                            OR avg_temperature NOT BETWEEN 0 AND 100 THEN 1 ELSE 0 END) = 0
             AND sum(CASE WHEN `model` IS NULL OR location_id IS NULL THEN 1 ELSE 0 END) = 0
            THEN 'WAP_PASS' ELSE 'WAP_FAIL' END                            AS verdict
FROM ice_audit.curated.device_health_published;
