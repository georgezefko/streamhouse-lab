-- Phase 3: query the COLD tier from StarRocks.
-- Connect:  mysql -h 127.0.0.1 -P 9030 -u root
-- StarRocks reads the tiered Iceberg tables via Nessie's REST catalog — not Fluss directly.

CREATE EXTERNAL CATALOG iceberg_nessie
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