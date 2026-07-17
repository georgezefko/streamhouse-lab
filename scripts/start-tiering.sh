#!/usr/bin/env bash
# Launch the Fluss Lakehouse Tiering Service as a long-running Flink job.
# This is what continuously moves data from Fluss (hot) into Iceberg-on-MinIO (cold), via Nessie.
# Only tables created with 'table.datalake.enabled' = 'true' are tiered.
#
# The jar filename is version-specific. If this fails, list what's actually in the image:
#   docker compose exec jobmanager ls /opt/flink/opt | grep tiering
set -euo pipefail

TIERING_JAR="/opt/flink/opt/fluss-flink-tiering-0.9.1-incubating.jar"

docker compose exec jobmanager \
  /opt/flink/bin/flink run \
  "${TIERING_JAR}" \
  --fluss.bootstrap.servers coordinator-server:9123 \
  --datalake.format iceberg \
  --datalake.iceberg.catalog-impl org.apache.iceberg.nessie.NessieCatalog \
  --datalake.iceberg.uri http://nessie:19120/api/v2 \
  --datalake.iceberg.ref main \
  --datalake.iceberg.warehouse s3://warehouse/ \
  --datalake.iceberg.io-impl org.apache.iceberg.aws.s3.S3FileIO \
  --datalake.iceberg.s3.endpoint http://minio:9000 \
  --datalake.iceberg.s3.access-key-id admin \
  --datalake.iceberg.s3.secret-access-key password \
  --datalake.iceberg.s3.path-style-access true \
  --datalake.iceberg.client.region us-east-1

echo "Tiering job submitted. Watch it at http://localhost:8083/"