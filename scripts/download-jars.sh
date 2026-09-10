#!/usr/bin/env bash
# Fluss SERVERS need the Iceberg + AWS jars mounted into /opt/fluss/plugins/iceberg/.
# The Flink side is already bundled in the fluss-quickstart-flink image; the Fluss server side is NOT.
#
# We use the NATIVE Nessie catalog (org.apache.iceberg.nessie.NessieCatalog), not Iceberg-REST.
# Fluss bundles only iceberg-core catalogs; NessieCatalog lives in iceberg-nessie, so we add it
# plus its nessie-client runtime deps. (Nessie's Iceberg-REST endpoint NPEs on createTable with
# Fluss 0.9.1's Iceberg 1.10 client — see docs/EXPLANATION.md, "Use the native Nessie
# catalog" — so the native catalog is the working path.)
set -euo pipefail

ICEBERG_VERSION="1.10.1"
NESSIE_VERSION="0.104.5"   # nessie-client version iceberg-nessie:1.10.1 depends on
JACKSON_VERSION="2.19.2"   # nessie-client needs jackson; the isolated plugin classloader has none
HADOOP_VERSION="3.3.6"     # Fluss's tiering writer (IcebergConfiguration) needs Hadoop on Flink
LIB_DIR="$(cd "$(dirname "$0")/.." && pwd)/lib"
mkdir -p "$LIB_DIR"

download() {
  local url="$1" out="$2"
  if [[ -f "$LIB_DIR/$out" ]]; then
    echo "  ✓ $out (cached)"
  else
    echo "  ↓ $out"
    curl -fL -o "$LIB_DIR/$out" "$url"
  fi
}

echo "Downloading Iceberg server-side jars into $LIB_DIR ..."
download "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar" \
         "iceberg-aws-${ICEBERG_VERSION}.jar"
download "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
         "iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# Native Nessie catalog + its runtime deps (jackson is already on the iceberg classpath)
download "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-nessie/${ICEBERG_VERSION}/iceberg-nessie-${ICEBERG_VERSION}.jar" \
         "iceberg-nessie-${ICEBERG_VERSION}.jar"
download "https://repo1.maven.org/maven2/org/projectnessie/nessie/nessie-client/${NESSIE_VERSION}/nessie-client-${NESSIE_VERSION}.jar" \
         "nessie-client-${NESSIE_VERSION}.jar"
download "https://repo1.maven.org/maven2/org/projectnessie/nessie/nessie-model/${NESSIE_VERSION}/nessie-model-${NESSIE_VERSION}.jar" \
         "nessie-model-${NESSIE_VERSION}.jar"
download "https://repo1.maven.org/maven2/org/eclipse/microprofile/openapi/microprofile-openapi-api/4.1/microprofile-openapi-api-4.1.jar" \
         "microprofile-openapi-api-4.1.jar"
for j in jackson-core jackson-databind jackson-annotations; do
  download "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/${j}/${JACKSON_VERSION}/${j}-${JACKSON_VERSION}.jar" \
           "${j}-${JACKSON_VERSION}.jar"
done

# The LakeStoragePlugin (bundles iceberg-core). Pre-baked into the Fluss server image, but the
# Flink image has NO iceberg — the tiering job needs this on /opt/flink/lib to load the catalog.
download "https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-iceberg/0.9.1-incubating/fluss-lake-iceberg-0.9.1-incubating.jar" \
         "fluss-lake-iceberg-0.9.1-incubating.jar"

# Hadoop (2 shaded uber jars = no transitive deps). Only the Flink tiering side needs these.
for h in hadoop-client-api hadoop-client-runtime; do
  download "https://repo1.maven.org/maven2/org/apache/hadoop/${h}/${HADOOP_VERSION}/${h}-${HADOOP_VERSION}.jar" \
           "${h}-${HADOOP_VERSION}.jar"
done

# failsafe: iceberg-aws S3FileIO uses it for retries. In the Fluss server base image, absent on Flink.
download "https://repo1.maven.org/maven2/dev/failsafe/failsafe/3.3.2/failsafe-3.3.2.jar" \
         "failsafe-3.3.2.jar"

# iceberg-flink connector: needed to READ the tiered cold tier via `<table>$lake` in Flink SQL.
download "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar" \
         "iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar"

# Kafka SQL connector: Experiment 4 only (the Fluss-vs-Kafka point-lookup comparison).
download "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar" \
         "flink-sql-connector-kafka-3.4.0-1.20.jar"

echo "Done. Jars:"
ls -1 "$LIB_DIR"