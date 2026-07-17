#!/usr/bin/env bash
# Tooling for poking the stack from outside the containers.
set -euo pipefail

# MinIO client
curl -fL -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x /usr/local/bin/mc

# DuckDB — great for reading the cold Iceberg tables independently (proves format portability)
curl -fL -o /tmp/duckdb.zip https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip -o /tmp/duckdb.zip -d /usr/local/bin && chmod +x /usr/local/bin/duckdb

# MySQL client for StarRocks
sudo apt-get update && sudo apt-get install -y default-mysql-client jq

# PyIceberg + Nessie for scripted catalog / branch operations and custom data generators
pip install --no-cache-dir pyiceberg[s3fs] pynessie

echo "Tooling ready: mc, duckdb, mysql, jq, pyiceberg, pynessie"