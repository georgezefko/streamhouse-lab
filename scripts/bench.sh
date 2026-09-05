#!/usr/bin/env bash
# Phase 4: how much does one point query cost on Fluss vs Kafka vs Iceberg?
# Requires the stack up, the tiering job running, and sql/05-bench-load.sql still streaming.
#
# Timings are Flink job durations from the REST API, not wall clock — `docker compose run`
# costs several seconds of container startup that would swamp the numbers we care about.
set -euo pipefail

FLINK=http://localhost:8083

before=$(curl -sf "$FLINK/jobs/overview" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["jobs"]))')

docker compose run --rm -T sql-client \
  /opt/flink/bin/sql-client.sh -f /sql/06-bench-query.sql

# ponytail: attributes jobs to queries by submission order, since sql-client names every batch
# job "collect". Breaks if something else submits concurrently — set parallelism 1 and don't.
curl -sf "$FLINK/jobs/overview" | python3 - "$before" <<'PY'
import sys, json
jobs = sorted(json.load(sys.stdin)["jobs"], key=lambda j: j["start-time"])[int(sys.argv[1]):]
labels = ["Fluss  (PK point lookup)", "Kafka  (scan to latest offset)", "Iceberg (cold Parquet scan)"]
print(f"\n{'engine':32} {'state':10} {'duration':>10}")
for label, j in zip(labels, jobs):
    print(f"{label:32} {j['state']:10} {j['duration']:>8} ms")
if len(jobs) != len(labels):
    print(f"\n! expected {len(labels)} jobs, saw {len(jobs)} — durations may be mislabelled", file=sys.stderr)
PY
