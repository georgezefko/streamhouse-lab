#!/usr/bin/env bash
# Scenario 4: how much does one point query cost on Fluss vs Kafka?
# Requires the stack up and sql/05-bench-load.sql still streaming in a `make sql` session.
#
# Timings are Flink job durations from the REST API, not wall clock — `docker compose run`
# costs several seconds of container startup that would swamp the numbers we care about.
set -euo pipefail

FLINK=http://localhost:8083

count_jobs() {
  python3 -c 'import sys,json,urllib.request
print(len(json.load(urllib.request.urlopen(sys.argv[1]))["jobs"]))' "$FLINK/jobs/overview"
}

before=$(count_jobs)

out=$(docker compose run --rm -T sql-client \
        /opt/flink/bin/sql-client.sh -f /sql/06-bench-query.sql 2>&1)
echo "$out"

# sql-client exits 0 even when a statement fails, so grep for it (same check demo.sh uses).
if grep -q '\[ERROR\]' <<<"$out"; then
  echo "✗ SQL failed — is sql/05-bench-load.sql running in a 'make sql' session?" >&2
  exit 1
fi

# ponytail: attributes jobs to queries by submission order, since sql-client names every batch
# job "collect". Breaks if something else submits concurrently — don't, while benching.
python3 -c '
import sys, json, urllib.request
jobs = sorted(json.load(urllib.request.urlopen(sys.argv[1]))["jobs"],
              key=lambda j: j["start-time"])[int(sys.argv[2]):]
labels = ["Fluss (PK point lookup)", "Kafka (scan to latest offset)"]
print("\n%-32s %-10s %10s" % ("engine", "state", "duration"))
for label, j in zip(labels, jobs):
    print("%-32s %-10s %8d ms" % (label, j["state"], j["duration"]))
if len(jobs) != len(labels):
    print("\n! expected %d jobs, saw %d — durations may be mislabelled" % (len(labels), len(jobs)),
          file=sys.stderr)
' "$FLINK/jobs/overview" "$before"
