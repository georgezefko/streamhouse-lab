#!/usr/bin/env bash
# Watch the cold tier chase the hot tier. Requires the stack up (`make up`), the producer
# (`make produce`) and sql/exp1-pipeline.sql running, and the tiering job (`make tiering`).
#
# SQL_FILE overrides which contrast to loop.
#
# Read across iterations: hot_plus_cold climbs continuously, cold_only jumps once per tiering
# flush (table.datalake.freshness = 30s), and rows_only_in_hot never reaches zero while the
# stream runs. That gap is what a traditional lakehouse makes you wait for.
set -euo pipefail

ITERATIONS="${1:-6}"
SQL_FILE="${SQL_FILE:-/sql/exp1-contrast.sql}"

for ((i = 1; i <= ITERATIONS; i++)); do
  printf '\n═══ %s  (%d/%d) ═══\n' "$(date +%H:%M:%S)" "$i" "$ITERATIONS"

  # Two notes on this invocation:
  #  - /opt/sql-client/sql-client (the image default command) hardcodes its args and drops "$@",
  #    so -f there is silently ignored. Call sql-client.sh directly.
  #  - the SQL client has no INCLUDE, so the shared catalog DDL is concatenated on here.
  out=$(docker compose run --rm -T sql-client sh -c \
          "cat /sql/common/catalog.sql $SQL_FILE > /tmp/run.sql && /opt/flink/bin/sql-client.sh -f /tmp/run.sql" 2>&1)
  echo "$out"

  # sql-client exits 0 even when a statement fails, so grep for it. This is the runnable check.
  # ponytail: does not parse the numbers themselves — to gate CI, also assert rows_only_in_hot > 0.
  if grep -q '\[ERROR\]' <<<"$out"; then
    echo "✗ SQL failed — is the stack up and the tiering job running?" >&2
    exit 1
  fi

  sleep 15
done

