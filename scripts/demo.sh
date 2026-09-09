#!/usr/bin/env bash
# Watch the cold tier chase the hot tier. Requires the stack up (`make up`), the pipeline from
# sql/07-iot-pipeline.sql running, and the tiering job running (`make tiering`).
#
# SQL_FILE overrides which contrast to loop:
#   SQL_FILE=/sql/03-contrast.sql bash scripts/demo.sh   # the orders appendix
#
# Read across iterations: hot_plus_cold climbs continuously, cold_only jumps once per tiering
# flush (table.datalake.freshness = 30s), and rows_only_in_hot never reaches zero while the
# stream runs. That gap is what a traditional lakehouse makes you wait for.
set -euo pipefail

ITERATIONS="${1:-6}"
SQL_FILE="${SQL_FILE:-/sql/08-iot-contrast.sql}"

for ((i = 1; i <= ITERATIONS; i++)); do
  printf '\n═══ %s  (%d/%d) ═══\n' "$(date +%H:%M:%S)" "$i" "$ITERATIONS"

  # NB: /opt/sql-client/sql-client (the image default command) hardcodes its args and drops "$@",
  # so -f there is silently ignored. Call sql-client.sh directly.
  out=$(docker compose run --rm -T sql-client \
          /opt/flink/bin/sql-client.sh -f "$SQL_FILE" 2>&1)
  echo "$out"

  # sql-client exits 0 even when a statement fails, so grep for it. This is the runnable check.
  # ponytail: does not parse the numbers themselves — to gate CI, also assert rows_only_in_hot > 0.
  if grep -q '\[ERROR\]' <<<"$out"; then
    echo "✗ SQL failed — is the stack up and the tiering job running?" >&2
    exit 1
  fi

  sleep 15
done

