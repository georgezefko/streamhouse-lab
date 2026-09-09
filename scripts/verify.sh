#!/usr/bin/env bash
# verify.sh — liveness gate. Confirms every component is running and network-wired
# BEFORE you build anything on the stack. This checks that things are up and talking;
# it does NOT assert the Fluss->Iceberg tiering seam (that only exists after `make tiering`
# — a separate deeper check).
#
# Usage:  bash scripts/verify.sh
# Exit 0 = all green.  Exit 1 = something's down (see the ✗ lines).

set -uo pipefail

GREEN=$'\033[0;32m'; RED=$'\033[0;31m'; YELLOW=$'\033[0;33m'; DIM=$'\033[2m'; RESET=$'\033[0m'
FAILED=0

pass() { printf "  ${GREEN}✓${RESET} %s\n" "$1"; }
fail() { printf "  ${RED}✗${RESET} %s${DIM}%s${RESET}\n" "$1" "${2:+  — $2}"; FAILED=1; }
info() { printf "${YELLOW}▸ %s${RESET}\n" "$1"; }

# Retry a command until it succeeds or timeout (seconds) elapses.
retry() {
  local timeout="$1"; shift
  local deadline=$(( $(date +%s) + timeout ))
  until "$@" >/dev/null 2>&1; do
    [ "$(date +%s)" -ge "$deadline" ] && return 1
    sleep 2
  done
  return 0
}

DC="docker compose"

# ── 1. Container states ────────────────────────────────────────────────────────
# Map each service to its expected state. minio-init is a one-shot (exited 0);
# sql-client is a run-on-demand target and is intentionally not checked.
info "Container states"

check_running() {
  local svc="$1"
  local cid; cid=$($DC ps -aq "$svc" 2>/dev/null | head -n1)
  if [ -z "$cid" ]; then fail "$svc" "not created — run 'make up'"; return; fi
  local status restarting; read -r status restarting < <(docker inspect -f '{{.State.Status}} {{.State.Restarting}}' "$cid")
  if [ "$status" = "running" ] && [ "$restarting" = "false" ]; then
    pass "$svc"
  else
    fail "$svc" "state=$status restarting=$restarting"
  fi
}

check_completed() {
  local svc="$1"
  local cid; cid=$($DC ps -aq "$svc" 2>/dev/null | head -n1)
  if [ -z "$cid" ]; then fail "$svc" "never ran"; return; fi
  local status code; read -r status code < <(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$cid")
  if [ "$status" = "exited" ] && [ "$code" = "0" ]; then
    pass "$svc ${DIM}(completed)${RESET}"
  else
    fail "$svc" "status=$status exit=$code — bucket init may have failed"
  fi
}

# Give the cluster a moment to settle before asserting (Fluss + Flink take ~20-40s).
retry 60 bash -c "$DC ps -q coordinator-server | grep -q ." || true

for svc in minio nessie zookeeper coordinator-server tablet-server jobmanager taskmanager kafka; do
  check_running "$svc"
done
check_completed minio-init

# ── 2. Endpoint reachability + real wiring ─────────────────────────────────────
info "Endpoints"

# MinIO S3 API alive
if retry 60 curl -sf http://localhost:9000/minio/health/live; then
  pass "MinIO  ${DIM}:9000${RESET}"
else
  fail "MinIO" "http://localhost:9000/minio/health/live unreachable"
fi

# Nessie core API
if retry 60 curl -sf http://localhost:19120/api/v2/config; then
  pass "Nessie API  ${DIM}:19120/api/v2/config${RESET}"
else
  fail "Nessie API" ":19120 unreachable"
fi

# Nessie Iceberg REST catalog actually serving (the seam Fluss tiering depends on)
if retry 30 curl -sf "http://localhost:19120/iceberg/v1/config?warehouse=warehouse"; then
  pass "Nessie Iceberg REST  ${DIM}/iceberg/v1/config${RESET}"
else
  fail "Nessie Iceberg REST" "catalog endpoint not serving — tiering will not find a catalog"
fi

# Flink up AND a TaskManager registered with the JobManager (proves JM<->TM wiring, not just liveness)
tm_registered() {
  local n; n=$(curl -sf http://localhost:8083/overview 2>/dev/null | grep -o '"taskmanagers":[0-9]\+' | grep -o '[0-9]\+')
  [ -n "$n" ] && [ "$n" -ge 1 ]
}
if retry 60 tm_registered; then
  pass "Flink  ${DIM}:8083 (taskmanager registered)${RESET}"
else
  fail "Flink" "no taskmanager registered with jobmanager"
fi

# Kafka broker accepting API requests. Kafka is the ingress for Tutorial 1 (sql/07 produces
# onto iot-telemetry / iot-events), so a dead broker means the pipeline silently reads nothing.
kafka_ready() {
  $DC exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server kafka:9092 >/dev/null 2>&1
}
if retry 60 kafka_ready; then
  pass "Kafka  ${DIM}:9092 (broker responding)${RESET}"
else
  fail "Kafka" ":9092 not accepting requests — Tutorial 1 has no ingress"
fi

# ── 3. Buckets exist ───────────────────────────────────────────────────────────
info "Object storage"
buckets=$($DC run --rm --no-deps --entrypoint /bin/sh minio-init -c \
  "mc alias set m http://minio:9000 admin password >/dev/null 2>&1 && mc ls m/" 2>/dev/null)
for b in fluss warehouse; do
  if echo "$buckets" | grep -q "$b"; then pass "bucket '$b'"; else fail "bucket '$b'" "missing"; fi
done

# ── 4. StarRocks (only if the Tutorial 3 overlay is running) ───────────────────
if docker ps --format '{{.Names}}' | grep -q starrocks; then
  info "StarRocks (Tutorial 3)"
  if retry 60 curl -sf http://localhost:8030/api/health; then
    pass "StarRocks FE  ${DIM}:8030${RESET}"
  else
    fail "StarRocks FE" ":8030 not healthy"
  fi
fi

# ── Summary ────────────────────────────────────────────────────────────────────
echo
if [ "$FAILED" -eq 0 ]; then
  printf "${GREEN}All components up and wired. Safe to build.${RESET}\n"
else
  printf "${RED}One or more checks failed — fix the ✗ items before building.${RESET}\n"
  printf "${DIM}Tip: 'make logs' tails the Fluss servers; 'docker compose logs <svc>' for the rest.${RESET}\n"
fi
exit "$FAILED"