# StarRocks lives in the overlay, so every command touching it needs both files.
SR_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.starrocks.yml

.PHONY: jars up verify down ps logs sql produce tiering demo starrocks sr-sql bench-load bench wap wap-break clean

# Setup — fetch Fluss server-side Iceberg jars (run once)
jars:
	bash scripts/download-jars.sh

# Bring-up — hot loop + lakehouse deps (ZK, MinIO, Nessie, Fluss, Flink, Kafka)
# Runs the liveness gate at the end so a green/red result is the last thing you see.
up: jars
	docker compose up -d
	@echo "Flink UI  http://localhost:8083"
	@echo "MinIO     http://localhost:9001  (admin/password)"
	@echo "Nessie    http://localhost:19120"
	@echo
	@$(MAKE) --no-print-directory verify

# Liveness gate — confirm every component is up and wired before building
verify:
	bash scripts/verify.sh

# Full reset. Includes the StarRocks overlay on purpose: StarRocks caches Iceberg metadata,
# so a survivor of `down -v` serves manifest paths whose files no longer exist in MinIO.
# The producer profiles must be named too — compose ignores containers whose profile is
# inactive, so without these the producers keep running against a torn-down broker.
down:
	$(SR_COMPOSE) --profile producer --profile bench down -v

ps:
	docker compose ps

logs:
	docker compose logs -f coordinator-server tablet-server

# Open the Flink SQL client. Paste sql/common/catalog.sql first, then the experiment's file.
# Throwaway container per invocation, so concurrent sessions are fine.
sql:
	docker compose run --rm sql-client

# Experiment 1, part A — the device fleet. This is the ingress; there is no other.
# `RATE=200 ROWS=0 make produce` to override.
produce:
	docker compose --profile producer up -d iot-producer
	@echo "producing to iot-telemetry / iot-events — docker compose logs -f iot-producer"

# Experiment 1, part B — start the Fluss -> Iceberg tiering job (after the tables exist)
tiering:
	bash scripts/start-tiering.sh

# Experiment 1, part C — hot vs cold, side by side. Needs the tiering job already running.
# `make demo N=12` for more iterations.
demo:
	bash scripts/demo.sh $(N)

# Experiment 1, part D — StarRocks over the cold tier
starrocks:
	$(SR_COMPOSE) up -d starrocks
	@echo "StarRocks (MySQL protocol)  mysql -h 127.0.0.1 -P 9030 -u root"

# Uses the host's mysql client if there is one (nicer paste behaviour), otherwise the one
# inside the StarRocks container. A bare macOS shell has no mysql; that is not a problem.
sr-sql:
	@if command -v mysql >/dev/null 2>&1; then \
	  mysql -h 127.0.0.1 -P 9030 -u root; \
	else \
	  echo "no host mysql client — using the one in the container"; \
	  $(SR_COMPOSE) exec starrocks mysql -h 127.0.0.1 -P 9030 -u root; \
	fi

# Experiment 3 — write-audit-publish on a Nessie branch. Needs the Exp 1 pipeline + tiering
# running (it publishes from datalake_device_health_1min).
wap:
	python3 scripts/wap.py

# The same cycle with one corrupt row injected: the audit fails, main is never touched.
wap-break:
	python3 scripts/wap.py --break

# Experiment 2, step 1 — bulk-load the same readings into a Kafka topic and a Fluss PK table.
# Starts the producer and submits a detached Flink job, then returns. Give it ~100 s before
# benching, and bench WHILE it is still loading.
bench-load:
	docker compose --profile bench up -d bench-producer
	docker compose run --rm -T sql-client sh -c \
	  "cat /sql/common/catalog.sql /sql/exp2-bench-load.sql > /tmp/run.sql && /opt/flink/bin/sql-client.sh -f /tmp/run.sql"
	@echo "loading bench-telemetry — docker compose logs -f bench-producer"

# Experiment 2, step 2 — point-lookup cost: Fluss vs Kafka. Nothing here is tiered.
# Run it two or three times a minute apart while `make bench-load` is still loading.
bench:
	bash scripts/bench.sh

clean: down
	rm -f lib/*.jar