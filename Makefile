# StarRocks lives in the overlay, so every command touching it needs both files.
SR_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.starrocks.yml

.PHONY: jars up verify down ps logs sql produce tiering demo starrocks sr-sql clean

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
# The producer profile must be named too — compose ignores containers whose profile is
# inactive, so without it the producer keeps running against a torn-down broker.
down:
	$(SR_COMPOSE) --profile producer down -v

ps:
	docker compose ps

logs:
	docker compose logs -f coordinator-server tablet-server

# Open the Flink SQL client. Paste sql/catalog.sql first, then the step's file.
# Throwaway container per invocation, so concurrent sessions are fine.
sql:
	docker compose run --rm sql-client

# Step 1 — the device fleet. This is the ingress; there is no other.
# `RATE=200 ROWS=0 make produce` to override.
produce:
	docker compose --profile producer up -d iot-producer
	@echo "producing to iot-telemetry / iot-events — docker compose logs -f iot-producer"

# Step 3 — start the Fluss -> Iceberg tiering job (after sql/01-pipeline.sql has run)
tiering:
	bash scripts/start-tiering.sh

# Step 4 — hot vs cold, side by side. Needs the tiering job already running.
# `make demo N=12` for more iterations.
demo:
	bash scripts/demo.sh $(N)

# Step 5 — StarRocks over the cold tier
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

clean: down
	rm -f lib/*.jar