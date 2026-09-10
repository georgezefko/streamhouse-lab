.PHONY: jars up verify down ps logs sql tiering demo demo-orders bench starrocks sr-sql clean

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
down:
	docker compose -f docker-compose.yml -f docker-compose.starrocks.yml down -v

ps:
	docker compose ps

logs:
	docker compose logs -f coordinator-server tablet-server

# Open the Flink SQL client. Experiment 1: paste sql/07-iot-produce.sql, then sql/08 in a
# second session.
# Throwaway container per invocation, so concurrent sessions are fine.
sql:
	docker compose run --rm sql-client

# Experiment 1 — a Python producer as the ingress instead of sql/07-iot-produce.sql.
# Run this OR sql/07, never both: same topics, so both together means double the data.
# `RATE=200 ROWS=0 make produce` to override.
produce:
	docker compose --profile producer up -d iot-producer
	@echo "producing to iot-telemetry / iot-events — docker compose logs -f iot-producer"

# Experiment 1 — start the Fluss -> Iceberg tiering job (after the tables exist)
tiering:
	bash scripts/start-tiering.sh

# Experiment 2 — hot vs cold, side by side. Needs the tiering job already running.
# `make demo N=12` for more iterations.
demo:
	bash scripts/demo.sh $(N)

# The same contrast on the orders appendix (sql/01-03).
demo-orders:
	SQL_FILE=/sql/03-contrast.sql bash scripts/demo.sh $(N)

# Experiment 3 — StarRocks over the cold tier
starrocks:
	docker compose -f docker-compose.yml -f docker-compose.starrocks.yml up -d starrocks
	@echo "StarRocks (MySQL protocol)  mysql -h 127.0.0.1 -P 9030 -u root"

# Needs a mysql client ON THE HOST (a bare macOS shell may not have one).
# Without it:  docker compose exec starrocks mysql -h 127.0.0.1 -P 9030 -u root
sr-sql:
	mysql -h 127.0.0.1 -P 9030 -u root

# Experiment 4 — point-lookup cost: Fluss vs Kafka. Nothing here is tiered.
# Needs sql/05-bench-load.sql still loading in a `make sql` session.
bench:
	bash scripts/bench.sh

clean: down
	rm -f lib/*.jar