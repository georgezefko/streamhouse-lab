.PHONY: jars up verify down ps logs sql tiering demo bench starrocks sr-sql clean

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

down:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f coordinator-server tablet-server

# Open the Flink SQL client (paste from sql/01-tables.sql then sql/02-ingest-and-query.sql)
sql:
	docker compose run --rm sql-client

# Scenario 2 — start the Fluss -> Iceberg tiering job (after tables exist)
tiering:
	bash scripts/start-tiering.sh

# Scenario 2 payoff — hot vs cold, side by side. Needs the tiering job already running.
# `make demo N=12` for more iterations.
demo:
	bash scripts/demo.sh $(N)

# Scenario 3 — StarRocks over the cold tier
starrocks:
	docker compose -f docker-compose.yml -f docker-compose.starrocks.yml up -d starrocks
	@echo "StarRocks (MySQL protocol)  mysql -h 127.0.0.1 -P 9030 -u root"

sr-sql:
	mysql -h 127.0.0.1 -P 9030 -u root

# Scenario 4 — point-lookup cost: Fluss vs Kafka vs Iceberg.
# Needs sql/05-bench-load.sql running in `make sql` and the tiering job up.
bench:
	bash scripts/bench.sh

clean: down
	rm -f lib/*.jar