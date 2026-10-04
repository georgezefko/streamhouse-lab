# Tutorial — running the lab

Run this, expect that. The narrative is in the blog post; this file is the six commands, what a
pass looks like, and what the common failures mean. Why it is built this way — the constraints,
the fixes, the error messages — is in [NOTES.md](NOTES.md).

| Step | Command | What it does |
|---|---|---|
| 0 | `make up` | bring the stack up, gate on liveness |
| 1 | `make produce` | sensor data onto Kafka |
| 2 | `make sql` → `sql/01-pipeline.sql` | land it in Fluss, enrich, window |
| 3 | `make tiering` | copy hot → cold, every 30 s |
| 4 | `sql/02-live.sql`, `make demo` | query hot vs cold |
| 5 | `make starrocks` → `sql/04-starrocks.sql` | read the cold tier as plain Iceberg |

Steps run in order. Every Flink SQL session starts with `sql/catalog.sql` — the SQL client has
no INCLUDE, so you paste it first interactively, and `demo.sh` concatenates it onto the file it
runs.

## Step 0 — bring the stack up

```bash
make up          # or `make verify` on its own, any time
```

**Pass:** every line ✓, exit 0, ending in `All components up and wired. Safe to build.`

It is a liveness gate only — containers, MinIO/Nessie/Flink endpoints, TaskManager registration,
both buckets. It does not assert the Fluss→Iceberg seam, which does not exist until step 3.

**When it fails:**
- *Fluss containers restarting* — expected on first bring-up. They use `restart: on-failure` to
  survive the Nessie boot race; `depends_on` waits for container start, not for Quarkus to be
  serving `:19120`. Give it a minute.

---

## Step 1 — put sensor data on Kafka

```bash
make produce                      # scripts/iot_producer.py in a container
```

50 readings/s for 200k readings (~66 min), plus ~5 events/s onto `iot-events`.
`RATE=200 ROWS=0 make produce` to override; `docker compose logs -f iot-producer` to watch.

**Pass:** the topic has messages.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic iot-telemetry --from-beginning --max-messages 2
```

```json
{"reading_id":19974442,"device_id":"device_9","event_time":"2026-09-09T19:21:03.810",
 "energy_usage":2.31,"temperature":26.4,"vibration":1.2,"signal_strength":88}
```

**When it fails:**
- *nothing on the topic* — check `make verify` shows Kafka green.
- The topics, field names and JSON encoding are the whole contract. Swapping in your own
  producer leaves step 2 onward unchanged; keep timestamps ISO-8601
  ([why](NOTES.md#json-timestamps-on-kafka-need-iso-8601)).
  `python3 scripts/iot_producer.py --selftest` checks the row shapes without a broker.

---

## Step 2 — land it in Fluss, enrich, window

```bash
make sql                          # paste sql/catalog.sql, then sql/01-pipeline.sql
```

Five Fluss tables and two detached jobs: job 1 lands both topics, job 2 does the lookup join
against `dim_device` and fans the enriched stream into the per-reading table and the 1-minute
windowed fact table. Each `EXECUTE STATEMENT SET` compiles into one job with two sinks, which is
why the Flink UI shows names like `insert-into_...datalake_device_telemetry,fluss_catalog...`.

**Pass:**
- the `dim_device` seed job shows **FINISHED** in the Flink UI
  ([`:8083`](http://localhost:8083)), with 2 jobs **RUNNING** alongside it
- readings are in Fluss — in the same session:
  ```sql
  SET 'sql-client.execution.result-mode' = 'tableau';
  SELECT * FROM iot_telemetry LIMIT 5;
  ```
  Five rows, then it stops. Add `WHERE temperature > 29` and it still answers straight away.
  Leave the mode on streaming: the bare table is unreadable in *batch* until the first lake
  snapshot exists, which does not happen until step 3.
- `SELECT * FROM datalake_device_telemetry LIMIT 5` has `temp_threshold`, `location_id` and
  `model` populated — the lookup join is resolving

**When it fails:**
- *`Batch mode can only be supported if one lake snapshot exists`* — you set batch mode before
  tiering. Expected; stay in streaming for now, or come back after step 3.
- *`Table fluss.<name> already exists` right after a `DROP TABLE`* — dropping a tiered table
  leaves an orphan in Nessie.
  [Recipe](NOTES.md#dropping-a-tiered-table-leaves-an-orphan-in-nessie).
- *`Table sink ... doesn't support consuming update and delete changes`* — a PK table is feeding
  an append-only sink. Everything downstream of `iot_telemetry` must be append-only.
  [Why](NOTES.md#a-log-table-sink-cannot-consume-a-pk-tables-changelog).
- *`temp_threshold` all NULL* — the seed did not finish before the derive jobs started.
  `sql/01-pipeline.sql` uses `SET 'table.dml-sync' = 'true'` around it; if you pasted out of
  order, re-run the seed.
- *every `event_time` NULL, everything else populated* — a JSON timestamp-format mismatch.
  [Why](NOTES.md#json-timestamps-on-kafka-need-iso-8601).
- *zero rows, jobs RUNNING* — nothing is on the topic. Run step 1 first.
- *`datalake_device_health_1min` stays empty past 2 minutes* — check the second derive job's
  *Exceptions* tab in the Flink UI.

---

## Step 3 — turn on tiering

```bash
make tiering
```

The Fluss Lakehouse Tiering Service — a long-running Flink job, not a compose service. Every
`table.datalake.freshness` (30 s here) it writes Parquet under `s3://warehouse/fluss/<name>/`
and commits one Iceberg snapshot per flush through Nessie.

**Pass:** a third job RUNNING in the Flink UI, and within ~30 s:

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT count(*) FROM datalake_device_telemetry$lake;       -- non-zero
SELECT * FROM datalake_device_telemetry$lake$snapshots;    -- one row per flush
```

**When it fails:**
- *`$lake` stuck at 0* — the job is not running or crashed. Flink UI, then `make logs`.
- *the Iceberg tables were already in Nessie before this step* — correct, they appear at
  `CREATE TABLE`, not here. [The seam](NOTES.md#when-the-iceberg-table-actually-appears-in-nessie).

---

## Step 4 — query hot and cold

Same table, same SQL, two read paths — the bare table unions hot + cold, the `$lake` suffix
reads cold only.

```bash
make sql                          # paste sql/catalog.sql, then sql/02-live.sql
make demo                         # `make demo N=12` for more iterations
```

`sql/02-live.sql` is paste-only and runs in the post's order: **A** the cold count in batch
(0 before tiering), **B** the same table streaming — the union-read count climbing in place,
then an anomaly feed — and **C** after tiering, the snapshot list and the end-to-end latency
histogram. `make demo` loops `sql/03-contrast.sql` and prints `hot_plus_cold`, `cold_only` and
`rows_only_in_hot` on one line.

**Pass:** `rows_only_in_hot` > 0 on every iteration, `cold_only` flat and then jumping when a
flush lands, and the anti-join naming actual readings — rows you can query right now that are
not on the Iceberg path yet. `table.datalake.freshness` is a target, not a promise: each flush
writes Parquet and commits an Iceberg snapshot, so on a laptop the lake often drifts to most of
a minute behind rather than the configured 30 s. The sharper version: cancel the tiering job in the Flink UI and re-run
`make demo`. `cold_only` freezes while `hot_plus_cold` keeps climbing; `make tiering` makes the
cold number catch up in one jump.

The fact-table counts are often **equal** between the two paths — windows close once a minute
while tiering flushes every 30 s, so the lake catches up between windows. A per-reading stream
shows a permanent gap; a windowed aggregate does not.

**When it fails:**
- *`cold_only` stuck at 0* — the tiering job is not running.
- *`Batch mode can only be supported if one lake snapshot exists`* — nothing tiered yet. Wait
  ~30 s after `make tiering`.
- *`lake records must instance of sorted view`* — a tiered table has a PRIMARY KEY.
  [Why that is fatal](NOTES.md#union-read-requires-log-tables).
- *`Trying to access closed classloader`* — Hadoop's static `FileSystem` cache pins the user
  classloader. Disabled via `classloader.check-leaked-classloader: false` in
  `docker-compose.yml`; if you see it, that setting did not reach the service that threw.
- *numbers identical between iterations, `rows_only_in_hot` = 0* — the producer finished and
  tiering caught up. Correct, but no longer a contrast. Run `make demo` **while `make produce`
  is still running** ([why](NOTES.md#why-the-producer-is-bounded-and-what-drains)).

---

## Step 5 — StarRocks reads the cold tier only

The cold tier is just Iceberg, so an OLAP engine reads it with Fluss nowhere in the path.

```bash
make starrocks                    # wait for the healthcheck (~1-2 min)
make sr-sql                       # paste sql/04-starrocks.sql
```

`sr-sql` uses the host's `mysql` client when there is one and the container's otherwise. To run
the file without pasting:

```bash
docker compose -f docker-compose.yml -f docker-compose.starrocks.yml exec -T starrocks \
  mysql -h 127.0.0.1 -P 9030 -u root < sql/04-starrocks.sql
```

**Pass:** `SHOW DATABASES FROM iceberg_nessie` lists the `fluss` database, and

```sql
SELECT count(*) FROM datalake_device_telemetry;
```

returns a value **below** the `hot_plus_cold` number from step 4. That gap is the point:
StarRocks sees only what tiering has flushed, reading Parquet out of MinIO through a Nessie
catalog, exactly as any Iceberg-aware engine would.

**When it fails:**
- *catalog registers but no databases* — nothing tiered yet. Run steps 1-3 first.
- *FE not healthy* — StarRocks `allin1` is memory-hungry; check Docker's allocation.
- *`Location does not exist: s3://warehouse/...`* —
  [StarRocks caches Iceberg metadata](NOTES.md#starrocks-caches-iceberg-metadata).
