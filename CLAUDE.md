# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Docker Compose lab, not an application: no build, no test suite, no linter. It demonstrates the
**streamhouse** pattern — Apache Fluss as a sub-second hot tier tiering into Iceberg-on-MinIO
(cataloged by Nessie), with Flink 1.20 as compute. It backs a blog series, so the deliverable is a
*reproducible tutorial*, not a feature.

The main narrative is a **real-time IoT pipeline** (`sql/07`-`sql/09`): faker sensors → Fluss →
a per-reading enriched table + a 1-minute windowed fact table, both tiered to Iceberg, read by
StarRocks. It is a streamhouse rebuild of the author's kappa-architecture post
(Kafka → Spark Structured Streaming → StarRocks, no lake).

Two claims the repo exists to demonstrate:
1. **vs a lakehouse** — the bare table answers now; the `$lake` path waits for the next flush.
2. **vs Kafka** — a topic has no index, so a point query means scanning every offset.

**Docs split:** `README.md` is the tutorials (run this, expect that).
`docs/EXPLANATION.md` is the why — the argument, the hard constraints, the Fluss ⇄ Nessie ⇄
Iceberg seam, sql-client gotchas, versions. Keep it that way: a "why" paragraph belongs in
EXPLANATION with a link from the README, not inline.

"Testing" means running the tutorials in the README and checking their stated pass criteria.

## Commands

```bash
make up          # jars + whole stack + verify gate
make verify      # liveness gate (containers, endpoints, TM registration, buckets)
make sql         # interactive Flink SQL client; paste sql/*.sql by hand
make tiering     # submit the Fluss→Iceberg tiering job
make demo        # Tutorial 2: loops sql/08-iot-contrast.sql
make demo-orders # same, on the orders appendix (SQL_FILE=/sql/03-contrast.sql)
make starrocks   # Tutorial 3 overlay
make bench       # Tutorial 4: Fluss vs Kafka point-lookup cost
make down        # docker compose down -v — the correct full reset
```

Running SQL non-interactively (what `demo.sh` and `bench.sh` do):

```bash
docker compose run --rm -T sql-client /opt/flink/bin/sql-client.sh -f /sql/<file>.sql
```

`./sql` is mounted at `/sql`. To run ad-hoc SQL, write a file into `./sql/`, run it, delete it —
do not try to pipe SQL through nested shell quoting, it mangles the doubled single quotes that
flink-faker expressions need.

## Hard constraints

These are non-obvious, cost real debugging time, and are load-bearing for the demo.

**Union read requires log tables.** Querying the bare table (hot ∪ cold) merges the lake snapshot
with the Fluss log. On a PK table that is a *sort-merge*, so
`LakeSnapshotAndLogSplitScanner` requires the lake reader to implement
`org.apache.fluss.lake.source.SortedRecordReader`. `fluss-lake-iceberg-0.9.1-incubating` does not
implement it anywhere — verified by unpacking the jar. The read fails with
`lake records must instance of sorted view`. This is why every tiered table here —
`datalake_device_telemetry`, `datalake_device_health_1min`, `iot_events`,
`datalake_enriched_orders` — has **no primary key**. Paimon implements it; Iceberg union read on PK tables is post-0.9.

**A log-table sink cannot consume a PK table's changelog.** Reading a PK table in streaming mode
emits `-U/+U`, and an append-only sink rejects it
(`doesn't support consuming update and delete changes`). So making the tiered table append-only
forces its source to be append-only too — hence `iot_telemetry` and `fluss_order` are also log
tables. Only `dim_device`, `fluss_customer` and `fluss_nation` stay PK: they are lookup-join
build sides, where the point lookups actually happen and nothing streams out.

Same reason the IoT fact table uses a **processing-time** tumbling window: a proctime tumble
needs no watermark and emits append-only. An unbounded `GROUP BY` would emit a changelog and the
sink would reject it.

**Dropping a tiered table leaves an orphan in Nessie.** `DROP TABLE` removes the Fluss table but
not the Iceberg one, so the recreate fails with `Table fluss.<name> already exists` even though
`SHOW TABLES` does not list it. Drop it through an Iceberg catalog (the jars are already on the
Flink classpath — recipe is in `docs/EXPLANATION.md`), or `make down`.

**The Fluss catalog ignores `CREATE TABLE IF NOT EXISTS`** — it still errors if the table exists.

**Use the native Nessie catalog, not Iceberg-REST.** `NessieCatalog` against `/api/v2`. Nessie's
Iceberg-REST `createTable` NPEs with Fluss 0.9.1's Iceberg 1.10 client. StarRocks still reads via
the REST endpoint — reads are fine, only REST writes NPE.

**Flink is pinned to 1.20.** The Fluss connector is built for it. Do not bump to 2.x.

## sql-client gotchas

- `/opt/sql-client/sql-client` (the image's default command) hardcodes its args and drops `"$@"`,
  so `-f` is silently ignored. Call `/opt/flink/bin/sql-client.sh` directly.
- It **exits 0 even when a statement fails**. Both scripts grep output for `[ERROR]` instead.
- `-f` skips the image's init script, so the pre-baked faker sources (`source_order`,
  `source_customer`, `source_nation`) do not exist in a scripted session. Scripted SQL must define
  its own sources — see `sql/07-iot-pipeline.sql` and `sql/05-bench-load.sql`.
- **Qualify every `CREATE TEMPORARY TABLE` / `CREATE TEMPORARY VIEW`.** An unqualified `CREATE`
  lands in whatever catalog is current, which breaks when a file is pasted after one ending in
  `USE CATALOG fluss_catalog`.
- The bare table is unreadable in batch until the first lake snapshot exists
  (`Batch mode can only be supported if one lake snapshot exists`).
- Live queries need the *interactive* client: `SET 'execution.runtime-mode' = 'streaming'` plus
  `result-mode = 'table'`. `-f` cannot render an updating view.

## Writing demo queries

The faker generates `reading_id` / `order_key` **at random**, not monotonically. `max(id)` is not
the newest row — it is usually one tiered long ago, so a `max()`-based freshness test reports a
false negative. Use an anti-join against `$lake` to find rows that genuinely are not in the lake
(`sql/08-iot-contrast.sql` and `sql/03-contrast.sql` both do this).

Every faker source is **bounded**: `sql/07`'s telemetry is 200k rows at 50/s (~66 min), events
20k at 5/s; `source_order` is 10k at 10/s (~16 min); `sql/05`'s is 20M at 20k/s (~17 min). Every
contrast in this repo only exists while data is still arriving. Benchmarking or demoing a drained
source shows frozen numbers that look like a bug and are not one.

## Jars

`lib/*.jar` is gitignored and fetched by `scripts/download-jars.sh`. The Flink image ships **no**
Iceberg at all, so the full plugin set is mounted via the `x-flink-iceberg-vols` anchor in
`docker-compose.yml`; the Fluss servers get their own set via `x-fluss-iceberg-vols`. Adding an
Iceberg-side dependency usually means editing both the download script and both anchors.

`classloader.check-leaked-classloader: false` is set on all three Flink services — Hadoop's static
`FileSystem` cache pins the user classloader and trips Flink's leak check intermittently across
repeated SQL sessions.
