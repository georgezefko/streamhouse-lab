# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Docker Compose lab, not an application: no build, no test suite, no linter. It demonstrates the
**streamhouse** pattern — Apache Fluss as a sub-second hot tier tiering into Iceberg-on-MinIO
(cataloged by Nessie), with Flink 1.20 as compute. It backs a blog post, so the deliverable is a
*reproducible tutorial*, not a feature.

Everything here is **Python or SQL**: `scripts/iot_producer.py` produces, Flink SQL processes.
There is no other producer — flink-faker was removed deliberately.

The narrative is a **real-time IoT pipeline**: `make produce` publishes JSON to the Kafka topics
`iot-telemetry` / `iot-events` → Flink (`sql/01-pipeline.sql`) lands them in Fluss → a
per-reading enriched table + a 1-minute windowed fact table, both tiered to Iceberg, read by
StarRocks.

Kafka is the **ingress**. The topics, the field names and the JSON encoding are the whole
contract: swap in any producer on the same topics and `sql/01-pipeline.sql` onward is unchanged.

**One tutorial, six steps**, and `sql/` is numbered to match: 0 `make up` · 1 `make produce` ·
2 `sql/01-pipeline.sql` · 3 `make tiering` · 4 `sql/02-live.sql` + `make demo`
(`sql/03-contrast.sql`) · 5 `make starrocks` + `sql/04-starrocks.sql`. Plus `sql/catalog.sql` —
the `fluss_catalog` DDL every Flink SQL session needs. The SQL client has **no INCLUDE**: paste
it first interactively, and `demo.sh` concatenates it
(`cat /sql/catalog.sql <file> > /tmp/run.sql`).

The claim the tutorial makes, and the only one — keep it that way: **vs a lakehouse**, the bare
table answers now while the `$lake` path waits for the next flush; and the cold copy is plain
Iceberg, which step 5 proves by reading it from StarRocks with Fluss nowhere in the path.
Two earlier experiments (a Kafka-vs-Fluss point-lookup benchmark, and write-audit-publish on a
Nessie branch) were removed on `feat/tutorial1`. Do not reintroduce them here.

**Docs split:** `README.md` is what this is, how to run it, and the repo layout — nothing longer
than the quick-start table. `docs/TUTORIAL.md` is everything else: the six steps (run this,
expect that), then "Why it is built this way" — hard constraints, the Fluss ⇄ Nessie ⇄ Iceberg
seam, sql-client gotchas, design notes, troubleshooting, versions. There is no EXPLANATION.md
any more; a "why" paragraph belongs in TUTORIAL's second half with an in-page link from the
step, not inline in the step.

"Testing" means running the steps in `docs/TUTORIAL.md` and checking their stated pass criteria.

## Commands

```bash
make up          # jars + whole stack + verify gate
make verify      # liveness gate (containers, endpoints, TM registration, buckets)
make sql         # interactive Flink SQL client; paste sql/catalog.sql, then a file
make produce     # step 1: scripts/iot_producer.py -> Kafka
make tiering     # step 3: submit the Fluss→Iceberg tiering job
make demo        # step 4: loops sql/03-contrast.sql
make starrocks   # step 5: StarRocks overlay
make down        # docker compose down -v — the correct full reset
```

Running SQL non-interactively (what `demo.sh` does — the catalog DDL is concatenated on because
the client has no INCLUDE):

```bash
docker compose run --rm -T sql-client sh -c \
  "cat /sql/catalog.sql /sql/<file>.sql > /tmp/run.sql && /opt/flink/bin/sql-client.sh -f /tmp/run.sql"
```

`./sql` is mounted at `/sql`. To run ad-hoc SQL, write a file into `./sql/`, run it, delete it —
do not try to pipe SQL through nested shell quoting, it mangles quoted SQL literals.

## Hard constraints

These are non-obvious, cost real debugging time, and are load-bearing for the demo.

**Union read requires log tables.** Querying the bare table (hot ∪ cold) merges the lake snapshot
with the Fluss log. On a PK table that is a *sort-merge*, so
`LakeSnapshotAndLogSplitScanner` requires the lake reader to implement
`org.apache.fluss.lake.source.SortedRecordReader`. `fluss-lake-iceberg-0.9.1-incubating` does not
implement it anywhere — verified by unpacking the jar. The read fails with
`lake records must instance of sorted view`. This is why every tiered table here —
`iot_telemetry`, `iot_events`, `datalake_device_telemetry`, `datalake_device_health_1min` — has
**no primary key**. Paimon implements it; Iceberg union read on PK tables is post-0.9.

**A log-table sink cannot consume a PK table's changelog.** Reading a PK table in streaming mode
emits `-U/+U`, and an append-only sink rejects it
(`doesn't support consuming update and delete changes`). So making the tiered table append-only
forces its source to be append-only too — hence `iot_telemetry` is also a log table. Only
`dim_device` stays PK: it is the lookup-join build side, where the point lookups actually happen
and nothing streams out.

Same reason the IoT fact table uses a **processing-time** tumbling window: a proctime tumble
needs no watermark and emits append-only. An unbounded `GROUP BY` would emit a changelog and the
sink would reject it.

**Nessie is ROCKSDB on a named volume** (`user: "0:0"` — a named volume mounts root-owned and the
image's uid 10000 cannot mkdir in it). Branches survive restarts; `make down` still wipes them.

**The Iceberg table appears in Nessie at `CREATE TABLE`, not at tiering time.** A
`table.datalake.enabled` CREATE makes the Fluss *coordinator* register an empty Iceberg table
`fluss.<name>` on Nessie's `main` branch, plus an empty `metadata.json` in MinIO; `make tiering`
is what then writes Parquet and commits a snapshot per flush. So an empty `$lake` with a live
Nessie entry is normal, not a failure.

**Dropping a tiered table leaves an orphan in Nessie.** `DROP TABLE` removes the Fluss table but
not the Iceberg one, so the recreate fails with `Table fluss.<name> already exists` even though
`SHOW TABLES` does not list it. Drop it through an Iceberg catalog (the jars are already on the
Flink classpath — recipe is in `docs/TUTORIAL.md`), or `make down`.

**The Fluss catalog ignores `CREATE TABLE IF NOT EXISTS`** — it still errors if the table exists.

**JSON timestamps on Kafka need `'json.timestamp-format.standard' = 'ISO-8601'`.** Python's
`datetime.isoformat()` emits `2025-09-09T20:15:30.123` with a `T`; Flink's JSON format defaults
to `SQL`, which expects a space, and silently yields NULL. Set it on both producer and consumer
(the producer and `sql/01-pipeline.sql` both do).

**Use the native Nessie catalog, not Iceberg-REST.** `NessieCatalog` against `/api/v2`. Nessie's
Iceberg-REST `createTable` NPEs with Fluss 0.9.1's Iceberg 1.10 client. StarRocks still reads via
the REST endpoint — reads are fine, only REST writes NPE.

**Flink is pinned to 1.20.** The Fluss connector is built for it. Do not bump to 2.x.

## sql-client gotchas

- `/opt/sql-client/sql-client` (the image's default command) hardcodes its args and drops `"$@"`,
  so `-f` is silently ignored. Call `/opt/flink/bin/sql-client.sh` directly.
- It **exits 0 even when a statement fails**. Both scripts grep output for `[ERROR]` instead.
- `-f` skips the image's init script, so its pre-baked demo sources do not exist in a scripted
  session. Scripted SQL must define every source it uses.
- **Qualify every `CREATE TEMPORARY TABLE` / `CREATE TEMPORARY VIEW`.** An unqualified `CREATE`
  lands in whatever catalog is current, which breaks when a file is pasted after one ending in
  `USE CATALOG fluss_catalog`.
- The bare table is unreadable in batch until the first lake snapshot exists
  (`Batch mode can only be supported if one lake snapshot exists`).
- Live queries need the *interactive* client: `SET 'execution.runtime-mode' = 'streaming'` plus
  `result-mode = 'table'`. `-f` cannot render an updating view.

## Writing demo queries

The producer draws `reading_id` **at random**, not monotonically. `max(reading_id)` is not the
newest row — it is usually one tiered long ago, so a `max()`-based freshness test reports a false
negative. Use an anti-join against `$lake` to find rows that genuinely are not in the lake
(`sql/03-contrast.sql` does this).

The producer is **bounded** by `ROWS`: `make produce` is 200k readings at 50/s (~66 min) plus
~20k events. Every contrast in this repo only exists while data is still arriving. Demoing after
the producer finishes shows frozen numbers that look like a bug and are not one.

## Diagrams

**Avoid `subgraph`.** Mermaid packs nodes against the top of a subgraph as soon as edges cross
its boundary, and the title is drawn over them — shortening the title does not fix it. Both
diagrams here are instead a linear chain of plain nodes, one per stage, with **numbered edge
labels** carrying the sequence (`1 · JSON, 50/s`). A node never overlaps its own label, the flow
reads in one direction, and a stage that loops back (Flink reading Fluss and writing it again) is
just two numbered edges. Put the contents of a stage in its node label across `<br/>` lines.

## Jars

`lib/*.jar` is gitignored and fetched by `scripts/download-jars.sh`. The Flink image ships **no**
Iceberg at all, so the full plugin set is mounted via the `x-flink-iceberg-vols` anchor in
`docker-compose.yml`; the Fluss servers get their own set via `x-fluss-iceberg-vols`. Adding an
Iceberg-side dependency usually means editing both the download script and both anchors.

`classloader.check-leaked-classloader: false` is set on all three Flink services — Hadoop's static
`FileSystem` cache pins the user classloader and trips Flink's leak check intermittently across
repeated SQL sessions.
