# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Docker Compose lab, not an application: no build, no test suite, no linter. It demonstrates the
**streamhouse** pattern — Apache Fluss as a sub-second hot tier tiering into Iceberg-on-MinIO
(cataloged by Nessie), with Flink 1.20 as compute. It backs a blog series, so the deliverable is a
*reproducible experiment*, not a feature.

Everything here is **Python or SQL**: `scripts/iot_producer.py` produces, Flink SQL processes.
There is no other producer — flink-faker was removed deliberately.

The narrative is a **real-time IoT pipeline**: `make produce` publishes JSON to the Kafka topics
`iot-telemetry` / `iot-events` → Flink (`sql/exp1-pipeline.sql`) lands them in Fluss → a
per-reading enriched table + a 1-minute windowed fact table, both tiered to Iceberg, read by
StarRocks. It mirrors the author's Mage/lambda pipeline (Kafka → Mage → a second Kafka topic →
StarRocks Routine Load), with the second copy removed.

Kafka is the **ingress**, not an Experiment-4-only dependency. The topics, the field names and
the JSON encoding are the whole contract: swap in any producer on the same topics and
`sql/exp1-pipeline.sql` onward is unchanged.

`sql/` is organised per experiment (`exp1-*` the pipeline and its reads, `exp2-*` the
Kafka-vs-Fluss benchmark), plus `sql/common/catalog.sql` — the
`fluss_catalog` DDL every Flink SQL session needs. The SQL client has **no INCLUDE**: paste it
first interactively, and the scripts concatenate it
(`cat /sql/common/catalog.sql <file> > /tmp/run.sql`).

Two claims, and **one experiment per claim** — keep it that way:
1. **Experiment 1, vs a lakehouse** — the bare table answers now; the `$lake` path waits for the
   next flush. Parts: A produce, B pipeline, C query hot vs cold, D StarRocks over the cold tier
   (part D is a *part*, not a third experiment — it proves the cold copy is plain Iceberg).
2. **Experiment 2, vs Kafka** — a topic has no index, so a point query means scanning every
   offset.
3. **Experiment 3, vs medallion staging** — the cold tier is versioned: write a curated table on
   a Nessie branch, audit it there, merge to `main` only on a pass. `make wap` / `make wap-break`.
   The cycle is **lake-only**: Flink is the compute, the curated table is Iceberg with no Fluss
   counterpart (never tiered, Fluss does not know it exists), the merge is one Nessie HTTP call,
   and StarRocks reads whichever ref you point a catalog at. Nothing in the loop writes Fluss or
   Kafka, which is why a failed audit cannot stall ingest.

**Docs split:** `README.md` is what this is, how to run it, and the repo layout — nothing
longer than the quick-start table. `docs/EXPERIMENTS.md` is the experiments (run this, expect that).
`docs/EXPLANATION.md` is the why — the argument, the hard constraints, the Fluss ⇄ Nessie ⇄
Iceberg seam, sql-client gotchas, versions. Keep it that way: a "why" paragraph belongs in
EXPLANATION with a link from EXPERIMENTS, not inline.

"Testing" means running the experiments in `docs/EXPERIMENTS.md` and checking their stated pass
criteria.

## Commands

```bash
make up          # jars + whole stack + verify gate
make verify      # liveness gate (containers, endpoints, TM registration, buckets)
make sql         # interactive Flink SQL client; paste sql/common/catalog.sql, then a file
make produce     # Experiment 1 ingress: scripts/iot_producer.py -> Kafka
make tiering     # submit the Fluss→Iceberg tiering job
make demo        # Exp 1 part C: loops sql/exp1-contrast.sql
make starrocks   # Exp 1 part D: StarRocks overlay
make bench-load  # Exp 2: bulk producer + the Flink load job
make bench       # Exp 2: Fluss vs Kafka point-lookup cost
make wap         # Exp 3: write-audit-publish on a Nessie branch
make wap-break   # Exp 3: the same cycle with a corrupt row — audit fails, main untouched
make down        # docker compose down -v — the correct full reset
```

Running SQL non-interactively (what `demo.sh`, `bench.sh` and `make bench-load` do — the catalog
DDL is concatenated on because the client has no INCLUDE):

```bash
docker compose run --rm -T sql-client sh -c \
  "cat /sql/common/catalog.sql /sql/<file>.sql > /tmp/run.sql && /opt/flink/bin/sql-client.sh -f /tmp/run.sql"
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
`datalake_device_telemetry`, `datalake_device_health_1min`, `iot_events` — has
**no primary key**. Paimon implements it; Iceberg union read on PK tables is post-0.9.

**A log-table sink cannot consume a PK table's changelog.** Reading a PK table in streaming mode
emits `-U/+U`, and an append-only sink rejects it
(`doesn't support consuming update and delete changes`). So making the tiered table append-only
forces its source to be append-only too — hence `iot_telemetry` is also a log table. Only
`dim_device` stays PK: it is the lookup-join build side, where the point lookups actually happen
and nothing streams out. (`bench_telemetry` in Experiment 2 is PK and untiered.)

Same reason the IoT fact table uses a **processing-time** tumbling window: a proctime tumble
needs no watermark and emits append-only. An unbounded `GROUP BY` would emit a changelog and the
sink would reject it.

**Nessie branches are for derived tables and readers, never for the streaming writer.** The
tiering service takes one `--datalake.iceberg.ref` and Fluss tracks one lake snapshot per table,
so there is a single writer to `main`. Merges are per content key, which is why Experiment 3's
`curated.*` branch merges cleanly while tiering keeps committing `fluss.*` to `main`.

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
Flink classpath — recipe is in `docs/EXPLANATION.md`), or `make down`.

**The Fluss catalog ignores `CREATE TABLE IF NOT EXISTS`** — it still errors if the table exists.

**JSON timestamps on Kafka need `'json.timestamp-format.standard' = 'ISO-8601'`.** Python's
`datetime.isoformat()` emits `2025-09-09T20:15:30.123` with a `T`; Flink's JSON format defaults
to `SQL`, which expects a space, and silently yields NULL. Set it on both producer and consumer
(the producer and `sql/exp1-pipeline.sql` both do).

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
(`sql/exp1-contrast.sql` does this).

Both producers are **bounded** by `ROWS`: `make produce` is 200k readings at 50/s (~66 min) plus
~20k events; `make bench-load` is 20M at 20k/s (~17 min). Every contrast in this repo only exists
while data is still arriving. Benchmarking or demoing after the producer finishes shows frozen
numbers that look like a bug and are not one.

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
