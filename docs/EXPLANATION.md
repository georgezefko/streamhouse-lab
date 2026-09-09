# Why the streamhouse — the argument, the constraints, the seam

The [README](../README.md) is the tutorial: run this, expect that. This file is the *why*.
Nothing here is needed to follow the tutorials; everything here is needed to change them.

---

## The two claims

The lab exists to make two claims reproducible on a laptop rather than argued in prose.

### 1. vs a lakehouse — the same table answers *now*

A lakehouse table is a set of files plus a manifest. It becomes queryable when a writer commits
a snapshot, so freshness is bounded below by the commit interval — 30 seconds here, minutes to
hours in most production setups. Between commits, the newest data exists but is not readable.

Fluss puts a hot tier in front of that: a log (and, for PK tables, an index) that is readable
the instant a record lands. `table.datalake.enabled` tiers it continuously into Iceberg, and
reading the **bare table** unions the two — hot Fluss log ∪ cold Iceberg snapshot. Reading
`<table>$lake` reads only the committed Iceberg side.

So the same table, the same SQL, gives two answers, and the difference between them is exactly
the data a lakehouse-only reader cannot see yet. That is what `make demo` prints, and what
Tutorial 2 walks through. The gap is not a bug being measured — it is the freshness budget
made visible.

**The sharper version:** cancel the tiering job in the Flink UI and re-run `make demo`.
`cold_only` freezes while `hot_plus_cold` keeps climbing. The lake tier is now visibly a stale
copy while the hot tier keeps serving. Restart it and the cold number catches up in one jump.

### 2. vs Kafka — a topic has no index

A Kafka topic holds the same records. It has offsets, not indexes. To answer *"what is order
424242?"* Flink must deserialize every record from earliest to latest offset. Cost is linear in
retention, and it grows all day.

A Fluss PK table answers the same question with a point lookup. Cost is flat in table size.
Tutorial 4 measures both against the same volume over the same key space; the numbers are
laptop-bound and uninteresting on their own, but the *divergence* as the topic grows is the
whole argument. Kafka + Iceberg gets you a queryable copy only by making a second copy.

### What this replaces

Two reference pipelines, same IoT domain, both the author's:

**Kappa** — Kafka → Spark Structured Streaming → StarRocks, with object storage holding
checkpoints only. No lake at all. Every byte lives in the broker until the streaming job writes
an aggregate into the serving database, so raw history is retention-bound and the only queryable
copy is the one the job decided in advance to compute.

**Lambda (Mage)** — the one this repo's IoT tutorial is shaped after:

```
                        ┌─▶ Mage streaming ─▶ kafka: iot-anomalies ─▶ StarRocks (Routine Load)
kafka: iot-telemetry ───┤
kafka: iot-events    ───┴─▶ Mage ingestion ─▶ MinIO bronze ─▶ Mage batch ─▶ StarRocks
```

Count the copies of the same reading: one on the source topic, one on the anomalies topic, one
in the StarRocks table the Routine Load maintains, one in bronze, one in whatever the batch path
produces. Each is a separate job that can lag, fail, or drift, and the speed layer and batch
layer have to be reconciled because they compute the same numbers by different routes.

**The streamhouse version keeps the ingress and deletes the copies:**

```
kafka: iot-telemetry ───┬─▶ Fluss ──(queryable NOW)──┬─▶ Iceberg on MinIO ─▶ StarRocks
kafka: iot-events    ───┘                            └─ same table, union read
```

Kafka stays — Fluss sits behind the broker rather than replacing it, which is why Tutorial 1
ingests from topics rather than writing into Fluss directly. What goes away is the second and
third copy. There is no anomalies topic, because the anomaly is a column on a row that is
queryable the moment it lands. There is no Routine Load, because StarRocks reads the Iceberg
table that Fluss tiers itself. And there is no speed-layer/batch-layer split to reconcile,
because `SELECT ... FROM t` and `SELECT ... FROM t$lake` are the same table at two freshnesses,
not two pipelines computing the same thing twice.

That is the whole architectural claim. Tutorials 1-3 are its proof.

---

## Hard constraints

These are non-obvious, cost real debugging time, and are load-bearing for the demo.

### Union read requires log tables

Querying the bare table (hot ∪ cold) merges the lake snapshot with the Fluss log. On a PK table
that merge is a **sort-merge**, so Fluss's `LakeSnapshotAndLogSplitScanner` requires the lake
reader to implement `org.apache.fluss.lake.source.SortedRecordReader`.
`fluss-lake-iceberg-0.9.1-incubating` does not implement it anywhere — verified by unpacking
the jar. The read fails with:

```
java.lang.UnsupportedOperationException: lake records must instance of sorted view.
```

Log tables concatenate rather than merge, so they union-read fine. This is why every tiered
table in this repo — `datalake_device_telemetry`, `datalake_device_health_1min`, `iot_events`,
`datalake_enriched_orders` — has **no primary key**.

Paimon's reader does implement the interface. Iceberg union read on PK tables is a post-0.9
roadmap item.

### A log-table sink cannot consume a PK table's changelog

Reading a PK table in streaming mode emits `-U/+U`, and an append-only sink rejects it:

```
Table sink ... doesn't support consuming update and delete changes
```

So making the tiered table append-only forces its source to be append-only too. `iot_telemetry`
and `fluss_order` are log tables for this reason. Only `dim_device`, `fluss_customer` and
`fluss_nation` stay PK: they are lookup-join build sides, where the point lookups actually
happen and where nothing streams *out*.

This is also why the fact table uses a **processing-time** tumbling window. A proctime tumble
needs no watermark and emits append-only rows; an unbounded `GROUP BY` would emit a changelog
and the sink would reject it.

### Dropping a tiered table leaves an orphan in Nessie

`DROP TABLE` removes the Fluss table but not the Iceberg one, so the recreate fails with
`Table fluss.<name> already exists` even though `SHOW TABLES` does not list it. Drop it through
an Iceberg catalog — the jars are already on the Flink classpath:

```sql
CREATE CATALOG ice WITH (
  'type'='iceberg',
  'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',
  'uri'='http://nessie:19120/api/v2',
  'ref'='main',
  'warehouse'='s3://warehouse/',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'='http://minio:9000',
  's3.access-key-id'='admin',
  's3.secret-access-key'='password',
  's3.path-style-access'='true',
  'client.region'='us-east-1'
);
USE CATALOG ice;
SHOW TABLES IN fluss;
DROP TABLE fluss.<name>;
```

Or just `make down`, which is the correct full reset.

### JSON timestamps on Kafka need `ISO-8601`

Python's `datetime.isoformat()` emits `2026-09-09T19:21:03.81` — with a `T`. Flink's JSON format
defaults to `'json.timestamp-format.standard' = 'SQL'`, which expects `2026-09-09 19:21:03.81`
with a space. On a mismatch it does not raise: the column arrives **NULL** and every other field
parses fine, so the pipeline looks healthy and the timestamps are silently gone.

`sql/07` sets it on the producer and `sql/08` on the consumer. Keep both if you swap the
producer. `'json.ignore-parse-errors' = 'true'` on the consumer is the related decision: against
a real topic one malformed message should not kill the job.

### StarRocks caches Iceberg metadata

An external Iceberg catalog in StarRocks caches manifest locations. Reset MinIO and Nessie
underneath a running StarRocks and it will serve paths whose files no longer exist:

```
Location does not exist: s3://warehouse/fluss/datalake_device_telemetry_<uuid>/metadata/<...>.avro
```

`make down` therefore tears down the StarRocks overlay too — otherwise it survives a
`docker compose down -v` that was issued against the base file alone. To recover without a full
reset: `REFRESH EXTERNAL TABLE iceberg_nessie.fluss.<table>;`.

### The Fluss catalog ignores `CREATE TABLE IF NOT EXISTS`

It still errors if the table exists.

### Use the native Nessie catalog, not Iceberg-REST

`NessieCatalog` against `/api/v2`. Nessie's Iceberg-REST `createTable` NPEs with Fluss 0.9.1's
Iceberg 1.10 client (it drops the deprecated `lastColumnId`). StarRocks still reads via the REST
endpoint — reads are fine, only REST *writes* NPE. That asymmetry is deliberate, not an
oversight.

### Flink is pinned to 1.20

The Fluss connector is built for it. Do not bump to 2.x.

---

## How the Fluss ⇄ Nessie ⇄ Iceberg seam actually works (validated)

Getting tiering working end-to-end took four non-obvious fixes. All are in the code now; this is
the map if you touch them.

1. **Use the NATIVE Nessie catalog, not Iceberg-REST.**
   `datalake.iceberg.catalog-impl: org.apache.iceberg.nessie.NessieCatalog` against Nessie's own
   API (`http://nessie:19120/api/v2`, `ref: main`) — **not** `RESTCatalog` against
   `/iceberg/main`. See the constraint above.
2. **The Flink tiering job needs the whole Iceberg plugin set on `/opt/flink/lib`.** The Flink
   image ships **no** Iceberg at all. `scripts/download-jars.sh` fetches `fluss-lake-iceberg`
   (the `LakeStoragePlugin`), `iceberg-nessie` + `nessie-client`/`nessie-model`/jackson/
   microprofile, `hadoop-client-*` (Fluss's tiering writer requires Hadoop), `failsafe`
   (iceberg-aws S3 retries), and `iceberg-flink-runtime` (to *read* `$lake`).
   `docker-compose.yml` mounts them via the `x-flink-iceberg-vols` anchor; the Fluss servers get
   their own set via `x-fluss-iceberg-vols`. Adding an Iceberg-side dependency usually means
   editing the download script and both anchors.
3. **S3 for local MinIO needs the STS assume-role endpoint set.** Fluss vends S3 delegation
   tokens to clients via STS; without pointing STS at MinIO it calls real AWS and gets
   `403 InvalidClientTokenId`. The Fluss servers set `s3.assumed.role.arn` +
   `s3.assumed.role.sts.endpoint: http://minio:9000`.
4. **`failsafe` also belongs in the Fluss server plugin dir** — reading an existing Iceberg
   table's metadata (e.g. `CREATE TABLE IF NOT EXISTS`) needs it server-side, not just on Flink.

`classloader.check-leaked-classloader: false` is set on all three Flink services: Hadoop's
static `FileSystem` cache pins the user classloader and trips Flink's leak check intermittently
across repeated SQL sessions.

---

## sql-client gotchas

- `/opt/sql-client/sql-client` (the image's default command) hardcodes its args and drops
  `"$@"`, so `-f` is silently ignored. Call `/opt/flink/bin/sql-client.sh` directly.
- It **exits 0 even when a statement fails.** `demo.sh` and `bench.sh` grep output for
  `[ERROR]` instead of trusting the exit code.
- `-f` skips the image's init script, so the pre-baked faker sources (`source_order`,
  `source_customer`, `source_nation`) do not exist in a scripted session. Scripted SQL must
  define its own sources — `sql/05-bench-load.sql` and `sql/07-iot-pipeline.sql` both do.
- **Qualify every `CREATE TEMPORARY TABLE` / `CREATE TEMPORARY VIEW`.** An unqualified `CREATE`
  lands in whatever catalog is current, which breaks after a `USE CATALOG fluss_catalog`.
- The bare table is unreadable in batch until the first lake snapshot exists
  (`Batch mode can only be supported if one lake snapshot exists`). `$lake` reads return 0 rows
  in that window; the union read errors.
- Live queries need the *interactive* client: `SET 'execution.runtime-mode' = 'streaming'` plus
  `result-mode = 'table'`. `-f` cannot render an updating view — which is why
  `sql/09-iot-live.sql` is paste-only.
- To run ad-hoc SQL non-interactively, write a file into `./sql/` (mounted at `/sql`), run it,
  delete it. Do not pipe SQL through nested shell quoting — it mangles the doubled single quotes
  that flink-faker expressions need.

---

## Design notes

### Why the anti-join, not `max()`

The faker generates `reading_id` and `order_key` **at random**, not monotonically. `max(id)` is
not the newest row — it is usually one tiered long ago, so a `max()`-based freshness test
reports a false negative. `sql/08-iot-contrast.sql` uses an anti-join against `$lake` to find
rows that genuinely are not in the lake yet.

### Why the sources are bounded, and what drains

Every contrast in this repo only exists **while data is still arriving**. Benchmarking or
demoing a drained source shows frozen numbers that look like a bug and are not one.

| Source | Rate | Rows | Window |
|---|---|---|---|
| `sql/07` `gen_telemetry` → `iot-telemetry` | 50/s | 200,000 | ~66 min |
| `sql/07` `gen_events` → `iot-events` | 5/s | 20,000 | ~66 min |
| `sql/02` `source_order` (image built-in) | 10/s | 10,000 | ~16 min |
| `sql/05` `bench_source` | 20,000/s | 20,000,000 | ~17 min |

If `rows_only_in_hot` hits 0 and stays there, the source drained: tiering caught up completely.
Correct behaviour, no longer a contrast. Reset and start over.

### Why the fact table counts anomalies instead of flagging them

`datalake_device_health_1min` carries both `anomaly_flag` (kappa's `max_temperature >
threshold_used` rule) and `cnt_anomalies` (how many readings in the window cleared the
threshold). Only the second one is useful here.

The producer draws temperatures uniformly from 18-30 °C, and a 1-minute window holds ~250
readings per device. The maximum of 250 uniform draws clears a 24-29 °C threshold always, so
`anomaly_flag` is TRUE for nearly every window and ranks nothing — every device ties. The
*rate*, `cnt_anomalies / cnt_points`, falls cleanly from 50.2% for device_1 (threshold 24.0 °C)
to 7.1% for device_11 (29.0 °C), which is exactly the per-device threshold ordering — and
matches the arithmetic, since temperature is uniform on 18-30 °C and (30-24)/12 = 50%.

The flag is kept for parity with the reference pipeline, whose generator forced a genuinely hot
device instead of drawing uniformly — with that input, `max() > threshold` does discriminate.
It is a good illustration of a demo statistic that looks right and measures nothing.

### Why proctime windows, and what was dropped from kappa

The reference pipeline uses a 5-minute **event-time** tumbling window with a 3-minute watermark,
because its generator deliberately simulates a hot device, a 90-second outage, and 25% late
data. Those pathologies exercise watermarks and completeness flags — a Flink lesson, not a
streamhouse one — so they are out of scope here.

Dropped, and what it would take to add back:

| Kappa feature | Why dropped | To add |
|---|---|---|
| event-time window + watermark | needs a synthetic clock the faker cannot drive | `WATERMARK FOR event_time` on `iot_telemetry`, then `DESCRIPTOR(event_time)` |
| `incomplete_by_coverage` / `_by_volume` | only meaningful with the outage simulation | a real producer instead of faker |
| `cnt_events` / `events_*` per window | needs the stream-stream join | see the row below |
| stream-stream telemetry ⋈ events | hardest part, adds nothing to the freshness argument | join two windowed aggregates on `(device_id, window_start)` |
| per-event enrichment (`fact_events_enriched`) | never wired up in the original either | — |

The window is 1 minute rather than 5 so the fact table produces rows inside a tutorial.

### Why `bench_order` is not tiered

Tutorial 4 prices a **pure hot-tier point lookup**. With `datalake.enabled` the bare table
becomes a union read, which Iceberg cannot do on a PK table at all (see the constraint above).
Tutorial 2 already prices the cold tier.

---

## Stack / versions

| Component | Pin | Notes |
|---|---|---|
| Fluss | `0.9.1-incubating` | CoordinatorServer + TabletServer + ZooKeeper 3.9.2 |
| Flink | `1.20` (`fluss-quickstart-flink:1.20-0.9.1-incubating`) | **Do not use 2.x** — connector is 1.20 |
| Object store | MinIO | buckets: `fluss` (hot remote), `warehouse` (cold Iceberg) |
| Table format | Iceberg `1.10.1` | server-side jars mounted into Fluss |
| Catalog | Nessie `0.108.2` | native Nessie API @ `:19120/api/v2` — `0.99.0` NPEs on Fluss's Iceberg 1.10 client (optional `lastColumnId`); needs ≥0.108 |
| Kafka | `apache/kafka:3.9.1` | Tutorial 4 only; single-node KRaft, no ZooKeeper |
| OLAP (opt) | StarRocks allin1 | external Iceberg catalog over Nessie's REST endpoint |

Ports: Flink `8083` · MinIO API `9000` / console `9001` (admin/password) · Nessie `19120` ·
Kafka `9092` · StarRocks `9030` (+ `8030`, `8040`).

### Smaller notes

- **Tiering jar filename is version-specific.** `start-tiering.sh` assumes
  `fluss-flink-tiering-0.9.1-incubating.jar`. If missing:
  `docker compose exec jobmanager ls /opt/flink/opt | grep tiering`.
- **Nessie is `IN_MEMORY`** — catalog state dies on `docker compose down`. For branch-lifecycle
  demos that survive restarts, switch to `nessie.version.store.type=ROCKSDB` with a mounted
  volume.
- **Kafka is core now.** It is the ingress for Tutorial 1 as well as the foil in Tutorial 4, so
  `verify.sh` gates on the broker alongside every other service.
- **`verify.sh` is a liveness gate only.** It does not assert the Fluss→Iceberg tiering seam,
  which does not exist until `make tiering`.
