# Notes — why it is built this way

The debugging notes behind the lab: the constraints that forced each table definition, the
fixes that made Fluss→Nessie→Iceberg work, and what the error messages mean. None of this is
in the blog post. The steps themselves are in [TUTORIAL.md](TUTORIAL.md).

The constraints are non-obvious and load-bearing: if you change a table definition, read them
first.

## Hard constraints

### Union read requires log tables

Fluss can tier a primary-key table to Iceberg. What fails in 0.9.1 is the **batch** read of
its bare name (hot ∪ cold). That read merges the lake snapshot with the Fluss log by key, a
**sort-merge**, so Fluss's `LakeSnapshotAndLogSplitScanner` requires the lake reader to
implement `org.apache.fluss.lake.source.SortedRecordReader`.
`fluss-lake-iceberg-0.9.1-incubating` does not implement it anywhere — verified by unpacking
the jar. The read fails with:

```
java.lang.UnsupportedOperationException: lake records must instance of sorted view.
```

Log tables concatenate rather than merge, so they union-read fine. This is why every tiered
table here — `iot_telemetry`, `iot_events`, `datalake_device_telemetry`,
`datalake_device_health_1min` — has **no primary key**.

Paimon's reader does implement the interface. For Iceberg, Fluss's own tests cover a tiered
PK table read as a stream and through `$lake`, in 0.9.1 and in 1.0; neither is exercised in
this lab. The 1.0 docs list PK union read as supported in both batch and streaming mode, but
the 1.0 Iceberg module still ships no sorted reader, so check the batch case before relying
on it after an upgrade.

### A log-table sink cannot consume a PK table's changelog

Reading a PK table in streaming mode emits `-U/+U`, and an append-only sink rejects it:

```
Table sink ... doesn't support consuming update and delete changes
```

So making the tiered table append-only forces its source to be append-only too — `iot_telemetry`
is a log table for this reason. Only `dim_device` stays PK: it is the lookup-join build side,
where the point lookups actually happen and where nothing streams *out*.

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

`scripts/iot_producer.py` writes ISO-8601 and `sql/01-pipeline.sql` reads it that way. Keep
both ends aligned if you swap the producer. `'json.ignore-parse-errors' = 'true'` on the consumer
is the related decision: against a real topic one malformed message should not kill the job.

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

## The Fluss ⇄ Nessie ⇄ Iceberg seam

### When the Iceberg table actually appears in Nessie

Two separate moments, and confusing them is what makes tiering look broken:

1. **`CREATE TABLE ... WITH ('table.datalake.enabled' = 'true')`** (step 2) — the Fluss
   **coordinator-server** immediately creates a matching *empty* Iceberg table `fluss.<name>` in
   Nessie, on branch `main`, warehouse `s3://warehouse/`. It does this itself, with the
   `datalake.iceberg.*` settings in `docker-compose.yml` and the `NessieCatalog` jars mounted at
   `/opt/fluss/plugins/iceberg/`. No Flink job is involved; MinIO gets the table's first
   `metadata.json` and **no data files**.
2. **`make tiering`** (step 3) — the tiering service writes Parquet and commits one Iceberg
   snapshot per flush.

So the catalog entry exists from DDL time and the *data* arrives only once tiering runs. That is
why a table can be queryable in Fluss, visible in Nessie, and still empty in `$lake`:

```sql
SELECT count(*) FROM datalake_device_telemetry;            -- hot ∪ cold, answers now
SELECT count(*) FROM datalake_device_telemetry$lake;       -- Iceberg only, as of the last commit
SELECT * FROM datalake_device_telemetry$lake$snapshots;    -- one row per flush
```

Check the catalog side directly, without Flink:

```bash
curl -s localhost:19120/api/v2/trees/main/entries | jq '.entries[].name.elements'
# fluss, fluss.datalake_device_telemetry, fluss.datalake_device_health_1min, fluss.iot_events

docker compose run --rm --entrypoint sh minio-init -c \
  'mc alias set m http://minio:9000 admin password >/dev/null && mc ls -r m/warehouse'
# before tiering: only <table>/metadata/00000-*.metadata.json — no data/ prefix
```

Two consequences worth remembering: the catalog and the MinIO files are separate lifetimes — the
Nessie volume can be dropped while Parquet survives, or the reverse, hence `make down` rather
than a partial reset; and `DROP TABLE` in Fluss removes the Fluss side only, leaving the Nessie
entry behind.

### The four fixes

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
- It **exits 0 even when a statement fails.** `demo.sh` greps output for `[ERROR]` instead of
  trusting the exit code.
- **There is no INCLUDE.** The shared catalog DDL lives in `sql/catalog.sql` and is either
  pasted first (interactive) or concatenated onto the file being run
  (`cat /sql/catalog.sql <file> > /tmp/run.sql`).
- `-f` also skips the image's init script, so its pre-baked demo sources do not exist in a
  scripted session. Scripted SQL must define every source it uses.
- **Qualify every `CREATE TEMPORARY TABLE` / `CREATE TEMPORARY VIEW`.** An unqualified `CREATE`
  lands in whatever catalog is current, which breaks after a `USE CATALOG fluss_catalog`.
- The bare table is unreadable in batch until the first lake snapshot exists
  (`Batch mode can only be supported if one lake snapshot exists`). `$lake` reads return 0 rows
  in that window; the union read errors.
- Live queries need the *interactive* client: `SET 'execution.runtime-mode' = 'streaming'` plus
  `result-mode = 'table'`. `-f` cannot render an updating view — which is why
  `sql/02-live.sql` is paste-only.
- To run ad-hoc SQL non-interactively, write a file into `./sql/` (mounted at `/sql`), run it,
  delete it. Do not pipe SQL through nested shell quoting — it mangles quoted SQL literals.

---

## Design notes

### Why the anti-join, not `max()`

The producer draws `reading_id` **at random**, not monotonically. `max(reading_id)` is not the
newest row — it is usually one tiered long ago, so a `max()`-based freshness test reports a
false negative. `sql/03-contrast.sql` uses an anti-join against `$lake` to find rows that
genuinely are not in the lake yet.

### Why the producer is bounded, and what drains

Every contrast here only exists **while data is still arriving**. Demoing after the producer
stops shows frozen numbers that look like a bug and are not one.

| Topic | Rate | Rows | Window |
|---|---|---|---|
| `iot-telemetry` | 50/s | 200,000 | ~66 min |
| `iot-events` | ~5/s | ~20,000 | ~66 min |

`ROWS` bounds it and `ROWS=0` runs forever; `RATE` and `ID_MAX` are the other knobs. Bounded by
default so a forgotten container cannot fill the disk.

If `rows_only_in_hot` hits 0 and stays there, the producer finished: tiering caught up
completely. Correct behaviour, no longer a contrast. Restart the producer or reset.

### Switching to event-time windows

The fact table tumbles on **processing** time: a reading is counted in the minute it arrived,
not the minute on its own `event_time`. That is why it emits append-only rows a log-table sink
can consume, and it is enough here because readings arrive within a second of being produced.

To group by the reading's own timestamp instead, declare a watermark on the source and point the
TVF at `event_time`:

```sql
CREATE TABLE iot_telemetry (
  ...
  `event_time` TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (...);
```

```sql
FROM TABLE(
  TUMBLE(TABLE ..., DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
)
```

An event-time tumble still emits append-only, so the sink is fine. What it buys you is correct
windows under late or out-of-order data; what it costs is that a window cannot close until the
watermark passes it, so a stalled device holds up its window. Simulating that — an outage, then
a backfill — would mean teaching `scripts/iot_producer.py` to stall, which is why the lab does
not.

### The lake still needs compaction

Every flush is an Iceberg commit that adds small Parquet files, so a tiered table accumulates
them exactly like any Iceberg table fed by a stream, and wants compaction and snapshot expiry.
Neither is set up here — the lab is short-lived.

Fluss has a table option for the first, `'table.datalake.auto-compaction' = 'true'`, which
makes the tiering service compact as it writes. It is off by default and left off here. Note
for upgrades: in Fluss 1.0 the option is a no-op for newly created Iceberg tables and only
applies to tables created by earlier versions, so 1.0 needs external Iceberg compaction.

What the streamhouse changes is who pays for the fix. Because Flink reads the newest rows from
the hot tier, `table.datalake.freshness` can be raised to minutes without making Flink queries
staler; only lake-only readers like StarRocks see the slower setting. Freshness is a target
rather than a promise in the other direction too: each flush writes Parquet and commits a
snapshot, so on a laptop a 30 s setting often drifts to most of a minute.

---

## Troubleshooting & reset

**`sql/01-pipeline.sql` errors on a re-run.** The Fluss catalog ignores
`CREATE TABLE IF NOT EXISTS` — it still errors if the table exists. `DROP TABLE` the ones you
need, or do a full reset.

**`Table fluss.<name> already exists` right after a successful `DROP TABLE`.** Dropping a
datalake-enabled Fluss table removes it from Fluss but **leaves the Iceberg table registered in
Nessie**, and the recreate fails on that orphan. `SHOW TABLES` in `fluss_catalog` will not list
it; the Iceberg catalog will. [The recipe for dropping it
there](#dropping-a-tiered-table-leaves-an-orphan-in-nessie).

**Full reset is `make down`** (`docker compose down -v`). This is the *correct* reset, not a
heavy one: it drops both volumes together, so the Nessie catalog and the MinIO files cannot
outlive each other — a half-reset leaves orphaned Iceberg data files or catalog entries pointing
at files that are gone. It also tears down the StarRocks overlay (which otherwise survives with a
stale Iceberg metadata cache) and the producer profile (compose ignores containers whose profile
is not named, so it would otherwise keep producing against a dead broker). Then start again from
step 1. `make clean` additionally deletes `lib/*.jar`, which `make up` re-fetches.

**Fluss containers restart a couple of times on first bring-up.** Expected — the Nessie boot
race. See step 0.

**Concurrent SQL sessions are fine.** `make sql` runs a throwaway container each time.

**Numbers frozen, no errors.** The producer finished. See
[why the producer is bounded](#why-the-producer-is-bounded-and-what-drains).

**Where to look when a job misbehaves:** Flink UI [`:8083`](http://localhost:8083) for job state
and exceptions, `make logs` for the Fluss servers, `docker compose logs <svc>` for everything
else.

---

## Stack / versions

| Component | Pin | Notes |
|---|---|---|
| Fluss | `0.9.1-incubating` | CoordinatorServer + TabletServer + ZooKeeper 3.9.2 |
| Flink | `1.20` (`fluss-quickstart-flink:1.20-0.9.1-incubating`) | **Do not use 2.x** — connector is 1.20 |
| Object store | MinIO | buckets: `fluss` (hot remote), `warehouse` (cold Iceberg) |
| Table format | Iceberg `1.10.1` | server-side jars mounted into Fluss |
| Catalog | Nessie `0.108.2` | native Nessie API @ `:19120/api/v2` — `0.99.0` NPEs on Fluss's Iceberg 1.10 client (optional `lastColumnId`); needs ≥0.108 |
| Kafka | `apache/kafka:3.9.1` | the ingress; single-node KRaft, no ZooKeeper |
| OLAP (opt) | StarRocks allin1 | external Iceberg catalog over Nessie's REST endpoint |

Ports: Flink `8083` · MinIO API `9000` / console `9001` (admin/password) · Nessie `19120` ·
Kafka `9092` · StarRocks `9030` (+ `8030`, `8040`).

### Smaller notes

- **Tiering jar filename is version-specific.** `start-tiering.sh` assumes
  `fluss-flink-tiering-0.9.1-incubating.jar`. If missing:
  `docker compose exec jobmanager ls /opt/flink/opt | grep tiering`.
- **Nessie runs on `ROCKSDB`** with a named volume, so the catalog survives a container restart.
  `make down` still drops the volume: that is the intended full reset. The container runs as
  `user: "0:0"` because a named volume mounts root-owned and the image's uid 10000 cannot create
  RocksDB's directory inside it.
- **Kafka is core.** It is the ingress, so `verify.sh` gates on the broker alongside every other
  service.
- **`verify.sh` is a liveness gate only.** It does not assert the Fluss→Iceberg tiering seam,
  which does not exist until step 3.
