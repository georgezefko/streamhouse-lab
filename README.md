# Fluss Streamhouse (local)

A local streamhouse: **Apache Fluss** as the sub-second hot tier, tiering continuously into
**Apache Iceberg on MinIO**, cataloged by **Nessie** (Iceberg REST), with **StarRocks** as an
optional OLAP engine over the cold tier. Compute is **Apache Flink 1.20**.

```
 faker source ──▶ Fluss (hot: PK/log tables) ──tiering job──▶ Iceberg on MinIO ──▶ StarRocks
                        │                                         ▲   (cold)
                        └──────────── union read ─────────────────┘
                     (query the table = hot ∪ cold;  table$lake = cold only)
```

The repo exists to make two claims reproducible on a laptop, rather than argued in prose:

1. **vs a lakehouse** — the same table, same SQL, answers *now* from the hot tier while the
   Iceberg path is still waiting for the next flush.
2. **vs Kafka** — a topic holds the same records but has no index, so answering "what is order
   N?" means reading every offset. Kafka+Iceberg only becomes queryable by making a second copy.

Companion to [*Query the Stream: An Introduction to the Streamhouse
Pattern*](https://georgioszefkilis.substack.com/p/query-the-stream-an-introduction).

---

## Prerequisites

- **Docker + Docker Compose v2**, with **≥8 GB** allocated to Docker. The taskmanager alone
  reserves 2 GB; the optional StarRocks `allin1` image wants ~4-6 GB more on top.
- **First `make up` pulls ~10 min of images** (MinIO, minio/mc, Nessie, ZooKeeper, Fluss, Flink,
  Kafka). `make jars` fetches 15 jars from Maven Central into `lib/` (gitignored, cached).
- **Ports** that must be free: `8083` `9000` `9001` `19120` `9092`, plus `9030` `8030` `8040`
  if you run Scenario 3.
- Optional: open the repo in the **devcontainer** (`.devcontainer/`). It forwards every UI and
  installs `mc`, `duckdb`, `mysql`, `jq`, `pyiceberg`, `pynessie` for poking the stack from
  outside the containers.

## Quick start

The happy path, in order. Only steps 1 and 5 block — everything else returns immediately and
leaves Flink jobs running in the background.

| # | Command | Blocks? | Result |
|---|---|---|---|
| 1 | `make up` | ~2 min (+pulls) | whole stack up; ends with the `verify.sh` gate |
| 2 | `make sql`, paste `sql/01-tables.sql` | no | Fluss catalog + 4 tables |
| 3 | same session, paste `sql/02` §1 then §2 | no | 2 detached Flink jobs, ~16 min of data |
| 4 | `make tiering` | no | tiering job appears in the Flink UI |
| 5 | `make demo` | ~90 s | the hot-vs-cold contrast |

Then optionally `make starrocks` (Scenario 3) and `make bench` (Scenario 4).

UIs: Flink [`:8083`](http://localhost:8083) · MinIO console [`:9001`](http://localhost:9001)
(admin/password) · Nessie [`:19120`](http://localhost:19120).

> **`sql/02` is paste-in-parts, not `-f`.** Its first half submits streaming INSERTs that detach
> as Flink jobs; its second half switches the session to `execution.runtime-mode = batch` to run
> SELECTs. Paste the two halves separately.

---

# Scenarios

Each scenario states what it proves, how to run it, what a pass looks like, and what the common
failures mean.

## Scenario 0 — the stack is wired

```bash
make verify
```

**Pass:** every line ✓, exit 0, ending in `All components up and wired. Safe to build.`

Checks container states, MinIO/Nessie/Flink endpoints, that a TaskManager actually registered with
the JobManager, and that both buckets exist. It is a **liveness gate only** — it does not assert
the Fluss→Iceberg tiering seam, which does not exist until `make tiering`.

`make up` runs this for you, so a green result is the last thing you see on bring-up.

## Scenario 1 — the hot tier serves

**Proves:** PK-table upserts, point lookups and lookup joins, all sub-second.

```bash
make sql        # paste sql/01-tables.sql, then sql/02 §1 and §2
```

**Pass:** two RUNNING jobs in the Flink UI, and within seconds of the ingest starting, a batch
`SELECT count(*) FROM datalake_enriched_orders` is non-zero and climbing. The rows are enriched —
`cust_name` and `nation_name` are populated — which means the lookup joins against
`fluss_customer` / `fluss_nation` are resolving per record.

**When it fails:**
- *0 rows, no jobs* — `sql/01` errored. Most likely the tables already exist; see
  [Troubleshooting](#troubleshooting--reset).
- *0 rows, jobs present* — the `EXECUTE STATEMENT SET` job died. Flink UI → the job →
  *Exceptions*.

## Scenario 2 — streamhouse vs lakehouse

**Proves:** the freshness claim. Same table, same SQL, two read paths — the bare table unions
hot+cold, the `$lake` suffix reads cold only.

```bash
make tiering    # once, after the tables exist
make demo       # `make demo N=12` for more iterations
```

`make demo` runs `sql/03-contrast.sql` on a loop and prints:

```
hot_plus_cold   cold_only   rows_only_in_hot
     1204881     1198340               6541

newest_order  42317      found_in_cold 0
```

**Pass:** `rows_only_in_hot` > 0 on every iteration, `cold_only` advancing in visible ~30 s steps
(`table.datalake.freshness`), and `found_in_cold` = 0 — the newest order is queryable in the
streamhouse and simply *not on the Iceberg path yet*.

**When it fails:**
- *`cold_only` stuck at 0* — the tiering job is not running or crashed. Check the Flink UI and
  `make logs`.
- *`rows_only_in_hot` = 0* — the faker source drained. It is finite: `source_order` is 10,000 rows
  at 10/s, so ~16 minutes. Once it drains, tiering catches up completely and the gap closes —
  correct behaviour, but no longer a contrast. Run `make demo` **while `sql/02` is still
  ingesting**, or reset and start over.

### The sharper version: kill the tiering job

Cancel the tiering job in the Flink UI, then re-run `make demo`. `cold_only` **freezes** while
`hot_plus_cold` keeps climbing — the lake tier is now visibly a stale copy while the hot tier
serves. `make tiering` restarts it and the cold number catches up in one jump.

## Scenario 3 — an external engine reads the cold tier

**Proves:** the cold tier is just Iceberg. An OLAP engine reads it with Fluss nowhere in the path.

```bash
make starrocks           # wait for the healthcheck to go healthy (~1-2 min)
make sr-sql              # paste sql/04-starrocks.sql
```

**Pass:** `SHOW DATABASES FROM iceberg_nessie` lists the Fluss database, and
`SELECT sum(total_price) FROM datalake_enriched_orders` returns a value **below** the hot-tier
number from Scenario 2. That gap is the point: StarRocks sees only what tiering has flushed.

**When it fails:**
- *Catalog registers but no databases* — nothing has been tiered yet. Run Scenario 2 first.
- *FE not healthy* — StarRocks `allin1` is memory-hungry; check Docker's allocation.

## Scenario 4 — Fluss vs Kafka ⚠ unverified

> **This scenario has not been run end to end.** The code is written and the compose/jar wiring is
> valid, but the numbers below show shape, not measured values. Known risks are listed at the
> bottom — check them on a first real run.

**Proves:** the queryability claim. A Kafka topic and a Fluss table hold the same volume of orders
over the same key space. Asking one question of each costs wildly different amounts.

Needs a **second SQL client session** alongside the one from Scenario 1 — `make sql` opens a
throwaway container per invocation, so concurrent sessions are fine.

```bash
make sql        # second session: paste sql/05-bench-load.sql, leave it running
                # wait ~100 s (2M rows at 20k/s)
make bench      # third shell
```

`sql/05` fans one bulk faker stream into both a Fluss PK table and a Kafka topic. `make bench`
then runs `sql/06-bench-query.sql` — *"what is order 424242?"* — three times:

```
engine                           state       duration
Fluss  (PK point lookup)         FINISHED         ... ms
Kafka  (scan to latest offset)   FINISHED         ... ms
Iceberg (cold Parquet scan)      FINISHED         ... ms
```

**Pass:** run `make bench` twice, a minute apart. The Fluss row stays **flat** while the Kafka row
**grows**. That divergence is the whole argument — absolute numbers are laptop-bound and not
interesting.

- **Fluss** has a primary-key index. A full-PK predicate is a point lookup, and its cost does not
  move as the table grows.
- **Kafka** has offsets, not indexes. Flink deserializes every record from earliest to latest
  offset (`scan.bounded.mode`), so cost is linear in retention. Kafka+Iceberg gets you a queryable
  copy only by *making a second copy*.
- **Iceberg** prunes by file and row group, so it beats the topic scan — but sees only flushed
  rows. Scenario 2's staleness, now with a price tag.

Timings are Flink job durations pulled from the REST API, not wall clock: `docker compose run`
costs several seconds of container startup that would swamp everything being measured.

**Known risks on a first run:**
1. Fluss may not push the full-PK predicate down to a point lookup in batch mode. If it full-scans
   instead, the headline number vanishes and the query needs rewriting as a lookup join.
2. The tiering job may not pick up `bench_order` if the table was created after the job started —
   restart it with `make tiering`.
3. The flink-faker expression quoting in `sql/05` (doubled single quotes) is untested.

Kafka is a Scenario-4-only dependency. Nothing else in the stack talks to it, and `verify.sh` does
not gate on it.

---

## Troubleshooting & reset

**`sql/01` errors on a re-run.** The Fluss catalog ignores `CREATE TABLE IF NOT EXISTS` — it still
errors if the table exists. `DROP TABLE` the ones you need, or do a full reset.

**Full reset is `make down`** (`docker compose down -v`). This is the *correct* reset, not a heavy
one: Nessie is `IN_MEMORY`, so its catalog dies with the container regardless, and dropping the
MinIO volume is what stops orphaned Iceberg data files from outliving their catalog entries. Then
start again from `sql/01`. `make clean` additionally deletes `lib/*.jar`, which `make up`
re-fetches.

**Fluss containers restart a couple of times on first bring-up.** Expected. They use
`restart: on-failure` to survive the Nessie boot race — `depends_on` waits for container start,
not for Quarkus to be serving `:19120`.

**Concurrent SQL sessions are fine.** `make sql` runs a throwaway container each time; Scenario 4
requires two at once.

**Where to look when a job misbehaves:** Flink UI [`:8083`](http://localhost:8083) for job state
and exceptions, `make logs` for the Fluss servers, `docker compose logs <svc>` for everything else.

---

# Reference

## Stack / versions

| Component | Pin | Notes |
|---|---|---|
| Fluss | `0.9.1-incubating` | CoordinatorServer + TabletServer + ZooKeeper 3.9.2 |
| Flink | `1.20` (`fluss-quickstart-flink:1.20-0.9.1-incubating`) | **Do not use 2.x** — connector is 1.20 |
| Object store | MinIO | buckets: `fluss` (hot remote), `warehouse` (cold Iceberg) |
| Table format | Iceberg `1.10.1` | server-side jars mounted into Fluss |
| Catalog | Nessie `0.108.2` | native Nessie API @ `:19120/api/v2` (see below) — `0.99.0` NPEs on Fluss's Iceberg 1.10 client (optional `lastColumnId`); needs ≥0.108 |
| Kafka | `apache/kafka:3.9.1` | Scenario 4 only; single-node KRaft, no ZooKeeper |
| OLAP (opt) | StarRocks allin1 | external Iceberg catalog over Nessie |

Ports: Flink `8083` · MinIO API `9000` / console `9001` (admin/password) · Nessie `19120` ·
Kafka `9092` · StarRocks `9030` (+ `8030`, `8040`).

## Repo layout

| Path | What |
|---|---|
| `docker-compose.yml` | ZK, MinIO(+init), Nessie, Fluss (coordinator+tablet), Flink (JM/TM/sql-client), Kafka |
| `docker-compose.starrocks.yml` | Scenario 3 overlay |
| `scripts/download-jars.sh` | fetches `lib/*.jar` (mounted into Flink + Fluss) |
| `scripts/verify.sh` | the liveness gate |
| `scripts/start-tiering.sh` | submits the Fluss→Iceberg tiering job |
| `scripts/demo.sh` | loops `sql/03-contrast.sql` |
| `scripts/bench.sh` | runs `sql/06`, reads job durations from the Flink REST API |
| `sql/01-tables.sql` | catalog, PK tables, the tiered table |
| `sql/02-ingest-and-query.sql` | faker → Fluss, lookup-join enrichment, union read |
| `sql/03-contrast.sql` | the hot-vs-cold contrast query |
| `sql/04-starrocks.sql` | external Iceberg catalog over Nessie |
| `sql/05-bench-load.sql` | bulk load into both Fluss and Kafka |
| `sql/06-bench-query.sql` | the same point query, three engines |

## How the Fluss ⇄ Nessie ⇄ Iceberg seam actually works (validated)

Getting tiering working end-to-end took four non-obvious fixes. All are in the code now; this is
the map if you touch them.

1. **Use the NATIVE Nessie catalog, not Iceberg-REST.** We use
   `datalake.iceberg.catalog-impl: org.apache.iceberg.nessie.NessieCatalog` against Nessie's own API
   (`http://nessie:19120/api/v2`, `ref: main`), **not** `RESTCatalog` against `/iceberg/main`.
   Nessie's Iceberg-REST `createTable` NPEs with Fluss 0.9.1's Iceberg 1.10 client (it drops the
   deprecated `lastColumnId`); the native catalog commits via Nessie's git-like API and works.
2. **The Flink tiering job needs the whole iceberg plugin set on `/opt/flink/lib`.** The Flink image
   ships **no** iceberg at all. `scripts/download-jars.sh` fetches `fluss-lake-iceberg` (the
   `LakeStoragePlugin`), `iceberg-nessie` + `nessie-client`/`nessie-model`/jackson/microprofile,
   `hadoop-client-*` (Fluss's tiering writer requires Hadoop), `failsafe` (iceberg-aws S3 retries),
   and `iceberg-flink-runtime` (to *read* `$lake`). `docker-compose.yml` mounts them via the
   `x-flink-iceberg-vols` anchor.
3. **S3 for local MinIO needs the STS assume-role endpoint set.** Fluss vends S3 delegation tokens
   to clients via STS; without pointing STS at MinIO it calls real AWS → `403 InvalidClientTokenId`.
   The Fluss servers set `s3.assumed.role.arn` + `s3.assumed.role.sts.endpoint: http://minio:9000`
   (matching the official quickstart's RustFS wiring).
4. **`failsafe` also belongs in the Fluss server plugin dir** — reading an existing Iceberg table's
   metadata (e.g. `CREATE TABLE IF NOT EXISTS`) needs it server-side, not just on Flink.

### Smaller notes
- **Tiering jar filename is version-specific.** `start-tiering.sh` assumes
  `fluss-flink-tiering-0.9.1-incubating.jar`. If missing: `docker compose exec jobmanager ls /opt/flink/opt | grep tiering`.
- **Nessie is `IN_MEMORY`** — catalog state dies on `docker compose down`. For branch-lifecycle demos
  that survive restarts, switch to `nessie.version.store.type=ROCKSDB` with a mounted volume.
- **`sql-client` drops `-f`.** `/opt/sql-client/sql-client` (the image default command) hardcodes
  its args, so `-f` is silently ignored. `demo.sh` and `bench.sh` call
  `/opt/flink/bin/sql-client.sh` directly.
- **`sql-client` exits 0 even when a statement fails.** Both scripts grep the output for `[ERROR]`
  instead of trusting the exit code.
