# Fluss Streamhouse (local)

A local **streamhouse**: [Apache Fluss](https://fluss.apache.org/) as the hot tier, tiering
continuously into **Apache Iceberg on MinIO**, cataloged by **Nessie**, with **StarRocks** as an
optional OLAP engine over the cold tier. Compute is **Apache Flink 1.20**.

![architecture](docs/architecture.png)

This is the lab behind the blog post — it builds one real-time IoT pipeline and queries it hot
and cold. Read the post for what the pattern is and why; read this repo to run it.

| | |
|---|---|
| [**Query the Stream in Practice**](TODO-new-post-url) | the post this lab backs — **start here** |
| [*An Introduction to the Streamhouse Pattern*](https://georgioszefkilis.substack.com/p/query-the-stream-an-introduction) | part 1 — what the pattern is, and why Fluss |
| [`docs/TUTORIAL.md`](docs/TUTORIAL.md) | the six commands: run this, expect that |
| [`docs/NOTES.md`](docs/NOTES.md) | why it is built this way — constraints, fixes, troubleshooting |

Everything here is **Python or SQL**: `scripts/iot_producer.py` produces, Flink SQL processes.
Event-time windows and outage simulation are out of scope
([how to switch](docs/NOTES.md#switching-to-event-time-windows)).

---

## Prerequisites

- **Docker + Docker Compose v2**, with **≥8 GB** allocated to Docker. The taskmanager alone
  reserves 2 GB; the optional StarRocks `allin1` image wants ~4-6 GB more on top.
- **First `make up` pulls ~10 min of images** (MinIO, minio/mc, Nessie, ZooKeeper, Fluss, Flink,
  Kafka). `make jars` fetches 15 jars from Maven Central into `lib/` (gitignored, cached).
- **Ports** that must be free: `8083` `9000` `9001` `19120` `9092`, plus `9030` `8030` `8040`
  for the StarRocks step.
- `make sr-sql` uses the host's **`mysql` client** if there is one and the container's otherwise,
  so a bare macOS shell is fine.

## Quick start

The happy path, in order. Only steps 0 and 4 block — everything else returns immediately and
leaves Flink jobs running in the background.

| # | Command | Blocks? | Result |
|---|---|---|---|
| 0 | `make up` | ~2 min (+pulls) | whole stack up; ends with the `verify.sh` gate |
| 1 | `make produce` | no | the Python device fleet publishing to Kafka, ~66 min of sensor data |
| 2 | `make sql`, paste `sql/catalog.sql` then `sql/01-pipeline.sql` | no | 5 Fluss tables, 2 detached jobs |
| 3 | `make tiering` | no | tiering job appears in the Flink UI |
| 4 | `make demo` | ~90 s | the hot-vs-cold contrast |
| 5 | `make starrocks`, then `make sr-sql` | ~2 min | StarRocks over the cold tier |

Each step, with what a pass looks like and what the failures mean, is in
[`docs/TUTORIAL.md`](docs/TUTORIAL.md).

UIs: Flink [`:8083`](http://localhost:8083) · MinIO console [`:9001`](http://localhost:9001)
(admin/password) · Nessie [`:19120`](http://localhost:19120).

Beyond the table:

```bash
make verify      # the liveness gate on its own, any time
make ps / logs   # container states / Fluss server logs
make down        # docker compose down -v — the correct full reset
make clean       # down, plus delete lib/*.jar
```

Concurrent SQL sessions are fine — `make sql` runs a throwaway container each time.

## Repo layout

| Path | What |
|---|---|
| `docs/TUTORIAL.md` | the six steps |
| `docs/NOTES.md` | the constraints and fixes — read before changing a table definition |
| `docker-compose.yml` | ZK, MinIO(+init), Nessie, Fluss (coordinator+tablet), Flink (JM/TM/sql-client), Kafka, the producer |
| `docker-compose.starrocks.yml` | the StarRocks overlay (step 5) |
| `scripts/iot_producer.py` | **the ingress** — sensors → Kafka (`make produce`) |
| `scripts/download-jars.sh` | fetches `lib/*.jar` (mounted into Flink + Fluss) |
| `scripts/verify.sh` | the liveness gate |
| `scripts/start-tiering.sh` | submits the Fluss→Iceberg tiering job |
| `scripts/demo.sh` | loops a contrast query; `SQL_FILE=` picks which |
| `sql/catalog.sql` | the `fluss_catalog` DDL every session needs first |
| `sql/01-pipeline.sql` | **step 2** — Kafka → Fluss → the tiered tables |
| `sql/02-live.sql` | **step 4** — live queries, interactive only |
| `sql/03-contrast.sql` | **step 4** — hot vs cold, `-f`-safe (what `make demo` loops) |
| `sql/04-starrocks.sql` | **step 5** — external Iceberg catalog over the cold tier |

Stack versions are in [`docs/NOTES.md`](docs/NOTES.md#stack--versions).
