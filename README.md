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
 
## Stack / versions
 
| Component | Pin | Notes |
|---|---|---|
| Fluss | `0.9.1-incubating` | CoordinatorServer + TabletServer + ZooKeeper 3.9.2 |
| Flink | `1.20` (`fluss-quickstart-flink:1.20-0.9.1-incubating`) | **Do not use 2.x** — connector is 1.20 |
| Object store | MinIO | buckets: `fluss` (hot remote), `warehouse` (cold Iceberg) |
| Table format | Iceberg `1.10.1` | server-side jars mounted into Fluss |
| Catalog | Nessie `0.108.2` | Iceberg REST @ `:19120/iceberg/main` — `0.99.0` NPEs on Fluss's Iceberg 1.10 client (optional `lastColumnId`); needs ≥0.108 |
| OLAP (opt) | StarRocks allin1 | external Iceberg catalog over Nessie |
 
## Prerequisites
Docker + Docker Compose v2. (Or open in the devcontainer — it forwards all UIs and installs
`mc`, `duckdb`, `mysql`, `pyiceberg`.)
 
## Phased bring-up
 
```bash
make jars       # 0: download Fluss server-side Iceberg jars
make up         # 1: ZK + MinIO + Nessie + Fluss + Flink
make sql        # 1: paste sql/01-tables.sql then sql/02-ingest-and-query.sql
make tiering    # 2: launch the Fluss->Iceberg tiering Flink job
                #    then re-run the union-read queries in sql/02
make starrocks  # 3: OLAP over the cold tier; register sql/04-starrocks.sql
```
 
**Phase 1** proves the hot tier: PK-table upserts, point lookups, lookup joins, all sub-second.
**Phase 2** makes it a *streamhouse*: the `$lake` suffix reads cold-only, the bare table unions
hot+cold. Watching `hot_plus_cold` outrun `cold_only` is the whole demo.
**Phase 3** shows an external engine (StarRocks) reading the same tiered Iceberg data — Fluss is
not in that path at all.
 
## Ports
Flink `8083` · MinIO API `9000` / console `9001` (admin/password) · Nessie `19120` · StarRocks `9030`.
 
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
- **The Fluss catalog ignores `CREATE TABLE IF NOT EXISTS`** — it still errors if the table exists.
  Drop-and-recreate, or `CREATE` only once.