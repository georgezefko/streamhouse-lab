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
 
## ⚠️ Known risk points (validate these first)
 
1. **Fluss tiering against a REST catalog is the unproven seam.** The official Fluss lakehouse
   quickstart ships with `JdbcCatalog` (Postgres), not `RESTCatalog`. `org.apache.iceberg.rest.RESTCatalog`
   is a standard Iceberg catalog and *should* load through `datalake.iceberg.catalog-impl`, but this
   exact Fluss+Nessie path isn't in the docs. **Smoke-test tiering before building anything on it.**
   If the tiering job fails to init the catalog, fall back to the JDBC catalog to unblock:
   add a `postgres:17` service and swap the four `datalake.iceberg.*` catalog lines for:
```
   datalake.iceberg.catalog-impl: org.apache.iceberg.jdbc.JdbcCatalog
   datalake.iceberg.uri: jdbc:postgresql://postgres:5432/iceberg
   datalake.iceberg.jdbc.user: iceberg
   datalake.iceberg.jdbc.password: iceberg
```
   (also mount `postgresql-42.7.4.jar` into the Fluss iceberg plugin dir) — then revisit Nessie.
 
2. **Credential vending vs static creds.** Nessie's REST catalog vends down-scoped S3 creds to
   clients. We *also* pass static MinIO creds to Fluss's `S3FileIO` so writes work regardless. If
   Nessie enforces request signing and the two paths disagree, align them (either lean fully on
   vending, or disable signing for local dev).
3. **Tiering jar filename is version-specific.** `start-tiering.sh` assumes
   `fluss-flink-tiering-0.9.1-incubating.jar`. If it's not found:
   `docker compose exec jobmanager ls /opt/flink/opt | grep tiering`.
4. **Nessie is `IN_MEMORY`** — branch state dies on `docker compose down`. For git-branch lifecycle
   demos that survive restarts, switch to `nessie.version.store.type=ROCKSDB` with a mounted volume.