# Build the Streamhouse: Kappa and Lambda in One Pattern

*Part 2 of the streamhouse series. [Part 1: Query the Stream — An Introduction to the Streamhouse Pattern](https://medium.com/data-engineer-things/query-the-stream-an-introduction-to-the-streamhouse-pattern-25f1afc1e961)*

---

In my previous article I covered what the streamhouse pattern is and how Apache Fluss makes it
work. The part that stayed with me was being able to query the stream directly.

So this time I built one. I took the Kappa pipeline from an older post — IoT sensors on
manufacturing devices, Kafka in, StarRocks out — and rebuilt the middle of it on Fluss, to find
out whether querying the stream is actually worth it in practice.

What I ended up with is the thing I did not expect. It is one pipeline that behaves like Kappa
and like Lambda at the same time, without keeping two copies of the data. The hot tier answers in
milliseconds and is the streaming path. The same table's cold tier is plain Iceberg on object
storage, thirty seconds behind, and is the batch path. Same table name, same SQL, one write.

Everything below runs locally: Docker Compose, a Python producer, and about ten minutes of SQL.
Repo at the end.

## Business Scenario: Industrial IoT Monitoring

A manufacturing company uses IoT sensors to monitor the health of industrial machines. The sensors
emit two streams onto two Kafka topics.

**Telemetry** — energy usage, temperature, vibration, one reading per device:

```json
{"reading_id": 19974442, "device_id": "device_9", "event_time": "2026-09-09T19:21:03.810",
 "energy_usage": 1.19, "temperature": 22.6, "vibration": 0.9, "signal_strength": 70}
```

**Event logs** — failures, maintenance, inspections. Every event carries `device_id`, `event_time` and
`severity`; beyond those, only the fields belonging to its `event_type` are present.

```json
{"device_id": "device_11", "event_time": "2026-09-09T19:21:03.032", "event_type": "failure",
 "severity": "low", "error_code": "ERR1221", "component": "bearing", "root_cause": "unknown"}
```

Each machine carries **its own** temperature threshold, so "too hot" is a per-device question,
not a global one.

The core requirement is to process temperature telemetry in real time and enrich it with
contextual operational data. Specifically, the system should:

- Aggregate temperature readings per device over a 1-minute window.
- Detect anomalies by comparing readings against that device's own threshold.
- Keep the event log available alongside the telemetry, so operational context can be attached
  to any anomaly.

For each 1-minute window, we want to answer:

- Is the behaviour anomalous against that device's threshold, and how much of the window was
  spent over it?
- What were the individual readings behind that window?
- What operational events occurred in the same time frame — failures, maintenance, inspections,
  severity?

The data pipeline architecture, and the technology at each step, is in the image below.

![The streamhouse pipeline](docs/streamhouse-architecture.png)

## The architecture: who does what

Same sensors, same Kafka topics, same StarRocks at the end. Ingestion and serving don't move.
What changes is everything between them.

- **Kafka** is the ingress. Two topics, `iot-telemetry` and `iot-events`, exactly as before. It
  is a bus, not a place data lives.
- **Flink 1.20** is the compute. All of it — landing, the lookup join, the window. There is no
  second engine anywhere in this pipeline.
- **Fluss** is the storage. Every step of the pipeline lands in a Fluss table, and every one of
  those tables is queryable while the job writing it is still running.
- **Tiering** is Fluss's own job, not mine. Each table is created with
  `'table.datalake.freshness' = '30s'`, and a tiering service flushes it into **Iceberg on
  MinIO**, catalogued by **Nessie**. I never write a sink, a compaction job or a catalog entry.
- **StarRocks** reads the Iceberg side. It has no Fluss connector and does not know Fluss exists,
  which is the point of the last demo.

A tiered table has two names. `SELECT ... FROM datalake_device_telemetry` reads **hot ∪ cold** and
answers now. `FROM datalake_device_telemetry$lake` reads **only** the Iceberg copy, as of the last
flush. Tiered means copied, not moved: the row is in both places at once.

The streamhouse puts storage where Kappa had job state. Flink still does the work, but each step
lands in a table on the way through — the topics as they arrive, then every reading joined to its
device's own temperature threshold with the anomaly flagged, then the one-minute window. Bronze,
silver, gold, except none of them is a copy waiting for a batch job. They are the pipeline,
written down and queryable while it runs.

The window still earns its place, for cost rather than capability. It collapses a minute of
readings per device into one row, so a dashboard reads thousands instead of millions. The
difference is that when a question doesn't fit it, you drop to the per-reading table and ask
there. In Kappa there was nothing to drop to.

Events don't get a stage at all. They land, they tier, and the join to readings happens at read
time, in whatever query needs it. Kappa had to decide that join in advance because the fact table
was the only readable thing. Here events are readable the moment they arrive, so the join can
wait for the question.

## Build it

```bash
git clone <repo> && cd streamhouse-lab
make up        # Flink, Fluss, Kafka, MinIO, Nessie, plus a liveness gate
make produce   # 11 devices, 50 readings/s onto iot-telemetry, ~5 events/s onto iot-events
```

The fleet is eleven machines across three plants. Each one has its own temperature threshold,
spread from 24.0 to 29.0 °C, while the producer draws temperature uniformly from 18 to 30 °C.
That detail matters later — it is what makes the final ranking mean something.

**Step 1 — the dimension.** The one primary-key table in the pipeline, and the only one not
tiered:

```sql
CREATE TABLE dim_device (
  device_id STRING NOT NULL, temp_threshold DOUBLE, location_id STRING,
  model STRING, status STRING,
  PRIMARY KEY (device_id) NOT ENFORCED
);
```

**Step 2 — land the topics.** Two log tables, tiered from birth:

```sql
CREATE TABLE iot_telemetry (
  reading_id BIGINT, device_id STRING NOT NULL, event_time TIMESTAMP(3),
  energy_usage DOUBLE, temperature DOUBLE, vibration DOUBLE, signal_strength INT,
  ptime AS PROCTIME()
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);
```

Those two properties are the entire tiering configuration. From here Fluss owns the cold copy.

A Kafka source table feeds it, and one thing there is worth calling out because it fails
silently: set `'json.timestamp-format.standard' = 'ISO-8601'`. Python's `datetime.isoformat()`
writes `2026-09-09T19:21:03.81` with a `T`, Flink's JSON format defaults to `SQL` which expects a
space, and you get NULL timestamps with no error anywhere.

**Step 3 — enrich.** Flink reads `iot_telemetry` as a stream *while the landing job is still
writing it*, and looks up each reading's own threshold:

```sql
SELECT t.*, d.temp_threshold, d.location_id, d.model
FROM iot_telemetry t
LEFT JOIN dim_device FOR SYSTEM_TIME AS OF t.ptime AS d
  ON t.device_id = d.device_id;
```

That read-while-writing loop is the thing a topic cannot do, and it is why the flush that follows
is a *copy*, not a handover.

**Step 4 — the window.** A one-minute processing-time tumble per device into a second tiered
table. One enriched stream feeds both sinks: per-reading detail and per-minute rollup, off a
single read.

**Step 5 — turn tiering on.**

```bash
make tiering
```

Two things will bite you here, and both cost me an evening.

**Tiered tables have no primary key.** Querying the bare table merges the Iceberg snapshot with
the Fluss log. On a PK table that merge is a sort-merge, which requires the lake reader to
implement Fluss's `SortedRecordReader` — and `fluss-lake-iceberg-0.9.1` does not implement it
anywhere. The read dies with `lake records must instance of sorted view`. Log tables concatenate
instead, and work. Paimon implements it; Iceberg union read on PK tables is post-0.9.

**Append-only propagates backwards.** Reading a PK table in streaming mode emits `-U/+U`, and an
append-only sink rejects it. So making the tiered table append-only forces its source to be
append-only too. Same reason the window is processing time rather than event time: a proctime
tumble needs no watermark and emits append-only, while an unbounded `GROUP BY` would emit a
changelog the sink refuses.

## Demo 1 — query the stream

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'table';

SELECT device_id, location_id, temperature, temp_threshold, vibration, event_time
FROM datalake_device_telemetry
WHERE anomaly_flag OR vibration_spike;
```

Rows scroll past as the sensors emit them. It is a streaming read of a *tiered* table: it opens
on the Iceberg snapshot and switches to the Fluss log, so you are watching hot ∪ cold advance in
one query. In Kappa that data is job state — there is nothing to point a SELECT at.

Watching a feed scroll is a debugging pleasure, not a business case. Here is the business case.

**The triage scenario.** A dashboard row goes red: `device_1`, last minute, 31 of 50 readings
over threshold. Three questions follow immediately, and all three are SQL against tables that
already exist.

```sql
SET 'execution.runtime-mode' = 'batch';

-- 1. which device, which minute
SELECT device_id, window_start, cnt_points, cnt_anomalies,
       round(max_temperature, 1) AS max_c, threshold_used
FROM datalake_device_health_1min
ORDER BY window_start DESC, cnt_anomalies DESC
LIMIT 5;

-- 2. the readings behind that row — per reading, not per minute
SELECT reading_id, event_time, temperature, temp_threshold, vibration, signal_strength
FROM datalake_device_telemetry
WHERE device_id = 'device_1'
  AND event_time > CURRENT_TIMESTAMP - INTERVAL '3' MINUTE
ORDER BY event_time DESC
LIMIT 20;

-- 3. did the machine log anything at the same time?
--    joined at read time, not baked into the pipeline
SELECT event_time, event_type, severity, error_code, component, root_cause
FROM iot_events
WHERE device_id = 'device_1'
  AND event_time > CURRENT_TIMESTAMP - INTERVAL '3' MINUTE;
```

Query 2 is the one that matters. It is the drill-down from the aggregate to the evidence, and in
Kappa it does not exist: the per-reading rows were job state, so the only place they survive is
the topic, where answering "device_1, last three minutes" means scanning offsets. Query 3 is a
join Kappa had to commit to in advance, because the fact table was the only readable thing.

Now run query 2 again against `datalake_device_telemetry$lake`. Fewer rows, and usually nothing
from the last thirty seconds. A lakehouse reader cannot answer this question yet. The streamhouse
answered it while the incident was still happening.

That is the real advantage, and it is not debugging. An operational question gets answered
against production tables, at the grain it is asked, with no new job, no new topic and no replay.
The same goes for a changed rule: move the vibration threshold from 3.0 to 2.5 and re-ask over
hot ∪ cold. You get the new answer over history *and* over the last ten seconds, from one query.

One caveat: the bare table is not readable in batch mode until the first flush exists
(`Batch mode can only be supported if one lake snapshot exists`). Wait thirty seconds after
`make tiering`.

## Demo 2 — the gap, and the small files problem

```bash
make demo
```

```
+---------------+-----------+------------------+
| hot_plus_cold | cold_only | rows_only_in_hot |
+---------------+-----------+------------------+
|         14700 |     14100 |              600 |
+---------------+-----------+------------------+
```

`rows_only_in_hot` is 600 readings that exist, are enriched, are queryable, and are not on the
Iceberg path yet. Watch `cold_only` across iterations and it advances in visible thirty-second
steps — that is `table.datalake.freshness`. Cancel the tiering job in the Flink UI and `cold_only`
freezes while `hot_plus_cold` keeps climbing. Restart it and the cold number catches up in one
jump. The two tiers are visibly independent.

A note on how that number is produced, because the obvious version is wrong: the producer draws
`reading_id` at random, so `max(reading_id)` is not the newest row and a max-based freshness test
reports a false negative. The honest test is an anti-join against `$lake`, which names actual
readings that are not in the lake yet.

This is also where the storage argument lands. You can write the middle layers out to object
storage yourself, of course. But then you are choosing: flush often and get fresh data in a pile
of small files, or flush rarely and get decent files that are minutes behind. Either way you now
own a sink, a compaction job and a catalog to make the files queryable.

The streamhouse does not abolish that trade-off. It moves it somewhere it stops mattering. You
still pick a flush interval — you just pick it for file size now, not for freshness, because
freshness is the hot tier's job. Thirty seconds here is a demo number. Make it five minutes and
every query above still answers the same, because the bare table reads both tiers. The sink, the
compaction and the catalog entry are Fluss's problem, not mine.

## Demo 3 — the cold tier is just Iceberg

```bash
make starrocks
```

StarRocks has no Fluss connector. It registers an external Iceberg catalog against Nessie and
reads Parquet out of MinIO. Fluss is not in this path at all:

```sql
CREATE EXTERNAL CATALOG iceberg_nessie PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "http://nessie:19120/iceberg/main",
  "iceberg.catalog.warehouse" = "warehouse",
  ...
);

SELECT device_id,
       max(threshold_used) AS threshold,
       sum(cnt_anomalies)  AS anomalous_readings,
       round(100.0 * sum(cnt_anomalies) / sum(cnt_points), 1) AS pct_over_threshold
FROM datalake_device_health_1min
GROUP BY device_id
ORDER BY pct_over_threshold DESC;
```

```
device_id  threshold  anomalous_readings  pct_over_threshold
device_1        24.0                 819                50.2
device_2        24.5                 752                46.2
device_3        25.0                 667                41.3
...
device_10       28.5                 208                12.9
device_11       29.0                 111                 7.1
```

That monotonic fall from 50% to 7% is the end-to-end proof, and it matches theory: temperature is
drawn uniformly from 18 to 30 °C, so a device with a 24.0 °C threshold should sit over it about
half the time and one at 29.0 °C about a twelfth. Those per-device thresholds were resolved by a
lookup join in Flink, tiered to Iceberg, and read here by an engine that has never heard of
Fluss. Nothing was re-modelled and nothing was copied a second time.

Run `SELECT count(*)` here and it comes back *below* the union-read count from demo 2. StarRocks
is reading one tier behind, which is exactly right for a batch consumer — and it is the Lambda
half of the claim. The same table that answered a sub-second operational question is also a plain
Iceberg table you can point Spark, Trino, dbt or a nightly job at.

**An option I did not take.** If you want StarRocks itself to be sub-second — a dashboard
refreshing every second rather than every thirty — you can point a Flink StarRocks sink at the
same enriched stream and write to a primary-key table directly, alongside the tiering. I have not
wired that into this lab, and it is worth being clear about why it is a choice and not an
upgrade: it is a second copy, the exact thing the pattern otherwise removes. The difference from
Lambda is that this copy is a cache you can drop and rebuild from the tables above, not the only
place the data exists. Serving-layer decision, not a storage one.

## One more thing: the catalog is a git repo

Everything above puts the cold tier in Iceberg under a Nessie catalog, and Nessie is a git-shaped
catalog — branches, commits, merges over table metadata.

Which means you can write a curated table to a branch, run your quality checks on the branch, and
merge to `main` only if they pass. The interesting part is that this works while the stream is
still running. The tiering job keeps committing its tables to `main`, the branch only touches the
curated ones, and Nessie merges per table, so there is nothing to conflict over. A failed quality
gate becomes an unmerged branch and an alert, not a stopped pipeline. And a consumer can read the
candidate: same SQL, same table name, one catalog property different.

That is a post of its own, and it is the next one.

## Closing

Two demos, one table. The per-reading table answered an incident question in milliseconds, and
the same rows, thirty seconds later, were Parquet in object storage that StarRocks aggregated
without knowing Fluss exists. That is Kappa's latency and Lambda's batch surface, from one write
and one copy of the data.

Which leaves Kafka as the way readings get in, not the place they live. Retention is a buffer
decision again. The history is in Iceberg.

The whole lab is here: [repo link]. `make up`, `make produce`, and the SQL above.
