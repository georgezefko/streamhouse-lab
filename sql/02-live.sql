-- Step 4: query the hot tier and the cold tier. Follows the demo in the blog post.
--
-- Paste into an INTERACTIVE session (`make sql`), after sql/catalog.sql. sql-client.sh -f
-- cannot render an updating view, so section B only works here — not through a script.
-- Ctrl-C / 'q' leaves a result view and returns you to the prompt.
--
-- Running this in a SECOND `make sql` window? Catalogs are per-session: paste
-- sql/catalog.sql there first, or every name here is "Object not found".

-- ═════════════════════════════════════════════════════════════════════════════
-- A) BEFORE TIERING — the cold side is empty.
-- ═════════════════════════════════════════════════════════════════════════════
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

-- A1) The $lake suffix reads only what has been flushed to Iceberg. Run this before
--     `make tiering` and it returns 0.
SELECT count(*) AS cold_only FROM datalake_device_telemetry$lake;

-- ═════════════════════════════════════════════════════════════════════════════
-- B) THE SAME TABLE, LIVE — the hot tier answers on its own.
-- ═════════════════════════════════════════════════════════════════════════════
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'table';

-- B1) The bare name reads hot ∪ cold. Streaming, so the count moves in place: it opens in
--     the thousands and keeps climbing, with nothing yet in Iceberg. The query never
--     finishes — it is a read of the Fluss log as rows arrive.
--
--     Streaming, not batch: a batch read of the bare table needs at least one lake snapshot
--     to start from, and before tiering there isn't one. Both modes work after the first flush.
SELECT count(*) AS hot_plus_cold FROM datalake_device_telemetry;

-- B2) Change the query and the same table is a live feed: every reading over its own
--     device's threshold or with a vibration spike, as it lands.
SELECT device_id, location_id, temperature, temp_threshold, event_time
FROM datalake_device_telemetry
WHERE anomaly_flag OR vibration_spike;

-- ═════════════════════════════════════════════════════════════════════════════
-- C) AFTER `make tiering` — both tiers, in batch.
-- ═════════════════════════════════════════════════════════════════════════════
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

-- C1) What tiering actually wrote: one Iceberg snapshot per flush.
SELECT snapshot_id, operation FROM datalake_device_telemetry$lake$snapshots;

-- C2) End-to-end latency. Silver carries two timestamps — event_time from the sensor and
--     ingest_time stamped by the enrichment job — so their difference is the whole path
--     from sensor through Kafka, the landing job and Fluss into silver.
SELECT TIMESTAMPDIFF(SECOND, event_time, ingest_time) AS lag_s,
       count(*) AS readings
FROM datalake_device_telemetry
WHERE event_time > CURRENT_TIMESTAMP - INTERVAL '1' MINUTE
GROUP BY TIMESTAMPDIFF(SECOND, event_time, ingest_time)
ORDER BY lag_s;

-- C3) "Which devices are running hot?" — plain SQL over hot ∪ cold. It is query 4 in
--     sql/03-contrast.sql, so `make demo` prints it on every round too.

-- A1 vs B1 is the whole point, and `make demo` puts them on one line and loops them so you
-- can watch the cold tier chase the hot one.
