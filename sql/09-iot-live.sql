-- Tutorial 2: query the hot tier and the cold tier on the go.
--
-- Paste into an INTERACTIVE session (`make sql`). sql-client.sh -f cannot render an
-- updating view, so the live queries below only work here — not through a script.
-- Ctrl-C / 'q' leaves a result view and returns you to the prompt.

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123',
  'iceberg.s3.access-key-id' = 'admin',
  'iceberg.s3.secret-access-key' = 'password'
);

USE CATALOG fluss_catalog;

-- ═════════════════════════════════════════════════════════════════════════════
-- A) LIVE — the hot tier, updating in place.
-- ═════════════════════════════════════════════════════════════════════════════
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'table';

-- A1) Anomaly feed. Every reading above its own device's threshold, as it lands.
--     This is a streaming read of the TIERED table — it starts from the lake snapshot
--     and switches to the Fluss log, so you are watching hot ∪ cold advance.
SELECT device_id, location_id, temperature, temp_threshold, ingest_time
FROM datalake_device_telemetry
WHERE anomaly_flag;

-- A2) Rolling health per device. Numbers move continuously; no batch, no refresh.
SELECT device_id,
       count(*) AS readings,
       sum(CASE WHEN anomaly_flag THEN 1 ELSE 0 END) AS anomalies,
       round(max(temperature), 1) AS worst_temp
FROM datalake_device_telemetry
GROUP BY device_id;

-- A3) The closed 1-minute windows, appearing one batch per minute.
SELECT device_id, window_start, cnt_points, round(avg_temperature, 1) AS avg_c, anomaly_flag
FROM datalake_device_health_1min;

-- ═════════════════════════════════════════════════════════════════════════════
-- B) THE THREE READ PATHS — same table, batch mode, side by side.
-- ═════════════════════════════════════════════════════════════════════════════
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

-- B1) hot ∪ cold — the bare table. Answers now.
SELECT count(*) AS hot_plus_cold FROM datalake_device_telemetry;

-- B2) cold only — the $lake suffix. This is what a lakehouse reader sees.
SELECT count(*) AS cold_only FROM datalake_device_telemetry$lake;

-- B3) what tiering actually wrote — the Iceberg snapshots, one per flush.
SELECT snapshot_id, operation FROM datalake_device_telemetry$lake$snapshots;

-- B1 > B2, always, while data is arriving. `make demo` puts the two on one line and
-- loops them so you can watch the cold tier chase the hot one.
