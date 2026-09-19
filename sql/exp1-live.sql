-- Experiment 1, part C: query the hot tier and the cold tier on the go.
--
-- Paste into an INTERACTIVE session (`make sql`), after sql/common/catalog.sql. sql-client.sh -f
-- cannot render an updating view, so the live queries below only work here — not through a script.
-- Ctrl-C / 'q' leaves a result view and returns you to the prompt.

-- ═════════════════════════════════════════════════════════════════════════════
-- A) LIVE — the hot tier, updating in place.
-- ═════════════════════════════════════════════════════════════════════════════
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'table';

-- A1) Anomaly feed. Every reading above its own device's threshold, as it lands.
--     This is a streaming read of the TIERED table — it starts from the lake snapshot
--     and switches to the Fluss log, so you are watching hot ∪ cold advance.
SELECT device_id, location_id, temperature, temp_threshold, vibration, event_time
FROM datalake_device_telemetry
WHERE anomaly_flag OR vibration_spike;

-- A2) Rolling health per device. Numbers move IN PLACE; no batch, no refresh.
--
--     The WHERE is what makes this watchable. Without it the streaming read starts from
--     the lake snapshot, so the counts open at whatever has already tiered — thousands —
--     and you cannot see them move. Filtering on event_time drops that backlog and the
--     query opens near zero.
--
--     Know what the filter does NOT do: a streaming GROUP BY never retracts a row once it
--     has been counted, so this is "since the query started", not a sliding window. Leave
--     it running an hour and it will have counted an hour. Use A2b for a true last-60s.
--     ponytail: a filter, not a window. A2b is the windowed version when you need one.
SELECT device_id,
       count(*) AS readings,
       sum(CASE WHEN anomaly_flag THEN 1 ELSE 0 END) AS anomalies,
       sum(CASE WHEN vibration_spike THEN 1 ELSE 0 END) AS vib_spikes,
       round(max(temperature), 1) AS worst_temp
FROM datalake_device_telemetry
WHERE event_time > CURRENT_TIMESTAMP - INTERVAL '2' MINUTE
GROUP BY device_id;

-- A2b) The same question as a TRUE sliding window: the last 60 seconds, recomputed every
--      10. Each emission is a fresh count that drops what aged out, which A2 cannot do.
--
--      A windowing TVF needs a time attribute, and datalake_device_telemetry has plain
--      TIMESTAMP columns — so mirror it with a proctime column added. Qualify the CREATE:
--      unqualified it lands in whatever catalog is current (see docs/EXPLANATION.md).
--
--      Proctime, so no watermark is needed. Output is append-only: one batch of rows every
--      10 seconds rather than numbers moving in place. Newest batch is at the bottom.
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`tel_live` (
  `ptime` AS PROCTIME()
) LIKE `fluss_catalog`.`fluss`.`datalake_device_telemetry`;

SELECT device_id,
       window_end,
       count(*) AS readings_60s,
       sum(CASE WHEN anomaly_flag THEN 1 ELSE 0 END) AS anomalies_60s,
       round(max(temperature), 1) AS worst_temp
FROM TABLE(
  HOP(TABLE `default_catalog`.`default_database`.`tel_live`,
      DESCRIPTOR(`ptime`), INTERVAL '10' SECOND, INTERVAL '60' SECOND)
)
GROUP BY device_id, window_start, window_end;

-- A3) The closed 1-minute windows, appearing one batch per minute.
SELECT device_id, window_start, cnt_points, round(avg_temperature, 1) AS avg_c, anomaly_flag
FROM datalake_device_health_1min;

-- ═════════════════════════════════════════════════════════════════════════════
-- B) THE THREE READ PATHS — same table, batch mode, side by side.
--
-- Running these in a SECOND `make sql` window? Catalogs are per-session: paste
-- sql/common/catalog.sql there first, or every name here is "Object not found".
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
