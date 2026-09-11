-- Experiment 1, part C: THE CONTRAST — a streamhouse answers from the hot tier now; a lakehouse waits
-- for the next flush.
--
-- Run by `make demo` (scripts/demo.sh), which prepends sql/common/catalog.sql. Pasting it into
-- an interactive session works too, same order.

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

-- 1) Raw readings. rows_only_in_hot = what a lakehouse-only reader cannot see yet.
SELECT (SELECT count(*) FROM datalake_device_telemetry)      AS hot_plus_cold,
       (SELECT count(*) FROM datalake_device_telemetry$lake) AS cold_only,
       (SELECT count(*) FROM datalake_device_telemetry)
     - (SELECT count(*) FROM datalake_device_telemetry$lake) AS rows_only_in_hot;

-- 2) The same gap on the FACT table. This is the one that matters: it is the table a
--    dashboard reads, and the lake copy of it is always a flush behind.
SELECT (SELECT count(*) FROM datalake_device_health_1min)      AS windows_hot_plus_cold,
       (SELECT count(*) FROM datalake_device_health_1min$lake) AS windows_cold_only;

-- 3) Name specific readings the lakehouse path cannot see.
--    NOT max(reading_id) — the producer draws reading_id at random, so the largest key is not
--    the newest row. An anti-join against $lake is the honest test.
SELECT t.reading_id AS reading_only_in_hot, t.device_id, t.temperature, t.anomaly_flag
FROM datalake_device_telemetry t
LEFT JOIN datalake_device_telemetry$lake l ON t.reading_id = l.reading_id
WHERE l.reading_id IS NULL
LIMIT 3;

-- 4) The operational question, answered off hot ∪ cold: which devices are running hot?
SELECT device_id,
       count(*)                        AS anomalies,
       round(max(temperature), 1)      AS worst_temp,
       sum(CASE WHEN vibration_spike THEN 1 ELSE 0 END) AS vib_spikes
FROM datalake_device_telemetry
WHERE anomaly_flag
GROUP BY device_id
ORDER BY anomalies DESC
LIMIT 5;
