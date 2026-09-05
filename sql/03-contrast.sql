-- THE CONTRAST: a streamhouse answers from the hot tier now; a lakehouse waits for the flush.
-- Run non-interactively (see scripts/demo.sh) — a fresh -f session has no catalog, so create it.

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123',
  'iceberg.s3.access-key-id' = 'admin',
  'iceberg.s3.secret-access-key' = 'password'
);

USE CATALOG fluss_catalog;

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

-- 1) The whole story in one row. rows_only_in_hot = what a lakehouse-only reader cannot see yet.
SELECT (SELECT count(*) FROM datalake_enriched_orders)      AS hot_plus_cold,
       (SELECT count(*) FROM datalake_enriched_orders$lake) AS cold_only,
       (SELECT count(*) FROM datalake_enriched_orders)
     - (SELECT count(*) FROM datalake_enriched_orders$lake) AS rows_only_in_hot;

-- 2) The sharper version: one specific order written seconds ago.
--    found_in_cold = 0 — the newest hot row is simply not on the Iceberg path yet.
SELECT (SELECT max(order_key) FROM datalake_enriched_orders) AS newest_order,
       (SELECT count(*) FROM datalake_enriched_orders$lake
          WHERE order_key = (SELECT max(order_key) FROM datalake_enriched_orders)) AS found_in_cold;
