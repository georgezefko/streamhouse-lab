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

-- 2) The sharper version: name specific orders that the lakehouse path cannot see.
--    NOT max(order_key) — the faker generates order_key at random, so the largest key is not
--    the newest row. An anti-join against $lake is the honest test.
SELECT o.order_key AS order_only_in_hot, o.total_price, o.cust_name
FROM datalake_enriched_orders o
LEFT JOIN datalake_enriched_orders$lake l ON o.order_key = l.order_key
WHERE l.order_key IS NULL
LIMIT 3;
