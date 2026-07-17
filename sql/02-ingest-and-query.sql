-- Ingest the faker sources into Fluss, then demonstrate the union read.

-- 1) Fan the source streams into the hot PK tables.
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO fluss_nation   SELECT * FROM `default_catalog`.`default_database`.source_nation;
  INSERT INTO fluss_customer SELECT * FROM `default_catalog`.`default_database`.source_customer;
  INSERT INTO fluss_order    SELECT * FROM `default_catalog`.`default_database`.source_order;
END;

-- 2) Enrich via lookup joins (point lookups against PK tables) into the tiered table.
INSERT INTO datalake_enriched_orders
SELECT o.order_key, o.cust_key, o.total_price, o.order_date, o.order_priority, o.clerk,
       c.name, c.phone, c.acctbal, c.mktsegment, n.name
FROM fluss_order o
LEFT JOIN fluss_customer FOR SYSTEM_TIME AS OF o.ptime AS c ON o.cust_key = c.cust_key
LEFT JOIN fluss_nation   FOR SYSTEM_TIME AS OF o.ptime AS n ON c.nation_key = n.nation_key;

-- ---- THE DEMO: same table, two read paths ----
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

-- COLD only (Iceberg snapshot Fluss tiered; wait ~freshness=30s after inserts):
SELECT sum(total_price) AS cold_only FROM datalake_enriched_orders$lake;

-- Inspect the Iceberg snapshots Fluss wrote:
SELECT snapshot_id, operation FROM datalake_enriched_orders$lake$snapshots;

-- UNION read (hot Fluss + cold Iceberg) — sub-second freshness, the streamhouse payoff:
SELECT sum(total_price) AS hot_plus_cold FROM datalake_enriched_orders;
-- Re-run the two queries a few times: hot_plus_cold moves faster than cold_only.