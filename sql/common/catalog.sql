-- The Fluss catalog. Every Flink SQL session in this repo needs it first.
--
--   interactive (`make sql`):  paste this file, then the experiment's file.
--   scripted:                  demo.sh / bench.sh concatenate it onto the file they run —
--                              the SQL client has no INCLUDE, so this is the whole mechanism.
--
-- The lake (Iceberg/Nessie) config is inherited from the Fluss servers; the s3 creds here are
-- only so this client can read `$lake` tables directly.
--
-- NB: the Fluss catalog ignores CREATE TABLE IF NOT EXISTS (it still errors if the table
-- exists) — but CREATE CATALOG IF NOT EXISTS does work, so re-pasting this is safe.

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123',
  'iceberg.s3.access-key-id' = 'admin',
  'iceberg.s3.secret-access-key' = 'password'
);

USE CATALOG fluss_catalog;
