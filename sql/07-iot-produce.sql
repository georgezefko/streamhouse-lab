-- Experiment 1, step 1: the sensors. Publish IoT JSON onto Kafka.
--
--   this file ──▶ kafka: iot-telemetry ──┐
--                 kafka: iot-events    ──┴──▶ sql/08-iot-pipeline.sql ──▶ Fluss ──▶ Iceberg
--
-- Nothing downstream knows or cares that flink-faker is the producer. The topics, the field
-- names and the JSON encoding are the contract — see "Swapping in a real producer" at the
-- bottom of this file.
--
-- Run this FIRST, in its own `make sql` session. It submits two detached jobs and returns.

-- Kafka lives in the default catalog — the Fluss catalog only holds Fluss tables. Everything
-- here is fully qualified so it does not matter which catalog is current when you paste it.

SET 'execution.runtime-mode' = 'streaming';

-- ─────────────────────────────────────────────────────────────────────────────
-- 1) The generator. 200k readings at 50/s ≈ 66 minutes — bounded on purpose, but
--    long enough that it does not drain mid-experiment.
--
--    Field ranges match the reference Python producer: temperature 18-30 °C,
--    vibration 0.1-2.0 with occasional spikes, energy 0.5-5.0, signal 70-100.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`gen_telemetry` (
  `reading_id`      BIGINT,
  `device_id`       STRING,
  `event_time`      TIMESTAMP(3),
  `energy_usage`    DOUBLE,
  `temperature`     DOUBLE,
  `vibration_base`  DOUBLE,
  `spike_roll`      DOUBLE,
  `signal_strength` INT
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '50',
  'number-of-rows' = '200000',
  'fields.reading_id.expression'      = '#{number.numberBetween ''1'',''100000000''}',
  'fields.device_id.expression'       = 'device_#{number.numberBetween ''1'',''12''}',
  'fields.event_time.expression'      = '#{date.past ''5'',''SECONDS''}',
  'fields.energy_usage.expression'    = '#{number.randomDouble ''2'',''0'',''5''}',
  'fields.temperature.expression'     = '#{number.randomDouble ''1'',''18'',''30''}',
  'fields.vibration_base.expression'  = '#{number.randomDouble ''1'',''0'',''2''}',
  'fields.spike_roll.expression'      = '#{number.randomDouble ''2'',''0'',''1''}',
  'fields.signal_strength.expression' = '#{number.numberBetween ''70'',''100''}'
);

CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`gen_events` (
  `device_id`   STRING,
  `event_time`  TIMESTAMP(3),
  `type_roll`   DOUBLE,
  `severity`    STRING,
  `error_code`  STRING,
  `component`   STRING,
  `root_cause`  STRING,
  `technician`  STRING,
  `duration_min` INT,
  `parts_replaced` STRING,
  `status`      STRING,
  `next_inspection_days` INT
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '5',
  'number-of-rows' = '20000',
  'fields.device_id.expression'    = 'device_#{number.numberBetween ''1'',''12''}',
  'fields.event_time.expression'   = '#{date.past ''5'',''SECONDS''}',
  'fields.type_roll.expression'    = '#{number.randomDouble ''3'',''0'',''1''}',
  'fields.severity.expression'     = '#{options.option ''low'',''medium'',''high''}',
  'fields.error_code.expression'   = 'ERR#{number.numberBetween ''1000'',''1999''}',
  'fields.component.expression'    = '#{options.option ''motor'',''bearing'',''sensor'',''battery''}',
  'fields.root_cause.expression'   = '#{options.option ''overheating'',''wear'',''power_surge'',''unknown''}',
  'fields.technician.expression'   = 'tech-#{number.numberBetween ''1'',''20''}',
  'fields.duration_min.expression' = '#{number.numberBetween ''15'',''240''}',
  'fields.parts_replaced.expression' = '#{options.option ''bearing'',''filter'',''battery''}',
  'fields.status.expression'       = '#{options.option ''passed'',''passed'',''failed''}',
  'fields.next_inspection_days.expression' = '#{number.numberBetween ''7'',''30''}'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 2) The topics.
--
--    'json.timestamp-format.standard' = 'ISO-8601' matters. Python's
--    datetime.isoformat() emits "2025-09-09T20:15:30.123", with a T. Flink's JSON
--    format defaults to 'SQL', which expects a space instead and silently fails to
--    parse. Set it on BOTH the producer and the consumer, or swap in a real
--    producer later and watch every timestamp arrive NULL.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`topic_telemetry` (
  `reading_id`      BIGINT,
  `device_id`       STRING,
  `event_time`      TIMESTAMP(3),
  `energy_usage`    DOUBLE,
  `temperature`     DOUBLE,
  `vibration`       DOUBLE,
  `signal_strength` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot-telemetry',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Events are a sparse union: every row carries the four common fields, and only the
-- ones belonging to its event_type. JSON is happy with that; the CASE expressions
-- below are what produce the NULLs.
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`topic_events` (
  `device_id`   STRING,
  `event_time`  TIMESTAMP(3),
  `event_type`  STRING,
  `severity`    STRING,
  `error_code`  STRING,
  `component`   STRING,
  `root_cause`  STRING,
  `technician`  STRING,
  `duration_min` INT,
  `parts_replaced` STRING,
  `status`      STRING,
  `next_inspection_days` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot-events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 3) Produce. Two detached jobs; this statement returns immediately.
-- ─────────────────────────────────────────────────────────────────────────────
EXECUTE STATEMENT SET
BEGIN
  -- Vibration spikes ~5% of the time, as in the reference producer.
  INSERT INTO `default_catalog`.`default_database`.topic_telemetry
  SELECT reading_id, device_id, event_time, energy_usage, temperature,
         round(vibration_base + CASE WHEN spike_roll > 0.95 THEN 3.0 ELSE 0.0 END, 1),
         signal_strength
  FROM `default_catalog`.`default_database`.gen_telemetry;

  -- 10% failure / 30% maintenance / 60% inspection, and only the fields that
  -- belong to each type are populated.
  INSERT INTO `default_catalog`.`default_database`.topic_events
  SELECT device_id,
         event_time,
         CASE WHEN type_roll < 0.10 THEN 'failure'
              WHEN type_roll < 0.40 THEN 'maintenance'
              ELSE 'inspection' END,
         severity,
         CASE WHEN type_roll < 0.10 THEN error_code  END,
         CASE WHEN type_roll < 0.10 THEN component   END,
         CASE WHEN type_roll < 0.10 THEN root_cause  END,
         CASE WHEN type_roll >= 0.10 AND type_roll < 0.40 THEN technician     END,
         CASE WHEN type_roll >= 0.10 AND type_roll < 0.40 THEN duration_min   END,
         CASE WHEN type_roll >= 0.10 AND type_roll < 0.40 THEN parts_replaced END,
         CASE WHEN type_roll >= 0.40 THEN status               END,
         CASE WHEN type_roll >= 0.40 THEN next_inspection_days END
  FROM `default_catalog`.`default_database`.gen_events;
END;

-- ─────────────────────────────────────────────────────────────────────────────
-- Swapping in a real producer
--
-- Nothing above is load-bearing. Point any producer at the same two topics with the
-- same field names and sql/08 keeps working unchanged — for example the confluent-kafka
-- generator from the Mage/lambda project, which produces exactly this shape:
--
--   producer.produce('iot-telemetry', key=device_id, value=json.dumps({
--       "device_id": ..., "timestamp": ..., "energy_usage": ...,
--       "temperature": ..., "vibration": ..., "signal_strength": ...}))
--
-- Two things to line up:
--   1. That producer names the field "timestamp"; sql/08 reads "event_time". Either
--      rename it there, or add 'timestamp' to sql/08's schema and drop event_time.
--   2. Keep ISO-8601 timestamps (datetime.isoformat() already is), and leave
--      'json.timestamp-format.standard' = 'ISO-8601' on the consumer.
--
-- Then skip this file entirely and start at sql/08.
-- ─────────────────────────────────────────────────────────────────────────────
