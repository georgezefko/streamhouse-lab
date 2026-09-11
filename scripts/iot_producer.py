#!/usr/bin/env python3
"""A stand-in device fleet: publishes JSON to the iot-telemetry / iot-events topics.

This is THE ingress for the pipeline — sql/exp1-pipeline.sql reads the topics it writes.
Swap in any producer on the same topics with the same field names and nothing downstream
changes.

  make produce      Experiment 1: iot-telemetry + iot-events, 50 readings/s, 200k rows
  make bench-load   Experiment 2: bench-telemetry only, 20k/s, 20M rows over 2M keys

Ported from the Mage/lambda project's confluent-kafka generator.
"""
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

BROKERS    = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC      = os.environ.get("TOPIC", "iot-telemetry")   # bench-telemetry for Experiment 2
RATE       = float(os.environ.get("RATE", "50"))        # telemetry rows/s
ROWS       = int(os.environ.get("ROWS", "200000"))      # 0 = run forever
EVENT_ODDS = float(os.environ.get("EVENT_ODDS", "0.1")) # -> ~5 events/s at RATE=50; 0 = none
ID_MAX     = int(os.environ.get("ID_MAX", "100000000")) # reading_id key space. Experiment 2
                                                        # narrows it so a given id exists.

# 11 devices, matching dim_device. The lookup join in sql/exp1-pipeline.sql NULLs anything else.
DEVICES = [f"device_{i}" for i in range(1, 12)]


def now():
    """Naive ISO-8601, e.g. 2026-09-09T20:15:30.123.

    Flink reads these as TIMESTAMP(3) with 'json.timestamp-format.standard' =
    'ISO-8601'. No UTC offset: a "+00:00" suffix only parses into TIMESTAMP_LTZ,
    and the consumer sets 'json.ignore-parse-errors' = 'true', so a mismatch here NULLs
    the column silently instead of failing.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds")


def telemetry(device_id):
    # Temperature 18-30 C against dim_device's 24.0-29.0 thresholds, so the
    # part-D ranking comes out ordered by threshold. Vibration spikes ~5%.
    return {
        "reading_id": random.randint(1, ID_MAX),
        "device_id": device_id,
        "event_time": now(),
        "energy_usage": round(random.uniform(0.0, 5.0), 2),
        "temperature": round(random.uniform(18.0, 30.0), 1),
        "vibration": round(random.uniform(0.0, 2.0) + (3.0 if random.random() > 0.95 else 0.0), 1),
        "signal_strength": random.randint(70, 100),
    }


def event(device_id):
    # A sparse union: only the fields belonging to this row's event_type are set,
    # and the rest are simply absent — Flink's JSON format reads a missing field
    # as NULL, which is exactly what src_events expects. 10/30/60 type mix.
    roll = random.random()
    e = {
        "device_id": device_id,
        "event_time": now(),
        "severity": random.choice(["low", "medium", "high"]),
    }
    if roll < 0.10:
        e["event_type"] = "failure"
        e["error_code"] = f"ERR{random.randint(1000, 1999)}"
        e["component"] = random.choice(["motor", "bearing", "sensor", "battery"])
        e["root_cause"] = random.choice(["overheating", "wear", "power_surge", "unknown"])
    elif roll < 0.40:
        e["event_type"] = "maintenance"
        e["technician"] = f"tech-{random.randint(1, 20)}"
        e["duration_min"] = random.randint(15, 240)
        e["parts_replaced"] = random.choice(["bearing", "filter", "battery"])
    else:
        e["event_type"] = "inspection"
        e["status"] = random.choice(["passed", "passed", "failed"])
        e["next_inspection_days"] = random.randint(7, 30)
    return e


def selftest():
    """The shapes sql/exp1-pipeline.sql reads. No broker needed: iot_producer.py --selftest"""
    t = telemetry("device_1")
    assert set(t) == {"reading_id", "device_id", "event_time", "energy_usage",
                      "temperature", "vibration", "signal_strength"}, t
    assert 18.0 <= t["temperature"] <= 30.0
    assert 1 <= t["reading_id"] <= ID_MAX
    assert "+" not in t["event_time"] and "T" in t["event_time"], t["event_time"]
    datetime.fromisoformat(t["event_time"])

    seen, sparse = set(), {
        "failure": {"error_code", "component", "root_cause"},
        "maintenance": {"technician", "duration_min", "parts_replaced"},
        "inspection": {"status", "next_inspection_days"},
    }
    for _ in range(2000):
        e = event("device_1")
        seen.add(e["event_type"])
        own = sparse[e["event_type"]]
        assert own <= set(e), e
        # no other type's fields leaked in
        for other, fields in sparse.items():
            if other != e["event_type"]:
                assert not (fields - own) & set(e), e
    assert seen == set(sparse), seen
    print("ok")


def produce(p, topic, key, payload):
    """produce(), waiting out a full local queue instead of dying on BufferError.

    librdkafka buffers 100k messages by default; at the Experiment 2 rate a slow broker
    can fill that, and an unhandled BufferError ends the run mid-benchmark.
    """
    while True:
        try:
            return p.produce(topic, key=key, value=json.dumps(payload))
        except BufferError:
            p.poll(0.1)


def main():
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": BROKERS, "client.id": "iot-data-producer",
                  "linger.ms": 50})
    sent = 0
    # Pace against a wall-clock deadline rather than sleeping per message: at 50/s
    # a per-message sleep drifts badly on the OS timer granularity.
    # ponytail: single-threaded json.dumps tops out somewhere around 20-50k msg/s, so the
    # Experiment 2 rate is a ceiling, not a promise. Falling short only means the topic grows
    # more slowly — the benchmark still diverges. Shard across processes if you need more.
    started = time.monotonic()
    print(f"producing to {BROKERS}/{TOPIC} at {RATE}/s, {ROWS or 'unbounded'} readings", flush=True)
    while ROWS == 0 or sent < ROWS:
        d = random.choice(DEVICES)
        produce(p, TOPIC, d, telemetry(d))
        if random.random() < EVENT_ODDS:
            produce(p, "iot-events", d, event(d))
        sent += 1
        p.poll(0)
        behind = started + sent / RATE - time.monotonic()
        if behind > 0:
            time.sleep(behind)
        if sent % (RATE * 60) == 0:
            print(f"{sent} readings", flush=True)
    p.flush()
    print(f"done: {sent} readings", flush=True)


if __name__ == "__main__":
    selftest() if "--selftest" in sys.argv else main()
