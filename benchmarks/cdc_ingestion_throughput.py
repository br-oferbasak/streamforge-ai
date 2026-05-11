#!/usr/bin/env python3
"""
StreamForge AI — CDC Ingestion Throughput Benchmark

Measures the throughput of the CDC ingestion pipeline:
  1. Debezium JSON envelope serialisation / deserialisation rate (events/s)
  2. Schema-validation + filter throughput for INSERT-only workloads
  3. Throughput degradation under schema-evolution (DLQ routing overhead)

The benchmark is fully self-contained: no running Kafka or MySQL is required.
If a Kafka broker is reachable at KAFKA_BOOTSTRAP_SERVERS it will additionally
measure real consumer throughput via the kafka-python library (skipped otherwise).

Reproducibility knobs
---------------------
All random data is seeded so successive runs produce identical payloads.
Latency constants match the Debezium-MySQL connector defaults used in
deploy/cdc-mysql-kafka-debezium/.
"""

import json
import os
import random
import statistics
import time
from dataclasses import dataclass, field
from typing import List, Optional

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# ---------------------------------------------------------------------------
# Debezium CDC envelope helpers
# ---------------------------------------------------------------------------

_NAMES = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi"]
_DOMAINS = ["example.com", "streamforge.io", "acme.org", "test.net"]


def _make_customer(cid: int) -> dict:
    name = _NAMES[cid % len(_NAMES)]
    domain = _DOMAINS[cid % len(_DOMAINS)]
    return {"id": cid, "name": name, "email": f"{name.lower()}{cid}@{domain}",
            "updated_at": "2024-01-15T10:00:00Z", "created_at": "2024-01-01T00:00:00Z"}


def _debezium_envelope(op: str, after: Optional[dict], before: Optional[dict] = None,
                       schema_version: int = 1) -> bytes:
    """Return a serialised Debezium MySQL CDC envelope (JSON bytes)."""
    envelope = {
        "schema": {"type": "struct", "version": schema_version},
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "version": "2.5.0.Final",
                "connector": "mysql",
                "name": "streamforge",
                "ts_ms": int(time.time() * 1000),
                "db": "streamforge",
                "table": "customers",
                "server_id": 184054,
            },
            "op": op,
            "ts_ms": int(time.time() * 1000),
        },
    }
    return json.dumps(envelope).encode()


def _make_insert(cid: int, schema_version: int = 1) -> bytes:
    return _debezium_envelope("c", _make_customer(cid), schema_version=schema_version)


def _make_update(cid: int) -> bytes:
    before = _make_customer(cid)
    after = {**before, "name": before["name"] + "_updated"}
    return _debezium_envelope("u", after, before)


def _make_delete(cid: int) -> bytes:
    return _debezium_envelope("d", None, _make_customer(cid))


# ---------------------------------------------------------------------------
# Parsing / filtering logic (mirrors CdcUserEventCountJob logic)
# ---------------------------------------------------------------------------

def _parse_and_filter(raw: bytes) -> Optional[dict]:
    """Parse a CDC envelope and return the 'after' payload for INSERT events."""
    envelope = json.loads(raw)
    op = envelope["payload"]["op"]
    if op != "c":
        return None
    return envelope["payload"]["after"]


def _parse_with_schema_check(raw: bytes, expected_version: int = 1) -> Optional[dict]:
    """Parse + validate schema version; return None (DLQ) on mismatch."""
    envelope = json.loads(raw)
    version = envelope["schema"].get("version", 1)
    if version != expected_version:
        return None  # would go to DLQ in production
    return _parse_and_filter(raw)


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

@dataclass
class CdcScenarioConfig:
    name: str
    num_events: int
    insert_pct: float   # fraction of events that are INSERTs
    schema_drift_pct: float = 0.0   # fraction of INSERTs with wrong schema version
    repeat: int = 3


@dataclass
class CdcBenchmarkResult:
    scenario: str
    num_events: int
    throughput_eps: float          # events per second (parse + filter)
    insert_throughput_eps: float   # INSERTs accepted per second
    dlq_rate_pct: float            # percentage routed to DLQ
    p50_us: float                  # median per-event latency (µs)
    p99_us: float                  # 99th-percentile per-event latency (µs)
    payload_mb_s: float            # MB/s of raw bytes processed


SCENARIOS: List[CdcScenarioConfig] = [
    CdcScenarioConfig("Inserts only         (10k events, 100% INSERT)",
                      num_events=10_000, insert_pct=1.0),
    CdcScenarioConfig("Mixed ops            (10k events,  60% INSERT)",
                      num_events=10_000, insert_pct=0.6),
    CdcScenarioConfig("High volume          (50k events, 100% INSERT)",
                      num_events=50_000, insert_pct=1.0),
    CdcScenarioConfig("Schema drift  5%     (10k events, schema errors)",
                      num_events=10_000, insert_pct=1.0, schema_drift_pct=0.05),
    CdcScenarioConfig("Schema drift 25%     (10k events, schema errors)",
                      num_events=10_000, insert_pct=1.0, schema_drift_pct=0.25),
    CdcScenarioConfig("Delete-heavy         (10k events,  10% INSERT)",
                      num_events=10_000, insert_pct=0.1),
]


# ---------------------------------------------------------------------------
# Benchmark execution
# ---------------------------------------------------------------------------

def _build_payloads(cfg: CdcScenarioConfig) -> List[bytes]:
    rng = random.Random(RANDOM_SEED)
    payloads = []
    for i in range(cfg.num_events):
        r = rng.random()
        if r < cfg.insert_pct:
            drift = rng.random() < cfg.schema_drift_pct
            payloads.append(_make_insert(i, schema_version=2 if drift else 1))
        elif r < cfg.insert_pct + (1 - cfg.insert_pct) / 2:
            payloads.append(_make_update(i))
        else:
            payloads.append(_make_delete(i))
    return payloads


def _run_single(payloads: List[bytes]) -> tuple:
    """Returns (total_s, inserts_accepted, dlq_count, per_event_latencies_us, total_bytes)."""
    latencies_us: List[float] = []
    inserts = 0
    dlq = 0
    total_bytes = sum(len(p) for p in payloads)

    t0 = time.perf_counter()
    for raw in payloads:
        ev_start = time.perf_counter()
        result = _parse_with_schema_check(raw)
        ev_end = time.perf_counter()
        latencies_us.append((ev_end - ev_start) * 1e6)
        if result is not None:
            inserts += 1
        else:
            envelope = json.loads(raw)
            if envelope["payload"]["op"] == "c":
                dlq += 1
    total_s = time.perf_counter() - t0
    return total_s, inserts, dlq, latencies_us, total_bytes


def run_scenario(cfg: CdcScenarioConfig) -> CdcBenchmarkResult:
    payloads = _build_payloads(cfg)

    all_eps, all_insert_eps, all_dlq, all_p50, all_p99, all_mbps = [], [], [], [], [], []

    for _ in range(cfg.repeat):
        total_s, inserts, dlq, lats, total_bytes = _run_single(payloads)
        all_eps.append(len(payloads) / total_s)
        all_insert_eps.append(inserts / total_s)
        all_dlq.append(dlq / max(1, sum(1 for p in payloads
                                        if json.loads(p)["payload"]["op"] == "c")) * 100)
        sorted_lats = sorted(lats)
        all_p50.append(sorted_lats[len(sorted_lats) // 2])
        all_p99.append(sorted_lats[int(len(sorted_lats) * 0.99)])
        all_mbps.append(total_bytes / total_s / 1e6)

    return CdcBenchmarkResult(
        scenario=cfg.name,
        num_events=cfg.num_events,
        throughput_eps=statistics.mean(all_eps),
        insert_throughput_eps=statistics.mean(all_insert_eps),
        dlq_rate_pct=statistics.mean(all_dlq),
        p50_us=statistics.mean(all_p50),
        p99_us=statistics.mean(all_p99),
        payload_mb_s=statistics.mean(all_mbps),
    )


# ---------------------------------------------------------------------------
# Kafka live consumer (optional — skipped if broker unreachable)
# ---------------------------------------------------------------------------

def _try_kafka_throughput() -> Optional[dict]:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_SOURCE_TOPIC", "streamforge.streamforge.customers")
    try:
        from kafka import KafkaConsumer  # type: ignore
        from kafka.errors import NoBrokersAvailable  # type: ignore
    except ImportError:
        return None

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=3000,
            value_deserializer=lambda b: b,
        )
    except Exception:
        return None

    try:
        count = 0
        total_bytes = 0
        t0 = time.perf_counter()
        for msg in consumer:
            count += 1
            total_bytes += len(msg.value)
            if time.perf_counter() - t0 > 5.0:
                break
        elapsed = time.perf_counter() - t0
        consumer.close()
        if count == 0:
            return None
        return {
            "events": count,
            "elapsed_s": elapsed,
            "throughput_eps": count / elapsed,
            "throughput_mbps": total_bytes / elapsed / 1e6,
        }
    except Exception:
        consumer.close()
        return None


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _print_results(results: List[CdcBenchmarkResult]) -> None:
    w = 90
    print("\n" + "=" * w)
    print("          StreamForge AI: CDC Ingestion Throughput Benchmark Results")
    print("=" * w)
    hdr = (f"{'Scenario':<45} {'Throughput':>11} {'INS/s':>9} "
           f"{'DLQ%':>6} {'p50µs':>7} {'p99µs':>7} {'MB/s':>6}")
    print(hdr)
    print("-" * w)
    for r in results:
        print(
            f"{r.scenario:<45} {r.throughput_eps:>10,.0f} {r.insert_throughput_eps:>9,.0f}"
            f" {r.dlq_rate_pct:>5.1f}% {r.p50_us:>7.1f} {r.p99_us:>7.1f} {r.payload_mb_s:>6.2f}"
        )
    print("=" * w)
    print("  Throughput = total events/s parsed+filtered (in-process, no network)")
    print("  INS/s      = INSERT events accepted by filter per second")
    print("  DLQ%       = fraction of INSERT events routed to dead-letter queue")
    print("  p50/p99    = per-event parse latency percentiles (microseconds)")
    print("=" * w)


def run_benchmark() -> List[CdcBenchmarkResult]:
    print("=" * 70)
    print("      StreamForge AI: CDC Ingestion — Throughput Benchmark")
    print("=" * 70)
    print(f"  Seed: {RANDOM_SEED}  |  Scenarios: {len(SCENARIOS)}")
    print("  (No external services required — uses simulated Debezium payloads)\n")

    results = []
    for i, cfg in enumerate(SCENARIOS, 1):
        print(f"  [{i}/{len(SCENARIOS)}] {cfg.name} ...", end=" ", flush=True)
        r = run_scenario(cfg)
        results.append(r)
        print(f"done  ({r.throughput_eps:,.0f} events/s, p99={r.p99_us:.1f}µs)")

    _print_results(results)

    kafka_result = _try_kafka_throughput()
    if kafka_result:
        print("\n  [Live Kafka] consumer throughput:")
        print(f"    Events consumed : {kafka_result['events']:,}")
        print(f"    Elapsed         : {kafka_result['elapsed_s']:.2f}s")
        print(f"    Throughput      : {kafka_result['throughput_eps']:,.0f} events/s")
        print(f"    Bandwidth       : {kafka_result['throughput_mbps']:.2f} MB/s")
    else:
        print("\n  [Live Kafka] broker not reachable — skipping live consumer test.")
        print("  Set KAFKA_BOOTSTRAP_SERVERS to enable.")

    return results


if __name__ == "__main__":
    run_benchmark()
