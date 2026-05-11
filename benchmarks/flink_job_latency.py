#!/usr/bin/env python3
"""
StreamForge AI — Flink Job Latency Benchmark

Measures the end-to-end latency of the StreamForge Flink stream-processing
pipeline by replaying the same logic that lives in:

    stream-processor/src/main/java/ai/streamforge/processor/CdcUserEventCountJob.java

Stages timed individually:
  1. Deserialisation   — JSON bytes → CdcEvent (mirrors SchemaAwareCdcDeserializationSchema)
  2. Filter            — op == "c" guard (mirrors the filter() call in the job)
  3. Window aggregation — tumbling time-window count (mirrors TumblingEventTimeWindows)
  4. Serialisation     — WindowResult → JSON bytes (mirrors UserEventCountSerializationSchema)

Because the benchmark is pure Python it cannot exercise real Flink watermark
scheduling, but the arithmetic and serialisation cost faithfully mirrors the
Java implementation. The window sizes and out-of-orderness constants are taken
from the defaults in CdcUserEventCountJob.

Reproducibility
---------------
All timestamps are deterministic (seed-driven). Running the benchmark twice
on the same machine must produce identical event counts (throughput numbers
may differ by ±5 % due to OS scheduling noise).
"""

import json
import random
import statistics
import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# ---------------------------------------------------------------------------
# Data models (Python equivalents of the Java model classes)
# ---------------------------------------------------------------------------

@dataclass
class CdcEvent:
    op: str                  # "c" insert, "u" update, "d" delete, "r" snapshot
    ts_ms: int               # Debezium source timestamp (event time)
    customer_id: int
    schema_version: int = 1


@dataclass
class WindowResult:
    window_start_ms: int
    window_end_ms: int
    user_id: int
    event_count: int
    emitted_at_ms: int


# ---------------------------------------------------------------------------
# Pipeline stages (Python mirrors of the Java operators)
# ---------------------------------------------------------------------------

def _deserialise(raw: bytes) -> CdcEvent:
    d = json.loads(raw)
    payload = d["payload"]
    after = payload.get("after") or {}
    return CdcEvent(
        op=payload["op"],
        ts_ms=payload["ts_ms"],
        customer_id=after.get("id", -1),
        schema_version=d["schema"].get("version", 1),
    )


def _filter_insert(event: CdcEvent) -> bool:
    return event.op == "c" and event.schema_version == 1


def _aggregate_window(events: List[CdcEvent], window_start_ms: int,
                      window_size_ms: int) -> List[WindowResult]:
    counts: Dict[int, int] = {}
    for e in events:
        counts[e.customer_id] = counts.get(e.customer_id, 0) + 1
    now_ms = int(time.time() * 1000)
    return [
        WindowResult(
            window_start_ms=window_start_ms,
            window_end_ms=window_start_ms + window_size_ms,
            user_id=uid,
            event_count=cnt,
            emitted_at_ms=now_ms,
        )
        for uid, cnt in counts.items()
    ]


def _serialise(result: WindowResult) -> bytes:
    return json.dumps({
        "window_start": result.window_start_ms,
        "window_end": result.window_end_ms,
        "user_id": result.user_id,
        "event_count": result.event_count,
        "emitted_at": result.emitted_at_ms,
    }).encode()


# ---------------------------------------------------------------------------
# Event generator
# ---------------------------------------------------------------------------

def _make_raw_event(seq: int, base_ts_ms: int, rng: random.Random,
                    insert_ratio: float = 0.9) -> bytes:
    op = "c" if rng.random() < insert_ratio else rng.choice(["u", "d"])
    customer_id = rng.randint(1, 200)
    after = {"id": customer_id, "name": f"user_{customer_id}",
             "email": f"u{customer_id}@example.com"} if op != "d" else None
    before = {"id": customer_id} if op in ("u", "d") else None
    ts_ms = base_ts_ms + seq * 10  # 10ms between events
    envelope = {
        "schema": {"type": "struct", "version": 1},
        "payload": {
            "before": before,
            "after": after,
            "op": op,
            "ts_ms": ts_ms,
            "source": {"connector": "mysql", "db": "streamforge", "table": "customers"},
        },
    }
    return json.dumps(envelope).encode()


def _build_event_stream(num_events: int, base_ts_ms: int,
                        insert_ratio: float = 0.9) -> List[bytes]:
    rng = random.Random(RANDOM_SEED)
    return [_make_raw_event(i, base_ts_ms, rng, insert_ratio) for i in range(num_events)]


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

@dataclass
class FlinkScenarioConfig:
    name: str
    num_events: int          # events in the stream
    window_size_s: int       # tumbling window size (seconds); matches WINDOW_SIZE_SECONDS env var
    insert_ratio: float = 0.9
    out_of_orderness_s: int = 5  # matches OUT_OF_ORDERNESS_SECONDS env var
    repeat: int = 5


@dataclass
class FlinkBenchmarkResult:
    scenario: str
    num_events: int
    windows_fired: int
    deser_ms: float          # total deserialisation time
    filter_ms: float         # total filter time
    window_ms: float         # total window aggregation time
    ser_ms: float            # total serialisation time
    e2e_ms: float            # total end-to-end pipeline time
    throughput_eps: float    # events processed per second (e2e)
    avg_window_latency_ms: float  # average per-window processing time


SCENARIOS: List[FlinkScenarioConfig] = [
    FlinkScenarioConfig("Small stream   (1k events,  30s window)",
                        num_events=1_000,  window_size_s=30),
    FlinkScenarioConfig("Medium stream  (10k events, 30s window)",
                        num_events=10_000, window_size_s=30),
    FlinkScenarioConfig("Large stream   (50k events, 30s window)",
                        num_events=50_000, window_size_s=30),
    FlinkScenarioConfig("Wide window    (10k events, 60s window)",
                        num_events=10_000, window_size_s=60),
    FlinkScenarioConfig("Narrow window  (10k events, 10s window)",
                        num_events=10_000, window_size_s=10),
    FlinkScenarioConfig("Low insert ratio (10k events, 30% INSERT)",
                        num_events=10_000, window_size_s=30, insert_ratio=0.3),
]


# ---------------------------------------------------------------------------
# Benchmark execution
# ---------------------------------------------------------------------------

def _run_pipeline(raw_events: List[bytes], window_size_ms: int) -> Tuple[
        float, float, float, float, int, List[float]]:
    """
    Run all four pipeline stages and return:
    (deser_ms, filter_ms, window_ms, ser_ms, windows_fired, per_window_latencies_ms)
    """
    # Stage 1 — Deserialisation
    t0 = time.perf_counter()
    events = [_deserialise(r) for r in raw_events]
    deser_ms = (time.perf_counter() - t0) * 1000

    # Stage 2 — Filter (INSERT + schema check)
    t1 = time.perf_counter()
    inserts = [e for e in events if _filter_insert(e)]
    filter_ms = (time.perf_counter() - t1) * 1000

    # Stage 3 — Tumbling window aggregation
    # Group events into windows by their ts_ms
    t2 = time.perf_counter()
    window_groups: Dict[int, List[CdcEvent]] = {}
    if inserts:
        min_ts = min(e.ts_ms for e in inserts)
        for e in inserts:
            bucket = ((e.ts_ms - min_ts) // window_size_ms) * window_size_ms + min_ts
            window_groups.setdefault(bucket, []).append(e)

    per_window_latencies: List[float] = []
    all_results: List[WindowResult] = []
    for ws, evs in window_groups.items():
        wt0 = time.perf_counter()
        results = _aggregate_window(evs, ws, window_size_ms)
        per_window_latencies.append((time.perf_counter() - wt0) * 1000)
        all_results.extend(results)
    window_ms = (time.perf_counter() - t2) * 1000

    # Stage 4 — Serialisation
    t3 = time.perf_counter()
    for r in all_results:
        _serialise(r)
    ser_ms = (time.perf_counter() - t3) * 1000

    return deser_ms, filter_ms, window_ms, ser_ms, len(window_groups), per_window_latencies


def run_scenario(cfg: FlinkScenarioConfig) -> FlinkBenchmarkResult:
    base_ts_ms = 1_700_000_000_000  # fixed epoch for reproducibility
    raw_events = _build_event_stream(cfg.num_events, base_ts_ms, cfg.insert_ratio)
    window_size_ms = cfg.window_size_s * 1000

    agg: Dict[str, List[float]] = {k: [] for k in
                                    ["deser", "filt", "win", "ser", "e2e", "wlat"]}
    windows_fired_runs: List[int] = []

    for _ in range(cfg.repeat):
        t_start = time.perf_counter()
        deser, filt, win, ser, nw, wlats = _run_pipeline(raw_events, window_size_ms)
        e2e = (time.perf_counter() - t_start) * 1000
        agg["deser"].append(deser)
        agg["filt"].append(filt)
        agg["win"].append(win)
        agg["ser"].append(ser)
        agg["e2e"].append(e2e)
        agg["wlat"].extend(wlats)
        windows_fired_runs.append(nw)

    avg_e2e_ms = statistics.mean(agg["e2e"])
    avg_wlat = statistics.mean(agg["wlat"]) if agg["wlat"] else 0.0

    return FlinkBenchmarkResult(
        scenario=cfg.name,
        num_events=cfg.num_events,
        windows_fired=round(statistics.mean(windows_fired_runs)),
        deser_ms=statistics.mean(agg["deser"]),
        filter_ms=statistics.mean(agg["filt"]),
        window_ms=statistics.mean(agg["win"]),
        ser_ms=statistics.mean(agg["ser"]),
        e2e_ms=avg_e2e_ms,
        throughput_eps=cfg.num_events / (avg_e2e_ms / 1000),
        avg_window_latency_ms=avg_wlat,
    )


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _print_results(results: List[FlinkBenchmarkResult]) -> None:
    w = 100
    print("\n" + "=" * w)
    print("            StreamForge AI: Flink Job Latency Benchmark Results")
    print("=" * w)
    hdr = (f"{'Scenario':<45} {'E2E ms':>8} {'Deser ms':>9} {'Win ms':>7} "
           f"{'Ser ms':>7} {'Win/s':>6} {'Events/s':>10}")
    print(hdr)
    print("-" * w)
    for r in results:
        print(
            f"{r.scenario:<45} {r.e2e_ms:>8.1f} {r.deser_ms:>9.1f} {r.window_ms:>7.1f}"
            f" {r.ser_ms:>7.1f} {r.windows_fired:>6} {r.throughput_eps:>10,.0f}"
        )
    print("=" * w)
    print("  E2E ms   = total pipeline latency for the full event stream (ms)")
    print("  Deser ms = deserialisation stage time")
    print("  Win ms   = window aggregation stage time (grouping + counting)")
    print("  Ser ms   = serialisation stage time (WindowResult → JSON bytes)")
    print("  Win/s    = number of tumbling windows fired")
    print("  Events/s = end-to-end throughput")
    print()
    print("  Stage breakdown (average across scenarios):")
    avg_e2e = statistics.mean(r.e2e_ms for r in results)
    avg_deser = statistics.mean(r.deser_ms for r in results)
    avg_win = statistics.mean(r.window_ms for r in results)
    avg_ser = statistics.mean(r.ser_ms for r in results)
    print(f"    Deserialisation : {avg_deser / avg_e2e * 100:5.1f}% of E2E")
    print(f"    Window agg      : {avg_win   / avg_e2e * 100:5.1f}% of E2E")
    print(f"    Serialisation   : {avg_ser   / avg_e2e * 100:5.1f}% of E2E")
    print("=" * w)


def run_benchmark() -> List[FlinkBenchmarkResult]:
    print("=" * 70)
    print("      StreamForge AI: Flink Job — Latency Benchmark")
    print("=" * 70)
    print(f"  Seed: {RANDOM_SEED}  |  Scenarios: {len(SCENARIOS)}")
    print("  (Pure-Python replay of CdcUserEventCountJob pipeline stages)\n")

    results = []
    for i, cfg in enumerate(SCENARIOS, 1):
        print(f"  [{i}/{len(SCENARIOS)}] {cfg.name} ...", end=" ", flush=True)
        r = run_scenario(cfg)
        results.append(r)
        print(f"done  (e2e={r.e2e_ms:.1f}ms, {r.throughput_eps:,.0f} events/s)")

    _print_results(results)
    return results


if __name__ == "__main__":
    run_benchmark()
