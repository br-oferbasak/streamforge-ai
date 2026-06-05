#!/usr/bin/env python3
"""
Simulates the full StreamForge pipeline and emits OpenLineage events.

Pipeline stages:
  1. cdc-ingestion    MySQL binlog → Kafka CDC topic
  2. stream-processor Kafka CDC   → Kafka feature counts (Flink window aggregation)
  3. feature-sink     Kafka       → MinIO feature artifacts
  4. prefetch-engine  MinIO       → local cache (hot-file selection + ML job)

Run:
  python lineage/demo.py [output_file]

Then view:
  python lineage/viewer.py [output_file]
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

# Allow running from project root or from lineage/ directory.
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from lineage.emitter import CompositeEmitter, ConsoleEmitter, FileEmitter
from lineage.tracker import dataset, kafka_dataset, lineage_run, minio_dataset


def run_demo(events_file: str = "lineage_events.ndjson") -> None:
    emitter = CompositeEmitter(FileEmitter(events_file), ConsoleEmitter())

    kafka_brokers = "kafka:9092"
    minio_ep = "minio:9000"

    print("\n── Stage 1: CDC Ingestion ─────────────────────────────────────────")
    with lineage_run(
        "streamforge",
        "cdc-ingestion",
        inputs=[dataset("mysql://userdb:3306", "userdb.user_events", table="user_events", format="debezium")],
        outputs=[kafka_dataset("debezium.mysql.userdb.user_events", kafka_brokers)],
        emitter=emitter,
    ) as run_id:
        time.sleep(0.02)
        print(f"  run_id={run_id}")

    print("\n── Stage 2: Stream Processor (Flink tumbling window) ──────────────")
    with lineage_run(
        "streamforge",
        "stream-processor",
        inputs=[kafka_dataset("debezium.mysql.userdb.user_events", kafka_brokers)],
        outputs=[kafka_dataset("streamforge.features.user_event_counts", kafka_brokers)],
        emitter=emitter,
    ) as run_id:
        time.sleep(0.02)
        print(f"  run_id={run_id}")

    print("\n── Stage 3: Feature Sink (Kafka → MinIO) ──────────────────────────")
    with lineage_run(
        "streamforge",
        "feature-sink",
        inputs=[kafka_dataset("streamforge.features.user_event_counts", kafka_brokers)],
        outputs=[minio_dataset("processed", "streamforge/features", minio_ep)],
        emitter=emitter,
    ) as run_id:
        time.sleep(0.02)
        print(f"  run_id={run_id}")

    print("\n── Stage 4: Prefetch Engine (MinIO → cache artifacts) ─────────────")
    with lineage_run(
        "streamforge",
        "prefetch-engine",
        inputs=[minio_dataset("processed", "streamforge/features", minio_ep)],
        outputs=[minio_dataset("processed", "streamforge/processed/run-latest", minio_ep)],
        emitter=emitter,
    ) as run_id:
        time.sleep(0.02)
        print(f"  run_id={run_id}")

    print(f"\n[demo] ✓ Events written to {events_file}")
    print(f"[demo]   View with: python lineage/viewer.py {events_file}")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "lineage_events.ndjson"
    run_demo(out)
