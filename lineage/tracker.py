"""
High-level lineage tracking API.

Usage
-----
from lineage.tracker import lineage_run, kafka_dataset, minio_dataset

with lineage_run(
    "streamforge", "feature-sink",
    inputs=[kafka_dataset("streamforge.features.user_event_counts")],
    outputs=[minio_dataset("processed", "streamforge/features")],
) as run_id:
    ...   # your job logic; COMPLETE emitted on exit, FAIL on exception
"""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator, List, Optional

from .emitter import LineageEmitter, default_emitter
from .models import Dataset, Job, LineageEvent, Run


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _new_run_id() -> str:
    return str(uuid.uuid4())


@contextmanager
def lineage_run(
    job_namespace: str,
    job_name: str,
    inputs: List[Dataset],
    outputs: List[Dataset],
    *,
    emitter: Optional[LineageEmitter] = None,
    run_id: Optional[str] = None,
    job_facets: Optional[dict] = None,
) -> Generator[str, None, None]:
    """
    Context manager that wraps a job execution with START → COMPLETE/FAIL events.

    Yields the run_id string so the caller can attach it to logs or output records.
    """
    em = emitter or default_emitter()
    rid = run_id or _new_run_id()
    job = Job(namespace=job_namespace, name=job_name, facets=job_facets or {})
    run = Run(runId=rid)

    em.emit(LineageEvent(eventType="START", eventTime=_now_iso(), run=run, job=job, inputs=inputs, outputs=outputs))
    try:
        yield rid
        em.emit(LineageEvent(eventType="COMPLETE", eventTime=_now_iso(), run=run, job=job, inputs=inputs, outputs=outputs))
    except Exception:
        em.emit(LineageEvent(eventType="FAIL", eventTime=_now_iso(), run=run, job=job, inputs=inputs, outputs=[]))
        raise


# ---------------------------------------------------------------------------
# Dataset factory helpers
# ---------------------------------------------------------------------------

def dataset(namespace: str, name: str, **custom_facets) -> Dataset:
    facets = {"custom": custom_facets} if custom_facets else {}
    return Dataset(namespace=namespace, name=name, facets=facets)


def kafka_dataset(topic: str, bootstrap_servers: str = "kafka:9092") -> Dataset:
    return Dataset(
        namespace=f"kafka://{bootstrap_servers}",
        name=topic,
        facets={"dataSource": {"name": "kafka", "uri": f"kafka://{bootstrap_servers}"}},
    )


def minio_dataset(bucket: str, key_prefix: str, endpoint: str = "minio:9000") -> Dataset:
    uri = f"s3://{endpoint}/{bucket}/{key_prefix.lstrip('/')}"
    return Dataset(
        namespace=f"s3://{endpoint}/{bucket}",
        name=key_prefix,
        facets={"dataSource": {"name": "minio", "uri": uri}},
    )


def file_dataset(path: str) -> Dataset:
    return Dataset(namespace="file", name=path)
