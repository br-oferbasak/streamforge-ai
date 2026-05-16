"""
StreamForge lineage — OpenLineage-compatible event capture and viewer.

Quick start:
    from lineage.tracker import lineage_run, kafka_dataset, minio_dataset
    from lineage.emitter import default_emitter

    with lineage_run("streamforge", "my-job",
                     inputs=[kafka_dataset("my.topic")],
                     outputs=[minio_dataset("bucket", "prefix")]):
        ...  # job logic

View captured events:
    python lineage/viewer.py lineage_events.ndjson
"""
from .emitter import CompositeEmitter, ConsoleEmitter, FileEmitter, LineageEmitter, default_emitter
from .models import Dataset, Job, LineageEvent, Run
from .tracker import dataset, file_dataset, kafka_dataset, lineage_run, minio_dataset

__all__ = [
    "lineage_run",
    "dataset",
    "kafka_dataset",
    "minio_dataset",
    "file_dataset",
    "default_emitter",
    "LineageEmitter",
    "FileEmitter",
    "ConsoleEmitter",
    "CompositeEmitter",
    "LineageEvent",
    "Dataset",
    "Job",
    "Run",
]
