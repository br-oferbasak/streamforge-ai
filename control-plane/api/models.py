from __future__ import annotations
from datetime import datetime
from enum import Enum
from pydantic import BaseModel


class ServiceStatus(str, Enum):
    running = "running"
    stopped = "stopped"
    restarting = "restarting"
    unknown = "unknown"


class Service(BaseModel):
    name: str
    container_id: str | None = None
    status: ServiceStatus
    started_at: datetime | None = None
    image: str | None = None


class SystemStatus(BaseModel):
    healthy: bool
    services: list[Service]
    checked_at: datetime


class LogEntry(BaseModel):
    timestamp: str
    line: str


class LogResponse(BaseModel):
    service: str
    lines: list[LogEntry]


class Artifact(BaseModel):
    key: str
    bucket: str
    size_bytes: int
    last_modified: datetime
    etag: str | None = None


class ArtifactResponse(BaseModel):
    bucket: str
    artifacts: list[Artifact]
    total: int
