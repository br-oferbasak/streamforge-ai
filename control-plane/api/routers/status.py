from datetime import datetime, timezone
from fastapi import APIRouter
from models import Service, ServiceStatus, SystemStatus

router = APIRouter(prefix="/status", tags=["status"])

# Canonical set of services defined in deploy/cdc-flink-minio-demo/docker-compose.yml
KNOWN_SERVICES = [
    "zookeeper",
    "kafka",
    "mysql",
    "connect",
    "jobmanager",
    "taskmanager",
    "minio",
    "feature-sink",
]


def _docker_client():
    try:
        import docker
        return docker.from_env()
    except Exception:
        return None


def _resolve_service(client, name: str) -> Service:
    """Find the container for a compose service by matching label or name suffix."""
    try:
        containers = client.containers.list(all=True, filters={"label": f"com.docker.compose.service={name}"})
        if not containers:
            # Fallback: match any container whose name ends with the service name
            containers = [
                c for c in client.containers.list(all=True)
                if c.name.endswith(f"-{name}-1") or c.name == name
            ]
        if not containers:
            return Service(name=name, status=ServiceStatus.stopped)
        c = containers[0]
        state = c.status  # "running", "exited", "restarting", etc.
        status = ServiceStatus.running if state == "running" else (
            ServiceStatus.restarting if state == "restarting" else ServiceStatus.stopped
        )
        started_at_str = c.attrs.get("State", {}).get("StartedAt", "")
        started_at = None
        if started_at_str and not started_at_str.startswith("0001"):
            try:
                started_at = datetime.fromisoformat(started_at_str.replace("Z", "+00:00"))
            except ValueError:
                pass
        return Service(
            name=name,
            container_id=c.short_id,
            status=status,
            started_at=started_at,
            image=c.image.tags[0] if c.image.tags else None,
        )
    except Exception:
        return Service(name=name, status=ServiceStatus.unknown)


@router.get("", response_model=SystemStatus)
def get_status():
    client = _docker_client()
    if client is None:
        services = [Service(name=n, status=ServiceStatus.unknown) for n in KNOWN_SERVICES]
        return SystemStatus(healthy=False, services=services, checked_at=datetime.now(timezone.utc))

    services = [_resolve_service(client, name) for name in KNOWN_SERVICES]
    healthy = all(s.status == ServiceStatus.running for s in services)
    return SystemStatus(healthy=healthy, services=services, checked_at=datetime.now(timezone.utc))
