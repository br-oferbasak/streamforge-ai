import re
from fastapi import APIRouter, HTTPException, Query
from models import LogEntry, LogResponse

router = APIRouter(prefix="/logs", tags=["logs"])

_ANSI_ESCAPE = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")


def _docker_client():
    try:
        import docker
        return docker.from_env()
    except Exception:
        return None


def _find_container(client, service: str):
    containers = client.containers.list(
        all=True, filters={"label": f"com.docker.compose.service={service}"}
    )
    if not containers:
        containers = [
            c for c in client.containers.list(all=True)
            if c.name.endswith(f"-{service}-1") or c.name == service
        ]
    return containers[0] if containers else None


def _parse_line(raw: bytes) -> LogEntry:
    """Strip the 8-byte Docker multiplexed stream header if present."""
    if len(raw) > 8 and raw[0] in (0, 1, 2):
        raw = raw[8:]
    text = raw.decode("utf-8", errors="replace").rstrip("\n")
    text = _ANSI_ESCAPE.sub("", text)
    # Try to split a leading timestamp (Docker --timestamps format)
    parts = text.split(" ", 1)
    if len(parts) == 2 and "T" in parts[0]:
        return LogEntry(timestamp=parts[0], line=parts[1])
    return LogEntry(timestamp="", line=text)


@router.get("/{service}", response_model=LogResponse)
def get_logs(
    service: str,
    tail: int = Query(default=100, ge=1, le=2000, description="Number of lines to return"),
):
    client = _docker_client()
    if client is None:
        raise HTTPException(status_code=503, detail="Docker unavailable")

    container = _find_container(client, service)
    if container is None:
        raise HTTPException(status_code=404, detail=f"Service '{service}' not found")

    try:
        raw = container.logs(tail=tail, timestamps=True, stream=False)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    lines = [_parse_line(chunk) for chunk in raw.splitlines(keepends=True) if chunk.strip()]
    return LogResponse(service=service, lines=lines)
