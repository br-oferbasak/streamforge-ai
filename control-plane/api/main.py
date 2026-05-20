from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import artifacts, logs, status

app = FastAPI(
    title="StreamForge Control Plane",
    description="Read-only status, logs, and artifact browser for StreamForge AI pipelines.",
    version="0.5.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(status.router, prefix="/api/v1")
app.include_router(logs.router, prefix="/api/v1")
app.include_router(artifacts.router, prefix="/api/v1")


@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"ok": True}
