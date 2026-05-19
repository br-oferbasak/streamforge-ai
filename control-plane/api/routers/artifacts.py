import os
from datetime import timezone
from fastapi import APIRouter, HTTPException, Query
from models import Artifact, ArtifactResponse

router = APIRouter(prefix="/artifacts", tags=["artifacts"])

_DEFAULT_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
_DEFAULT_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
_DEFAULT_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
_DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "processed")


def _s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=_DEFAULT_ENDPOINT,
        aws_access_key_id=_DEFAULT_ACCESS,
        aws_secret_access_key=_DEFAULT_SECRET,
        config=Config(signature_version="s3v4"),
    )


@router.get("", response_model=ArtifactResponse)
def list_artifacts(
    bucket: str = Query(default=_DEFAULT_BUCKET),
    prefix: str = Query(default=""),
    limit: int = Query(default=50, ge=1, le=500),
):
    try:
        s3 = _s3_client()
    except ImportError:
        raise HTTPException(status_code=503, detail="boto3 not installed")

    try:
        paginator = s3.get_paginator("list_objects_v2")
        items: list[Artifact] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"MaxItems": limit}):
            for obj in page.get("Contents", []):
                last_mod = obj["LastModified"]
                if last_mod.tzinfo is None:
                    last_mod = last_mod.replace(tzinfo=timezone.utc)
                items.append(
                    Artifact(
                        key=obj["Key"],
                        bucket=bucket,
                        size_bytes=obj["Size"],
                        last_modified=last_mod,
                        etag=obj.get("ETag", "").strip('"'),
                    )
                )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"MinIO error: {exc}")

    items.sort(key=lambda a: a.last_modified, reverse=True)
    return ArtifactResponse(bucket=bucket, artifacts=items[:limit], total=len(items))
