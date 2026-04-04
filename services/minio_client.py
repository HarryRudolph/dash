"""MinIO blob store service helpers.

The Minio client is stored on app.state.minio at startup.
Pass it to these functions.

Update bucket names / object paths below to match your deployment.
"""

from __future__ import annotations

from typing import Any


def list_objects(minio, bucket: str, prefix: str = "") -> list[dict[str, Any]]:
    """List objects in a MinIO bucket under the given prefix."""
    if minio is None:
        return []
    try:
        objects = minio.list_objects(bucket, prefix=prefix, recursive=False)
        return [
            {
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
            }
            for obj in objects
        ]
    except Exception:
        return []


def get_object_bytes(minio, bucket: str, object_name: str) -> bytes | None:
    """Download an object from MinIO and return its bytes."""
    if minio is None:
        return None
    try:
        response = minio.get_object(bucket, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        return data
    except Exception:
        return None


def check_health(minio) -> bool:
    """Return True if MinIO is reachable."""
    if minio is None:
        return False
    try:
        minio.list_buckets()
        return True
    except Exception:
        return False
