import os
from datetime import datetime

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException

load_dotenv()

router = APIRouter()

_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
_CONTAINERS = {
    "raw": os.getenv("AZURE_CONTAINER_RAW", "raw"),
    "staging": os.getenv("AZURE_CONTAINER_STAGING", "staging"),
    "models": os.getenv("AZURE_CONTAINER_MODELS", "models"),
}


def _container_stats(blob_service: BlobServiceClient, container_name: str) -> dict:
    try:
        container_client = blob_service.get_container_client(container_name)
        total_blobs = 0
        total_bytes = 0
        for blob in container_client.list_blobs():
            total_blobs += 1
            total_bytes += blob.size or 0
        return {"blobs": total_blobs, "size_mb": round(total_bytes / (1024 * 1024), 3)}
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to inspect container '{container_name}': {exc}",
        )


@router.get("/stats")
def stats():
    try:
        blob_service = BlobServiceClient.from_connection_string(_CONNECTION_STRING)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Azure connection failed: {exc}")

    return {
        **{label: _container_stats(blob_service, name) for label, name in _CONTAINERS.items()},
        "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
    }
