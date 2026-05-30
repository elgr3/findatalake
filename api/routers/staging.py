import os

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException

load_dotenv()

router = APIRouter()

_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
_CONTAINER_STAGING = os.getenv("AZURE_CONTAINER_STAGING", "staging")


@router.get("/staging")
def list_staging(zone: str = "market_data", limit: int = 20):
    try:
        blob_service = BlobServiceClient.from_connection_string(_CONNECTION_STRING)
        container_client = blob_service.get_container_client(_CONTAINER_STAGING)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Azure connection failed: {exc}")

    prefix = f"{zone}/"

    try:
        blobs = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            if len(blobs) >= limit:
                break
            blobs.append(
                {
                    "name": blob.name,
                    "size_bytes": blob.size or 0,
                    "last_modified": blob.last_modified.isoformat() if blob.last_modified else None,
                }
            )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list blobs: {exc}")

    return {
        "container": _CONTAINER_STAGING,
        "prefix": prefix,
        "count": len(blobs),
        "blobs": blobs,
    }
