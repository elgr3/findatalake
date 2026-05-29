import os
from datetime import datetime
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from fastapi import APIRouter

load_dotenv()

router = APIRouter()

_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
_DUCKDB_PATH = Path(__file__).resolve().parents[2] / "findatalake.duckdb"


@router.get("/health")
def health():
    services = {}

    # Azure Blob Storage check
    try:
        client = BlobServiceClient.from_connection_string(_CONNECTION_STRING)
        list(client.list_containers(max_results=1))
        services["blob_storage"] = "ok"
    except Exception:
        services["blob_storage"] = "error"

    # DuckDB file check
    services["duckdb"] = "ok" if _DUCKDB_PATH.exists() else "missing"

    overall = "ok" if all(v == "ok" for v in services.values()) else "degraded"

    return {
        "status": overall,
        "services": services,
        "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
    }
