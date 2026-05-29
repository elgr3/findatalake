import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = {
    "ticker", "name", "sector", "market_cap_bn_eur",
    "per", "dividend_yield_pct", "revenue_bn_eur", "employees",
}

# Path relative to the project root (findatalake/)
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV_PATH = _PROJECT_ROOT / "data" / "cac40_fundamentals.csv"


class FileIngestor:
    def __init__(self):
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_raw = os.getenv("AZURE_CONTAINER_RAW", "raw")
        self.csv_path = DEFAULT_CSV_PATH

        if not connection_string:
            raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING is not set")

        self.blob_service = BlobServiceClient.from_connection_string(connection_string)
        logger.info("FileIngestor initialized — source: %s", self.csv_path)

    def validate_csv(self, df: pd.DataFrame) -> bool:
        """Checks required columns and absence of fully-empty rows."""
        missing = EXPECTED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"CSV is missing required columns: {missing}")

        empty_rows = df[df.isnull().all(axis=1)]
        if not empty_rows.empty:
            raise ValueError(
                f"CSV contains {len(empty_rows)} fully-empty row(s) at index: "
                f"{empty_rows.index.tolist()}"
            )

        logger.info(
            "CSV validation passed — %d rows, %d columns", len(df), len(df.columns)
        )
        return True

    def upload_to_blob(self, filepath: str) -> str:
        """Uploads the raw CSV file to Azure Blob Storage and returns the blob path."""
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        blob_path = f"files/cac40_fundamentals_{date_str}.csv"

        try:
            blob_client = self.blob_service.get_blob_client(
                container=self.container_raw, blob=blob_path
            )
            with open(filepath, "rb") as f:
                blob_client.upload_blob(f, overwrite=True)
        except AzureError as exc:
            raise RuntimeError(
                f"Failed to upload '{filepath}' to container "
                f"'{self.container_raw}/{blob_path}': {exc}"
            ) from exc

        logger.info("Uploaded %s → %s/%s", filepath, self.container_raw, blob_path)
        return blob_path

    def run(self) -> dict:
        """Loads the CSV, validates it, uploads it, and returns a status report."""
        filepath = str(self.csv_path)

        if not self.csv_path.exists():
            raise FileNotFoundError(f"Source CSV not found: {filepath}")

        logger.info("Loading CSV from %s", filepath)
        df = pd.read_csv(filepath)

        self.validate_csv(df)
        blob_path = self.upload_to_blob(filepath)

        return {
            "file": filepath,
            "rows": len(df),
            "blob": blob_path,
            "status": "success",
        }


if __name__ == "__main__":
    import json

    ingestor = FileIngestor()
    result = ingestor.run()
    print(json.dumps(result, indent=2, ensure_ascii=False))
