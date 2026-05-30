import io
import json
import logging
import os
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


class StagingTransformer:
    def __init__(self):
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_raw = os.getenv("AZURE_CONTAINER_RAW", "raw")
        self.container_staging = os.getenv("AZURE_CONTAINER_STAGING", "staging")

        if not connection_string:
            raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING is not set")

        self.blob_service = BlobServiceClient.from_connection_string(connection_string)
        logger.info(
            "StagingTransformer initialized — raw: %s → staging: %s",
            self.container_raw,
            self.container_staging,
        )

    def list_raw_blobs(self, prefix: str) -> list:
        """Lists all blob names in the raw container under the given prefix."""
        container_client = self.blob_service.get_container_client(self.container_raw)
        blobs = [b.name for b in container_client.list_blobs(name_starts_with=prefix)]
        logger.info("Found %d blob(s) under raw/%s", len(blobs), prefix)
        return blobs

    def transform_market_data(self, blob_path: str) -> pd.DataFrame:
        """Downloads a raw market JSON blob and returns a cleaned, enriched DataFrame."""
        logger.info("Transforming market data: %s", blob_path)

        blob_client = self.blob_service.get_blob_client(
            container=self.container_raw, blob=blob_path
        )
        payload = json.loads(blob_client.download_blob().readall())
        ticker = payload.get("ticker") or _ticker_from_path(blob_path)

        data_dict = payload["data"]

        # Flatten column names: handles both single-level ("Open") and
        # MultiIndex ([["Open", "MC.PA"]]) produced by newer yfinance versions.
        flat_columns = [str(c[0]) if isinstance(c, list) else str(c) for c in data_dict["columns"]]
        df = pd.DataFrame(
            data=data_dict["data"],
            columns=flat_columns,
            index=pd.to_datetime(data_dict["index"]),
        )
        df.index.name = "datetime"

        # Rename columns to snake_case lowercase
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]

        # Drop rows with invalid close prices before any calculation
        df = df[df["close"].notna() & (df["close"] > 0)].copy()

        df["ticker"] = ticker
        df["daily_return"] = (df["close"] - df["open"]) / df["open"] * 100
        df["moving_avg_20"] = df["close"].rolling(window=20).mean()
        df["ingested_at"] = datetime.utcnow().isoformat(timespec="seconds")

        df = df.reset_index()  # datetime index becomes a regular column

        logger.info("Transformed %s — %d rows", ticker, len(df))
        return df

    def transform_fundamentals(self, blob_path: str) -> pd.DataFrame:
        """Downloads a raw fundamentals CSV blob and returns a cleaned DataFrame."""
        logger.info("Transforming fundamentals: %s", blob_path)

        blob_client = self.blob_service.get_blob_client(
            container=self.container_raw, blob=blob_path
        )
        raw_bytes = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(raw_bytes))

        df.columns = df.columns.str.strip()

        for col in ["market_cap_bn_eur", "per", "dividend_yield_pct", "revenue_bn_eur"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

        df["employees"] = pd.to_numeric(df["employees"], errors="coerce").astype("Int64")

        df = df.drop_duplicates(subset=["ticker"])
        df["ingested_at"] = datetime.utcnow().isoformat(timespec="seconds")

        logger.info("Transformed fundamentals — %d rows", len(df))
        return df

    def upload_parquet(self, df: pd.DataFrame, zone: str, filename: str) -> str:
        """Serializes df to Parquet in memory and uploads it to the staging container."""
        now = datetime.utcnow()
        blob_path = (
            f"{zone}/"
            f"year={now.strftime('%Y')}/"
            f"month={now.strftime('%m')}/"
            f"{filename}.parquet"
        )

        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buffer)
        buffer.seek(0)

        try:
            blob_client = self.blob_service.get_blob_client(
                container=self.container_staging, blob=blob_path
            )
            blob_client.upload_blob(buffer, overwrite=True)
        except AzureError as exc:
            raise RuntimeError(f"Failed to upload Parquet to staging/{blob_path}: {exc}") from exc

        logger.info("Uploaded Parquet → staging/%s", blob_path)
        return blob_path

    def run(self) -> dict:
        """Orchestrates raw → staging transformation and returns a summary report."""
        now = datetime.utcnow()
        today_prefix = (
            f"api/"
            f"year={now.strftime('%Y')}/"
            f"month={now.strftime('%m')}/"
            f"day={now.strftime('%d')}/"
        )

        report = {
            "market_data": {"processed": 0, "failed": 0},
            "fundamentals": {"processed": 0, "failed": 0},
        }

        for blob_path in self.list_raw_blobs(today_prefix):
            try:
                df = self.transform_market_data(blob_path)
                self.upload_parquet(df, "market_data", _stem(blob_path))
                report["market_data"]["processed"] += 1
            except Exception as exc:
                logger.error("Failed market blob %s — %s", blob_path, exc)
                report["market_data"]["failed"] += 1

        for blob_path in self.list_raw_blobs("files/"):
            try:
                df = self.transform_fundamentals(blob_path)
                self.upload_parquet(df, "fundamentals", _stem(blob_path))
                report["fundamentals"]["processed"] += 1
            except Exception as exc:
                logger.error("Failed fundamentals blob %s — %s", blob_path, exc)
                report["fundamentals"]["failed"] += 1

        logger.info("Transformation complete — %s", report)
        return report


def _ticker_from_path(blob_path: str) -> str:
    """Fallback: extracts ticker from a path like .../MC_PA_20260529T143000.json.

    The safe_ticker format used by api_ingestor replaces '.' with '_', so
    MC.PA → MC_PA and STLAM.MI → STLAM_MI. The timestamp is always the last
    underscore-separated token; the exchange code is the second-to-last.
    """
    stem = blob_path.split("/")[-1].rsplit(".", 1)[0]
    parts = stem.split("_")
    if len(parts) >= 3:
        # parts[-1] = timestamp, parts[-2] = exchange, parts[:-2] = name
        exchange = parts[-2]
        name = "_".join(parts[:-2])
        return f"{name}.{exchange}"
    return stem


def _stem(blob_path: str) -> str:
    """Returns the filename without extension from a blob path."""
    return blob_path.split("/")[-1].rsplit(".", 1)[0]


if __name__ == "__main__":
    import json as _json

    transformer = StagingTransformer()
    result = transformer.run()
    print(_json.dumps(result, indent=2, ensure_ascii=False))
