import json
import logging
import os
from datetime import datetime

import yfinance as yf
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

CAC40_TICKERS = [
    "AI.PA", "AIR.PA", "ALO.PA", "MT.AS", "CS.PA",
    "BNP.PA", "EN.PA", "CAP.PA", "CA.PA", "ACA.PA",
    "BN.PA", "DSY.PA", "ENGI.PA", "EL.PA", "ERF.PA",
    "RMS.PA", "KER.PA", "LR.PA", "OR.PA", "MC.PA",
    "ML.PA", "ORA.PA", "RI.PA", "PUB.PA", "RNO.PA",
    "SAF.PA", "SGO.PA", "SAN.PA", "SU.PA", "GLE.PA",
    "STLAM.MI", "STM.MI", "TEP.PA", "HO.PA", "TTE.PA",
    "URW.PA", "VIE.PA", "DG.PA", "VIV.PA", "WLN.PA",
]


class APIIngestor:
    def __init__(self):
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_raw = os.getenv("AZURE_CONTAINER_RAW", "raw")

        if not connection_string:
            raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING is not set")

        self.blob_service = BlobServiceClient.from_connection_string(connection_string)
        logger.info("APIIngestor initialized — container: %s", self.container_raw)

    def fetch_ticker_data(self, ticker: str) -> dict:
        """Fetches 1-day 1-minute interval OHLCV data from yfinance for a given ticker."""
        logger.info("Fetching data for %s", ticker)
        df = yf.download(ticker, period="1d", interval="1m", progress=False, auto_adjust=True)

        if df.empty:
            raise ValueError(f"No data returned by yfinance for ticker '{ticker}'")

        return {
            "ticker": ticker,
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "source": "yfinance",
            "period": "1d",
            "interval": "1m",
            "data": json.loads(df.to_json(orient="split", date_format="iso")),
        }

    def upload_to_blob(self, data: dict, ticker: str) -> str:
        """Uploads a JSON payload to Azure Blob Storage and returns the blob path."""
        now = datetime.utcnow()
        safe_ticker = ticker.replace(".", "_")
        timestamp_str = now.strftime("%Y%m%dT%H%M%S")

        blob_path = (
            f"api/"
            f"year={now.strftime('%Y')}/"
            f"month={now.strftime('%m')}/"
            f"day={now.strftime('%d')}/"
            f"{safe_ticker}_{timestamp_str}.json"
        )

        payload = json.dumps(data, ensure_ascii=False, default=str)

        try:
            blob_client = self.blob_service.get_blob_client(
                container=self.container_raw, blob=blob_path
            )
            blob_client.upload_blob(payload, overwrite=True)
        except AzureError as exc:
            raise RuntimeError(
                f"Failed to upload blob '{blob_path}' to container '{self.container_raw}': {exc}"
            ) from exc

        logger.info("Uploaded %s → %s/%s", ticker, self.container_raw, blob_path)
        return blob_path

    def run(self, tickers: list = None) -> dict:
        """Ingests all tickers and returns a summary report."""
        tickers = tickers or CAC40_TICKERS
        report = {"success": [], "failed": [], "total": len(tickers)}

        for ticker in tickers:
            try:
                data = self.fetch_ticker_data(ticker)
                blob_path = self.upload_to_blob(data, ticker)
                report["success"].append({"ticker": ticker, "blob": blob_path})
            except (ValueError, RuntimeError) as exc:
                logger.error("Skipping %s — %s", ticker, exc)
                report["failed"].append({"ticker": ticker, "error": str(exc)})

        logger.info(
            "Ingestion complete — success: %d / failed: %d / total: %d",
            len(report["success"]),
            len(report["failed"]),
            report["total"],
        )
        return report


if __name__ == "__main__":
    ingestor = APIIngestor()
    result = ingestor.run()
    print(json.dumps(result, indent=2, ensure_ascii=False))
