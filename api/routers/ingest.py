import asyncio
import time
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ingestion.api_ingestor import APIIngestor, CAC40_TICKERS

router = APIRouter()


class IngestBody(BaseModel):
    tickers: Optional[list[str]] = None


@router.post("/ingest")
def ingest(body: IngestBody = None):
    """Ingests market data for the given tickers sequentially."""
    tickers = (body.tickers if body and body.tickers else None) or CAC40_TICKERS

    try:
        ingestor = APIIngestor()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ingestor init failed: {exc}")

    t0 = time.perf_counter()
    report = ingestor.run(tickers)
    duration = round(time.perf_counter() - t0, 3)

    return {"status": "ok", "duration_seconds": duration, "report": report}


@router.post("/ingest_fast")
async def ingest_fast(body: IngestBody = None):
    """Ingests market data for all tickers concurrently using a thread-pool executor.

    yfinance is a blocking library, so each ticker is dispatched to asyncio's default
    ThreadPoolExecutor via run_in_executor. asyncio.gather() then runs all tasks in
    parallel, reducing wall-clock time by ~N× compared to the sequential /ingest endpoint.
    """
    tickers = (body.tickers if body and body.tickers else None) or CAC40_TICKERS

    try:
        ingestor = APIIngestor()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ingestor init failed: {exc}")

    loop = asyncio.get_running_loop()

    async def _process(ticker: str) -> dict:
        try:
            data = await loop.run_in_executor(
                None, ingestor.fetch_ticker_data, ticker
            )
            blob_path = await loop.run_in_executor(
                None, ingestor.upload_to_blob, data, ticker
            )
            return {"ticker": ticker, "blob": blob_path}
        except Exception as exc:
            return {"ticker": ticker, "error": str(exc)}

    t0 = time.perf_counter()
    results = await asyncio.gather(*[_process(t) for t in tickers])
    duration = round(time.perf_counter() - t0, 3)

    success = [r for r in results if "blob" in r]
    failed = [r for r in results if "error" in r]

    return {
        "status": "ok",
        "mode": "async_parallel",
        "duration_seconds": duration,
        "report": {
            "success": success,
            "failed": failed,
            "total": len(tickers),
        },
    }
