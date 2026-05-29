from pathlib import Path
from typing import Optional

import duckdb
from fastapi import APIRouter, HTTPException

router = APIRouter()

_DUCKDB_PATH = Path(__file__).resolve().parents[2] / "findatalake.duckdb"


@router.get("/curated")
def get_curated(ticker: Optional[str] = None, limit: int = 100):
    if not _DUCKDB_PATH.exists():
        return {
            "count": 0,
            "data": [],
            "message": "Curated layer not yet populated",
        }

    try:
        with duckdb.connect(str(_DUCKDB_PATH), read_only=True) as con:
            if ticker:
                df = con.execute(
                    "SELECT * FROM fact_market_daily WHERE ticker = ? LIMIT ?",
                    [ticker, limit],
                ).fetchdf()
            else:
                df = con.execute(
                    "SELECT * FROM fact_market_daily LIMIT ?",
                    [limit],
                ).fetchdf()
    except duckdb.CatalogException:
        return {
            "count": 0,
            "data": [],
            "message": "Curated layer not yet populated",
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"DuckDB query failed: {exc}")

    return {"count": len(df), "data": df.to_dict(orient="records")}
