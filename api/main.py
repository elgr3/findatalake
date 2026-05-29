import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import curated, health, ingest, raw, staging, stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="FinDataLake API",
    description="API Gateway pour le datalake financier CAC 40",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(stats.router)
app.include_router(raw.router)
app.include_router(staging.router)
app.include_router(curated.router)
app.include_router(ingest.router)


@app.on_event("startup")
async def on_startup():
    logger.info("FinDataLake API v1.0.0 started — ready to serve requests")
