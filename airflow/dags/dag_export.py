import logging
import os
import sys
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

logger = logging.getLogger(__name__)

_API_BASE_URL = os.getenv("FINDATALAKE_API_URL", "http://localhost:8000")

default_args = {
    "owner": "findatalake",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def generate_stats(**context):
    url = f"{_API_BASE_URL}/stats"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        stats = response.json()
        logger.info("Stats fetched from %s — %s", url, stats)
        return stats
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch stats from {url}: {exc}") from exc


def log_weekly_report(**context):
    ti = context["ti"]
    stats = ti.xcom_pull(task_ids="generate_stats")

    if not stats:
        logger.warning("No stats received from generate_stats")
        return False

    raw = stats.get("raw", {})
    staging = stats.get("staging", {})
    models = stats.get("models", {})
    generated_at = stats.get("generated_at", "N/A")

    logger.info("============================================")
    logger.info("  FinDataLake — Weekly Report")
    logger.info("  Generated at : %s", generated_at)
    logger.info("--------------------------------------------")
    logger.info(
        "  RAW      — %4d blobs, %8.3f MB",
        raw.get("blobs", 0),
        raw.get("size_mb", 0.0),
    )
    logger.info(
        "  STAGING  — %4d blobs, %8.3f MB",
        staging.get("blobs", 0),
        staging.get("size_mb", 0.0),
    )
    logger.info(
        "  MODELS   — %4d blobs, %8.3f MB",
        models.get("blobs", 0),
        models.get("size_mb", 0.0),
    )
    total_blobs = raw.get("blobs", 0) + staging.get("blobs", 0) + models.get("blobs", 0)
    total_mb = raw.get("size_mb", 0.0) + staging.get("size_mb", 0.0) + models.get("size_mb", 0.0)
    logger.info("--------------------------------------------")
    logger.info("  TOTAL    — %4d blobs, %8.3f MB", total_blobs, total_mb)
    logger.info("============================================")

    return {
        "total_blobs": total_blobs,
        "total_size_mb": round(total_mb, 3),
        "generated_at": generated_at,
    }


with DAG(
    dag_id="export_report",
    default_args=default_args,
    description="Rapport hebdomadaire des statistiques du datalake via l'API",
    schedule_interval="0 7 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["findatalake", "finance"],
) as dag:

    t_stats = PythonOperator(
        task_id="generate_stats",
        python_callable=generate_stats,
        do_xcom_push=True,
    )

    t_report = PythonOperator(
        task_id="log_weekly_report",
        python_callable=log_weekly_report,
        do_xcom_push=True,
    )

    t_stats >> t_report
