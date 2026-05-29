import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

logger = logging.getLogger(__name__)

default_args = {
    "owner": "findatalake",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def fetch_market_data(**context):
    from ingestion.api_ingestor import APIIngestor

    ingestor = APIIngestor()
    report = ingestor.run()
    logger.info(
        "Ingestion complete — success: %d / failed: %d / total: %d",
        len(report["success"]),
        len(report["failed"]),
        report["total"],
    )
    return report


def log_report(**context):
    ti = context["ti"]
    report = ti.xcom_pull(task_ids="fetch_market_data")

    if not report:
        logger.warning("No report received from fetch_market_data")
        return False

    success_tickers = [r["ticker"] for r in report.get("success", [])]
    failed_tickers = [r["ticker"] for r in report.get("failed", [])]

    logger.info("=== API Ingestion Report ===")
    logger.info("Total tickers  : %d", report.get("total", 0))
    logger.info("Success        : %d — %s", len(success_tickers), success_tickers)
    logger.info("Failed         : %d — %s", len(failed_tickers), failed_tickers)

    return {"success_count": len(success_tickers), "failed_count": len(failed_tickers)}


with DAG(
    dag_id="ingest_api",
    default_args=default_args,
    description="Ingestion toutes les 2h des cours boursiers CAC 40 via yfinance",
    schedule_interval="0 */2 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["findatalake", "finance"],
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_market_data",
        python_callable=fetch_market_data,
        do_xcom_push=True,
    )

    t_log = PythonOperator(
        task_id="log_report",
        python_callable=log_report,
        do_xcom_push=True,
    )

    t_fetch >> t_log
