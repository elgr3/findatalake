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


def transform_raw_to_staging(**context):
    from transformation.spark_staging import StagingTransformer

    transformer = StagingTransformer()
    report = transformer.run()
    logger.info(
        "Transformation complete — market_data: %s, fundamentals: %s",
        report["market_data"],
        report["fundamentals"],
    )
    return report


def log_report(**context):
    ti = context["ti"]
    report = ti.xcom_pull(task_ids="transform_raw_to_staging")

    if not report:
        logger.warning("No report received from transform_raw_to_staging")
        return False

    md = report.get("market_data", {})
    fu = report.get("fundamentals", {})

    logger.info("=== Staging Transformation Report ===")
    logger.info(
        "Market data  — processed: %d / failed: %d",
        md.get("processed", 0),
        md.get("failed", 0),
    )
    logger.info(
        "Fundamentals — processed: %d / failed: %d",
        fu.get("processed", 0),
        fu.get("failed", 0),
    )

    total_processed = md.get("processed", 0) + fu.get("processed", 0)
    total_failed = md.get("failed", 0) + fu.get("failed", 0)
    return {"total_processed": total_processed, "total_failed": total_failed}


with DAG(
    dag_id="transform_staging",
    default_args=default_args,
    description="Transformation RAW → Staging en Parquet chaque soir de semaine",
    schedule_interval="0 20 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["findatalake", "finance"],
) as dag:

    t_transform = PythonOperator(
        task_id="transform_raw_to_staging",
        python_callable=transform_raw_to_staging,
        do_xcom_push=True,
    )

    t_log = PythonOperator(
        task_id="log_report",
        python_callable=log_report,
        do_xcom_push=True,
    )

    t_transform >> t_log
