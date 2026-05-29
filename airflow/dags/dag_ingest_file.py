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


def upload_fundamentals(**context):
    from ingestion.file_ingestor import FileIngestor

    ingestor = FileIngestor()
    report = ingestor.run()
    logger.info(
        "Fundamentals uploaded — file: %s, rows: %d, blob: %s",
        report["file"],
        report["rows"],
        report["blob"],
    )
    return report


def log_report(**context):
    ti = context["ti"]
    report = ti.xcom_pull(task_ids="upload_fundamentals")

    if not report:
        logger.warning("No report received from upload_fundamentals")
        return False

    logger.info("=== File Ingestion Report ===")
    logger.info("Source file : %s", report.get("file"))
    logger.info("Rows loaded : %d", report.get("rows", 0))
    logger.info("Blob path   : %s", report.get("blob"))
    logger.info("Status      : %s", report.get("status"))

    return {"rows": report.get("rows", 0), "blob": report.get("blob")}


with DAG(
    dag_id="ingest_file",
    default_args=default_args,
    description="Upload hebdomadaire du fichier de fondamentaux CAC 40 vers Azure Blob",
    schedule_interval="0 7 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["findatalake", "finance"],
) as dag:

    t_upload = PythonOperator(
        task_id="upload_fundamentals",
        python_callable=upload_fundamentals,
        do_xcom_push=True,
    )

    t_log = PythonOperator(
        task_id="log_report",
        python_callable=log_report,
        do_xcom_push=True,
    )

    t_upload >> t_log
