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


def train_model(**context):
    from ml.train import AnomalyDetector

    detector = AnomalyDetector()
    report = detector.run()
    logger.info(
        "Model trained — rows: %d, model: %s, scaler: %s",
        report["trained_on_rows"],
        report["model_blob"],
        report["scaler_blob"],
    )
    return report


def score_data(**context):
    ti = context["ti"]
    train_report = ti.xcom_pull(task_ids="train_model")

    if train_report:
        logger.info(
            "Scoring with model '%s' trained on %d rows",
            train_report.get("model_blob"),
            train_report.get("trained_on_rows", 0),
        )

    from ml.predict import AnomalyScorer

    scorer = AnomalyScorer()
    result_df = scorer.run()

    anomaly_count = int(result_df["is_anomaly"].sum())
    total = len(result_df)
    anomaly_rate = round(anomaly_count / total * 100, 2) if total > 0 else 0.0

    scoring_report = {
        "total_rows": total,
        "anomaly_count": anomaly_count,
        "anomaly_rate_pct": anomaly_rate,
        "model_used": train_report.get("model_blob") if train_report else "unknown",
    }

    logger.info(
        "Scoring complete — %d anomalies detected out of %d rows (%.2f%%)",
        anomaly_count,
        total,
        anomaly_rate,
    )
    return scoring_report


def log_anomalies(**context):
    ti = context["ti"]
    scoring_report = ti.xcom_pull(task_ids="score_data")

    if not scoring_report:
        logger.warning("No scoring report received from score_data")
        return False

    logger.info("=== ML Anomaly Scoring Report ===")
    logger.info("Model used      : %s", scoring_report.get("model_used"))
    logger.info("Total rows scored: %d", scoring_report.get("total_rows", 0))
    logger.info(
        "Anomalies detected: %d (%.2f%%)",
        scoring_report.get("anomaly_count", 0),
        scoring_report.get("anomaly_rate_pct", 0.0),
    )

    if scoring_report.get("anomaly_count", 0) > 0:
        logger.warning(
            "ACTION REQUIRED: %d anomalies detected in market data — review recommended.",
            scoring_report["anomaly_count"],
        )

    return scoring_report


with DAG(
    dag_id="ml_scoring",
    default_args=default_args,
    description="Entraînement Isolation Forest et scoring des anomalies de marché",
    schedule_interval="0 21 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["findatalake", "finance"],
) as dag:

    t_train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
        do_xcom_push=True,
    )

    t_score = PythonOperator(
        task_id="score_data",
        python_callable=score_data,
        do_xcom_push=True,
    )

    t_log = PythonOperator(
        task_id="log_anomalies",
        python_callable=log_anomalies,
        do_xcom_push=True,
    )

    t_train >> t_score >> t_log
