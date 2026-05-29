import io
import logging
import os

import joblib
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

FEATURES = ["close", "volume", "daily_return", "moving_avg_20"]


class AnomalyScorer:
    def __init__(self):
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_staging = os.getenv("AZURE_CONTAINER_STAGING", "staging")
        self.container_models = os.getenv("AZURE_CONTAINER_MODELS", "models")

        if not connection_string:
            raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING is not set")

        self.blob_service = BlobServiceClient.from_connection_string(connection_string)
        logger.info("AnomalyScorer initialized")

    def load_latest_model(self) -> tuple[IsolationForest, StandardScaler]:
        """Loads the most recent IsolationForest and scaler from the models container.

        Blobs are sorted in descending alphabetical order; since filenames embed the
        date as YYYY-MM-DD, this gives the most recently trained artifacts first.
        """
        container_client = self.blob_service.get_container_client(self.container_models)
        all_blobs = [b.name for b in container_client.list_blobs()]

        model_blobs = sorted(
            [b for b in all_blobs if b.startswith("isolation_forest_")],
            reverse=True,
        )
        scaler_blobs = sorted(
            [b for b in all_blobs if b.startswith("scaler_")],
            reverse=True,
        )

        if not model_blobs or not scaler_blobs:
            raise FileNotFoundError(
                "No trained model found in models container — run ml/train.py first."
            )

        def _load(blob_name: str):
            raw_bytes = (
                self.blob_service.get_blob_client(
                    container=self.container_models, blob=blob_name
                )
                .download_blob()
                .readall()
            )
            return joblib.load(io.BytesIO(raw_bytes))

        model: IsolationForest = _load(model_blobs[0])
        scaler: StandardScaler = _load(scaler_blobs[0])

        logger.info("Loaded model: %s | scaler: %s", model_blobs[0], scaler_blobs[0])
        return model, scaler

    def score_dataframe(
        self,
        df: pd.DataFrame,
        model: IsolationForest,
        scaler: StandardScaler,
    ) -> pd.DataFrame:
        """Applies the scaler and model; adds anomaly_score (-1/1) and is_anomaly columns."""
        missing = [f for f in FEATURES if f not in df.columns]
        if missing:
            raise ValueError(f"DataFrame missing required feature columns: {missing}")

        df = df.copy()

        # Impute with column median (mirrors the training strategy without requiring
        # the original imputer artifact, keeping the models/ container lightweight).
        X = df[FEATURES].fillna(df[FEATURES].median(numeric_only=True))
        X_scaled = scaler.transform(X)

        df["anomaly_score"] = model.predict(X_scaled)
        df["is_anomaly"] = df["anomaly_score"] == -1

        anomaly_count = int(df["is_anomaly"].sum())
        logger.info(
            "Scored %d rows — anomalies: %d (%.1f%%)",
            len(df),
            anomaly_count,
            anomaly_count / len(df) * 100 if len(df) > 0 else 0.0,
        )
        return df

    def _load_staging_data(self) -> pd.DataFrame:
        """Downloads all Parquet blobs from staging/market_data/ and concatenates them."""
        container_client = self.blob_service.get_container_client(self.container_staging)
        blobs = [
            b.name
            for b in container_client.list_blobs(name_starts_with="market_data/")
        ]

        if not blobs:
            raise FileNotFoundError("No data found under staging/market_data/")

        frames = []
        for blob_path in blobs:
            try:
                raw_bytes = (
                    self.blob_service.get_blob_client(
                        container=self.container_staging, blob=blob_path
                    )
                    .download_blob()
                    .readall()
                )
                frames.append(pd.read_parquet(io.BytesIO(raw_bytes)))
            except Exception as exc:
                logger.warning("Skipping blob %s — %s", blob_path, exc)

        df = pd.concat(frames, ignore_index=True)
        logger.info("Loaded %d rows from staging for scoring", len(df))
        return df

    def run(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """Loads the latest model, scores the data, and returns the enriched DataFrame."""
        model, scaler = self.load_latest_model()

        if df is None:
            df = self._load_staging_data()

        return self.score_dataframe(df, model, scaler)


if __name__ == "__main__":
    scorer = AnomalyScorer()
    result_df = scorer.run()

    anomalies = result_df[result_df["is_anomaly"]]
    print(f"Total rows scored : {len(result_df)}")
    print(f"Anomalies detected: {len(anomalies)}")

    if not anomalies.empty:
        display_cols = ["datetime", "ticker", "close", "daily_return", "anomaly_score"]
        available = [c for c in display_cols if c in anomalies.columns]
        print(anomalies[available].to_string(index=False))
