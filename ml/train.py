import io
import logging
import os
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

FEATURES = ["close", "volume", "daily_return", "moving_avg_20"]


class AnomalyDetector:
    def __init__(self):
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_staging = os.getenv("AZURE_CONTAINER_STAGING", "staging")
        self.container_models = os.getenv("AZURE_CONTAINER_MODELS", "models")

        if not connection_string:
            raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING is not set")

        self.blob_service = BlobServiceClient.from_connection_string(connection_string)
        logger.info("AnomalyDetector initialized")

    def load_staging_data(self) -> pd.DataFrame:
        """Downloads all Parquet blobs from staging/market_data/ and concatenates them."""
        container_client = self.blob_service.get_container_client(self.container_staging)
        blobs = [b.name for b in container_client.list_blobs(name_starts_with="market_data/")]

        if not blobs:
            raise FileNotFoundError("No Parquet blobs found under staging/market_data/")

        logger.info("Loading %d Parquet blob(s) from staging", len(blobs))
        frames = []
        for blob_path in blobs:
            try:
                blob_client = self.blob_service.get_blob_client(
                    container=self.container_staging, blob=blob_path
                )
                raw_bytes = blob_client.download_blob().readall()
                frames.append(pd.read_parquet(io.BytesIO(raw_bytes)))
            except Exception as exc:
                logger.warning("Skipping blob %s — %s", blob_path, exc)

        if not frames:
            raise RuntimeError("All staging Parquet blobs failed to load")

        df = pd.concat(frames, ignore_index=True)
        logger.info("Loaded %d total rows from staging", len(df))
        return df

    def prepare_features(self, df: pd.DataFrame) -> tuple[np.ndarray, StandardScaler]:
        """Selects features, imputes NaN with median, and normalizes with StandardScaler.

        Returns (X_scaled, scaler) so the scaler can be persisted alongside the model.
        """
        X_raw = df[FEATURES].copy()
        nan_count = int(X_raw.isnull().sum().sum())

        imputer = SimpleImputer(strategy="median")
        X_imputed = imputer.fit_transform(X_raw)

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_imputed)

        logger.info(
            "Features prepared — shape: %s, NaN imputed: %d",
            X_scaled.shape,
            nan_count,
        )
        return X_scaled, scaler

    def train(self, X: np.ndarray) -> IsolationForest:
        """Trains an IsolationForest on the feature array."""
        logger.info("Training IsolationForest on %d samples", len(X))
        model = IsolationForest(
            contamination=0.05,
            random_state=42,
            n_estimators=100,
        )
        model.fit(X)
        logger.info("Training complete")
        return model

    def save_model(self, model: IsolationForest, scaler: StandardScaler) -> dict:
        """Serializes model and scaler with joblib and uploads both to the models container.

        Returns {"model_blob": str, "scaler_blob": str}.
        """
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        artifacts = {
            f"isolation_forest_{date_str}.pkl": model,
            f"scaler_{date_str}.pkl": scaler,
        }
        paths = {}

        for blob_name, artifact in artifacts.items():
            buffer = io.BytesIO()
            joblib.dump(artifact, buffer)
            buffer.seek(0)

            try:
                blob_client = self.blob_service.get_blob_client(
                    container=self.container_models, blob=blob_name
                )
                blob_client.upload_blob(buffer, overwrite=True)
                logger.info("Uploaded %s → models/%s", type(artifact).__name__, blob_name)
            except AzureError as exc:
                raise RuntimeError(
                    f"Failed to upload '{blob_name}' to models container: {exc}"
                ) from exc

            paths[blob_name] = blob_name

        return {
            "model_blob": f"isolation_forest_{date_str}.pkl",
            "scaler_blob": f"scaler_{date_str}.pkl",
        }

    def run(self) -> dict:
        """Orchestrates load → prepare → train → save and returns a summary report."""
        df = self.load_staging_data()
        X, scaler = self.prepare_features(df)
        model = self.train(X)
        blob_paths = self.save_model(model, scaler)

        return {
            "model_blob": blob_paths["model_blob"],
            "scaler_blob": blob_paths["scaler_blob"],
            "trained_on_rows": int(len(X)),
            "features": FEATURES,
        }


if __name__ == "__main__":
    import json

    detector = AnomalyDetector()
    result = detector.run()
    print(json.dumps(result, indent=2, ensure_ascii=False))
