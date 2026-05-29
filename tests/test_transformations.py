import io
import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def env_setup(monkeypatch):
    """Sets the minimum env vars required by StagingTransformer."""
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "fake_conn_str")
    monkeypatch.setenv("AZURE_CONTAINER_RAW", "raw")
    monkeypatch.setenv("AZURE_CONTAINER_STAGING", "staging")


@pytest.fixture
def market_json_payload():
    """Returns a JSON payload that matches the structure produced by api_ingestor."""
    np.random.seed(0)
    n = 30
    idx = pd.date_range("2026-05-29 09:00", periods=n, freq="1min")
    df = pd.DataFrame(
        {
            "Open": np.random.uniform(100, 200, n),
            "High": np.random.uniform(200, 250, n),
            "Low": np.random.uniform(80, 100, n),
            "Close": np.random.uniform(100, 200, n),
            "Volume": np.random.uniform(1000, 10000, n),
        },
        index=idx,
    )
    return {
        "ticker": "TTE.PA",
        "timestamp": "2026-05-29T12:00:00",
        "source": "yfinance",
        "period": "1d",
        "interval": "1m",
        "data": json.loads(df.to_json(orient="split", date_format="iso")),
    }


@pytest.fixture
def mock_transformer(env_setup):
    """Returns an instantiated StagingTransformer with a mocked BlobServiceClient."""
    with patch("transformation.spark_staging.BlobServiceClient") as mock_bsc:
        mock_instance = MagicMock()
        mock_bsc.from_connection_string.return_value = mock_instance
        from transformation.spark_staging import StagingTransformer
        transformer = StagingTransformer()
        transformer._mock_blob_service = mock_instance
        yield transformer, mock_instance


# ---------------------------------------------------------------------------
# StagingTransformer tests
# ---------------------------------------------------------------------------

class TestStagingTransformer:

    def test_init_raises_without_env(self, monkeypatch):
        """EnvironmentError must be raised when the connection string is missing."""
        monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
        from transformation.spark_staging import StagingTransformer
        with pytest.raises(EnvironmentError, match="AZURE_STORAGE_CONNECTION_STRING"):
            StagingTransformer()

    def test_list_raw_blobs_returns_list(self, mock_transformer):
        """list_raw_blobs must return a list of blob name strings."""
        transformer, mock_service = mock_transformer

        mock_blob_1 = MagicMock()
        mock_blob_1.name = "api/year=2026/month=05/day=29/MC_PA_20260529T120000.json"
        mock_blob_2 = MagicMock()
        mock_blob_2.name = "api/year=2026/month=05/day=29/TTE_PA_20260529T120001.json"

        mock_service.get_container_client.return_value.list_blobs.return_value = [
            mock_blob_1, mock_blob_2
        ]

        result = transformer.list_raw_blobs("api/year=2026/")

        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(name, str) for name in result)

    def test_transform_market_data_adds_required_columns(self, mock_transformer, market_json_payload):
        """transform_market_data must add daily_return, moving_avg_20, ticker, ingested_at."""
        transformer, mock_service = mock_transformer

        raw_bytes = json.dumps(market_json_payload).encode()
        mock_service.get_blob_client.return_value.download_blob.return_value.readall.return_value = raw_bytes

        blob_path = "api/year=2026/month=05/day=29/TTE_PA_20260529T120000.json"
        df = transformer.transform_market_data(blob_path)

        for col in ("daily_return", "moving_avg_20", "ticker", "ingested_at"):
            assert col in df.columns, f"Column '{col}' missing from transformed DataFrame"

        assert (df["ticker"] == "TTE.PA").all()
        assert len(df) > 0

    def test_transform_market_data_filters_invalid_close(self, mock_transformer, market_json_payload):
        """transform_market_data must drop rows where close is null or <= 0."""
        transformer, mock_service = mock_transformer

        # Inject a zero and a negative close value
        data_dict = market_json_payload["data"]
        data_dict["data"][0][3] = 0.0    # index 3 = Close column
        data_dict["data"][1][3] = -5.0

        raw_bytes = json.dumps(market_json_payload).encode()
        mock_service.get_blob_client.return_value.download_blob.return_value.readall.return_value = raw_bytes

        blob_path = "api/year=2026/month=05/day=29/TTE_PA_20260529T120000.json"
        df = transformer.transform_market_data(blob_path)

        assert (df["close"] > 0).all(), "Rows with close <= 0 were not filtered"

    def test_transform_fundamentals_strips_column_whitespace(self, mock_transformer):
        """transform_fundamentals must strip leading/trailing whitespace from column names."""
        transformer, mock_service = mock_transformer

        csv_content = (
            " ticker , name , sector , market_cap_bn_eur , per ,"
            " dividend_yield_pct , revenue_bn_eur , employees \n"
            "MC.PA,LVMH,Consumer Discretionary,271.3,20.4,2.0,84.7,196000\n"
            "TTE.PA,TotalEnergies,Energy,143.6,8.1,5.4,207.4,101300\n"
        ).encode()

        mock_service.get_blob_client.return_value.download_blob.return_value.readall.return_value = csv_content

        df = transformer.transform_fundamentals("files/cac40_fundamentals_2026-05-29.csv")

        assert "ticker" in df.columns, "Column 'ticker' not found after strip"
        assert "market_cap_bn_eur" in df.columns
        for col in df.columns:
            assert col == col.strip(), f"Column '{col}' still has surrounding whitespace"

    def test_upload_parquet_returns_partitioned_path(self, mock_transformer):
        """upload_parquet must return a blob path with year= / month= partitioning and .parquet ext."""
        transformer, mock_service = mock_transformer

        mock_blob_client = MagicMock()
        mock_service.get_blob_client.return_value = mock_blob_client

        df = pd.DataFrame({"ticker": ["MC.PA"], "close": [271.3], "daily_return": [0.5]})
        blob_path = transformer.upload_parquet(df, "market_data", "MC_PA_20260529T120000")

        assert "year=" in blob_path
        assert "month=" in blob_path
        assert blob_path.endswith(".parquet")
        mock_blob_client.upload_blob.assert_called_once()
