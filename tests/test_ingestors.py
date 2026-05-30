import json
import os
from datetime import datetime
from unittest.mock import MagicMock, patch, call

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def env_setup(monkeypatch):
    """Sets the minimum env vars required by APIIngestor and FileIngestor."""
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "fake_conn_str")
    monkeypatch.setenv("AZURE_CONTAINER_RAW", "raw")


@pytest.fixture
def sample_ohlcv_df():
    """Returns a 30-row OHLCV DataFrame that mimics yfinance output."""
    np.random.seed(42)
    idx = pd.date_range("2026-05-29 09:00", periods=30, freq="1min")
    return pd.DataFrame(
        {
            "Open": np.random.uniform(100, 200, 30),
            "High": np.random.uniform(200, 250, 30),
            "Low": np.random.uniform(80, 100, 30),
            "Close": np.random.uniform(100, 200, 30),
            "Volume": np.random.randint(1000, 10000, 30).astype(float),
        },
        index=idx,
    )


@pytest.fixture
def sample_fundamentals_df():
    """Returns a valid fundamentals DataFrame with all required columns."""
    return pd.DataFrame(
        {
            "ticker": ["MC.PA", "TTE.PA"],
            "name": ["LVMH", "TotalEnergies"],
            "sector": ["Consumer Discretionary", "Energy"],
            "market_cap_bn_eur": [271.3, 143.6],
            "per": [20.4, 8.1],
            "dividend_yield_pct": [2.0, 5.4],
            "revenue_bn_eur": [84.7, 207.4],
            "employees": [196000, 101300],
        }
    )


# ---------------------------------------------------------------------------
# APIIngestor tests
# ---------------------------------------------------------------------------


class TestAPIIngestor:

    def test_init_raises_without_env(self, monkeypatch):
        """EnvironmentError must be raised when the connection string is missing."""
        monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
        from ingestion.api_ingestor import APIIngestor

        with pytest.raises(EnvironmentError, match="AZURE_STORAGE_CONNECTION_STRING"):
            APIIngestor()

    def test_fetch_ticker_data_returns_dict(self, env_setup, sample_ohlcv_df):
        """fetch_ticker_data must return a dict with the required metadata keys."""
        with (
            patch("ingestion.api_ingestor.BlobServiceClient"),
            patch("ingestion.api_ingestor.yf.download", return_value=sample_ohlcv_df),
        ):
            from ingestion.api_ingestor import APIIngestor

            ingestor = APIIngestor()
            result = ingestor.fetch_ticker_data("MC.PA")

        assert isinstance(result, dict)
        for key in ("ticker", "timestamp", "source", "data"):
            assert key in result, f"Missing key: {key}"
        assert result["ticker"] == "MC.PA"
        assert result["source"] == "yfinance"

    def test_fetch_ticker_data_raises_on_empty_df(self, env_setup):
        """fetch_ticker_data must raise ValueError when yfinance returns no data."""
        with (
            patch("ingestion.api_ingestor.BlobServiceClient"),
            patch("ingestion.api_ingestor.yf.download", return_value=pd.DataFrame()),
        ):
            from ingestion.api_ingestor import APIIngestor

            ingestor = APIIngestor()
            with pytest.raises(ValueError, match="No data returned"):
                ingestor.fetch_ticker_data("FAKE.PA")

    def test_upload_to_blob_returns_partitioned_path(self, env_setup):
        """upload_to_blob must return a path with Hive-style partitioning and .json extension."""
        with patch("ingestion.api_ingestor.BlobServiceClient") as mock_bsc:
            mock_blob_client = MagicMock()
            mock_bsc.from_connection_string.return_value.get_blob_client.return_value = (
                mock_blob_client
            )

            from ingestion.api_ingestor import APIIngestor

            ingestor = APIIngestor()
            payload = {
                "ticker": "MC.PA",
                "timestamp": "2026-05-29T12:00:00",
                "source": "yfinance",
                "period": "1d",
                "interval": "1m",
                "data": {},
            }
            path = ingestor.upload_to_blob(payload, "MC.PA")

        assert "year=" in path
        assert "month=" in path
        assert "day=" in path
        assert path.endswith(".json")
        mock_blob_client.upload_blob.assert_called_once()

    def test_run_report_has_correct_structure(self, env_setup, sample_ohlcv_df):
        """run() must return a dict with success, failed, and total keys."""
        with patch("ingestion.api_ingestor.BlobServiceClient"):
            from ingestion.api_ingestor import APIIngestor

            ingestor = APIIngestor()

        with (
            patch.object(
                ingestor, "fetch_ticker_data", return_value={"ticker": "MC.PA", "data": {}}
            ),
            patch.object(ingestor, "upload_to_blob", return_value="raw/api/year=2026/MC_PA.json"),
        ):
            report = ingestor.run(["MC.PA", "TTE.PA"])

        assert "success" in report
        assert "failed" in report
        assert "total" in report
        assert report["total"] == 2

    def test_run_continues_on_single_ticker_failure(self, env_setup):
        """run() must skip failed tickers and continue processing the rest."""
        with patch("ingestion.api_ingestor.BlobServiceClient"):
            from ingestion.api_ingestor import APIIngestor

            ingestor = APIIngestor()

        def mock_fetch(ticker):
            if ticker == "FAIL.PA":
                raise ValueError("Simulated yfinance error")
            return {"ticker": ticker, "data": {}}

        with (
            patch.object(ingestor, "fetch_ticker_data", side_effect=mock_fetch),
            patch.object(ingestor, "upload_to_blob", return_value="raw/api/..."),
        ):
            report = ingestor.run(["FAIL.PA", "MC.PA", "TTE.PA"])

        assert len(report["failed"]) == 1
        assert report["failed"][0]["ticker"] == "FAIL.PA"
        assert len(report["success"]) == 2
        assert report["total"] == 3


# ---------------------------------------------------------------------------
# FileIngestor tests
# ---------------------------------------------------------------------------


class TestFileIngestor:

    def test_validate_csv_passes_with_correct_columns(self, env_setup, sample_fundamentals_df):
        """validate_csv must return True when all required columns are present."""
        with patch("ingestion.file_ingestor.BlobServiceClient"):
            from ingestion.file_ingestor import FileIngestor

            ingestor = FileIngestor()

        result = ingestor.validate_csv(sample_fundamentals_df)
        assert result is True

    def test_validate_csv_raises_on_missing_column(self, env_setup, sample_fundamentals_df):
        """validate_csv must raise ValueError when a required column is absent."""
        with patch("ingestion.file_ingestor.BlobServiceClient"):
            from ingestion.file_ingestor import FileIngestor

            ingestor = FileIngestor()

        df_missing = sample_fundamentals_df.drop(columns=["per"])
        with pytest.raises(ValueError, match="per"):
            ingestor.validate_csv(df_missing)

    def test_validate_csv_raises_on_empty_row(self, env_setup, sample_fundamentals_df):
        """validate_csv must raise ValueError when a fully-empty row is present."""
        with patch("ingestion.file_ingestor.BlobServiceClient"):
            from ingestion.file_ingestor import FileIngestor

            ingestor = FileIngestor()

        empty_row = pd.DataFrame(
            [[None] * len(sample_fundamentals_df.columns)], columns=sample_fundamentals_df.columns
        )
        df_with_empty = pd.concat([sample_fundamentals_df, empty_row], ignore_index=True)
        with pytest.raises(ValueError, match="fully-empty"):
            ingestor.validate_csv(df_with_empty)

    def test_run_raises_if_csv_missing(self, env_setup):
        """run() must raise FileNotFoundError when the source CSV does not exist."""
        with (
            patch("ingestion.file_ingestor.BlobServiceClient"),
            patch("ingestion.file_ingestor.DEFAULT_CSV_PATH") as mock_path,
        ):
            mock_path.exists.return_value = False

            from ingestion.file_ingestor import FileIngestor

            ingestor = FileIngestor()
            ingestor.csv_path = mock_path  # point ingestor to the mock path

            with pytest.raises(FileNotFoundError):
                ingestor.run()

    def test_upload_to_blob_returns_csv_path(self, env_setup, tmp_path):
        """upload_to_blob must return a path ending with .csv."""
        with patch("ingestion.file_ingestor.BlobServiceClient") as mock_bsc:
            mock_blob_client = MagicMock()
            mock_bsc.from_connection_string.return_value.get_blob_client.return_value = (
                mock_blob_client
            )

            from ingestion.file_ingestor import FileIngestor

            ingestor = FileIngestor()

            # Write a temporary CSV file to avoid FileNotFoundError on open()
            tmp_csv = tmp_path / "test.csv"
            tmp_csv.write_text("ticker,name\nMC.PA,LVMH\n")

            blob_path = ingestor.upload_to_blob(str(tmp_csv))

        assert blob_path.endswith(".csv")
        assert "cac40_fundamentals" in blob_path
        mock_blob_client.upload_blob.assert_called_once()
