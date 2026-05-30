from datetime import datetime
from unittest.mock import MagicMock, patch

import duckdb as real_duckdb
import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Client fixture — imported once per session to avoid redundant app setup
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client():
    from api.main import app

    return TestClient(app)


# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------


def _make_blob(name: str, size: int = 4096):
    b = MagicMock()
    b.name = name
    b.size = size
    b.last_modified = datetime(2026, 5, 29, 12, 0, 0)
    return b


def _make_blob_service(*blob_lists):
    """Returns a mock BlobServiceClient whose containers yield the given blob lists in order."""
    mock_service = MagicMock()
    mock_service.list_containers.return_value = iter([MagicMock()])
    container_clients = [MagicMock() for _ in blob_lists]
    for cc, blobs in zip(container_clients, blob_lists):
        cc.list_blobs.return_value = iter(blobs)
    mock_service.get_container_client.side_effect = container_clients
    return mock_service


# ---------------------------------------------------------------------------
# /health tests
# ---------------------------------------------------------------------------


class TestHealthEndpoint:

    def test_health_ok(self, client):
        """GET /health returns 200 and status='ok' when all services are reachable."""
        mock_service = MagicMock()
        mock_service.list_containers.return_value = iter([MagicMock()])

        with (
            patch("api.routers.health.BlobServiceClient") as mock_bsc,
            patch("api.routers.health._DUCKDB_PATH") as mock_path,
        ):
            mock_bsc.from_connection_string.return_value = mock_service
            mock_path.exists.return_value = True

            response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["services"]["blob_storage"] == "ok"
        assert data["services"]["duckdb"] == "ok"
        assert "timestamp" in data

    def test_health_degraded_when_azure_fails(self, client):
        """GET /health returns status='degraded' when Azure Blob is unreachable."""
        with (
            patch("api.routers.health.BlobServiceClient") as mock_bsc,
            patch("api.routers.health._DUCKDB_PATH") as mock_path,
        ):
            mock_bsc.from_connection_string.side_effect = Exception("Connection refused")
            mock_path.exists.return_value = True

            response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "degraded"
        assert data["services"]["blob_storage"] == "error"

    def test_health_duckdb_missing(self, client):
        """GET /health reports duckdb='missing' when the .duckdb file does not exist."""
        mock_service = MagicMock()
        mock_service.list_containers.return_value = iter([MagicMock()])

        with (
            patch("api.routers.health.BlobServiceClient") as mock_bsc,
            patch("api.routers.health._DUCKDB_PATH") as mock_path,
        ):
            mock_bsc.from_connection_string.return_value = mock_service
            mock_path.exists.return_value = False

            response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["services"]["duckdb"] == "missing"


# ---------------------------------------------------------------------------
# /stats tests
# ---------------------------------------------------------------------------


class TestStatsEndpoint:

    def test_stats_returns_three_containers(self, client):
        """GET /stats must return keys for raw, staging, and models containers."""
        mock_blob = _make_blob("some/blob.json", size=1024 * 512)
        mock_service = _make_blob_service([mock_blob], [mock_blob], [])

        with patch("api.routers.stats.BlobServiceClient") as mock_bsc:
            mock_bsc.from_connection_string.return_value = mock_service

            response = client.get("/stats")

        assert response.status_code == 200
        data = response.json()
        for key in ("raw", "staging", "models"):
            assert key in data, f"Missing container key: {key}"
        assert "generated_at" in data

    def test_stats_blob_count_matches_mock(self, client):
        """GET /stats blob counts must reflect what list_blobs returns."""
        blobs = [_make_blob(f"raw/blob_{i}.json") for i in range(3)]
        mock_service = _make_blob_service(blobs, [], [])

        with patch("api.routers.stats.BlobServiceClient") as mock_bsc:
            mock_bsc.from_connection_string.return_value = mock_service

            response = client.get("/stats")

        assert response.status_code == 200
        assert response.json()["raw"]["blobs"] == 3


# ---------------------------------------------------------------------------
# /raw tests
# ---------------------------------------------------------------------------


class TestRawEndpoint:

    def test_raw_returns_blob_list_with_count(self, client):
        """GET /raw must return count=3 when 3 blobs exist under the prefix."""
        blobs = [
            _make_blob("api/year=2026/month=05/day=29/MC_PA_20260529T120000.json"),
            _make_blob("api/year=2026/month=05/day=29/TTE_PA_20260529T120001.json"),
            _make_blob("api/year=2026/month=05/day=29/SAN_PA_20260529T120002.json"),
        ]
        mock_service = MagicMock()
        mock_service.get_container_client.return_value.list_blobs.return_value = iter(blobs)

        with patch("api.routers.raw.BlobServiceClient") as mock_bsc:
            mock_bsc.from_connection_string.return_value = mock_service

            response = client.get("/raw?prefix=api/&limit=20")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 3
        assert len(data["blobs"]) == 3
        assert data["container"] == "raw"

    def test_raw_respects_limit(self, client):
        """GET /raw must not return more blobs than the limit parameter."""
        blobs = [_make_blob(f"api/blob_{i}.json") for i in range(10)]
        mock_service = MagicMock()
        mock_service.get_container_client.return_value.list_blobs.return_value = iter(blobs)

        with patch("api.routers.raw.BlobServiceClient") as mock_bsc:
            mock_bsc.from_connection_string.return_value = mock_service

            response = client.get("/raw?limit=3")

        assert response.status_code == 200
        assert response.json()["count"] == 3


# ---------------------------------------------------------------------------
# /curated tests
# ---------------------------------------------------------------------------


class TestCuratedEndpoint:

    def test_curated_empty_when_duckdb_missing(self, client):
        """GET /curated returns count=0 when the .duckdb file does not exist."""
        with patch("api.routers.curated._DUCKDB_PATH") as mock_path:
            mock_path.exists.return_value = False

            response = client.get("/curated")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert data["data"] == []
        assert "not yet populated" in data.get("message", "")

    def test_curated_empty_when_no_table(self, client):
        """GET /curated returns count=0 with a message when the table does not exist."""
        mock_con = MagicMock()
        mock_con.__enter__ = lambda s: mock_con
        mock_con.__exit__ = MagicMock(return_value=False)
        mock_con.execute.side_effect = real_duckdb.CatalogException("Table not found")

        with (
            patch("api.routers.curated._DUCKDB_PATH") as mock_path,
            patch("api.routers.curated.duckdb") as mock_duckdb,
        ):
            mock_path.exists.return_value = True
            mock_duckdb.CatalogException = real_duckdb.CatalogException
            mock_duckdb.connect.return_value = mock_con

            response = client.get("/curated")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert "not yet populated" in data.get("message", "")


# ---------------------------------------------------------------------------
# /ingest tests
# ---------------------------------------------------------------------------


class TestIngestEndpoint:

    def test_ingest_post_returns_duration_and_ok(self, client):
        """POST /ingest must return status='ok', duration_seconds, and a report."""
        mock_report = {
            "success": [{"ticker": "MC.PA", "blob": "raw/api/year=2026/MC_PA.json"}],
            "failed": [],
            "total": 1,
        }

        with patch("api.routers.ingest.APIIngestor") as mock_cls:
            mock_cls.return_value.run.return_value = mock_report

            response = client.post("/ingest", json={"tickers": ["MC.PA"]})

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "duration_seconds" in data
        assert isinstance(data["duration_seconds"], float)
        assert "report" in data

    def test_ingest_post_without_body_uses_all_tickers(self, client):
        """POST /ingest with no body must default to all CAC 40 tickers."""
        from ingestion.api_ingestor import CAC40_TICKERS

        mock_report = {"success": [], "failed": [], "total": len(CAC40_TICKERS)}

        with patch("api.routers.ingest.APIIngestor") as mock_cls:
            mock_cls.return_value.run.return_value = mock_report

            response = client.post("/ingest")

        assert response.status_code == 200
        assert response.json()["report"]["total"] == len(CAC40_TICKERS)
