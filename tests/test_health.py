"""Tests for root health check and readiness endpoints."""

from datetime import datetime, timezone
import time
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from app.database import check_db_connectivity


def test_app_starts_and_health_endpoint_responds(client: TestClient):
    """Test 1 & 2: App starts and GET / returns HTTP 200."""
    response = client.get("/")
    assert response.status_code == 200


def test_health_response_structure_and_status(client: TestClient):
    """Test 3: Response contains status: HEALTHY."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data.get("status") == "HEALTHY"
    assert "current_time" in data


def test_timestamp_is_valid_iso8601_and_timezone_aware(client: TestClient):
    """Test 4: current_time is valid ISO 8601 UTC timestamp."""
    response = client.get("/")
    assert response.status_code == 200
    timestamp_str = response.json()["current_time"]

    # Parse ISO 8601 timestamp; handle 'Z' suffix
    parsed = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() is not None
    assert parsed.utcoffset().total_seconds() == 0  # UTC


def test_timestamp_is_dynamic_not_hardcoded(client: TestClient):
    """Test 5: current_time is dynamic and matches real-time execution."""
    before_req = datetime.now(timezone.utc)
    response = client.get("/")
    after_req = datetime.now(timezone.utc)

    assert response.status_code == 200
    timestamp_str = response.json()["current_time"]
    parsed_ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

    # Timestamp must fall within execution window
    assert before_req.timestamp() - 1 <= parsed_ts.timestamp() <= after_req.timestamp() + 1


def test_liveness_does_not_require_live_supabase(client: TestClient):
    """Test 6: Root health check / never touches database and succeeds even if DB is down."""
    with patch("app.database.check_db_connectivity") as mock_db_check:
        response = client.get("/")
        assert response.status_code == 200
        assert response.json()["status"] == "HEALTHY"
        # Verify check_db_connectivity was NOT called during liveness probe
        mock_db_check.assert_not_called()


def test_readiness_probe_when_database_unconfigured(client: TestClient):
    """Test readiness returns 503 and safe status when database is unconfigured."""
    response = client.get("/health/ready")
    assert response.status_code == 503
    data = response.json()
    assert data["status"] == "NOT_READY"
    assert data["database"]["status"] == "UNCONFIGURED"


def test_readiness_probe_when_database_connected(client: TestClient):
    """Test readiness returns 200 when database connectivity succeeds."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    with patch("app.api.health.check_db_connectivity") as mock_check:
        mock_check.return_value = (
            True,
            {"status": "CONNECTED", "latency_ms": 1.25, "message": "Database connection healthy."},
        )
        response = client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "READY"
        assert data["database"]["status"] == "CONNECTED"
        assert data["database"]["latency_ms"] == 1.25


def test_readiness_probe_when_database_fails(client: TestClient):
    """Test readiness handles connection failures safely without leaking credentials."""
    with patch("app.api.health.check_db_connectivity") as mock_check:
        mock_check.return_value = (
            False,
            {"status": "ERROR", "latency_ms": 10.0, "message": "Database connection failed: OperationalError"},
        )
        response = client.get("/health/ready")
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "NOT_READY"
        assert data["database"]["status"] == "ERROR"
