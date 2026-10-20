"""Tests for configuration management and database connectivity layer."""

from unittest.mock import MagicMock
import pytest
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError

from app.config import Settings
from app.database import check_db_connectivity


def test_settings_defaults():
    """Verify sensible default configuration values."""
    settings = Settings(DATABASE_URL=None)
    assert settings.APP_NAME == "Transaction Webhook Service"
    assert settings.APP_ENV == "development"
    assert settings.LOG_LEVEL == "INFO"
    assert settings.DB_POOL_SIZE == 5
    assert settings.DATABASE_URL is None


def test_settings_invalid_environment():
    """Verify validation error on invalid APP_ENV."""
    with pytest.raises(ValidationError):
        Settings(APP_ENV="unknown_env")


def test_settings_invalid_log_level():
    """Verify validation error on invalid LOG_LEVEL."""
    with pytest.raises(ValidationError):
        Settings(LOG_LEVEL="TRACE")


def test_settings_invalid_database_url_scheme():
    """Verify validation fails if DATABASE_URL doesn't start with postgres/postgresql."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(DATABASE_URL="mysql://user:pass@localhost:3306/db")
    assert "DATABASE_URL must start with" in str(exc_info.value)


def test_settings_sanitized_database_url_masks_password():
    """Verify that credentials are never leaked in sanitized database URL."""
    raw_url = "postgresql+psycopg://postgres:super_secret_password@db.xyz.supabase.co:5432/postgres"
    settings = Settings(DATABASE_URL=raw_url)

    sanitized = settings.sanitized_database_url
    assert "super_secret_password" not in sanitized
    assert "***" in sanitized
    assert "db.xyz.supabase.co:5432/postgres" in sanitized


def test_check_db_connectivity_unconfigured():
    """Verify connectivity check returns unconfigured status when database is not configured."""
    unconfigured_settings = Settings(DATABASE_URL=None)
    connected, details = check_db_connectivity(engine=None, settings=unconfigured_settings)
    assert not connected
    assert details["status"] == "UNCONFIGURED"


def test_check_db_connectivity_success():
    """Verify connectivity check executes SELECT 1 and returns latency."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    connected, details = check_db_connectivity(engine=mock_engine)
    assert connected
    assert details["status"] == "CONNECTED"
    assert "latency_ms" in details
    mock_conn.execute.assert_called_once()


def test_check_db_connectivity_failure_masks_secrets():
    """Verify connectivity check safely catches operational errors without leaking secrets."""
    mock_engine = MagicMock()
    mock_engine.connect.side_effect = OperationalError("connection to postgres:secret failed", params=None, orig=Exception())

    connected, details = check_db_connectivity(engine=mock_engine)
    assert not connected
    assert details["status"] == "ERROR"
    assert "OperationalError" in details["message"]
    # Secret must not be in the message
    assert "secret" not in details["message"]
