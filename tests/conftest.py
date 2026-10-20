"""Pytest shared fixtures and test configuration."""

from typing import Generator
import pytest
from fastapi.testclient import TestClient

from app.config import Settings, get_settings
from app.database import reset_database_engine
from app.main import create_app


@pytest.fixture(autouse=True)
def cleanup_database():
    """Ensure clean database engine state before and after each test."""
    reset_database_engine()
    yield
    reset_database_engine()


@pytest.fixture
def test_settings() -> Settings:
    """Return an isolated test Settings instance."""
    return Settings(
        APP_NAME="Transaction Webhook Service Test",
        APP_VERSION="0.1.0-test",
        APP_ENV="test",
        LOG_LEVEL="DEBUG",
        DATABASE_URL=None,
    )


@pytest.fixture
def client(test_settings: Settings) -> Generator[TestClient, None, None]:
    """Provide a FastAPI TestClient configured for testing without external dependencies."""
    app = create_app(settings=test_settings)
    with TestClient(app) as test_client:
        yield test_client
