import os
import shutil
import tempfile
from typing import Generator
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings, get_settings
from app.database import get_db, reset_database_engine
from app.main import create_app
from app.models.transaction import Base


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
def test_db_engine():
    """Provide an isolated database engine for testing with concurrent WAL support."""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test.db")
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False, "timeout": 30},
        execution_options={"schema_translate_map": {"confluencr_asgnmt": None}},
    )
    with engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL;"))
        conn.commit()

    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()
    try:
        shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception:
        pass


@pytest.fixture
def db_session(test_db_engine) -> Generator[Session, None, None]:
    """Provide an isolated database session for direct test assertions."""
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_db_engine)
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client(test_settings: Settings, test_db_engine) -> Generator[TestClient, None, None]:
    """Provide a FastAPI TestClient with per-request session isolation."""
    app = create_app(settings=test_settings)
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_db_engine)

    def override_get_db() -> Generator[Session, None, None]:
        session = TestingSessionLocal()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_settings] = lambda: test_settings
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
