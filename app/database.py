"""Database connectivity, engine abstraction, and session management for Supabase PostgreSQL."""

from contextlib import contextmanager
import time
from typing import Any, Dict, Generator, Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings, get_settings
from app.logging_config import get_logger

logger = get_logger("database")

# Module-level holders initialized lazily or during application startup
_engine: Optional[Engine] = None
_SessionFactory: Optional[sessionmaker] = None


def get_engine(settings: Optional[Settings] = None) -> Optional[Engine]:
    """Retrieve or lazily initialize the SQLAlchemy engine.

    Never performs network connection or pings at initialization time.
    """
    global _engine, _SessionFactory
    if _engine is not None:
        return _engine

    cfg = settings or get_settings()
    raw_url = cfg.get_raw_database_url()
    if not raw_url:
        logger.warning("DATABASE_URL is not configured; database engine cannot be initialized.")
        return None

    logger.info("Initializing database engine with pool (url: %s)", cfg.sanitized_database_url)

    # In Supabase (especially transaction pooler on port 6543), prepared statements should be disabled
    connect_args: Dict[str, Any] = {}
    if "6543" in raw_url or "pooler.supabase.com" in raw_url:
        # psycopg 3 prepare_threshold=None disables server-side prepared statements
        connect_args["prepare_threshold"] = None

    _engine = create_engine(
        raw_url,
        pool_size=cfg.DB_POOL_SIZE,
        max_overflow=cfg.DB_MAX_OVERFLOW,
        pool_timeout=cfg.DB_POOL_TIMEOUT,
        pool_recycle=cfg.DB_POOL_RECYCLE,
        pool_pre_ping=True,  # Validates connection health before issuing queries
        connect_args=connect_args,
    )
    _SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    return _engine


def get_session_factory(settings: Optional[Settings] = None) -> Optional[sessionmaker]:
    """Retrieve or initialize the SQLAlchemy sessionmaker."""
    global _SessionFactory
    if _SessionFactory is None:
        get_engine(settings)
    return _SessionFactory


def reset_database_engine() -> None:
    """Dispose current engine and reset state (primarily used in testing)."""
    global _engine, _SessionFactory
    if _engine is not None:
        try:
            _engine.dispose()
        except Exception as e:
            logger.warning("Error disposing database engine: %s", e)
    _engine = None
    _SessionFactory = None


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency for yielding database sessions per request.

    Guarantees session cleanup upon request completion.
    """
    factory = get_session_factory()
    if factory is None:
        raise RuntimeError("Database connection is not configured. Set DATABASE_URL to enable database access.")

    session: Session = factory()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """Context manager for programmatic session handling outside FastAPI requests."""
    factory = get_session_factory()
    if factory is None:
        raise RuntimeError("Database connection is not configured. Set DATABASE_URL to enable database access.")

    session: Session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_db_connectivity(
    engine: Optional[Engine] = None,
    settings: Optional[Settings] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Perform explicit database connectivity check with safe sanitized output.

    Returns:
        (is_connected, details_dict)
    """
    cfg = settings or get_settings()
    if engine is None and not cfg.get_raw_database_url():
        return False, {
            "status": "UNCONFIGURED",
            "message": "DATABASE_URL is not configured.",
        }

    target_engine = engine or get_engine(cfg)
    if target_engine is None:
        return False, {
            "status": "UNCONFIGURED",
            "message": "DATABASE_URL is not configured.",
        }

    start_time = time.perf_counter()
    try:
        with target_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
        return True, {
            "status": "CONNECTED",
            "latency_ms": latency_ms,
            "message": "Database connection healthy.",
        }
    except SQLAlchemyError as exc:
        latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
        # Avoid leaking credentials; extract only safe error class description
        error_name = exc.__class__.__name__
        logger.error("Database connectivity check failed: %s", error_name)
        return False, {
            "status": "ERROR",
            "latency_ms": latency_ms,
            "message": f"Database connection failed: {error_name}",
        }
    except Exception as exc:
        latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
        error_name = exc.__class__.__name__
        logger.error("Unexpected error during database connectivity check: %s", error_name)
        return False, {
            "status": "ERROR",
            "latency_ms": latency_ms,
            "message": f"Unexpected error checking database: {error_name}",
        }
