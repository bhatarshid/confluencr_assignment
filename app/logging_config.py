"""Centralized logging configuration for Transaction Webhook Service."""

import logging
import sys
from typing import Optional


def configure_logging(level_name: str = "INFO") -> None:
    """Configure standard logging with clean format and level without leaking secrets."""
    numeric_level = getattr(logging, level_name.upper(), logging.INFO)

    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    # Tone down noisy third-party loggers if desired
    logging.getLogger("uvicorn.access").setLevel(numeric_level)
    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.INFO if numeric_level == logging.DEBUG else logging.WARNING
    )


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Obtain an application logger instance."""
    prefix = "transaction_webhook_service"
    if name:
        return logging.getLogger(f"{prefix}.{name}")
    return logging.getLogger(prefix)
