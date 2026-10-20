"""Centralized application configuration management using Pydantic Settings."""

from functools import lru_cache
from typing import Literal, Optional
from urllib.parse import urlparse, urlunparse

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Core Application Settings
    APP_NAME: str = Field(default="Transaction Webhook Service", description="Application display name")
    APP_VERSION: str = Field(default="0.1.0", description="Application semantic version")
    APP_ENV: Literal["development", "staging", "production", "test"] = Field(
        default="development",
        description="Runtime environment name",
    )
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Application logging level",
    )
    API_PREFIX: str = Field(default="", description="Base path prefix for API routes")

    # Server Settings
    HOST: str = Field(default="0.0.0.0", description="Host to bind server")
    PORT: int = Field(default=8000, description="Port to bind server")

    # Database Configuration (Supabase PostgreSQL)
    DB_SCHEMA: Optional[str] = Field(
        default="confluencr_asgnmt",
        description="PostgreSQL schema name for application tables",
    )
    DATABASE_URL: Optional[SecretStr] = Field(
        default=None,
        description="Full PostgreSQL connection string (e.g. postgresql+psycopg://user:pass@host:port/dbname)",
    )
    DB_POOL_SIZE: int = Field(default=5, ge=1, le=50, description="Database connection pool size")
    DB_MAX_OVERFLOW: int = Field(default=10, ge=0, le=50, description="Maximum overflow connections")
    DB_POOL_TIMEOUT: int = Field(default=30, ge=1, description="Timeout waiting for connection from pool in seconds")
    DB_POOL_RECYCLE: int = Field(
        default=1800,
        ge=60,
        description="Recycle connections after N seconds to prevent stale dropped connections",
    )

    @field_validator("DATABASE_URL")
    @classmethod
    def validate_database_url(cls, v: Optional[SecretStr]) -> Optional[SecretStr]:
        """Validate format of DATABASE_URL if supplied."""
        if v is None:
            return v
        raw = v.get_secret_value().strip()
        if not raw:
            return None
        valid_prefixes = ("postgresql://", "postgresql+psycopg://", "postgresql+psycopg2://", "postgres://")
        if not raw.startswith(valid_prefixes):
            raise ValueError(
                f"DATABASE_URL must start with one of: {', '.join(valid_prefixes)}. Provided: '{raw.split('://')[0]}://...'"
            )
        return v

    def get_raw_database_url(self) -> Optional[str]:
        """Return raw database connection string normalized for SQLAlchemy psycopg driver."""
        if not self.DATABASE_URL:
            return None
        url = self.DATABASE_URL.get_secret_value().strip()
        # Normalize legacy postgres:// or standard postgresql:// to postgresql+psycopg:// for SQLAlchemy 2.x
        if url.startswith("postgres://"):
            url = "postgresql+psycopg://" + url[len("postgres://"):]
        elif url.startswith("postgresql://"):
            url = "postgresql+psycopg://" + url[len("postgresql://"):]
        return url

    @property
    def sanitized_database_url(self) -> str:
        """Return database URL with password masked for safe logging and inspection."""
        raw_url = self.get_raw_database_url()
        if not raw_url:
            return "None (not configured)"
        try:
            parsed = urlparse(raw_url)
            if parsed.password:
                netloc = f"{parsed.username}:***@{parsed.hostname}"
                if parsed.port:
                    netloc += f":{parsed.port}"
                masked = parsed._replace(netloc=netloc)
                return urlunparse(masked)
            return raw_url
        except Exception:
            return "<masked-url>"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached Settings instance."""
    return Settings()
