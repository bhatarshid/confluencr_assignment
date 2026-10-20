"""Health check and readiness endpoints."""

from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, Field

from app.config import Settings, get_settings
from app.database import check_db_connectivity

router = APIRouter(tags=["Health"])


class HealthResponse(BaseModel):
    """Schema for the primary root health-check response."""

    status: str = Field(examples=["HEALTHY"], description="Service health state")
    current_time: str = Field(
        examples=["2026-10-20T12:30:00Z"],
        description="Current dynamic UTC timestamp in ISO 8601 format",
    )


class ReadinessResponse(BaseModel):
    """Schema for the system readiness and dependency check response."""

    status: str = Field(description="Readiness state: READY or NOT_READY")
    current_time: str = Field(description="Current dynamic UTC timestamp")
    database: Dict[str, Any] = Field(description="Database connectivity status and latency")


@router.get(
    "/",
    response_model=HealthResponse,
    summary="Root Service Health Check",
    description="Returns immediate liveness status with dynamic UTC timestamp without performing database queries.",
    status_code=status.HTTP_200_OK,
)
def get_health() -> HealthResponse:
    """Liveness check endpoint.

    Must return fast and reliably regardless of external database status.
    """
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return HealthResponse(
        status="HEALTHY",
        current_time=utc_now,
    )


@router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    summary="Service Readiness and Dependency Check",
    description="Checks downstream dependencies such as Supabase PostgreSQL database connectivity.",
)
def get_readiness(
    response: Response,
    settings: Settings = Depends(get_settings),
) -> ReadinessResponse:
    """Readiness probe checking database connectivity without leaking credentials."""
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    is_connected, db_info = check_db_connectivity(settings=settings)

    if not is_connected:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return ReadinessResponse(
            status="NOT_READY",
            current_time=utc_now,
            database=db_info,
        )

    return ReadinessResponse(
        status="READY",
        current_time=utc_now,
        database=db_info,
    )
