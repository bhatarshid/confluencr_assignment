"""FastAPI Application entry point for Transaction Webhook Service."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api import health_router, transactions_router, webhooks_router
from app.config import Settings, get_settings
from app.database import check_db_connectivity, get_engine, reset_database_engine
from app.logging_config import configure_logging, get_logger

logger = get_logger("main")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Handle application startup and shutdown lifecycle events."""
    settings = get_settings()
    configure_logging(level_name=settings.LOG_LEVEL)

    logger.info(
        "Starting %s v%s in '%s' environment",
        settings.APP_NAME,
        settings.APP_VERSION,
        settings.APP_ENV,
    )

    # Validate database configuration without failing startup in dev/test if unavailable
    if settings.DATABASE_URL:
        logger.info("Database configured with target: %s", settings.sanitized_database_url)
        # Attempt non-blocking connectivity check to log status
        connected, details = check_db_connectivity()
        if connected:
            logger.info("Database connectivity established successfully (latency: %sms)", details.get("latency_ms"))
        else:
            logger.warning("Database connectivity check failed during startup: %s", details.get("message"))
    else:
        logger.warning("No DATABASE_URL configured. Database features will be unavailable.")

    yield

    # Clean shutdown
    logger.info("Shutting down %s, releasing database resources...", settings.APP_NAME)
    reset_database_engine()
    logger.info("Application shutdown complete.")


def create_app(settings: Settings | None = None) -> FastAPI:
    """Application factory for Transaction Webhook Service."""
    cfg = settings or get_settings()

    app = FastAPI(
        title=cfg.APP_NAME,
        version=cfg.APP_VERSION,
        description="Transaction Webhook Service for reliable payment webhook processing.",
        docs_url="/docs" if cfg.APP_ENV != "production" else None,
        redoc_url="/redoc" if cfg.APP_ENV != "production" else None,
        lifespan=lifespan,
    )

    # Basic safe CORS configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Global safe exception handler to avoid leaking secrets for unexpected server errors
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        if isinstance(exc, HTTPException):
            return JSONResponse(
                status_code=exc.status_code,
                content=exc.detail if isinstance(exc.detail, dict) else {"error": "HttpError", "message": exc.detail},
            )
        logger.exception("Unhandled server exception: %s", exc.__class__.__name__)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "InternalServerError",
                "message": "An unexpected server error occurred. Please try again later.",
            },
        )

    # Mount routers
    app.include_router(health_router, prefix=cfg.API_PREFIX)
    app.include_router(webhooks_router, prefix=cfg.API_PREFIX)
    app.include_router(transactions_router, prefix=cfg.API_PREFIX)

    return app


# Default application instance for ASGI servers (e.g. uvicorn app.main:app)
app = create_app()
