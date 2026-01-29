"""FastAPI application for BI Data Compare."""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import auth, comparisons, connections, history, notifications, scheduler
from src.core.logging import get_logger

logger = get_logger(__name__)


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    app = FastAPI(
        title="BI Data Compare API",
        description="REST API for SQL Server database comparison",
        version="1.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
    )

    # CORS middleware - configure allowed origins from environment
    # SECURITY: Do not use allow_origins=["*"] with allow_credentials=True in production
    cors_origins_env = os.environ.get("CORS_ALLOWED_ORIGINS", "")
    if cors_origins_env:
        # Parse comma-separated list of origins from environment
        allowed_origins = [origin.strip() for origin in cors_origins_env.split(",") if origin.strip()]
    else:
        # Default to localhost for development
        allowed_origins = [
            "http://localhost:8501",
            "http://localhost:8502",
            "http://localhost:8503",
            "http://127.0.0.1:8501",
            "http://127.0.0.1:8502",
            "http://127.0.0.1:8503",
        ]
        logger.warning(
            "CORS_ALLOWED_ORIGINS not set. Using default localhost origins. "
            "Set CORS_ALLOWED_ORIGINS environment variable for production."
        )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-API-Key"],
    )

    # Include routers
    app.include_router(
        auth.router,
        prefix="/api/v1/auth",
        tags=["authentication"],
    )
    app.include_router(
        connections.router,
        prefix="/api/v1/connections",
        tags=["connections"],
    )
    app.include_router(
        comparisons.router,
        prefix="/api/v1/comparisons",
        tags=["comparisons"],
    )
    app.include_router(
        history.router,
        prefix="/api/v1/history",
        tags=["history"],
    )
    app.include_router(
        scheduler.router,
        prefix="/api/v1/scheduler",
        tags=["scheduler"],
    )
    app.include_router(
        notifications.router,
        prefix="/api/v1/notifications",
        tags=["notifications"],
    )

    @app.get("/api/health", tags=["health"])
    async def health_check():
        """Health check endpoint."""
        return {"status": "healthy", "service": "bi-data-compare"}

    @app.get("/api/v1", tags=["info"])
    async def api_info():
        """API information endpoint."""
        return {
            "name": "BI Data Compare API",
            "version": "1.0.0",
            "endpoints": {
                "auth": "/api/v1/auth",
                "connections": "/api/v1/connections",
                "comparisons": "/api/v1/comparisons",
                "history": "/api/v1/history",
                "scheduler": "/api/v1/scheduler",
                "notifications": "/api/v1/notifications",
            },
        }

    logger.info("FastAPI application created")
    return app


# Create app instance
app = create_app()
