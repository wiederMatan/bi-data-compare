"""FastAPI application for BI Data Compare."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import comparisons, connections, history
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

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
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
                "connections": "/api/v1/connections",
                "comparisons": "/api/v1/comparisons",
                "history": "/api/v1/history",
            },
        }

    logger.info("FastAPI application created")
    return app


# Create app instance
app = create_app()
