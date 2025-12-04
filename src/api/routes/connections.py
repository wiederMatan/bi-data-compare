"""Connection management API routes."""

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.core.logging import get_logger
from src.data.database import DatabaseConnection, get_cached_connection
from src.data.models import AuthType, ConnectionInfo
from src.data.repositories import MetadataRepository

logger = get_logger(__name__)
router = APIRouter()


class ConnectionRequest(BaseModel):
    """Request model for database connection."""

    server: str = Field(..., description="SQL Server hostname or IP")
    database: str = Field(..., description="Database name")
    username: Optional[str] = Field(None, description="Username for SQL auth")
    password: Optional[str] = Field(None, description="Password for SQL auth")
    use_windows_auth: bool = Field(False, description="Use Windows authentication")
    connection_timeout: int = Field(30, description="Connection timeout in seconds")


class ConnectionResponse(BaseModel):
    """Response model for connection status."""

    connected: bool
    server: str
    database: str
    message: str


class TablesResponse(BaseModel):
    """Response model for tables list."""

    schema_name: str
    tables: list[str]
    count: int


@router.post("/test", response_model=ConnectionResponse)
async def test_connection(request: ConnectionRequest):
    """
    Test database connection.

    Tests if a connection can be established with the provided credentials.
    """
    try:
        conn_info = ConnectionInfo(
            server=request.server,
            database=request.database,
            username=request.username,
            password=request.password,
            auth_type=AuthType.WINDOWS if request.use_windows_auth else AuthType.SQL,
            connection_timeout=request.connection_timeout,
        )

        conn = DatabaseConnection(conn_info)
        conn.connect()
        is_connected = conn.test_connection()
        conn.disconnect()

        return ConnectionResponse(
            connected=is_connected,
            server=request.server,
            database=request.database,
            message="Connection successful" if is_connected else "Connection test failed",
        )

    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/tables", response_model=TablesResponse)
async def get_tables(request: ConnectionRequest, schema: str = "dbo"):
    """
    Get list of tables in a database.

    Returns all tables in the specified schema.
    """
    try:
        conn_info = ConnectionInfo(
            server=request.server,
            database=request.database,
            username=request.username,
            password=request.password,
            auth_type=AuthType.WINDOWS if request.use_windows_auth else AuthType.SQL,
            connection_timeout=request.connection_timeout,
        )

        conn = get_cached_connection(conn_info)
        repo = MetadataRepository(conn)
        tables = [t.table_name for t in repo.get_tables(schema)]

        return TablesResponse(
            schema_name=schema,
            tables=tables,
            count=len(tables),
        )

    except Exception as e:
        logger.error(f"Failed to get tables: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/databases")
async def get_databases(request: ConnectionRequest):
    """
    Get list of databases on the server.

    Returns all accessible databases on the SQL Server instance.
    """
    try:
        # Connect to master to list databases
        conn_info = ConnectionInfo(
            server=request.server,
            database="master",
            username=request.username,
            password=request.password,
            auth_type=AuthType.WINDOWS if request.use_windows_auth else AuthType.SQL,
            connection_timeout=request.connection_timeout,
        )

        conn = DatabaseConnection(conn_info)
        conn.connect()
        databases = conn.get_databases()
        conn.disconnect()

        return {
            "server": request.server,
            "databases": databases,
            "count": len(databases),
        }

    except Exception as e:
        logger.error(f"Failed to get databases: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
