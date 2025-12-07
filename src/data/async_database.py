"""Async database connection and management."""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from src.core.exceptions import ConnectionError, DatabaseError
from src.core.logging import get_logger
from src.data.models import ConnectionInfo, AuthType
from src.utils.odbc_driver import get_odbc_driver_string

logger = get_logger(__name__)


class AsyncDatabaseConnection:
    """Async database connection manager."""

    def __init__(self, connection_info: ConnectionInfo) -> None:
        """
        Initialize async database connection.

        Args:
            connection_info: Connection information
        """
        self.connection_info = connection_info
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[sessionmaker] = None

    async def connect(self) -> None:
        """
        Establish async database connection.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            connection_string = self._build_connection_string()
            logger.info(
                f"Async connecting to {self.connection_info.get_display_name()}"
            )

            # Create async engine
            # Note: For SQL Server, we use aioodbc
            self._engine = create_async_engine(
                f"mssql+aioodbc:///?odbc_connect={connection_string}",
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                pool_pre_ping=True,
                echo=False,
            )

            # Test connection
            async with self._engine.begin() as conn:
                await conn.execute(text("SELECT 1"))

            logger.info(
                f"Async connection established to {self.connection_info.get_display_name()}"
            )

        except Exception as e:
            logger.error(f"Async connection failed: {str(e)}")
            raise ConnectionError(
                f"Failed to connect to database: {str(e)}",
                server=self.connection_info.server,
                database=self.connection_info.database,
            ) from e

    async def disconnect(self) -> None:
        """Close async database connection."""
        if self._engine:
            logger.info(
                f"Disconnecting async from {self.connection_info.get_display_name()}"
            )
            await self._engine.dispose()
            self._engine = None

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Get an async database session.

        Yields:
            Async SQLAlchemy session
        """
        if not self._engine:
            raise ConnectionError(
                "Not connected to database. Call connect() first.",
                server=self.connection_info.server,
                database=self.connection_info.database,
            )

        async_session = sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

        async with async_session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def execute_query(
        self, query: str, params: Optional[Any] = None
    ) -> list[dict[str, Any]]:
        """
        Execute async SQL query and return results.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            List of result rows as dictionaries
        """
        try:
            async with self._engine.begin() as conn:
                if params is not None:
                    result = await conn.execute(text(query), params)
                else:
                    result = await conn.execute(text(query))

                if result.returns_rows:
                    columns = result.keys()
                    rows = result.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
                return []

        except Exception as e:
            logger.error(f"Async query failed: {str(e)}")
            raise DatabaseError(
                f"Query execution failed: {str(e)}",
                query=query,
            ) from e

    async def execute_scalar(
        self, query: str, params: Optional[Any] = None
    ) -> Any:
        """
        Execute async query and return scalar value.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            Scalar result value
        """
        try:
            async with self._engine.begin() as conn:
                if params is not None:
                    result = await conn.execute(text(query), params)
                else:
                    result = await conn.execute(text(query))
                row = result.fetchone()
                return row[0] if row else None

        except Exception as e:
            logger.error(f"Async scalar query failed: {str(e)}")
            raise DatabaseError(
                f"Scalar query failed: {str(e)}",
                query=query,
            ) from e

    def _build_connection_string(self) -> str:
        """Build ODBC connection string."""
        driver = get_odbc_driver_string()
        parts = [
            f"DRIVER={driver}",
            f"SERVER={self.connection_info.server}",
            f"DATABASE={self.connection_info.database}",
            "TrustServerCertificate=yes",
        ]

        if self.connection_info.auth_type == AuthType.WINDOWS:
            parts.append("Trusted_Connection=yes")
        else:
            parts.append(f"UID={self.connection_info.username}")
            parts.append(f"PWD={self.connection_info.password}")

        if self.connection_info.connection_timeout:
            parts.append(f"Connection Timeout={self.connection_info.connection_timeout}")

        return ";".join(parts)

    async def __aenter__(self) -> "AsyncDatabaseConnection":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.disconnect()


async def run_async(coro):
    """
    Helper to run async function from sync context.

    Args:
        coro: Coroutine to run

    Returns:
        Result of the coroutine
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(coro)
