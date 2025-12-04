"""Database connection and management."""

from contextlib import contextmanager
from typing import Any, Generator, Optional

import pyodbc
from sqlalchemy import create_engine, event, pool, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.core.exceptions import ConnectionError, DatabaseError
from src.core.logging import get_logger
from src.data.models import ConnectionInfo, AuthType

logger = get_logger(__name__)

# Global connection cache
_connection_cache: dict[str, "DatabaseConnection"] = {}


def get_cached_connection(connection_info: ConnectionInfo) -> "DatabaseConnection":
    """
    Get a cached database connection or create a new one.

    Args:
        connection_info: Connection information

    Returns:
        DatabaseConnection instance (cached or new)
    """
    cache_key = f"{connection_info.server}_{connection_info.database}"

    if cache_key in _connection_cache:
        conn = _connection_cache[cache_key]
        # Check if still connected
        if conn._engine is not None:
            try:
                conn.test_connection()
                return conn
            except Exception:
                # Connection lost, remove from cache
                del _connection_cache[cache_key]

    # Create new connection
    conn = DatabaseConnection(connection_info)
    conn.connect()
    _connection_cache[cache_key] = conn
    return conn


def clear_connection_cache() -> None:
    """Clear all cached connections."""
    global _connection_cache
    for conn in _connection_cache.values():
        try:
            conn.disconnect()
        except Exception:
            pass
    _connection_cache.clear()
    logger.info("Connection cache cleared")


class DatabaseConnection:
    """Manages a single database connection."""

    def __init__(self, connection_info: ConnectionInfo) -> None:
        """
        Initialize database connection.

        Args:
            connection_info: Connection information
        """
        self.connection_info = connection_info
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None

    def connect(self) -> None:
        """
        Establish database connection.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            connection_string = self._build_connection_string()
            logger.info(
                f"Connecting to {self.connection_info.get_display_name()}"
            )

            self._engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={connection_string}",
                poolclass=pool.QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                pool_pre_ping=True,
                echo=False,
            )

            # Test connection
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            self._session_factory = sessionmaker(bind=self._engine)

            logger.info(
                f"Successfully connected to {self.connection_info.get_display_name()}"
            )

        except Exception as e:
            logger.error(
                f"Failed to connect to {self.connection_info.get_display_name()}: {str(e)}"
            )
            raise ConnectionError(
                f"Failed to connect to database: {str(e)}",
                server=self.connection_info.server,
                database=self.connection_info.database,
            ) from e

    def disconnect(self) -> None:
        """Close database connection."""
        if self._engine:
            logger.info(
                f"Disconnecting from {self.connection_info.get_display_name()}"
            )
            self._engine.dispose()
            self._engine = None
            self._session_factory = None

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get a database session.

        Yields:
            SQLAlchemy session

        Raises:
            ConnectionError: If not connected
        """
        if not self._session_factory:
            raise ConnectionError(
                "Not connected to database. Call connect() first.",
                server=self.connection_info.server,
                database=self.connection_info.database,
            )

        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """
        Get a raw database connection.

        Yields:
            Database connection

        Raises:
            ConnectionError: If not connected
        """
        if not self._engine:
            raise ConnectionError(
                "Not connected to database. Call connect() first.",
                server=self.connection_info.server,
                database=self.connection_info.database,
            )

        conn = self._engine.raw_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute_query(
        self, query: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            List of result rows as dictionaries

        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    results = []
                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))
                    return results
                return []

        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}\nQuery: {query}")
            raise DatabaseError(
                f"Query execution failed: {str(e)}",
                query=query,
            ) from e

    def execute_scalar(
        self, query: str, params: Optional[dict[str, Any]] = None
    ) -> Any:
        """
        Execute a SQL query and return a single scalar value.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            Scalar result value

        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                row = cursor.fetchone()
                return row[0] if row else None

        except Exception as e:
            logger.error(f"Scalar query execution failed: {str(e)}\nQuery: {query}")
            raise DatabaseError(
                f"Scalar query execution failed: {str(e)}",
                query=query,
            ) from e

    def test_connection(self) -> bool:
        """
        Test if connection is alive.

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            self.execute_scalar("SELECT 1")
            return True
        except Exception:
            return False

    def get_databases(self) -> list[str]:
        """
        Get a list of all databases on the server.

        Returns:
            List of database names.

        Raises:
            DatabaseError: If query execution fails.
        """
        query = "SELECT name FROM sys.databases WHERE state = 0 ORDER BY name;"
        try:
            # We connect to master database to get list of all databases
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                databases = [row[0] for row in cursor.fetchall()]
                return databases
        except Exception as e:
            logger.error(f"Failed to get database list: {str(e)}")
            raise DatabaseError(f"Failed to get database list: {str(e)}") from e

    def _build_connection_string(self) -> str:
        """
        Build ODBC connection string.

        Returns:
            ODBC connection string
        """
        driver = "{ODBC Driver 18 for SQL Server}"
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

    def __enter__(self) -> "DatabaseConnection":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.disconnect()


class DatabaseManager:
    """Manages multiple database connections."""

    def __init__(self) -> None:
        """Initialize database manager."""
        self._connections: dict[str, DatabaseConnection] = {}

    def add_connection(
        self, name: str, connection_info: ConnectionInfo
    ) -> DatabaseConnection:
        """
        Add a new database connection.

        Args:
            name: Connection name (e.g., 'source', 'target')
            connection_info: Connection information

        Returns:
            Database connection instance
        """
        connection = DatabaseConnection(connection_info)
        self._connections[name] = connection
        return connection

    def get_connection(self, name: str) -> Optional[DatabaseConnection]:
        """
        Get a database connection by name.

        Args:
            name: Connection name

        Returns:
            Database connection or None if not found
        """
        return self._connections.get(name)

    def connect_all(self) -> None:
        """Connect all registered connections."""
        for name, connection in self._connections.items():
            logger.info(f"Connecting to '{name}' database...")
            connection.connect()

    def disconnect_all(self) -> None:
        """Disconnect all connections."""
        for name, connection in self._connections.items():
            logger.info(f"Disconnecting from '{name}' database...")
            connection.disconnect()

    def test_all_connections(self) -> dict[str, bool]:
        """
        Test all connections.

        Returns:
            Dictionary of connection names to test results
        """
        results = {}
        for name, connection in self._connections.items():
            results[name] = connection.test_connection()
        return results

    def __enter__(self) -> "DatabaseManager":
        """Context manager entry."""
        self.connect_all()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.disconnect_all()
