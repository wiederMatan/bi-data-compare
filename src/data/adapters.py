"""Database adapters for multi-database support."""

import os
import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from src.core.exceptions import ConnectionError, DatabaseError
from src.core.logging import get_logger
from src.data.models import AuthType, ConnectionInfo
from src.utils.odbc_driver import get_odbc_driver_string

logger = get_logger(__name__)


def _validate_adapter_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Validate SQL identifier for use in queries.

    Args:
        identifier: The identifier to validate
        identifier_type: Type of identifier for error messages

    Returns:
        The validated identifier

    Raises:
        ValueError: If identifier is invalid
    """
    if not identifier:
        raise ValueError(f"{identifier_type} cannot be empty")

    if len(identifier) > 128:
        raise ValueError(f"{identifier_type} cannot exceed 128 characters")

    # SQL identifier pattern: alphanumeric, underscore, @, #, $
    pattern = r"^[a-zA-Z_@#][a-zA-Z0-9_@#$]*$"
    if not re.match(pattern, identifier):
        raise ValueError(f"Invalid {identifier_type}: contains invalid characters")

    return identifier


def _truncate_query_for_log(query: str, max_length: int = 100) -> str:
    """
    Truncate query for safe logging to avoid exposing sensitive data.

    Args:
        query: The query to truncate
        max_length: Maximum length of the truncated query

    Returns:
        Truncated query string
    """
    sanitized = query.strip()
    if len(sanitized) > max_length:
        return sanitized[:max_length] + "... [truncated]"
    return sanitized


class DatabaseType(str, Enum):
    """Supported database types."""

    SQL_SERVER = "sqlserver"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters."""

    def __init__(self, connection_info: ConnectionInfo):
        self.connection_info = connection_info
        self._engine: Optional[Engine] = None

    @abstractmethod
    def build_connection_string(self) -> str:
        """Build database-specific connection string."""
        pass

    @abstractmethod
    def get_tables_query(self, schema: str) -> str:
        """Get query to list tables in schema."""
        pass

    @abstractmethod
    def get_columns_query(self, schema: str, table: str) -> str:
        """Get query to list columns in table."""
        pass

    @abstractmethod
    def get_row_count_query(self, schema: str, table: str) -> str:
        """Get query to count rows in table."""
        pass

    def connect(self) -> None:
        """Establish database connection."""
        try:
            connection_string = self.build_connection_string()
            logger.info(f"Connecting to {self.connection_info.get_display_name()}")

            self._engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                pool_pre_ping=True,
                echo=False,
            )

            # Test connection
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            logger.info(f"Connected to {self.connection_info.get_display_name()}")

        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            raise ConnectionError(
                f"Failed to connect: {str(e)}",
                server=self.connection_info.server,
                database=self.connection_info.database,
            ) from e

    def disconnect(self) -> None:
        """Close database connection."""
        if self._engine:
            logger.info(f"Disconnecting from {self.connection_info.get_display_name()}")
            self._engine.dispose()
            self._engine = None

    def execute_query(self, query: str, params: Optional[Any] = None) -> list[dict]:
        """Execute query and return results."""
        if not self._engine:
            raise ConnectionError("Not connected")

        try:
            with self._engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))

                if result.returns_rows:
                    columns = result.keys()
                    return [dict(zip(columns, row)) for row in result.fetchall()]
                return []

        except Exception as e:
            # SECURITY: Truncate query in logs to avoid exposing sensitive data
            logger.error(f"Query failed: {str(e)}")
            raise DatabaseError(
                f"Query failed: {str(e)}",
                query=_truncate_query_for_log(query),
            ) from e


class SQLServerAdapter(DatabaseAdapter):
    """Adapter for Microsoft SQL Server."""

    def build_connection_string(self) -> str:
        """Build SQL Server connection string."""
        driver = get_odbc_driver_string()

        # SECURITY: TrustServerCertificate should be 'no' in production
        # Set DB_TRUST_SERVER_CERTIFICATE=no for production environments
        trust_cert = os.environ.get("DB_TRUST_SERVER_CERTIFICATE", "yes").lower()
        trust_cert_value = "yes" if trust_cert in ("yes", "true", "1") else "no"

        if self.connection_info.auth_type == AuthType.WINDOWS:
            conn_str = (
                f"DRIVER={driver};"
                f"SERVER={self.connection_info.server};"
                f"DATABASE={self.connection_info.database};"
                "Trusted_Connection=yes;"
                f"TrustServerCertificate={trust_cert_value}"
            )
        else:
            conn_str = (
                f"DRIVER={driver};"
                f"SERVER={self.connection_info.server};"
                f"DATABASE={self.connection_info.database};"
                f"UID={self.connection_info.username};"
                f"PWD={self.connection_info.password};"
                f"TrustServerCertificate={trust_cert_value}"
            )

        return f"mssql+pyodbc:///?odbc_connect={conn_str}"

    def get_tables_query(self, schema: str) -> str:
        """Get SQL Server tables query."""
        # Validate identifier to prevent SQL injection
        _validate_adapter_identifier(schema, "schema")
        return f"""
            SELECT TABLE_NAME as table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """

    def get_columns_query(self, schema: str, table: str) -> str:
        """Get SQL Server columns query."""
        # Validate identifiers to prevent SQL injection
        _validate_adapter_identifier(schema, "schema")
        _validate_adapter_identifier(table, "table")
        return f"""
            SELECT
                COLUMN_NAME as column_name,
                DATA_TYPE as data_type,
                CHARACTER_MAXIMUM_LENGTH as max_length,
                NUMERIC_PRECISION as precision,
                NUMERIC_SCALE as scale,
                IS_NULLABLE as is_nullable
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """

    def get_row_count_query(self, schema: str, table: str) -> str:
        """Get SQL Server row count query."""
        # Validate identifiers to prevent SQL injection
        _validate_adapter_identifier(schema, "schema")
        _validate_adapter_identifier(table, "table")
        return f"SELECT COUNT(*) FROM [{schema}].[{table}]"


class PostgreSQLAdapter(DatabaseAdapter):
    """Adapter for PostgreSQL."""

    def build_connection_string(self) -> str:
        """Build PostgreSQL connection string."""
        return (
            f"postgresql://{self.connection_info.username}:"
            f"{self.connection_info.password}@"
            f"{self.connection_info.server}/"
            f"{self.connection_info.database}"
        )

    def get_tables_query(self, schema: str) -> str:
        """Get PostgreSQL tables query."""
        # Validate identifier to prevent SQL injection
        _validate_adapter_identifier(schema, "schema")
        return f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

    def get_columns_query(self, schema: str, table: str) -> str:
        """Get PostgreSQL columns query."""
        # Validate identifiers to prevent SQL injection
        _validate_adapter_identifier(schema, "schema")
        _validate_adapter_identifier(table, "table")
        return f"""
            SELECT
                column_name,
                data_type,
                character_maximum_length as max_length,
                numeric_precision as precision,
                numeric_scale as scale,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table}'
            ORDER BY ordinal_position
        """

    def get_row_count_query(self, schema: str, table: str) -> str:
        """Get PostgreSQL row count query."""
        # Validate identifiers to prevent SQL injection
        _validate_adapter_identifier(schema, "schema")
        _validate_adapter_identifier(table, "table")
        return f'SELECT COUNT(*) FROM "{schema}"."{table}"'


class MySQLAdapter(DatabaseAdapter):
    """Adapter for MySQL."""

    def build_connection_string(self) -> str:
        """Build MySQL connection string."""
        return (
            f"mysql+pymysql://{self.connection_info.username}:"
            f"{self.connection_info.password}@"
            f"{self.connection_info.server}/"
            f"{self.connection_info.database}"
        )

    def get_tables_query(self, schema: str) -> str:
        """Get MySQL tables query."""
        # Note: MySQL uses database name instead of schema
        return f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{self.connection_info.database}'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

    def get_columns_query(self, schema: str, table: str) -> str:
        """Get MySQL columns query."""
        # Validate table identifier to prevent SQL injection
        _validate_adapter_identifier(table, "table")
        return f"""
            SELECT
                column_name,
                data_type,
                character_maximum_length as max_length,
                numeric_precision as `precision`,
                numeric_scale as scale,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{self.connection_info.database}'
            AND table_name = '{table}'
            ORDER BY ordinal_position
        """

    def get_row_count_query(self, schema: str, table: str) -> str:
        """Get MySQL row count query."""
        # Validate table identifier to prevent SQL injection
        _validate_adapter_identifier(table, "table")
        return f"SELECT COUNT(*) FROM `{table}`"


def get_adapter(db_type: DatabaseType, connection_info: ConnectionInfo) -> DatabaseAdapter:
    """
    Factory function to get the appropriate database adapter.

    Args:
        db_type: Type of database
        connection_info: Connection information

    Returns:
        Appropriate database adapter instance
    """
    adapters = {
        DatabaseType.SQL_SERVER: SQLServerAdapter,
        DatabaseType.POSTGRESQL: PostgreSQLAdapter,
        DatabaseType.MYSQL: MySQLAdapter,
    }

    adapter_class = adapters.get(db_type)
    if not adapter_class:
        raise ValueError(f"Unsupported database type: {db_type}")

    return adapter_class(connection_info)
