"""Pytest configuration and fixtures."""

import pytest
from unittest.mock import MagicMock

from src.core.config import Settings
from src.data.database import DatabaseConnection
from src.data.models import AuthType, ColumnInfo, ConnectionInfo, TableInfo


@pytest.fixture
def mock_settings():
    """Create mock settings."""
    settings = Settings()
    settings.source_server = "test_server"
    settings.source_database = "test_db"
    settings.target_server = "test_server"
    settings.target_database = "test_db_target"
    return settings


@pytest.fixture
def source_connection_info():
    """Create source connection info for testing."""
    return ConnectionInfo(
        server="test_server",
        database="source_db",
        username="test_user",
        password="test_pass",
        auth_type=AuthType.SQL,
    )


@pytest.fixture
def target_connection_info():
    """Create target connection info for testing."""
    return ConnectionInfo(
        server="test_server",
        database="target_db",
        username="test_user",
        password="test_pass",
        auth_type=AuthType.SQL,
    )


@pytest.fixture
def mock_database_connection(source_connection_info):
    """Create mock database connection."""
    connection = MagicMock(spec=DatabaseConnection)
    connection.connection_info = source_connection_info
    connection.test_connection.return_value = True
    return connection


@pytest.fixture
def sample_table_info():
    """Create sample table info for testing."""
    return TableInfo(
        schema_name="dbo",
        table_name="Users",
        row_count=1000,
        data_size_kb=500.0,
        index_size_kb=100.0,
        total_size_kb=600.0,
        compression_type="NONE",
    )


@pytest.fixture
def sample_columns():
    """Create sample column info for testing."""
    return [
        ColumnInfo(
            column_name="Id",
            data_type="int",
            is_nullable=False,
            is_identity=True,
            ordinal_position=1,
        ),
        ColumnInfo(
            column_name="Name",
            data_type="nvarchar",
            max_length=100,
            is_nullable=False,
            ordinal_position=2,
        ),
        ColumnInfo(
            column_name="Email",
            data_type="nvarchar",
            max_length=255,
            is_nullable=True,
            ordinal_position=3,
        ),
    ]
