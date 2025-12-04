"""Unit tests for validators."""

import pytest

from src.core.exceptions import ValidationError
from src.utils.validators import (
    validate_chunk_size,
    validate_column_name,
    validate_connection_string,
    validate_credentials,
    validate_database_name,
    validate_schema_name,
    validate_server_name,
    validate_table_name,
)


class TestValidateConnectionString:
    """Tests for connection string validation."""

    def test_valid_connection_string(self):
        """Test valid connection string."""
        conn_str = "SERVER=localhost;DATABASE=testdb;UID=user;PWD=pass"
        assert validate_connection_string(conn_str) is True

    def test_empty_connection_string(self):
        """Test empty connection string."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_connection_string("")

    def test_missing_server(self):
        """Test connection string without SERVER."""
        with pytest.raises(ValidationError, match="must contain SERVER"):
            validate_connection_string("DATABASE=testdb")

    def test_missing_database(self):
        """Test connection string without DATABASE."""
        with pytest.raises(ValidationError, match="must contain DATABASE"):
            validate_connection_string("SERVER=localhost")


class TestValidateServerName:
    """Tests for server name validation."""

    def test_valid_server_names(self):
        """Test valid server names."""
        assert validate_server_name("localhost") is True
        assert validate_server_name("192.168.1.1") is True
        assert validate_server_name("SERVER\\INSTANCE") is True
        assert validate_server_name("server.domain.com") is True

    def test_empty_server_name(self):
        """Test empty server name."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_server_name("")

    def test_invalid_server_name(self):
        """Test invalid server name."""
        with pytest.raises(ValidationError, match="Invalid server name format"):
            validate_server_name("server@invalid")


class TestValidateDatabaseName:
    """Tests for database name validation."""

    def test_valid_database_names(self):
        """Test valid database names."""
        assert validate_database_name("TestDB") is True
        assert validate_database_name("Test_DB_123") is True

    def test_empty_database_name(self):
        """Test empty database name."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_database_name("")

    def test_too_long_database_name(self):
        """Test database name exceeding max length."""
        with pytest.raises(ValidationError, match="cannot exceed 128 characters"):
            validate_database_name("x" * 129)

    def test_invalid_characters(self):
        """Test database name with invalid characters."""
        with pytest.raises(ValidationError, match="cannot contain"):
            validate_database_name("Test/DB")


class TestValidateSchemaName:
    """Tests for schema name validation."""

    def test_valid_schema_names(self):
        """Test valid schema names."""
        assert validate_schema_name("dbo") is True
        assert validate_schema_name("_schema") is True
        assert validate_schema_name("Schema_123") is True

    def test_empty_schema_name(self):
        """Test empty schema name."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_schema_name("")

    def test_invalid_schema_name(self):
        """Test invalid schema name (starts with number)."""
        with pytest.raises(ValidationError, match="Invalid schema name format"):
            validate_schema_name("123schema")

    def test_too_long_schema_name(self):
        """Test schema name exceeding max length."""
        with pytest.raises(ValidationError, match="cannot exceed 128 characters"):
            validate_schema_name("s" * 129)


class TestValidateTableName:
    """Tests for table name validation."""

    def test_valid_table_names(self):
        """Test valid table names."""
        assert validate_table_name("Users") is True
        assert validate_table_name("_table") is True
        assert validate_table_name("Table_123") is True

    def test_empty_table_name(self):
        """Test empty table name."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_table_name("")

    def test_invalid_table_name(self):
        """Test invalid table name."""
        with pytest.raises(ValidationError, match="Invalid table name format"):
            validate_table_name("123table")


class TestValidateColumnName:
    """Tests for column name validation."""

    def test_valid_column_names(self):
        """Test valid column names."""
        assert validate_column_name("Id") is True
        assert validate_column_name("_column") is True
        assert validate_column_name("Column_123") is True

    def test_empty_column_name(self):
        """Test empty column name."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_column_name("")


class TestValidateCredentials:
    """Tests for credentials validation."""

    def test_valid_sql_credentials(self):
        """Test valid SQL Server credentials."""
        assert validate_credentials("user", "pass", False) is True

    def test_valid_windows_auth(self):
        """Test valid Windows authentication."""
        assert validate_credentials(None, None, True) is True

    def test_missing_username(self):
        """Test missing username for SQL auth."""
        with pytest.raises(ValidationError, match="Username is required"):
            validate_credentials(None, "pass", False)

    def test_missing_password(self):
        """Test missing password for SQL auth."""
        with pytest.raises(ValidationError, match="Password is required"):
            validate_credentials("user", None, False)


class TestValidateChunkSize:
    """Tests for chunk size validation."""

    def test_valid_chunk_size(self):
        """Test valid chunk size."""
        assert validate_chunk_size(1000) is True
        assert validate_chunk_size(50000) is True

    def test_chunk_size_too_small(self):
        """Test chunk size below minimum."""
        with pytest.raises(ValidationError, match="must be at least"):
            validate_chunk_size(50)

    def test_chunk_size_too_large(self):
        """Test chunk size above maximum."""
        with pytest.raises(ValidationError, match="cannot exceed"):
            validate_chunk_size(2000000)
