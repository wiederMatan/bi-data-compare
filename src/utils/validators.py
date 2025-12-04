"""Validation utility functions."""

import re
from typing import Optional

from src.core.exceptions import ValidationError


def validate_connection_string(connection_string: str) -> bool:
    """
    Validate SQL Server connection string format.

    Args:
        connection_string: Connection string to validate

    Returns:
        True if valid

    Raises:
        ValidationError: If connection string is invalid
    """
    if not connection_string:
        raise ValidationError(
            "Connection string cannot be empty",
            field="connection_string",
        )

    required_parts = ["SERVER", "DATABASE"]
    connection_upper = connection_string.upper()

    for part in required_parts:
        if part not in connection_upper:
            raise ValidationError(
                f"Connection string must contain {part}",
                field="connection_string",
                value=connection_string,
            )

    return True


def validate_server_name(server: str) -> bool:
    """
    Validate SQL Server server name.

    Args:
        server: Server name to validate

    Returns:
        True if valid

    Raises:
        ValidationError: If server name is invalid
    """
    if not server:
        raise ValidationError(
            "Server name cannot be empty",
            field="server",
        )

    # Allow server names, IPs, and named instances
    pattern = r"^[a-zA-Z0-9\.\-_\\,]+$"
    if not re.match(pattern, server):
        raise ValidationError(
            "Invalid server name format",
            field="server",
            value=server,
        )

    return True


def validate_database_name(database: str) -> bool:
    """
    Validate database name.

    Args:
        database: Database name to validate

    Returns:
        True if valid

    Raises:
        ValidationError: If database name is invalid
    """
    if not database:
        raise ValidationError(
            "Database name cannot be empty",
            field="database",
        )

    # SQL Server naming rules
    if len(database) > 128:
        raise ValidationError(
            "Database name cannot exceed 128 characters",
            field="database",
            value=database,
        )

    # Check for invalid characters
    invalid_chars = ['/', '\\', '[', ']', ':', '|', '<', '>', '+', '=', ';', ',', '?', '*']
    for char in invalid_chars:
        if char in database:
            raise ValidationError(
                f"Database name cannot contain '{char}'",
                field="database",
                value=database,
            )

    return True


def validate_schema_name(schema: str) -> bool:
    """
    Validate schema name.

    Args:
        schema: Schema name to validate

    Returns:
        True if valid

    Raises:
        ValidationError: If schema name is invalid
    """
    if not schema:
        raise ValidationError(
            "Schema name cannot be empty",
            field="schema",
        )

    # SQL Server identifier rules
    pattern = r"^[a-zA-Z_][a-zA-Z0-9_@#$]*$"
    if not re.match(pattern, schema):
        raise ValidationError(
            "Invalid schema name format. Must start with letter or underscore.",
            field="schema",
            value=schema,
        )

    if len(schema) > 128:
        raise ValidationError(
            "Schema name cannot exceed 128 characters",
            field="schema",
            value=schema,
        )

    return True


def validate_table_name(table: str) -> bool:
    """
    Validate table name.

    Args:
        table: Table name to validate

    Returns:
        True if valid

    Raises:
        ValidationError: If table name is invalid
    """
    if not table:
        raise ValidationError(
            "Table name cannot be empty",
            field="table",
        )

    # SQL Server identifier rules
    pattern = r"^[a-zA-Z_][a-zA-Z0-9_@#$]*$"
    if not re.match(pattern, table):
        raise ValidationError(
            "Invalid table name format. Must start with letter or underscore.",
            field="table",
            value=table,
        )

    if len(table) > 128:
        raise ValidationError(
            "Table name cannot exceed 128 characters",
            field="table",
            value=table,
        )

    return True


def validate_column_name(column: str) -> bool:
    """
    Validate column name.

    Args:
        column: Column name to validate

    Returns:
        True if valid

    Raises:
        ValidationError: If column name is invalid
    """
    if not column:
        raise ValidationError(
            "Column name cannot be empty",
            field="column",
        )

    # SQL Server identifier rules
    pattern = r"^[a-zA-Z_][a-zA-Z0-9_@#$]*$"
    if not re.match(pattern, column):
        raise ValidationError(
            "Invalid column name format. Must start with letter or underscore.",
            field="column",
            value=column,
        )

    if len(column) > 128:
        raise ValidationError(
            "Column name cannot exceed 128 characters",
            field="column",
            value=column,
        )

    return True


def validate_credentials(username: Optional[str], password: Optional[str], use_windows_auth: bool) -> bool:
    """
    Validate authentication credentials.

    Args:
        username: Username (required for SQL auth)
        password: Password (required for SQL auth)
        use_windows_auth: Whether Windows authentication is used

    Returns:
        True if valid

    Raises:
        ValidationError: If credentials are invalid
    """
    if not use_windows_auth:
        if not username:
            raise ValidationError(
                "Username is required for SQL Server authentication",
                field="username",
            )
        if not password:
            raise ValidationError(
                "Password is required for SQL Server authentication",
                field="password",
            )

    return True


def validate_chunk_size(chunk_size: int, min_size: int = 100, max_size: int = 1000000) -> bool:
    """
    Validate chunk size for data processing.

    Args:
        chunk_size: Chunk size to validate
        min_size: Minimum allowed size
        max_size: Maximum allowed size

    Returns:
        True if valid

    Raises:
        ValidationError: If chunk size is invalid
    """
    if chunk_size < min_size:
        raise ValidationError(
            f"Chunk size must be at least {min_size}",
            field="chunk_size",
            value=chunk_size,
        )

    if chunk_size > max_size:
        raise ValidationError(
            f"Chunk size cannot exceed {max_size}",
            field="chunk_size",
            value=chunk_size,
        )

    return True
