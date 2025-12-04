"""Formatting utility functions."""

from typing import Optional


def format_bytes(bytes_value: float, precision: int = 2) -> str:
    """
    Format bytes to human-readable string.

    Args:
        bytes_value: Number of bytes
        precision: Decimal precision

    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.{precision}f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.{precision}f} PB"


def format_number(number: int) -> str:
    """
    Format number with thousand separators.

    Args:
        number: Number to format

    Returns:
        Formatted string (e.g., "1,234,567")
    """
    return f"{number:,}"


def format_percentage(value: float, total: float, precision: int = 1) -> str:
    """
    Format percentage.

    Args:
        value: Numerator value
        total: Denominator value
        precision: Decimal precision

    Returns:
        Formatted percentage string (e.g., "75.5%")
    """
    if total == 0:
        return "0.0%"
    percentage = (value / total) * 100
    return f"{percentage:.{precision}f}%"


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted duration (e.g., "2h 15m 30s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes = seconds / 60
    if minutes < 60:
        return f"{int(minutes)}m {int(seconds % 60)}s"

    hours = minutes / 60
    remaining_minutes = int(minutes % 60)
    if hours < 24:
        return f"{int(hours)}h {remaining_minutes}m"

    days = hours / 24
    remaining_hours = int(hours % 24)
    return f"{int(days)}d {remaining_hours}h"


def format_table_name(schema: str, table: str) -> str:
    """
    Format schema and table name.

    Args:
        schema: Schema name
        table: Table name

    Returns:
        Formatted table name (e.g., "[dbo].[Users]")
    """
    return f"[{schema}].[{table}]"


def truncate_string(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """
    Truncate string to maximum length.

    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add when truncated

    Returns:
        Truncated string
    """
    if len(text) <= max_length:
        return text
    return text[: max_length - len(suffix)] + suffix


def format_sql_value(value: any) -> str:
    """
    Format Python value for SQL query.

    Args:
        value: Value to format

    Returns:
        SQL-formatted string
    """
    if value is None:
        return "NULL"
    elif isinstance(value, str):
        # Escape single quotes
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return "1" if value else "0"
    else:
        return f"'{str(value)}'"
