"""Custom exception classes for the application."""

from typing import Any, Optional


class ApplicationError(Exception):
    """Base exception for all application errors."""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the application error.

        Args:
            message: Error message
            error_code: Optional error code for categorization
            details: Optional additional error details
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}

    def __str__(self) -> str:
        """Return string representation of the error."""
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class ConfigurationError(ApplicationError):
    """Exception raised for configuration-related errors."""

    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the configuration error.

        Args:
            message: Error message
            config_key: Optional configuration key that caused the error
            details: Optional additional error details
        """
        super().__init__(message, "CONFIG_ERROR", details)
        self.config_key = config_key


class ConnectionError(ApplicationError):
    """Exception raised for database connection errors."""

    def __init__(
        self,
        message: str,
        server: Optional[str] = None,
        database: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the connection error.

        Args:
            message: Error message
            server: Optional server name
            database: Optional database name
            details: Optional additional error details
        """
        super().__init__(message, "CONNECTION_ERROR", details)
        self.server = server
        self.database = database


class DatabaseError(ApplicationError):
    """Exception raised for database operation errors."""

    def __init__(
        self,
        message: str,
        query: Optional[str] = None,
        table: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the database error.

        Args:
            message: Error message
            query: Optional SQL query that caused the error
            table: Optional table name
            details: Optional additional error details
        """
        super().__init__(message, "DATABASE_ERROR", details)
        self.query = query
        self.table = table


class ValidationError(ApplicationError):
    """Exception raised for validation errors."""

    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the validation error.

        Args:
            message: Error message
            field: Optional field name that failed validation
            value: Optional value that failed validation
            details: Optional additional error details
        """
        super().__init__(message, "VALIDATION_ERROR", details)
        self.field = field
        self.value = value


class ComparisonError(ApplicationError):
    """Exception raised for comparison operation errors."""

    def __init__(
        self,
        message: str,
        source_table: Optional[str] = None,
        target_table: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the comparison error.

        Args:
            message: Error message
            source_table: Optional source table name
            target_table: Optional target table name
            details: Optional additional error details
        """
        super().__init__(message, "COMPARISON_ERROR", details)
        self.source_table = source_table
        self.target_table = target_table


class ExportError(ApplicationError):
    """Exception raised for export operation errors."""

    def __init__(
        self,
        message: str,
        export_format: Optional[str] = None,
        file_path: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the export error.

        Args:
            message: Error message
            export_format: Optional export format (excel, csv, etc.)
            file_path: Optional file path
            details: Optional additional error details
        """
        super().__init__(message, "EXPORT_ERROR", details)
        self.export_format = export_format
        self.file_path = file_path


class CompressionError(ApplicationError):
    """Exception raised for compression analysis errors."""

    def __init__(
        self,
        message: str,
        table: Optional[str] = None,
        compression_type: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the compression error.

        Args:
            message: Error message
            table: Optional table name
            compression_type: Optional compression type
            details: Optional additional error details
        """
        super().__init__(message, "COMPRESSION_ERROR", details)
        self.table = table
        self.compression_type = compression_type
