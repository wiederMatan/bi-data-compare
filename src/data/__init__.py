"""Data access layer components."""

from src.data.database import DatabaseConnection, DatabaseManager
from src.data.models import (
    ColumnInfo,
    CompressionAnalysis,
    CompressionRecommendation,
    ComparisonResult,
    ConnectionInfo,
    DataDifference,
    IndexInfo,
    SchemaDifference,
    TableInfo,
)
from src.data.repositories import (
    CompressionRepository,
    MetadataRepository,
    TableDataRepository,
)

__all__ = [
    # Database
    "DatabaseConnection",
    "DatabaseManager",
    # Models
    "ConnectionInfo",
    "TableInfo",
    "ColumnInfo",
    "IndexInfo",
    "ComparisonResult",
    "SchemaDifference",
    "DataDifference",
    "CompressionAnalysis",
    "CompressionRecommendation",
    # Repositories
    "MetadataRepository",
    "TableDataRepository",
    "CompressionRepository",
]
