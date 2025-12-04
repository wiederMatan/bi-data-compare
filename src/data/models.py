"""Data models for the application."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class AuthType(Enum):
    """Database authentication types."""

    WINDOWS = "windows"
    SQL = "sql"


class ComparisonMode(Enum):
    """Comparison modes."""

    QUICK = "quick"


class DifferenceType(Enum):
    """Types of differences found during comparison."""

    SCHEMA_ONLY_SOURCE = "schema_only_source"
    SCHEMA_ONLY_TARGET = "schema_only_target"
    SCHEMA_DIFFERENT = "schema_different"
    DATA_ONLY_SOURCE = "data_only_source"
    DATA_ONLY_TARGET = "data_only_target"
    DATA_DIFFERENT = "data_different"
    MATCH = "match"


class CompressionType(Enum):
    """SQL Server compression types."""

    NONE = "NONE"
    PAGE = "PAGE"
    ROW = "ROW"
    COLUMNSTORE = "COLUMNSTORE"


@dataclass
class ConnectionInfo:
    """Database connection information."""

    server: str
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    auth_type: AuthType = AuthType.SQL
    connection_timeout: int = 30
    command_timeout: int = 300

    def get_display_name(self) -> str:
        """Get a display-friendly connection name."""
        return f"{self.server}/{self.database}"

    def mask_password(self) -> "ConnectionInfo":
        """Return a copy with masked password for logging."""
        return ConnectionInfo(
            server=self.server,
            database=self.database,
            username=self.username,
            password="****" if self.password else None,
            auth_type=self.auth_type,
            connection_timeout=self.connection_timeout,
            command_timeout=self.command_timeout,
        )


@dataclass
class ColumnInfo:
    """Table column information."""

    column_name: str
    data_type: str
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_nullable: bool = True
    is_identity: bool = False
    is_computed: bool = False
    default_value: Optional[str] = None
    ordinal_position: int = 0

    def get_full_type(self) -> str:
        """Get the full data type string including length/precision."""
        if self.max_length and self.max_length > 0:
            if self.max_length == -1:
                return f"{self.data_type}(MAX)"
            return f"{self.data_type}({self.max_length})"
        elif self.precision and self.scale is not None:
            return f"{self.data_type}({self.precision},{self.scale})"
        elif self.precision:
            return f"{self.data_type}({self.precision})"
        return self.data_type

    def __eq__(self, other: object) -> bool:
        """Compare columns for equality."""
        if not isinstance(other, ColumnInfo):
            return NotImplemented
        return (
            self.column_name.lower() == other.column_name.lower()
            and self.data_type.lower() == other.data_type.lower()
            and self.max_length == other.max_length
            and self.precision == other.precision
            and self.scale == other.scale
            and self.is_nullable == other.is_nullable
        )


@dataclass
class IndexInfo:
    """Table index information."""

    index_name: str
    index_type: str
    is_unique: bool
    is_primary_key: bool
    columns: list[str] = field(default_factory=list)
    included_columns: list[str] = field(default_factory=list)
    filter_definition: Optional[str] = None

    def __eq__(self, other: object) -> bool:
        """Compare indexes for equality."""
        if not isinstance(other, IndexInfo):
            return NotImplemented
        return (
            self.index_name.lower() == other.index_name.lower()
            and self.index_type.lower() == other.index_type.lower()
            and self.is_unique == other.is_unique
            and [c.lower() for c in self.columns]
            == [c.lower() for c in other.columns]
        )


@dataclass
class TableInfo:
    """Table metadata information."""

    schema_name: str
    table_name: str
    row_count: int = 0
    data_size_kb: float = 0.0
    index_size_kb: float = 0.0
    total_size_kb: float = 0.0
    compression_type: str = "NONE"
    columns: list[ColumnInfo] = field(default_factory=list)
    indexes: list[IndexInfo] = field(default_factory=list)
    primary_key_columns: list[str] = field(default_factory=list)

    def get_full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema_name}.{self.table_name}"

    def get_size_mb(self) -> float:
        """Get total size in MB."""
        return self.total_size_kb / 1024.0

    def has_primary_key(self) -> bool:
        """Check if table has a primary key."""
        return len(self.primary_key_columns) > 0


@dataclass
class SchemaDifference:
    """Schema-level difference between tables."""

    table_name: str
    difference_type: DifferenceType
    column_name: Optional[str] = None
    source_value: Optional[str] = None
    target_value: Optional[str] = None
    description: str = ""

    def get_severity(self) -> str:
        """Get the severity level of the difference."""
        if self.difference_type == DifferenceType.MATCH:
            return "info"
        elif self.difference_type in [
            DifferenceType.SCHEMA_ONLY_SOURCE,
            DifferenceType.SCHEMA_ONLY_TARGET,
        ]:
            return "error"
        else:
            return "warning"


@dataclass
class DataDifference:
    """Data-level difference between rows."""

    table_name: str
    primary_key_values: dict[str, Any]
    difference_type: DifferenceType
    column_name: Optional[str] = None
    source_value: Optional[Any] = None
    target_value: Optional[Any] = None

    def get_pk_display(self) -> str:
        """Get display string for primary key values."""
        return ", ".join(
            [f"{k}={v}" for k, v in self.primary_key_values.items()]
        )


@dataclass
class ComparisonResult:
    """Results of a table comparison."""

    source_table: str
    target_table: str
    mode: ComparisonMode
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "pending"  # pending, running, completed, failed
    error_message: Optional[str] = None

    # Schema comparison
    schema_match: bool = False
    schema_differences: list[SchemaDifference] = field(default_factory=list)

    # Data comparison
    source_row_count: int = 0
    target_row_count: int = 0
    matching_rows: int = 0
    different_rows: int = 0
    source_only_rows: int = 0
    target_only_rows: int = 0
    data_differences: list[DataDifference] = field(default_factory=list)

    # Performance metrics
    duration_seconds: float = 0.0
    rows_per_second: float = 0.0

    def get_match_percentage(self) -> float:
        """Calculate percentage of matching rows."""
        total = max(self.source_row_count, self.target_row_count)
        if total == 0:
            return 100.0
        return (self.matching_rows / total) * 100.0

    def is_match(self) -> bool:
        """Check if tables match completely."""
        return (
            self.schema_match
            and self.source_row_count == self.target_row_count
            and self.different_rows == 0
            and self.source_only_rows == 0
            and self.target_only_rows == 0
        )

    def get_summary(self) -> str:
        """Get a summary string of the comparison."""
        if self.status == "failed":
            return f"Failed: {self.error_message}"
        if self.is_match():
            return f"✓ Complete match ({self.source_row_count} rows)"
        parts = []
        if not self.schema_match:
            parts.append(f"{len(self.schema_differences)} schema diffs")
        if self.different_rows > 0:
            parts.append(f"{self.different_rows} data diffs")
        if self.source_only_rows > 0:
            parts.append(f"{self.source_only_rows} source-only")
        if self.target_only_rows > 0:
            parts.append(f"{self.target_only_rows} target-only")
        return ", ".join(parts)


@dataclass
class CompressionAnalysis:
    """Compression analysis results for a table."""

    table_name: str
    current_compression: CompressionType
    current_size_kb: float
    row_count: int
    analyzed_at: datetime = field(default_factory=datetime.now)

    # Estimated sizes with different compression types
    none_size_kb: Optional[float] = None
    row_size_kb: Optional[float] = None
    page_size_kb: Optional[float] = None
    columnstore_size_kb: Optional[float] = None

    def get_savings_percent(
        self, compression_type: CompressionType
    ) -> Optional[float]:
        """Calculate potential savings percentage for a compression type."""
        type_map = {
            CompressionType.NONE: self.none_size_kb,
            CompressionType.ROW: self.row_size_kb,
            CompressionType.PAGE: self.page_size_kb,
            CompressionType.COLUMNSTORE: self.columnstore_size_kb,
        }

        new_size = type_map.get(compression_type)
        if new_size is None or self.current_size_kb == 0:
            return None

        return ((self.current_size_kb - new_size) / self.current_size_kb) * 100.0


@dataclass
class CompressionRecommendation:
    """Compression recommendation for a table."""

    table_name: str
    current_compression: CompressionType
    recommended_compression: CompressionType
    current_size_mb: float
    estimated_size_mb: float
    estimated_savings_mb: float
    estimated_savings_percent: float
    reason: str
    priority: str = "medium"  # low, medium, high

    def should_apply(self) -> bool:
        """Determine if recommendation should be applied."""
        return (
            self.recommended_compression != self.current_compression
            and self.estimated_savings_percent > 10.0
        )


# ============ DBA Analysis Models ============


@dataclass
class ConnectionSource:
    """Represents a unique connection source to the database."""

    program_name: str
    host_name: str
    login_name: str
    session_count: int = 0
    active_requests: int = 0
    idle_connections: int = 0
    total_cpu_ms: int = 0
    total_reads: int = 0
    total_writes: int = 0
    total_memory_kb: int = 0
    open_transactions: int = 0
    longest_transaction_seconds: float = 0.0
    blocked_count: int = 0
    blocking_count: int = 0

    def get_display_name(self) -> str:
        """Get a display-friendly source name."""
        return f"{self.program_name or 'Unknown'} ({self.host_name or 'Unknown'})"

    def get_resource_score(self) -> float:
        """Calculate a resource cost score (higher = more expensive)."""
        # Weighted scoring: CPU is most important, then I/O, then memory
        cpu_score = self.total_cpu_ms / 1000.0  # Convert to seconds
        io_score = (self.total_reads + self.total_writes) / 10000.0
        memory_score = self.total_memory_kb / 1024.0  # Convert to MB
        lock_score = (self.blocked_count + self.blocking_count * 2) * 10

        return cpu_score + io_score + memory_score + lock_score


@dataclass
class QueryPattern:
    """Represents a query pattern from a specific source."""

    query_hash: str
    query_text: str
    source_program: str
    source_host: str
    execution_count: int = 0
    total_worker_time_ms: int = 0
    total_elapsed_time_ms: int = 0
    total_logical_reads: int = 0
    total_logical_writes: int = 0
    total_physical_reads: int = 0
    avg_worker_time_ms: float = 0.0
    avg_elapsed_time_ms: float = 0.0
    avg_logical_reads: float = 0.0
    last_execution_time: Optional[datetime] = None
    plan_count: int = 1

    def get_truncated_query(self, max_length: int = 200) -> str:
        """Get truncated query text for display."""
        if len(self.query_text) <= max_length:
            return self.query_text
        return self.query_text[:max_length] + "..."

    def get_cost_score(self) -> float:
        """Calculate query cost score."""
        return (
            self.total_worker_time_ms / 1000.0
            + self.total_logical_reads / 1000.0
            + self.total_physical_reads / 100.0
        )

    def is_expensive(self) -> bool:
        """Check if query is considered expensive."""
        return (
            self.avg_worker_time_ms > 1000  # > 1 second CPU
            or self.avg_logical_reads > 10000  # > 10K reads
            or self.avg_elapsed_time_ms > 5000  # > 5 seconds elapsed
        )


@dataclass
class BlockingInfo:
    """Represents blocking information between sessions."""

    blocking_session_id: int
    blocked_session_id: int
    blocking_program: str
    blocking_host: str
    blocked_program: str
    blocked_host: str
    wait_type: str
    wait_time_ms: int
    wait_resource: str
    blocking_query: Optional[str] = None
    blocked_query: Optional[str] = None

    def get_wait_time_seconds(self) -> float:
        """Get wait time in seconds."""
        return self.wait_time_ms / 1000.0


@dataclass
class LockInfo:
    """Represents lock information for a session."""

    session_id: int
    program_name: str
    host_name: str
    resource_type: str
    request_mode: str
    request_status: str
    resource_description: str
    lock_count: int = 1


@dataclass
class SystemScorecard:
    """Scorecard for a connecting system."""

    system_name: str
    host_name: str
    login_name: str

    # Connection metrics
    total_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    connection_pool_efficiency: float = 0.0

    # Resource metrics
    cpu_cost_seconds: float = 0.0
    io_reads: int = 0
    io_writes: int = 0
    memory_mb: float = 0.0
    lock_wait_seconds: float = 0.0
    deadlock_count: int = 0

    # Query metrics
    total_queries: int = 0
    distinct_query_patterns: int = 0
    expensive_queries: int = 0
    redundant_queries: int = 0

    # Blocking metrics
    times_blocked: int = 0
    times_blocking: int = 0
    avg_block_duration_ms: float = 0.0

    # Overall score (higher = more problematic)
    resource_score: float = 0.0
    rank: int = 0

    def calculate_score(self) -> None:
        """Calculate overall resource score."""
        self.resource_score = (
            self.cpu_cost_seconds * 10
            + (self.io_reads + self.io_writes) / 10000
            + self.memory_mb
            + self.lock_wait_seconds * 5
            + self.deadlock_count * 100
            + self.times_blocking * 20
            + self.expensive_queries * 5
        )


@dataclass
class RedundancyFinding:
    """Represents a redundancy finding across systems."""

    query_pattern: str
    systems_involved: list[str] = field(default_factory=list)
    total_executions: int = 0
    potential_savings_percent: float = 0.0
    recommendation: str = ""
    severity: str = "medium"  # low, medium, high


@dataclass
class DBAAnalysisResult:
    """Complete DBA analysis result."""

    analyzed_at: datetime = field(default_factory=datetime.now)
    server_name: str = ""
    database_name: str = ""

    # Connection analysis
    connection_sources: list[ConnectionSource] = field(default_factory=list)
    total_connections: int = 0
    total_active: int = 0
    total_idle: int = 0
    connection_issues: list[str] = field(default_factory=list)

    # Query analysis
    query_patterns: list[QueryPattern] = field(default_factory=list)
    top_expensive_queries: list[QueryPattern] = field(default_factory=list)

    # Blocking analysis
    current_blocking: list[BlockingInfo] = field(default_factory=list)
    blocking_hotspots: list[str] = field(default_factory=list)

    # System scorecards
    system_scorecards: list[SystemScorecard] = field(default_factory=list)

    # Redundancy findings
    redundancy_findings: list[RedundancyFinding] = field(default_factory=list)

    # Recommendations
    recommendations: list[str] = field(default_factory=list)

    def get_summary(self) -> dict[str, Any]:
        """Get analysis summary."""
        return {
            "total_systems": len(self.connection_sources),
            "total_connections": self.total_connections,
            "active_connections": self.total_active,
            "idle_connections": self.total_idle,
            "blocking_chains": len(self.current_blocking),
            "expensive_queries": len(self.top_expensive_queries),
            "redundancy_issues": len(self.redundancy_findings),
            "recommendations": len(self.recommendations),
        }
