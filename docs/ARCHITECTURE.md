## Architecture Overview

This document describes the architecture and design decisions of the SQL Server Data Comparison & Compression Tool.

## High-Level Architecture

The application follows **Clean Architecture** principles with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                     Presentation Layer                       │
│                    (Streamlit UI Pages)                      │
├─────────────────────────────────────────────────────────────┤
│                     Application Layer                        │
│              (Services: Comparison, Compression)             │
├─────────────────────────────────────────────────────────────┤
│                      Domain Layer                            │
│               (Models, Business Rules)                       │
├─────────────────────────────────────────────────────────────┤
│                  Infrastructure Layer                        │
│         (Repositories, Database, External Services)          │
└─────────────────────────────────────────────────────────────┘
```

## Layer Descriptions

### 1. Core Layer (`src/core/`)

Foundational components used across all layers:

- **Configuration**: Environment and YAML-based settings management
- **Exceptions**: Custom exception hierarchy for error handling
- **Logging**: Structured logging with file and console outputs

**Key Classes**:
- `Settings`: Pydantic-based configuration with validation
- `ApplicationError`: Base exception for all custom errors
- `setup_logging()`: Centralized logging configuration

### 2. Data Layer (`src/data/`)

Handles all data access and persistence:

**Models** (`models.py`):
- `ConnectionInfo`: Database connection parameters
- `TableInfo`: Table metadata and statistics
- `ColumnInfo`: Column schema information
- `ComparisonResult`: Comparison execution results
- `CompressionAnalysis`: Compression analysis data

**Database** (`database.py`):
- `DatabaseConnection`: Single database connection management
- `DatabaseManager`: Multi-connection management with pooling

**Repositories** (`repositories.py`):
- `MetadataRepository`: Schema metadata operations
- `TableDataRepository`: Data querying and chunking
- `CompressionRepository`: Compression analysis operations

### 3. Services Layer (`src/services/`)

Business logic and orchestration:

**CompressionService** (`compression.py`):
- Analyzes compression opportunities
- Generates recommendations based on table characteristics
- Estimates space savings
- Applies compression settings

**ExportService** (`export.py`):
- Exports results to multiple formats (Excel, CSV, JSON, HTML)
- Generates formatted reports
- Handles large dataset exports

**SyncScriptGenerator** (`sync_script.py`):
- Generates SQL sync scripts (MERGE, INSERT, UPDATE, DELETE)
- Creates schema alteration scripts
- Produces executable SQL for synchronization

### 4. UI Layer (`src/ui/`)

Streamlit-based web interface:

**Pages**:
- `connection_page.py`: Database connection configuration
- `comparison_page.py`: Table selection and comparison execution
- `results_page.py`: Results visualization and export

**Components**:
- Modular, reusable UI components
- Real-time progress tracking
- Interactive charts and tables

### 5. Utils Layer (`src/utils/`)

Cross-cutting utilities:

- **Formatters**: Number, byte, duration formatting
- **Validators**: Input validation for SQL identifiers
- **Security**: Credential encryption and management

## Design Patterns

### Repository Pattern

Abstracts data access behind a clean interface:

```python
class MetadataRepository:
    def get_tables(self, schema: str) -> list[TableInfo]:
        # Implementation details hidden
        pass
```

Benefits:
- Decouples business logic from data access
- Easy to mock for testing
- Supports multiple data sources

### Factory Pattern

Used for creating comparison strategies:

```python
mode_map = {
    "Quick": ComparisonMode.QUICK,
    "Standard": ComparisonMode.STANDARD,
    "Deep": ComparisonMode.DEEP,
}
```

### Strategy Pattern

Different comparison algorithms based on mode:

- **Quick**: Checksum-based comparison
- **Standard**: Row-by-row comparison
- **Deep**: Complete analysis with indexes

### Singleton Pattern

Configuration management:

```python
@lru_cache()
def get_settings() -> Settings:
    return Settings()
```

## Data Flow

### Comparison Flow

```
User Request → UI Page → ComparisonService
                              ↓
                    MetadataRepository ← DatabaseConnection
                              ↓
                    TableDataRepository ← DatabaseConnection
                              ↓
                    ComparisonResult → ExportService → Output Files
```

### Compression Flow

```
User Request → UI Page → CompressionService
                              ↓
                    CompressionRepository ← DatabaseConnection
                              ↓
                    CompressionAnalysis → Recommendations
                              ↓
                    SQL Scripts / Applied Changes
```

## Database Interaction

### Connection Management

- **Connection Pooling**: SQLAlchemy with configurable pool size
- **Auto-reconnect**: Pre-ping to detect stale connections
- **Timeout Handling**: Configurable connection and command timeouts
- **Context Managers**: Automatic cleanup and connection release

### Query Execution

- **Parameterized Queries**: Protection against SQL injection
- **Chunked Reading**: Memory-efficient large dataset processing
- **Parallel Execution**: Multiple tables compared concurrently
- **Transaction Management**: Proper rollback on errors

## Performance Optimizations

### Chunked Processing

Large tables processed in configurable chunks:

```python
for chunk in get_data_chunked(schema, table, chunk_size=10000):
    process_chunk(chunk)
```

Benefits:
- Constant memory usage
- Progress tracking
- Graceful handling of large datasets

### Parallel Processing

Multiple tables compared concurrently:

```python
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(compare, table) for table in tables]
```

### Lazy Loading

Metadata loaded on-demand:

```python
table_info.columns  # Loaded when accessed
table_info.indexes  # Loaded when accessed
```

### Caching

Configuration and metadata cached:

```python
@lru_cache()
def get_settings() -> Settings:
    # Cached after first call
```

## Security Considerations

### Credential Management

- Environment variables for sensitive data
- Fernet encryption for stored credentials
- No plaintext passwords in logs
- Masked password display

### SQL Injection Prevention

- Parameterized queries exclusively
- Input validation before execution
- No dynamic SQL construction from user input

### Session Security

- Timeout-based session expiration
- Secure credential storage in session state
- No credentials persisted to disk

## Testing Strategy

### Unit Tests

- Isolated component testing
- Mock dependencies
- 80%+ code coverage target

### Integration Tests

- Database interaction testing
- End-to-end comparison scenarios
- Performance benchmarking

### Test Fixtures

Reusable test data:

```python
@pytest.fixture
def sample_table_info():
    return TableInfo(
        schema_name="dbo",
        table_name="Users",
        row_count=1000,
    )
```

## Extensibility

### Adding New Comparison Modes

1. Add enum value to `ComparisonMode`
2. Implement comparison logic in `ComparisonService`
3. Update UI mode selector
4. Add tests

### Adding New Export Formats

1. Create export method in `ExportService`
2. Add format to configuration
3. Update UI export options
4. Add tests

### Adding New Database Support

1. Create database-specific connection class
2. Implement repository interface
3. Update configuration
4. Add database-specific queries

## Deployment Architecture

### Standalone Deployment

```
User Browser → Streamlit App → SQL Server(s)
```

### Docker Deployment

```
User Browser → Docker Container (Streamlit) → External SQL Server(s)
```

### Multi-User Deployment

```
User Browsers → Load Balancer → Multiple Streamlit Instances → SQL Servers
```

## Configuration Management

### Environment-Based

```
Development → .env.development
Staging     → .env.staging
Production  → .env.production
```

### Hierarchical Configuration

1. Default values (in code)
2. YAML configuration file
3. Environment variables (override)
4. Command-line arguments (highest priority)

## Error Handling

### Exception Hierarchy

```
ApplicationError
├── ConfigurationError
├── ConnectionError
├── DatabaseError
├── ComparisonError
├── CompressionError
└── ExportError
```

### Error Propagation

1. Low-level exceptions caught and wrapped
2. Context added at each layer
3. User-friendly messages in UI
4. Detailed logs for debugging

## Future Enhancements

### Planned Features

- Multi-database support (PostgreSQL, MySQL)
- REST API for programmatic access
- Scheduled comparisons
- Email notifications
- Advanced filtering rules

### Scalability Improvements

- Distributed comparison workers
- Result streaming for very large datasets
- Incremental comparison (only changed tables)
- Comparison result caching
