# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BI Data Compare is a SQL Server data comparison and compression tool built with Python and Streamlit. It compares tables between source and target SQL Server databases, identifying schema differences and data discrepancies.

## Commands

### Run the Application
```bash
source venv/bin/activate
streamlit run src/ui/app.py
```
The app runs at `http://localhost:8501`

### Run Tests
```bash
# All tests with coverage
pytest

# Single test file
pytest tests/unit/test_models.py

# Specific test
pytest tests/unit/test_models.py::test_function_name

# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration
```

### Code Quality
```bash
black src tests                  # Format code
isort src tests                  # Sort imports
flake8 src tests                 # Lint
mypy src                         # Type check
pylint src                       # Additional linting
```

## Architecture

The application follows Clean Architecture with four layers:

```
src/
├── ui/          # Presentation - Streamlit pages and components
├── services/    # Application - Business logic (comparison, compression, export)
├── data/        # Domain - Models, repositories, database connections
├── core/        # Infrastructure - Config, logging, exceptions
└── utils/       # Cross-cutting - Formatters, validators, security
```

### Key Components

**Comparison Flow:**
`UI → ComparisonService → MetadataRepository/TableDataRepository → DatabaseConnection → Results`

**Comparison Modes:**
- `QUICK`: Checksum-based, fast, doesn't show specific differences
- `STANDARD`: Row-by-row, shows which rows/columns differ
- `DEEP`: Complete audit (indexes/constraints planned but not implemented)

### Core Files
- `src/services/comparison.py` - Main comparison logic with `ComparisonService`
- `src/data/models.py` - All data models (`ComparisonResult`, `TableInfo`, `ColumnInfo`, etc.)
- `src/data/database.py` - Database connection management with SQLAlchemy
- `src/data/repositories.py` - Data access layer (`MetadataRepository`, `TableDataRepository`)
- `src/core/config.py` - Pydantic-based configuration with YAML support

### Configuration
- Environment variables via `.env` file
- YAML configuration in `config/config.yaml`
- Settings singleton accessed via `get_settings()` from `src/core/config`

### Data Processing
- Large tables processed in configurable chunks (`chunk_size` setting, default 10000)
- Multiple tables compared in parallel using `ThreadPoolExecutor` (`max_workers` setting)
- Supports chunked data export for memory efficiency

## Prerequisites
- Python 3.9+
- ODBC Driver 17 for SQL Server
- SQL Server database access
