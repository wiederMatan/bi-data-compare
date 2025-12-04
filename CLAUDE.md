# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BI Data Compare is a SQL Server data comparison tool built with Python and Streamlit. It compares tables between source and target SQL Server databases, identifying schema differences and data discrepancies.

## Commands

### Run with Docker (Recommended)
```bash
docker compose up -d --build
```
The app runs at `http://localhost:8503`

### Run Locally
```bash
source venv/bin/activate
streamlit run src/ui/app.py
```

### Run Tests
```bash
pytest                              # All tests with coverage
pytest tests/unit/test_models.py   # Single test file
pytest -m unit                      # Unit tests only
pytest -m integration               # Integration tests only
```

### Code Quality
```bash
black src tests                  # Format code
isort src tests                  # Sort imports
flake8 src tests                 # Lint
mypy src                         # Type check
```

## Architecture

The application follows Clean Architecture with four layers:

```
src/
├── ui/          # Presentation - Streamlit pages and components
├── services/    # Application - Business logic (comparison, export)
├── data/        # Domain - Models, repositories, database connections
├── core/        # Infrastructure - Config, logging, exceptions
└── utils/       # Cross-cutting - Formatters, validators, security
```

### Key Components

**Comparison Flow:**
`UI → ComparisonService → MetadataRepository/TableDataRepository → DatabaseConnection → Results`

**Connection Management:**
- Connections are cached globally via `get_cached_connection()` in `src/data/database.py`
- Avoids duplicate connect/disconnect cycles
- Use `clear_connection_cache()` to reset

**Table Selection Rules:**
- Dim/stg/mrr tables: Can select multiple
- Fact/Link tables: Can only select ONE at a time (no mixing with other tables)

**Incremental Comparison:**
- Available for fact tables only
- Select a date column to compare max values between source and target

### Core Files
- `src/services/comparison.py` - Main comparison logic (runs sequentially, not parallel)
- `src/data/models.py` - All data models (`ComparisonResult`, `TableInfo`, `ColumnInfo`, etc.)
- `src/data/database.py` - Database connection management with caching
- `src/data/repositories.py` - Data access layer
- `src/ui/pages/2_Comparison.py` - Main comparison UI with log viewer
- `src/ui/pages/4_Drill_Down.py` - EXCEPT and row-by-row comparison

### Configuration
- Environment variables via `.env` file
- YAML configuration in `config/config.yaml`
- Settings singleton accessed via `get_settings()` from `src/core/config`

### Docker Setup
- `docker-compose.yml` - Defines app + 2 SQL Server containers (dev/qa)
- App container: Port 8503 (external) → 8502 (internal)
- SQL Server dev: Port 1434 (external) → 1433 (internal)
- SQL Server qa: Port 1433

## Prerequisites
- Python 3.9+
- ODBC Driver 18 for SQL Server
- Docker (for containerized setup)
