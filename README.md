# BI Data Compare

SQL Server database comparison tool built with Python and Streamlit. Compare tables between source and target databases, identify schema differences, and analyze data discrepancies.

## Quick Start

### Run with Docker (Recommended)
```bash
docker compose up -d --build
```
Open http://localhost:8503

### Run Locally
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
streamlit run src/ui/app.py
```

## Features

- **Connection Management** - Connect to multiple SQL Server databases with cached connections
- **Table Comparison** - Compare schema and data between source and target
- **Drill-Down Analysis** - EXCEPT queries and row-by-row value comparison
- **Incremental Comparison** - Compare max date values for fact tables
- **Export Results** - Excel and HTML report generation
- **Sync Scripts** - Generate SQL scripts to synchronize differences

## Table Selection Rules

| Table Type | Selection |
|------------|-----------|
| dim_*, stg_*, mrr_*, dwh_* | Multiple tables allowed |
| fact_*, link_*, lnk_* | Only ONE table at a time |

## Comparison Modes

| Feature | QUICK | STANDARD | DEEP |
|---------|-------|----------|------|
| Speed | Fastest | Moderate | Slowest |
| Schema comparison | Yes | Yes | Yes |
| Data comparison | Checksum | Row-by-row | Row-by-row |
| Shows specific differences | No | Yes | Yes |
| Shows which rows differ | No | Yes | Yes |

## Docker Setup

The `docker-compose.yml` includes:
- **App**: Streamlit UI on port 8503
- **sqlserver-qa**: SQL Server on port 1433
- **sqlserver-dev**: SQL Server on port 1434

Default credentials:
- Username: `sa`
- Password: `YourStrong@Passw0rd`

## Project Structure

```
src/
├── ui/          # Streamlit pages
├── services/    # Business logic (comparison, export)
├── data/        # Models, repositories, database
├── core/        # Config, logging, exceptions
└── utils/       # Formatters, validators
```

## Requirements

- Python 3.9+
- ODBC Driver 18 for SQL Server
- Docker (for containerized setup)

## License

MIT
