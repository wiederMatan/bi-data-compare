# BI Data Compare

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/wiederMatan/bi-data-compare)

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

## Deploy to Render (One-Click)

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/wiederMatan/bi-data-compare)

Click the button above to deploy instantly to Render's free tier.

## Deploy to Streamlit Cloud

1. **Fork/Push** this repository to your GitHub account

2. **Go to** [share.streamlit.io](https://share.streamlit.io)

3. **Click** "New app" and select:
   - Repository: `your-username/bi-data-compare`
   - Branch: `main`
   - Main file path: `src/ui/app.py`

4. **Configure Secrets** in Advanced Settings:
   ```toml
   [database]
   source_server = "your-server.database.windows.net"
   source_database = "your_db"
   source_username = "username"
   source_password = "password"
   ```

5. **Deploy** - The app will be live at `your-app.streamlit.app`

See `.streamlit/secrets.toml.example` for full secrets configuration.

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
