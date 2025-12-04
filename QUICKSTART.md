# Quick Start Guide

Get up and running with the SQL Server Data Comparison Tool in 5 minutes!

## Prerequisites

- Python 3.9 or higher installed
- SQL Server database access (source and target)
- ODBC Driver 17 for SQL Server ([Download here](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server))


# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Run the Application

```bash
streamlit run src/ui/app.py
```

The app will open in your browser at `http://localhost:8501`


### Issue: "Memory error with large table"

**Solutions**:
- Reduce chunk_size in Advanced Options (try 5000 instead of 10000)
- Compare fewer tables at once
- Use Quick mode instead of Standard
- Close other applications to free memory

### Issue: "Comparison is slow"

**Solutions**:
- Use Quick mode for initial check
- Reduce number of tables
- Increase max_workers if you have more CPU cores
- Ensure good network connection to database servers
- Add indexes to primary key columns

**Happy comparing! 🎉**
