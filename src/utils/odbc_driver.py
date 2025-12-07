"""ODBC driver detection and configuration utilities."""

import os
import re
from functools import lru_cache
from typing import Optional

from src.core.logging import get_logger

logger = get_logger(__name__)

# Known SQL Server ODBC driver names in order of preference
KNOWN_SQL_SERVER_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "ODBC Driver 11 for SQL Server",
    "SQL Server Native Client 11.0",
    "SQL Server Native Client 10.0",
    "SQL Server",
]


def _get_drivers_from_odbcinst() -> list[str]:
    """Get ODBC drivers from odbcinst.ini file."""
    drivers = []

    # Standard locations for odbcinst.ini
    odbc_paths = [
        "/etc/odbcinst.ini",
        "/usr/local/etc/odbcinst.ini",
        os.path.expanduser("~/.odbcinst.ini"),
    ]

    # Also check ODBCSYSINI environment variable
    odbcsysini = os.environ.get("ODBCSYSINI")
    if odbcsysini:
        odbc_paths.insert(0, os.path.join(odbcsysini, "odbcinst.ini"))

    for path in odbc_paths:
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    content = f.read()
                    # Parse section headers [DriverName]
                    matches = re.findall(r"^\[([^\]]+)\]", content, re.MULTILINE)
                    drivers.extend(matches)
            except Exception as e:
                logger.debug(f"Could not read {path}: {e}")

    return drivers


def _get_drivers_from_pyodbc() -> list[str]:
    """Get ODBC drivers using pyodbc."""
    try:
        import pyodbc
        return list(pyodbc.drivers())
    except Exception as e:
        logger.debug(f"Could not get drivers from pyodbc: {e}")
        return []


@lru_cache(maxsize=1)
def get_available_drivers() -> list[str]:
    """
    Get list of available ODBC drivers.

    Returns:
        List of available ODBC driver names
    """
    drivers = set()

    # Try pyodbc first (most reliable)
    drivers.update(_get_drivers_from_pyodbc())

    # Also try reading from odbcinst.ini
    drivers.update(_get_drivers_from_odbcinst())

    return list(drivers)


def find_sql_server_driver() -> Optional[str]:
    """
    Find the best available SQL Server ODBC driver.

    Returns:
        Driver name if found, None otherwise
    """
    available = get_available_drivers()

    if not available:
        logger.warning("No ODBC drivers found on the system")
        return None

    logger.debug(f"Available ODBC drivers: {available}")

    # Look for known drivers in order of preference
    for driver in KNOWN_SQL_SERVER_DRIVERS:
        if driver in available:
            logger.debug(f"Found SQL Server driver: {driver}")
            return driver

    # Check for any driver with "SQL Server" in the name
    for driver in available:
        if "sql server" in driver.lower():
            logger.debug(f"Found SQL Server driver by pattern: {driver}")
            return driver

    return None


@lru_cache(maxsize=1)
def get_odbc_driver() -> str:
    """
    Get the ODBC driver to use for SQL Server connections.

    Order of precedence:
    1. ODBC_DRIVER environment variable
    2. Auto-detected best available driver
    3. Default "ODBC Driver 18 for SQL Server" (may fail if not installed)

    Returns:
        ODBC driver name (without curly braces)

    Raises:
        RuntimeError: If no driver is found and none specified
    """
    # Check environment variable first
    env_driver = os.environ.get("ODBC_DRIVER")
    if env_driver:
        # Strip curly braces if present
        driver = env_driver.strip("{}")
        logger.info(f"Using ODBC driver from environment: {driver}")
        return driver

    # Try to auto-detect
    detected_driver = find_sql_server_driver()
    if detected_driver:
        logger.info(f"Auto-detected ODBC driver: {detected_driver}")
        return detected_driver

    # Fall back to default (may fail if not installed)
    default_driver = "ODBC Driver 18 for SQL Server"
    logger.warning(
        f"No ODBC driver detected. Falling back to default: {default_driver}. "
        "If connection fails, install an ODBC driver or set ODBC_DRIVER environment variable."
    )
    return default_driver


def get_odbc_driver_string() -> str:
    """
    Get the ODBC driver string formatted for connection strings.

    Returns:
        Driver name wrapped in curly braces, e.g., "{ODBC Driver 18 for SQL Server}"
    """
    return "{" + get_odbc_driver() + "}"


def validate_driver_available(driver: Optional[str] = None) -> tuple[bool, str]:
    """
    Validate that an ODBC driver is available.

    Args:
        driver: Specific driver to check. If None, checks the configured driver.

    Returns:
        Tuple of (is_valid, message)
    """
    if driver is None:
        driver = get_odbc_driver()

    available = get_available_drivers()

    if not available:
        return False, (
            "No ODBC drivers found on the system. "
            "Please install the Microsoft ODBC Driver for SQL Server. "
            "See: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
        )

    # Strip braces for comparison
    driver_name = driver.strip("{}")

    if driver_name in available:
        return True, f"Driver '{driver_name}' is available"

    # Check if any SQL Server driver is available
    sql_server_drivers = [d for d in available if "sql server" in d.lower()]
    if sql_server_drivers:
        return False, (
            f"Driver '{driver_name}' not found. "
            f"Available SQL Server drivers: {sql_server_drivers}. "
            f"Set ODBC_DRIVER environment variable to use one of these."
        )

    return False, (
        f"Driver '{driver_name}' not found. "
        f"Available drivers: {available}. "
        "Please install the Microsoft ODBC Driver for SQL Server."
    )
