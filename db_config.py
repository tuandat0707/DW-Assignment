"""
db_config.py
Connection config tập trung cho SQL Server Docker
Container: flight-dw-sqlserver | Port: 1433
"""
import pyodbc

DB_CONFIG = {
    "server": "127.0.0.1",
    "port": 1433,
    "database": "DataWarehouse",
    "username": "SA",
    "password": "FlightDW@2024",
}

SCHEMA = "DataWarehouse"

# Auto-detect ODBC driver
_DRIVERS_TO_TRY = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server",
]

def _detect_driver():
    available = [d for d in pyodbc.drivers() if "SQL Server" in d]
    for preferred in _DRIVERS_TO_TRY:
        if preferred in available:
            return preferred
    if available:
        return available[0]
    raise RuntimeError(f"No SQL Server ODBC driver found. Available: {pyodbc.drivers()}")

_DRIVER = _detect_driver()

def get_connection_string(database=None):
    db = database or DB_CONFIG["database"]
    return (
        f"mssql+pyodbc://{DB_CONFIG['username']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['server']},{DB_CONFIG['port']}"
        f"/{db}"
        f"?driver={_DRIVER.replace(' ', '+')}"
        f"&TrustServerCertificate=yes"
        f"&Encrypt=no"
    )

CONNECTION_STRING = get_connection_string()

