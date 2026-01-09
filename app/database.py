import pyodbc
from sqlalchemy import create_engine
import urllib
from config import Config
from datetime import datetime, date

# --- NEW: Create a Global Connection Pool ---
# This runs once when the app starts, not every request.
params = urllib.parse.quote_plus(
    f"DRIVER={Config.DRIVER};"
    f"SERVER={Config.SERVER};"
    f"PORT=1433;"
    f"DATABASE={Config.DATABASE};"
    f"UID={Config.USERNAME};"
    f"PWD={Config.PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

# SQLAlchemy connection string format for pyodbc
db_connection_str = f"mssql+pyodbc:///?odbc_connect={params}"

# Create the engine with pooling enabled
engine = create_engine(
    db_connection_str,
    pool_size=10,        # Keep 10 connections ready in the pool
    max_overflow=20,     # Allow up to 20 surge connections
    pool_timeout=30,     # Wait 30s before failing
    pool_recycle=1800    # Recycle connections every 30 mins to prevent stale errors
)

def get_db_connection():
    """
    Returns a raw pyodbc connection from the SQLAlchemy pool.
    This is much faster than creating a new connection every time.
    """
    try:
        # engine.raw_connection() returns a proxy that behaves exactly like a pyodbc connection
        return engine.raw_connection()
    except Exception as e:
        print(f"Database Connection Error: {e}")
        return None

def row_to_dict(cursor, row):
    result_dict = {}
    for i, column_description in enumerate(cursor.description):
        column_name = column_description[0]
        value = row[i]
        if column_name.upper() in ('DOJ', 'TODAY'):
            if not isinstance(value, (datetime, date)):
                value = None
        
        is_numeric = column_description[1] in (
            pyodbc.SQL_INTEGER, pyodbc.SQL_BIGINT, pyodbc.SQL_SMALLINT,
            pyodbc.SQL_TINYINT, pyodbc.SQL_DECIMAL, pyodbc.SQL_NUMERIC,
            pyodbc.SQL_FLOAT, pyodbc.SQL_REAL
        )

        if value is None and is_numeric:
            result_dict[column_name] = 0
        else:
            result_dict[column_name] = value

    return result_dict