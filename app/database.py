import pyodbc
from config import Config
from datetime import datetime, date

def get_db_connection():
    try:
        # Check if Username and Password are set (Azure / Production Mode)
        # We use getattr to safely check if these attributes exist in your Config
        username = getattr(Config, 'USERNAME', None)
        password = getattr(Config, 'PASSWORD', None)

        if username and password:
            # Azure SQL Connection String
            connection_string = (
                f"DRIVER={Config.DRIVER};"
                f"SERVER={Config.SERVER};"
                f"DATABASE={Config.DATABASE};"
                f"UID={username};"
                f"PWD={password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=yes;"  # Set to yes if you get certificate errors during testing
                f"Connection Timeout=30;"
            )
        else:
            # Local Windows Authentication (Development Mode)
            connection_string = (
                f"DRIVER={Config.DRIVER};"
                f"SERVER={Config.SERVER};"
                f"DATABASE={Config.DATABASE};"
                f"Trusted_Connection=yes;"
            )

        conn = pyodbc.connect(connection_string)
        return conn

    except pyodbc.Error as ex:
        # Check if args exists before accessing index 0 to avoid index errors on some drivers
        sqlstate = ex.args[0] if ex.args else "Unknown"
        print(f"ERROR: Database connection failed.")
        print(f"SQLSTATE: {sqlstate}")
        print(ex)
        return None


def row_to_dict(cursor, row):
    """
    Helper to convert SQL rows into a clean Python dictionary.
    Handles numeric NULLs by converting them to 0.
    """
    result_dict = {}
    for i, column_description in enumerate(cursor.description):
        column_name = column_description[0]
        value = row[i]
        
        # Date Handling safety check
        if column_name.upper() in ('DOJ', 'TODAY'):
            if not isinstance(value, (datetime, date)):
                value = None
        
        # Check if the column type is numeric
        is_numeric = column_description[1] in (
            pyodbc.SQL_INTEGER, pyodbc.SQL_BIGINT, pyodbc.SQL_SMALLINT,
            pyodbc.SQL_TINYINT, pyodbc.SQL_DECIMAL, pyodbc.SQL_NUMERIC,
            pyodbc.SQL_FLOAT, pyodbc.SQL_REAL
        )

        # Convert None (NULL) to 0 for numeric columns (Dashboard logic requirement)
        if value is None and is_numeric:
            result_dict[column_name] = 0
        else:
            result_dict[column_name] = value

    return result_dict