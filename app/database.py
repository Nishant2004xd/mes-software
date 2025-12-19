import pyodbc
from config import Config
from datetime import datetime, date

def get_db_connection():
    try:
        connection_string = (
            f"DRIVER={Config.DRIVER};"
            f"SERVER={Config.SERVER};"
            f"DATABASE={Config.DATABASE};"
            f"Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(connection_string)
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"ERROR: Database connection failed.")
        print(f"SQLSTATE: {sqlstate}")
        print(ex)
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