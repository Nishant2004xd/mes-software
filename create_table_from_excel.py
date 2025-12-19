import pandas as pd
import pyodbc
import re

# --- Configuration ---
# IMPORTANT: Update this path to where your Excel file is located.
EXCEL_FILE_PATH = 'updated_1.xlsx'
SHEET_TO_IMPORT = 'shift and station details'

# --- Database Details ---
SERVER_NAME = 'localhost\\MSSQLSERVER01'
DATABASE_NAME = 'ELGI_Dash'
# The new table will be named based on the sheet name, but cleaned up.
TABLE_NAME = re.sub(r'[^a-zA-Z0-9_]', '', SHEET_TO_IMPORT.replace(' ', '_'))


# --- Helper Function to Determine SQL Data Types ---
def get_sql_type(dtype):
    """Maps pandas data types to appropriate SQL Server data types."""
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME2'
    else:
        # Default to NVARCHAR(MAX) for strings or any other complex type
        return 'NVARCHAR(MAX)'


# --- Main Script Logic ---
print(f"--- Starting Import for sheet: '{SHEET_TO_IMPORT}' ---")

try:
    # --- Part 1: Read the specific Excel sheet ---
    print(f"\n[Step 1] Reading '{SHEET_TO_IMPORT}' from '{EXCEL_FILE_PATH}'...")
    df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=SHEET_TO_IMPORT)

    # Clean column names for SQL compatibility
    original_columns = df.columns
    df.columns = [re.sub(r'[^a-zA-Z0-9_]', '', col.replace(' ', '_')) for col in original_columns]
    print("Cleaned column names for the database table.")

    # --- Part 2: Connect to the database and create the table ---
    conn_db_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};Trusted_Connection=yes;'

    with pyodbc.connect(conn_db_str) as conn:
        cursor = conn.cursor()
        print(f"\n[Step 2] Connected to database '{DATABASE_NAME}'.")

        # Drop the table if it already exists to ensure a fresh, clean import
        print(f"Checking for existing table '{TABLE_NAME}'...")
        cursor.execute(f"IF OBJECT_ID('dbo.{TABLE_NAME}', 'U') IS NOT NULL DROP TABLE dbo.{TABLE_NAME};")
        print(f"Old table '{TABLE_NAME}' (if any) has been removed.")

        # Dynamically generate the CREATE TABLE query from the DataFrame's structure
        column_definitions = []
        for col_name, dtype in df.dtypes.items():
            sql_type = get_sql_type(dtype)
            column_definitions.append(f"[{col_name}] {sql_type}")

        create_table_query = f"CREATE TABLE {TABLE_NAME} ({', '.join(column_definitions)});"

        print("Creating new table with the following structure:")
        # print(create_table_query) # Uncomment to see the full CREATE query

        cursor.execute(create_table_query)
        print(f"Table '{TABLE_NAME}' created successfully.")

        # --- Part 3: Insert the data ---
        print(f"\n[Step 3] Preparing to insert {len(df)} rows...")
        # Replace pandas' NaN (Not a Number) with None for SQL NULL compatibility
        df = df.where(pd.notnull(df), None)

        # Prepare the insert statement for efficient bulk insertion
        columns = ', '.join([f'[{col}]' for col in df.columns])
        placeholders = ', '.join(['?'] * len(df.columns))
        insert_sql = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"

        # Use cursor.fast_executemany for a massive performance improvement
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, df.values.tolist())
        conn.commit()
        print(f"Successfully inserted {cursor.rowcount} rows into '{TABLE_NAME}'.")

except FileNotFoundError:
    print(f"\n--- ERROR ---")
    print(f"The file was not found at the specified path: '{EXCEL_FILE_PATH}'")
    print("Please make sure the path is correct and the file exists.")
except ValueError as ve:
    if f"Worksheet named '{SHEET_TO_IMPORT}' not found" in str(ve):
        print(f"\n--- ERROR ---")
        print(f"A sheet named '{SHEET_TO_IMPORT}' was not found in the Excel file.")
        print("Please check the sheet name for typos and try again.")
    else:
        print(f"\n--- AN UNEXPECTED VALUE ERROR OCCURRED ---")
        print(f"Error details: {ve}")
except Exception as e:
    print(f"\n--- AN UNEXPECTED ERROR OCCURRED ---")
    print(f"Error details: {e}")

finally:
    print("\n--- Script finished. ---")
