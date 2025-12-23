import pandas as pd
import pyodbc
from datetime import datetime

# --- CONFIGURATION ---
FILE_NAME = '06 Despatch details on Dec.xlsx'
SHEET_NAME = 'Dec'
YEAR = 2025

# Database Connection Details
SERVER = 'DESKTOP-29MHU7D\\MSSQLSERVER01'
DATABASE = 'ELGI_Dash'
DRIVER = '{ODBC Driver 17 for SQL Server}'

def get_db_connection():
    try:
        connection_string = (
            f"DRIVER={DRIVER};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"Trusted_Connection=yes;"
        )
        return pyodbc.connect(connection_string)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def import_dispatch_log():
    # 1. Read the Excel File
    try:
        print(f"Reading '{SHEET_NAME}' sheet from {FILE_NAME}...")
        df = pd.read_excel(FILE_NAME, sheet_name=SHEET_NAME)
        
        # Clean column names (remove accidental spaces)
        df.columns = [str(c).strip() if isinstance(c, str) else c for c in df.columns]

        # Deduplicate columns (just in case)
        new_columns = []
        seen_columns = {}
        for col in df.columns:
            col_str = str(col)
            if col_str in seen_columns:
                seen_columns[col_str] += 1
                new_columns.append(f"{col_str}.{seen_columns[col_str]}")
            else:
                seen_columns[col_str] = 0
                new_columns.append(col)
        df.columns = new_columns

        # Find Item Code Column
        item_code_col = None
        for col in df.columns:
            if str(col).lower() == 'item code':
                item_code_col = col
                break
        
        if not item_code_col:
            print("Error: Could not find 'Item code' column.")
            return

        print(f"Loaded {len(df)} rows. Using '{item_code_col}' as Item Code.")

    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # 2. Identify Date Columns
    # Exclude standard info columns to find the dates
    info_columns = [str(item_code_col).lower(), 'description', 'vertical', 'model', 'category', 'type', 'total', 's.no']
    
    date_columns = []
    for col in df.columns:
        # Check if it looks like a date column (not in exclusion list and not unnamed)
        if str(col).lower() not in info_columns and not str(col).startswith('Unnamed'):
            date_columns.append(col)

    print(f"Found {len(date_columns)} potential date columns.")

    # 3. Transform Data for production_log
    log_records = []
    
    print("Processing data...")
    for index, row in df.iterrows():
        raw_code = row[item_code_col]
        if pd.isna(raw_code): continue
        item_code = str(raw_code).strip()
        
        for col in date_columns:
            raw_val = row[col]
            
            # Skip empty/zero values
            if pd.isna(raw_val) or raw_val == '': continue
            try:
                qty = int(raw_val)
                if qty <= 0: continue
            except (ValueError, TypeError):
                continue

            # Parse Date
            moved_at = None
            
            # Case A: Already a Timestamp
            if isinstance(col, (datetime, pd.Timestamp)):
                moved_at = col
                
            # Case B: String format (e.g. "01-Dec")
            elif isinstance(col, str):
                try:
                    clean_col_name = col.split('.')[0].strip() # Remove .1 suffix if exists
                    date_str = f"{clean_col_name}-{YEAR}"
                    # Create a datetime object (defaulting to midnight)
                    moved_at = datetime.strptime(date_str, "%d-%b-%Y")
                except ValueError:
                    continue
            
            if moved_at:
                # Append record: (item_code, quantity, from, to, moved_at)
                log_records.append((item_code, qty, 'FG', 'Dispatch', moved_at))

    if not log_records:
        print("No valid dispatch records found to insert.")
        return

    print(f"Prepared {len(log_records)} log entries for insertion.")

    # 4. Insert into Database
    conn = get_db_connection()
    if not conn: return

    cursor = conn.cursor()

    try:
        # Check if table exists (it should, based on your prompt, but good safety)
        cursor.execute("IF OBJECT_ID('production_log', 'U') IS NULL PRINT 'Error: production_log table missing'")

        print("Inserting records into production_log...")
        
        insert_sql = """
            INSERT INTO production_log (item_code, quantity, from_stage, to_stage, moved_at)
            VALUES (?, ?, ?, ?, ?)
        """
        
        # Batch insert
        cursor.executemany(insert_sql, log_records)
        
        conn.commit()
        print("------------------------------------------------")
        print(f"Success! Inserted {len(log_records)} dispatch records into production_log.")
        print("------------------------------------------------")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred during SQL execution: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    import_dispatch_log()