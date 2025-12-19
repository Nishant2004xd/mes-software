import pandas as pd
import numpy as np
import pyodbc

# ==========================================
# 1. READ EXCEL & FIND HEADER
# ==========================================

file_path = 'WIP_Ageing (92).xlsx'  # REPLACE with your actual file path

print("Reading Excel file...")

# Read blindly first to find the header row
df_temp = pd.read_excel(file_path, sheet_name='TotalWIP', header=None, engine='openpyxl')

header_row_index = None
for index, row in df_temp.iterrows():
    if row.astype(str).str.contains('ItemCode', case=False).any():
        header_row_index = index
        break

if header_row_index is None:
    raise ValueError("Could not find a row with 'ItemCode' in the Excel file.")

print(f"Header found at row index: {header_row_index}")

# Load data with correct header
df = pd.read_excel(file_path, sheet_name='TotalWIP', header=header_row_index, engine='openpyxl')
df.columns = df.columns.str.strip()  # Remove hidden spaces

# Group by ItemCode and sum WIP Qty
grouped_df = df.groupby('ItemCode').agg({'WIP Qty': 'sum'}).reset_index()

# ==========================================
# 2. PREPARE DATA (SPLIT LOGIC)
# ==========================================

db_stages = [
    'Long seam', 'Dish fit up', 'Cirseam welding', 'Part assembly',
    'Full welding', 'Hydro Testing', 'Powder coating', 'PDI'
]


def split_quantity_randomly(total_qty, n_buckets):
    if total_qty <= 0: return [0] * n_buckets
    return np.random.multinomial(total_qty, [1 / n_buckets] * n_buckets)


update_data = []

for index, row in grouped_df.iterrows():
    item_code = row['ItemCode']
    total_inv = int(row['WIP Qty'])

    splits = split_quantity_randomly(total_inv, len(db_stages))

    row_data = {'ItemCode': item_code, 'Total Inv': total_inv}
    for i, stage in enumerate(db_stages):
        row_data[stage] = int(splits[i])

    update_data.append(row_data)

print(f"Processed {len(update_data)} items. Connecting to Database...")

# ==========================================
# 3. UPDATE SQL DATABASE
# ==========================================

# *** UPDATED SERVER CONFIGURATION ***
# We use an 'r' string or double backslashes for the server name
server = r'DESKTOP-29MHU7D\MSSQLSERVER01'
database = 'ELGI_Dash'

# Driver detection
drivers = [d for d in pyodbc.drivers()]
if 'ODBC Driver 17 for SQL Server' in drivers:
    driver = 'ODBC Driver 17 for SQL Server'
else:
    driver = 'SQL Server'

connection_string = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    print(f"Connecting to {server}...")
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    print("SUCCESS! Connected.")

    for row in update_data:
        # Dynamic Update Query
        set_clauses = ["[Total Inv] = ?"]
        params = [row['Total Inv']]

        for stage in db_stages:
            set_clauses.append(f"[{stage}] = ?")
            params.append(row[stage])

        set_query = ", ".join(set_clauses)

        query = f"UPDATE [dbo].[master] SET {set_query} WHERE [Item code] = ?"
        params.append(row['ItemCode'])

        cursor.execute(query, params)

    conn.commit()
    print("Database updated successfully!")

except Exception as e:
    print(f"\nERROR: {e}")
    print("-" * 30)
    print("CHECK:")
    print(f"1. Does the database '{database}' exist on instance '{server}'?")
    print("2. Did you run the permission script on THIS specific instance?")

finally:
    if 'conn' in locals():
        conn.close()