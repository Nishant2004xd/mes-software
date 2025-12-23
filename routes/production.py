from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, current_app
from app.database import get_db_connection, row_to_dict
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import os
import traceback
from werkzeug.utils import secure_filename
from math import ceil
import pyodbc

# --- Initialize Blueprint ---
production_bp = Blueprint('production', __name__)

# --- Constants & Helpers ---

DEFAULT_AHP_WEIGHTS = {'type': 0.5396, 'rpl': 0.2583, 'category': 0.1387, 'modified_plan': 0.0634}

def check_and_clear_daily_tables():
    conn = get_db_connection()
    if not conn: return

    try:
        cursor = conn.cursor()
        # Ensure status table exists
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='app_status' and xtype='U')
            CREATE TABLE app_status (
                status_key NVARCHAR(255) PRIMARY KEY,
                status_value NVARCHAR(255) 
            );
        """)
        conn.commit()

        # Determine Current Production Date (2 AM cutoff)
        now = datetime.now()
        if now.hour < 2:
            current_production_date = (now - timedelta(days=1)).date()
        else:
            current_production_date = now.date()

        current_production_date_str = current_production_date.strftime('%Y-%m-%d')

        # Check/Reset Pending Triggers
        cursor.execute("SELECT status_value FROM app_status WHERE status_key = 'last_triggers_upload_date'")
        row_triggers = cursor.fetchone()
        last_triggers_date_str = row_triggers[0] if row_triggers and row_triggers[0] else None

        needs_trigger_reset = True
        if last_triggers_date_str:
            try:
                last_triggers_date = datetime.strptime(last_triggers_date_str, '%Y-%m-%d').date()
                if last_triggers_date >= current_production_date:
                    needs_trigger_reset = False
            except ValueError:
                pass

        if needs_trigger_reset:
            cursor.execute("IF OBJECT_ID('dbo.pending_triggers', 'U') IS NOT NULL DELETE FROM pending_triggers")
            cursor.execute("DELETE FROM app_status WHERE status_key = 'last_triggers_upload_timestamp'")
            
            upsert_sql_triggers = """
                MERGE app_status AS target
                USING (SELECT 'last_triggers_upload_date' AS status_key, ? AS status_value) AS source
                ON (target.status_key = source.status_key)
                WHEN MATCHED THEN UPDATE SET status_value = source.status_value
                WHEN NOT MATCHED THEN INSERT (status_key, status_value) VALUES (source.status_key, source.status_value);
            """
            cursor.execute(upsert_sql_triggers, current_production_date_str)
            conn.commit()

    except Exception as e:
        print(f"Error during daily check/reset: {e}")
        traceback.print_exc()
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def replace_table_with_df(df, table_name, cursor):
    cursor.execute(f"IF OBJECT_ID('dbo.{table_name}', 'U') IS NOT NULL DROP TABLE dbo.{table_name}")
    df.columns = [f"[{col.strip()}]" for col in df.columns]
    
    for col in df.columns:
        if 'item code' in col.lower():
            df[col] = df[col].astype(str).str.strip().str.lstrip("'")
            break
            
    numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
    df[numeric_cols] = df[numeric_cols].fillna(0)
    df = df.replace({pd.NaT: None})
    
    dtype_mapping = {'int64': 'INT', 'float64': 'FLOAT', 'datetime64[ns]': 'DATETIME', 'object': 'NVARCHAR(MAX)'}
    col_definitions = []
    for col, dtype in df.dtypes.items():
        sql_type = dtype_mapping.get(str(dtype), 'NVARCHAR(MAX)')
        col_definitions.append(f"{col} {sql_type}")
        
    create_table_sql = f"CREATE TABLE {table_name} ({', '.join(col_definitions)})"
    cursor.execute(create_table_sql)
    
    if df.empty: return
    insert_sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['?' for _ in df.columns])})"
    params = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.executemany(insert_sql, params)

def prioritize_plan_with_ahp(df_plan, weights):
    def score_type(x):
        return 1 if str(x).strip().lower() == "lean" else 0

    def score_rpl(x):
        return 1 / (1 + float(x)) if pd.notnull(x) and x > 0 else 0

    def score_category(x):
        val = str(x).strip().lower()
        if val == "milk run": return 1.0
        elif val == "runner": return 0.75
        elif val == "repeater": return 0.5
        elif val == "stranger": return 0.25
        else: return 0

    plan_col = 'Modified Release plan'
    if plan_col not in df_plan.columns: plan_col = 'Daily Suggested Release Plan'

    if df_plan.empty:
        df_plan["priority_weight"] = 0
        return df_plan

    max_qty = df_plan[plan_col].max()
    df_plan["qty_score"] = df_plan[plan_col] / max_qty if max_qty > 0 else 0
    df_plan["type_score"] = df_plan["Type"].apply(score_type)
    df_plan["rpl_score"] = df_plan["RPL days to Delivery"].apply(score_rpl)
    df_plan["category_score"] = df_plan["Category"].apply(score_category)
    
    df_plan["priority_weight"] = (
            df_plan["type_score"] * weights.get("type", 0) +
            df_plan["rpl_score"] * weights.get("rpl", 0) +
            df_plan["category_score"] * weights.get("category", 0) +
            df_plan["qty_score"] * weights.get("modified_plan", 0)
    )

    df_plan.loc[df_plan[plan_col] <= 0, 'priority_weight'] = 0
    return df_plan

# --- Route Functions ---

@production_bp.route('/', endpoint='production_planning')
def production_planning():
    check_and_clear_daily_tables()
    ahp_weights = session.get('ahp_weights', DEFAULT_AHP_WEIGHTS.copy())

    filter_trigger_type = request.args.get('trigger_type', 'All')
    filter_item_code = request.args.get('item_code', '')
    
    conn = get_db_connection()
    plans, overdue_items, trigger_type_list, item_code_list = [], [], [], []
    master_has_data = False
    triggers_uploaded_today = False
    plan_generated_today = False

    if conn:
        try:
            cursor = conn.cursor()

            # 1. Check Master Data
            try:
                cursor.execute("SELECT COUNT(*) FROM master")
                if cursor.fetchone()[0] > 0: master_has_data = True
            except pyodbc.ProgrammingError:
                master_has_data = False

            # 2. Check Trigger Upload Status
            cursor.execute("SELECT status_value FROM app_status WHERE status_key = 'last_triggers_upload_timestamp'")
            row = cursor.fetchone()
            if row and row[0]:
                upload_time = pd.to_datetime(row[0])
                now = datetime.now()
                if now.hour < 2:
                    current_prod_date = (now - timedelta(days=1)).date()
                else:
                    current_prod_date = now.date()

                if upload_time.date() >= current_prod_date:
                    triggers_uploaded_today = True

            # 3. Check if Plan Exists
            try:
                cursor.execute("SELECT COUNT(*) FROM Production_pl")
                total = cursor.fetchone()[0]
                if total > 0:
                    plan_generated_today = True
                    sql_select_plans = "SELECT * FROM Production_pl ORDER BY [priority_weight] DESC"
                    cursor.execute(sql_select_plans)
                    plans = [row_to_dict(cursor, row) for row in cursor.fetchall()]
            except pyodbc.ProgrammingError:
                pass

            # 4. Fetch Overdue Items
            try:
                cursor.execute("SELECT DISTINCT [TRIGGER TYPE] FROM pending_triggers WHERE [TRIGGER TYPE] IS NOT NULL ORDER BY [TRIGGER TYPE]")
                trigger_type_list = [row[0] for row in cursor.fetchall()]
                cursor.execute("SELECT DISTINCT [ITEM CODE] FROM pending_triggers WHERE [ITEM CODE] IS NOT NULL ORDER BY [ITEM CODE]")
                item_code_list = [row[0] for row in cursor.fetchall()]

                sql_overdue = "SELECT * FROM pending_triggers WHERE CAST([DUE DT] AS DATE) < ?"
                params = []
                if filter_trigger_type != 'All':
                    sql_overdue += " AND [TRIGGER TYPE] = ?"
                    params.append(filter_trigger_type)
                if filter_item_code:
                    sql_overdue += " AND [ITEM CODE] LIKE ?"
                    params.append(f"%{filter_item_code}%")
                sql_overdue += " ORDER BY [DUE DT] ASC"

                cursor.execute(sql_overdue, [date.today()] + params)
                overdue_items = [row_to_dict(cursor, row) for row in cursor.fetchall()]
            except pyodbc.ProgrammingError:
                pass

        except Exception as e:
            flash(f'An unexpected error occurred: {e}', 'error')
            traceback.print_exc()
        finally:
            if conn: conn.close()

    return render_template('productionpl.html',
                           plans=plans,
                           weights=ahp_weights,
                           overdue_items=overdue_items,
                           table_exists=master_has_data,
                           triggers_uploaded_today=triggers_uploaded_today,
                           plan_generated_today=plan_generated_today,
                           trigger_type_list=trigger_type_list,
                           item_code_list=item_code_list,
                           selected_trigger_type=filter_trigger_type,
                           selected_item_code=filter_item_code,
                           pagination=None)

@production_bp.route('/upload_master', methods=['POST'], endpoint='upload_master_file')
def upload_master_file():
    if 'file' not in request.files:
        flash('No file part in the request.', 'error')
        # UPDATED: production.production_planning
        return redirect(request.referrer or url_for('production.production_planning'))

    file = request.files['file']
    if file.filename == '' or not file.filename.endswith(('.xlsx', '.xls')):
        flash('Invalid file. Please upload .xlsx or .xls', 'error')
        # UPDATED: production.production_planning
        return redirect(request.referrer or url_for('production.production_planning'))

    filepath, conn = None, None
    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        conn = get_db_connection()
        if not conn: raise ConnectionError("Database connection failed.")

        cursor = conn.cursor()
        df_master = pd.read_excel(filepath, sheet_name=0)

        item_code_col = None
        for col in df_master.columns:
            if 'item code' in str(col).strip().lower():
                item_code_col = col
                break

        if item_code_col:
            df_master[item_code_col] = df_master[item_code_col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            df_master.drop_duplicates(subset=[item_code_col], keep='first', inplace=True)
        else:
            flash("Upload Failed: Could not find 'Item code' column.", "error")
            return redirect(request.referrer)

        for col in df_master.columns:
            if col != item_code_col:
                df_master[col] = pd.to_numeric(df_master[col], errors='ignore')

        numeric_cols = df_master.select_dtypes(include=np.number).columns
        df_master[numeric_cols] = df_master[numeric_cols].fillna(0).astype(int)

        replace_table_with_df(df_master, 'master', cursor)
        conn.commit()
        flash('Master data uploaded successfully!', 'success')

    except Exception as e:
        if conn: conn.rollback()
        traceback.print_exc()
        flash(f"An unexpected error occurred: {e}", 'error')
    finally:
        if conn: conn.close()
        if filepath and os.path.exists(filepath): os.remove(filepath)

    # UPDATED: production.production_planning
    return redirect(request.referrer or url_for('production.production_planning'))

@production_bp.route('/upload_triggers', methods=['POST'], endpoint='upload_triggers')
def upload_triggers():
    if 'file' not in request.files or request.files['file'].filename == '':
        flash('No file selected.', 'error')
        # UPDATED: production.production_planning
        return redirect(request.referrer or url_for('production.production_planning'))

    file = request.files['file']
    if not file.filename.endswith('.csv'):
        flash('Invalid file type. Please upload a .csv file.', 'error')
        # UPDATED: production.production_planning
        return redirect(request.referrer or url_for('production.production_planning'))

    filepath, conn = None, None
    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        df = pd.read_csv(filepath, low_memory=False)

        item_code_col_actual = next((col for col in df.columns if str(col).strip().upper() == 'ITEM CODE'), None)
        qty_col_actual = next((col for col in df.columns if str(col).strip().upper() == 'QTY'), None)

        if not item_code_col_actual or not qty_col_actual:
            flash("Upload failed: Missing 'ITEM CODE' or 'QTY' columns.", 'error')
            return redirect(request.referrer)

        df.rename(columns={item_code_col_actual: 'Item code', qty_col_actual: 'Qty'}, inplace=True)
        item_code_col, qty_col = 'Item code', 'Qty'

        df[item_code_col] = df[item_code_col].astype(str).str.strip().str.lstrip("'")
        df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
        
        aggregated_triggers = df.groupby(item_code_col)[qty_col].sum().reset_index()

        conn = get_db_connection()
        if not conn: raise ConnectionError("Database connection failed.")
        cursor = conn.cursor()

        replace_table_with_df(df, 'pending_triggers', cursor)

        update_count = 0
        cursor.execute("UPDATE master SET [Pending] = 0")
        for index, row in aggregated_triggers.iterrows():
            item_code = row[item_code_col]
            total_qty = row[qty_col]
            update_sql = "UPDATE master SET [Pending] = ? WHERE [Item code] = ?"
            cursor.execute(update_sql, total_qty, str(item_code))
            update_count += cursor.rowcount

        upsert_sql = """
            MERGE app_status AS target
            USING (SELECT 'last_triggers_upload_timestamp' AS src_key, ? AS src_val) AS src 
            ON (target.status_key = src.src_key)
            WHEN MATCHED THEN UPDATE SET status_value = src.src_val
            WHEN NOT MATCHED THEN INSERT (status_key, status_value) VALUES (src.src_key, src.src_val);
        """
        cursor.execute(upsert_sql, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        conn.commit()
        flash(f'Pending triggers uploaded. {update_count} items updated.', 'success')

    except Exception as e:
        if conn: conn.rollback()
        flash(f'Error upload triggers: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()
        if filepath and os.path.exists(filepath): os.remove(filepath)

    # UPDATED: production.production_planning
    return redirect(request.referrer or url_for('production.production_planning'))

@production_bp.route('/generate_plan', methods=['POST'], endpoint='generate_plan')
def generate_plan():
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('production.production_planning'))

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT status_value FROM app_status WHERE status_key = 'last_triggers_upload_timestamp'")
        row = cursor.fetchone()

        triggers_valid = False
        if row and row[0]:
            upload_time = pd.to_datetime(row[0])
            now = datetime.now()
            current_prod_date = (now - timedelta(days=1)).date() if now.hour < 2 else now.date()
            if upload_time.date() >= current_prod_date:
                triggers_valid = True

        if not triggers_valid:
            flash('Error: Upload Pending Triggers for today first.', 'error')
            return redirect(url_for('production.production_planning'))

        ahp_weights = session.get('ahp_weights', DEFAULT_AHP_WEIGHTS.copy())

        # Ensure Columns in Master
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'Daily Max Plan' AND Object_ID = Object_ID(N'master'))
                ALTER TABLE master ADD [Daily Max Plan] INT DEFAULT 0 WITH VALUES;
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'Max Inv' AND Object_ID = Object_ID(N'master'))
                ALTER TABLE master ADD [Max Inv] INT DEFAULT 0 WITH VALUES;
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'Live Dispatch' AND Object_ID = Object_ID(N'master'))
                ALTER TABLE master ADD [Live Dispatch] INT DEFAULT 0 WITH VALUES;
        """)
        conn.commit()

        # Generate Plan Logic
        wip_stages_sql = "ISNULL([Long seam], 0) + ISNULL([Dish fit up], 0) + ISNULL([Cirseam welding], 0) + ISNULL([Part assembly], 0) + ISNULL([Full welding], 0) + ISNULL([Hydro Testing], 0) + ISNULL([Powder coating], 0) + ISNULL([PDI], 0)"

        sql_query = f"""
        WITH CalculatedData AS (
            SELECT 
                [Item code], [Description], [Vertical], [Category], [Type], [Model], [RPL days to Delivery],
                ISNULL([Pending], 0) AS [Pending_Trigger],
                ISNULL([Max Inv], 0) AS [Max_Inventory],
                ISNULL([Daily Max Plan], 0) AS [Max_Daily_Plan],
                ({wip_stages_sql}) AS [Current_WIP],
                ISNULL([Live Dispatch], 0) as [Current_Dispatch]
            FROM dbo.master
        )
        SELECT * FROM CalculatedData
        """
        plan_df = pd.read_sql_query(sql_query, conn)

        plan_df['Available_WIP_Space'] = plan_df['Max_Inventory'] - plan_df['Current_WIP']
        plan_df['Total_Potential'] = plan_df['Available_WIP_Space'] + plan_df['Pending_Trigger']

        def calculate_actual(row):
            val = min(row['Total_Potential'], row['Available_WIP_Space'], row['Max_Daily_Plan'])
            return max(0, val)

        plan_df['Daily Suggested Release Plan'] = plan_df.apply(calculate_actual, axis=1).astype(int)
        plan_df['Item code'] = plan_df['Item code'].astype(str).str.strip()
        plan_df['Modified Release plan'] = plan_df['Daily Suggested Release Plan']
        plan_df['CalculatedTrigger'] = plan_df['Daily Suggested Release Plan']

        prioritized_df = prioritize_plan_with_ahp(plan_df, ahp_weights)
        final_plan_to_save = prioritized_df.sort_values(by='priority_weight', ascending=False)
        final_plan_to_save.reset_index(drop=True, inplace=True)
        final_plan_to_save.insert(0, 'S.No.', final_plan_to_save.index + 1)

        # --- CRITICAL FIXES HERE ---
        final_plan_to_save['Today Triggered'] = 0
        final_plan_to_save['FailureInTrigger'] = 0  # Changed from 'Failure in Trigger' to match Dashboard
        final_plan_to_save['SavedReason'] = None    # Initialize column for reasons
        final_plan_to_save['Dispatch'] = final_plan_to_save['Current_Dispatch']

        final_plan_to_save.rename(columns={'Item code': 'Item code ', 'Pending_Trigger': 'Pending'}, inplace=True)
        cols_to_drop = ['Available_WIP_Space', 'Total_Potential', 'Max_Inventory', 'Max_Daily_Plan', 'Current_WIP',
                        'Current_Dispatch', 'type_score', 'rpl_score', 'category_score', 'qty_score']
        final_plan_to_save.drop(columns=cols_to_drop, inplace=True, errors='ignore')

        replace_table_with_df(final_plan_to_save, 'Production_pl', cursor)
        conn.commit()
        flash('Production Plan generated successfully.', 'success')

    except Exception as e:
        if conn: conn.rollback()
        flash(f'Error generating plan: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()

    return redirect(url_for('production.production_planning'))

@production_bp.route('/release_plan', methods=['POST'], endpoint='release_plan')
def release_plan():
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        # UPDATED: production.production_planning
        return redirect(url_for('production.production_planning'))

    try:
        cursor = conn.cursor()
        sync_sql = """
            IF EXISTS (SELECT * FROM sys.columns WHERE Name = N'Not Started' AND Object_ID = Object_ID(N'master'))
            BEGIN
                UPDATE m
                SET m.[Not Started] = p.[Modified Release plan]
                FROM master m
                INNER JOIN Production_pl p ON m.[Item code] = p.[Item code ]
            END
        """
        cursor.execute(sync_sql)
        cursor.execute("UPDATE Production_pl SET [Today Triggered] = [Modified Release plan]")
        conn.commit()
        flash('Plan Released successfully!', 'success')

    except Exception as e:
        if conn: conn.rollback()
        flash(f'Error releasing plan: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()

    # UPDATED: production.production_planning
    return redirect(url_for('production.production_planning'))

@production_bp.route('/delete_triggers', methods=['POST'], endpoint='delete_triggers')
def delete_triggers():
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        # UPDATED: production.production_planning
        return redirect(request.referrer or url_for('production.production_planning'))

    try:
        cursor = conn.cursor()
        cursor.execute("IF OBJECT_ID('dbo.pending_triggers', 'U') IS NOT NULL DELETE FROM pending_triggers")
        cursor.execute("IF OBJECT_ID('dbo.master', 'U') IS NOT NULL UPDATE master SET [Pending] = 0")
        cursor.execute("UPDATE app_status SET status_value = NULL WHERE status_key = 'last_triggers_upload_timestamp'")
        conn.commit()
        flash('Pending triggers deleted.', 'success')
    except Exception as e:
        flash(f'Error: {e}', 'error')
        if conn: conn.rollback()
    finally:
        conn.close()

    # UPDATED: production.production_planning
    return redirect(request.referrer or url_for('production.production_planning'))

@production_bp.route('/update_single_plan', methods=['POST'])
def update_single_plan():
    data = request.get_json()
    item_code = data.get('item_code')

    try:
        new_plan_value = int(data.get('new_plan_value') or 0)
    except ValueError:
        return jsonify({'status': 'error', 'message': 'Invalid number'}), 400

    if not item_code:
        return jsonify({'status': 'error', 'message': 'Item code is missing.'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'status': 'error', 'message': 'Database connection failed.'}), 500

    try:
        cursor = conn.cursor()

        # 1. Update Production Plan Table
        sql_plan = "UPDATE Production_pl SET [Modified Release plan] = ? WHERE [Item code ] LIKE ?"
        cursor.execute(sql_plan, (new_plan_value, item_code))

        # 2. Update Master Table (Not Started)
        sql_master = """
            IF EXISTS (SELECT * FROM sys.columns WHERE Name = N'Not Started' AND Object_ID = Object_ID(N'master'))
            BEGIN
                UPDATE master 
                SET [Not Started] = ? 
                WHERE [Item code] LIKE ?
            END
        """
        cursor.execute(sql_master, (new_plan_value, item_code))

        # 3. Recalculate Priorities (AHP)
        updated_plan_df = pd.read_sql("SELECT * FROM Production_pl", conn)

        if 'Item code' in updated_plan_df.columns:
            updated_plan_df.rename(columns={'Item code': 'Item code '}, inplace=True)

        ahp_weights = session.get('ahp_weights', DEFAULT_AHP_WEIGHTS.copy())
        re_prioritized_df = prioritize_plan_with_ahp(updated_plan_df, ahp_weights)

        # Update weights in DB
        for index, row in re_prioritized_df.iterrows():
            code = row['Item code ']
            new_weight = row['priority_weight']
            update_sql = "UPDATE Production_pl SET [priority_weight] = ? WHERE [Item code ] = ?"
            cursor.execute(update_sql, new_weight, code)

        conn.commit()
        return jsonify({'status': 'success'})

    except Exception as e:
        if conn: conn.rollback()
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        if conn: conn.close()

@production_bp.route('/update_weights', methods=['POST'], endpoint='update_weights')
def update_weights():
    try:
        raw_weights = {'type': float(request.form.get('weight_type', 0)),
                       'rpl': float(request.form.get('weight_rpl', 0)),
                       'category': float(request.form.get('weight_category', 0)),
                       'modified_plan': float(request.form.get('weight_modified_plan', 0))}
        total_weight = sum(raw_weights.values())
        if total_weight == 0:
            flash('All weights cannot be zero. Please enter at least one positive value.', 'error')
            # UPDATED: production.production_planning
            return redirect(url_for('production.production_planning'))
        session['ahp_weights'] = {key: value / total_weight for key, value in raw_weights.items()}
        flash('AHP weights have been updated and normalized successfully!', 'success')
    except ValueError:
        flash('Invalid input. Please enter valid numbers for all weight fields.', 'error')
    except Exception as e:
        flash(f'An unexpected error occurred: {e}', 'error')
        traceback.print_exc()
    # UPDATED: production.production_planning
    return redirect(url_for('production.production_planning'))

@production_bp.route('/delete_master', methods=['POST'], endpoint='delete_master_data')
def delete_master_data():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("IF OBJECT_ID('dbo.master', 'U') IS NOT NULL DROP TABLE dbo.master")
            cursor.execute("IF OBJECT_ID('dbo.Production_pl', 'U') IS NOT NULL DELETE FROM dbo.Production_pl")
            cursor.execute("IF OBJECT_ID('dbo.production_log', 'U') IS NOT NULL DELETE FROM dbo.production_log")
            cursor.execute("IF OBJECT_ID('dbo.flagged_items', 'U') IS NOT NULL DELETE FROM dbo.flagged_items")
            cursor.execute("IF OBJECT_ID('dbo.pending_triggers', 'U') IS NOT NULL DELETE FROM dbo.pending_triggers")
            cursor.execute("IF OBJECT_ID('dbo.app_status', 'U') IS NOT NULL DELETE FROM dbo.app_status")
            conn.commit()
            flash("All master data cleared.", "success")
        except Exception as e:
            flash(f"Error deleting data: {e}", "error")
        finally:
            conn.close()
    # UPDATED: production.production_planning
    return redirect(request.referrer or url_for('production.production_planning'))