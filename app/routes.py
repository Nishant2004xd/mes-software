import os
import pandas as pd
import numpy as np
import pyodbc
import traceback
from datetime import datetime, date, timedelta
from flask import render_template, request, jsonify, redirect, url_for, flash, send_file, session
from werkzeug.utils import secure_filename
from collections import defaultdict
import calendar
import io
from math import ceil
from app import app
from app.database import get_db_connection, row_to_dict

# --- NEW IMPORTS FOR LOGIN ---
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from app import login_manager


class User(UserMixin):
    def __init__(self, user_id, username, name):
        self.id = user_id      # Primary Key (int)
        self.username = username
        self.name = name

    def __repr__(self):
        return f"{self.id}/{self.username}"

# --- HELPER: INITIALIZE USER TABLE (Structure Only) ---
def init_user_db():
    """Creates the users table if missing. Does NOT create default users."""
    conn = get_db_connection()
    if not conn: return
    try:
        cursor = conn.cursor()
        # Create Table Only
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='app_users' and xtype='U')
            CREATE TABLE app_users (
                user_id INT IDENTITY(1,1) PRIMARY KEY,
                username NVARCHAR(50) NOT NULL UNIQUE,
                password NVARCHAR(255) NOT NULL,
                full_name NVARCHAR(100)
            );
        """)
        conn.commit()
    except Exception as e:
        print(f"Error initializing user DB: {e}")
    finally:
        conn.close()
# --- USER LOADER (For Session) ---
@login_manager.user_loader
def load_user(user_id):
    conn = get_db_connection()
    if not conn: return None
    try:
        cursor = conn.cursor()
        # Ensure user_id is safe (int)
        cursor.execute("SELECT user_id, username, full_name FROM app_users WHERE user_id = ?", user_id)
        row = cursor.fetchone()
        if row:
            return User(row[0], row[1], row[2])
    except Exception:
        return None
    finally:
        conn.close()
    return None

# --- LOGIN ROUTES ---


@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))


# --- LOGIN ROUTES ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    # Ensure DB table exists when visiting login
    if request.method == 'GET':
        init_user_db()

    if current_user.is_authenticated:
        return redirect(url_for('production_planning'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        conn = get_db_connection()
        user_found = None
        
        if conn:
            try:
                cursor = conn.cursor()
                # Check credentials
                cursor.execute("SELECT user_id, username, full_name FROM app_users WHERE username = ? AND password = ?", (username, password))
                row = cursor.fetchone()
                
                if row:
                    user_found = User(row[0], row[1], row[2])
            finally:
                conn.close()

        if user_found:
            login_user(user_found)
            return redirect(request.args.get('next') or url_for('production_planning'))
        else:
            flash('Invalid username or password', 'error')

    return render_template('login.html')



UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)



STAGES = [
    "Long seam", "Dish fit up", "Cirseam welding", "Part assembly",
    "Full welding", "Hydro Testing", "Powder coating", "PDI", "FG" # REVERTED to actual DB column name
]
DEFAULT_AHP_WEIGHTS = {'type': 0.5396, 'rpl': 0.2583, 'category': 0.1387, 'modified_plan': 0.0634}


FAILURE_REASONS = [
    "Machine breakdown",
    "Man power not avialable (Absentism )",
    "Dispatch stopped at HO (Excess stock at HO)",
    "Parts shortages",
    "Quality issue",
    "Planning delay",
    "Inspection delay(ASME/SPVD)",
    "RT delay",
    "Capacity issue",
    "Rejected at PDI",
    "Excess Trigger / Short Lead time btn Triggers",
    "Design / ECR Changes (R&D)",
    "Frequent Changes in Bin Qty / RPL",
    "System Issue (PO, ASN, NVC Trigger Can, etc.,)",
    "Transportation Issue (Logistics)",
    "Non-lean Trigger Short Lead Time",
    "Space Constrain at HO",
    "Trolley shortage",
    "Outsourcing delay"
]

def check_and_clear_daily_tables():
    conn = get_db_connection()
    if not conn: return

    try:
        cursor = conn.cursor()
        # ... (keep existing table check logic) ...

        # --- Determine Current Production Date (adjusting for 2 AM cutoff) ---
        now = datetime.now()
        if now.hour < 2:
            current_production_date = (now - timedelta(days=1)).date()
        else:
            current_production_date = now.date()

        current_production_date_str = current_production_date.strftime('%Y-%m-%d')

        # Check last upload date
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
            print("Performing automatic daily trigger reset...")
            # 1. Clear raw triggers table for the new day
            cursor.execute("IF OBJECT_ID('dbo.pending_triggers', 'U') IS NOT NULL DELETE FROM pending_triggers")
            
            # 2. Reset master table columns
            cursor.execute("IF OBJECT_ID('dbo.master', 'U') IS NOT NULL UPDATE master SET [Pending] = 0, [Next Pending] = 0")
            
            # NOTE: We DO NOT delete from trigger_history here. 
            # This preserves the record of the day that just finished.

            # 3. Clear timestamp and update status date
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
            print("Automatic reset complete. History preserved.")

    except Exception as e:
        print(f"Error during daily check/reset: {e}")
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
        if val == "milk run":
            return 1.0
        elif val == "runner":
            return 0.75
        elif val == "repeater":
            return 0.5
        elif val == "stranger":
            return 0.25
        else:
            return 0

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

    # This final line ensures any item with a plan of 0 gets a priority of 0
    df_plan.loc[df_plan[plan_col] <= 0, 'priority_weight'] = 0

    return df_plan

# In app/routes.py

# In app/routes.py

# In app/routes.py

@app.route('/', endpoint='production_planning')
@login_required
def production_planning():
    check_and_clear_daily_tables()
    ahp_weights = session.get('ahp_weights', DEFAULT_AHP_WEIGHTS.copy())

    # --- FILTERS ---
    filter_search = request.args.get('search', '')
    filter_vertical = request.args.get('vertical', '')
    filter_category = request.args.get('category', '')
    
    # Check if this is an AJAX request for real-time filtering
    ajax_request = request.args.get('ajax')

    # Overdue View filters
    filter_trigger_type = request.args.get('trigger_type', 'All')
    filter_item_code = request.args.get('item_code', '')
    
    current_view = request.args.get('view', 'production')

    # Pagination
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    offset = (page - 1) * per_page

    conn = get_db_connection()
    plans, overdue_items = [], []
    trigger_type_list, item_code_list = [], []
    all_verticals, all_categories = [], []

    master_has_data = False
    triggers_uploaded_today = False
    plan_generated_today = False
    pagination = None
    total_pages = 1

    if conn:
        try:
            cursor = conn.cursor()

            # 1. Check Master Data & Status
            try:
                cursor.execute("SELECT COUNT(*) FROM master")
                if cursor.fetchone()[0] > 0: master_has_data = True
            except: master_has_data = False

            cursor.execute("SELECT status_value FROM app_status WHERE status_key = 'last_triggers_upload_timestamp'")
            row = cursor.fetchone()
            if row and row[0]:
                if pd.to_datetime(row[0]).date() >= datetime.now().date():
                    triggers_uploaded_today = True

            # 2. Fetch Dropdowns (Ensures RCB and others appear)
            try:
                cursor.execute("SELECT DISTINCT [Vertical] FROM Production_pl WHERE [Vertical] IS NOT NULL ORDER BY [Vertical]")
                all_verticals = [row[0] for row in cursor.fetchall()]
                
                cursor.execute("SELECT DISTINCT [Category] FROM Production_pl WHERE [Category] IS NOT NULL ORDER BY [Category]")
                all_categories = [row[0] for row in cursor.fetchall()]
            except: pass

            # 3. Fetch Production Plan (With Filters & WIP)
            try:
                base_where = " WHERE 1=1 "
                params = []

                if filter_search:
                    base_where += " AND (p.[Item code ] LIKE ? OR p.[Description] LIKE ?) "
                    params.extend([f'%{filter_search}%', f'%{filter_search}%'])
                if filter_vertical:
                    base_where += " AND p.[Vertical] = ? "
                    params.append(filter_vertical)
                if filter_category:
                    base_where += " AND p.[Category] = ? "
                    params.append(filter_category)

                # Count Total
                cursor.execute(f"SELECT COUNT(*) FROM Production_pl p {base_where}", params)
                total_items = cursor.fetchone()[0]
                
                # Check generation flag
                cursor.execute("IF OBJECT_ID('dbo.Production_pl', 'U') IS NOT NULL SELECT 1 ELSE SELECT 0")
                if cursor.fetchone()[0] == 1: plan_generated_today = True

                if total_items > 0:
                    total_pages = ceil(total_items / per_page)
                    
                    # WIP Logic
                    try:
                        all_stages = get_dynamic_stages(cursor)
                    except: all_stages = []
                    if not all_stages: all_stages = STAGES
                    wip_stages = [s for s in all_stages if s not in ['FG', 'Not Started']]
                    wip_sum_sql = " + ".join([f"ISNULL(m.[{s}], 0)" for s in wip_stages]) if wip_stages else "0"

                    sql_select_plans = f"""
                        SELECT p.*, ({wip_sum_sql}) as [WIP]
                        FROM Production_pl p
                        LEFT JOIN master m ON p.[Item code ] = m.[Item code]
                        {base_where}
                        ORDER BY p.[priority_weight] DESC
                        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
                    """
                    cursor.execute(sql_select_plans, params + [offset, per_page])
                    plans = [row_to_dict(cursor, row) for row in cursor.fetchall()]

                    pagination = {
                        'page': page, 'per_page': per_page, 'total': total_items,
                        'total_pages': total_pages, 'has_prev': page > 1, 'has_next': page < total_pages,
                        'prev_num': page - 1, 'next_num': page + 1
                    }
            except Exception as e:
                print(f"Plan Query Error: {e}")

            # 4. Fetch Overdue (Only if View is Overdue)
            if current_view == 'overdue':
                try:
                    cursor.execute("SELECT DISTINCT [TRIGGER TYPE] FROM pending_triggers WHERE [TRIGGER TYPE] IS NOT NULL")
                    trigger_type_list = [row[0] for row in cursor.fetchall()]
                    
                    sql_overdue = "SELECT * FROM pending_triggers WHERE CAST([DUE DT] AS DATE) < ?"
                    ov_params = [date.today()]
                    if filter_trigger_type != 'All':
                        sql_overdue += " AND [TRIGGER TYPE] = ?"
                        ov_params.append(filter_trigger_type)
                    if filter_item_code:
                        sql_overdue += " AND [ITEM CODE] LIKE ?"
                        ov_params.append(f"%{filter_item_code}%")
                    sql_overdue += " ORDER BY [DUE DT] ASC"
                    cursor.execute(sql_overdue, ov_params)
                    overdue_items = [row_to_dict(cursor, row) for row in cursor.fetchall()]
                except: pass

        finally:
            conn.close()

    # --- AJAX RESPONSE (JSON for JS Rendering) ---
    if ajax_request:
        return jsonify({
            'status': 'success',
            'plans': plans, 
            'total_pages': total_pages,
            'current_page': page,
            'per_page': per_page
        })

    # --- FULL PAGE RESPONSE ---
    return render_template('productionpl.html',
                           plans=plans,
                           weights=ahp_weights,
                           overdue_items=overdue_items,
                           table_exists=master_has_data,
                           triggers_uploaded_today=triggers_uploaded_today,
                           plan_generated_today=plan_generated_today,
                           all_verticals=all_verticals,
                           all_categories=all_categories,
                           filter_search=filter_search,
                           filter_vertical=filter_vertical,
                           filter_category=filter_category,
                           trigger_type_list=trigger_type_list,
                           selected_trigger_type=filter_trigger_type,
                           selected_item_code=filter_item_code,
                           pagination=pagination,
                           current_per_page=per_page)


@app.route('/wip_tracking', endpoint='wip_tracking')
@login_required
def wip_tracking():
    # 1. Get Per Page from URL
    per_page = request.args.get('per_page', 20, type=int)
    page = request.args.get('page', 1, type=int)
    offset = (page - 1) * per_page
    
    search_query = request.args.get('search', '')
    selected_type = request.args.get('type', '')
    selected_category = request.args.get('category', '')

    type_options = ['Lean', 'Non Lean']
    category_options = ['Stranger', 'Runner', 'Repeater', 'Milk Run']

    # --- GROUP DEFINITION (Ordered) ---
    BEFORE_HYDRO_GROUP = ["Long seam", "Dish fit up", "Cirseam welding", "Part assembly", "Full welding"]

    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('dashboard'))

    master_items = []
    display_stages = [] # Stages to show in table
    pagination = None

    try:
        cursor = conn.cursor()

        # 1. FETCH ALL DB STAGES
        cursor.execute("SELECT stage_name FROM manufacturing_stages ORDER BY display_order ASC")
        all_db_stages = [row[0] for row in cursor.fetchall()]

        # 2. Define Display Stages (Grouped View)
        display_stages = ['Before Hydro']
        for s in all_db_stages:
            if s not in BEFORE_HYDRO_GROUP and s != 'Not Started':
                display_stages.append(s)

        # 3. Build Query
        base_query = "SELECT * FROM master WHERE 1=1"
        count_query = "SELECT COUNT(*) FROM master WHERE 1=1"
        params = []

        if search_query:
            base_query += " AND ([Item code] LIKE ? OR [Description] LIKE ?)"
            count_query += " AND ([Item code] LIKE ? OR [Description] LIKE ?)"
            params.extend([f'%{search_query}%', f'%{search_query}%'])

        if selected_type:
            base_query += " AND [Type] = ?"
            count_query += " AND [Type] = ?"
            params.append(selected_type)

        if selected_category:
            base_query += " AND [Category] = ?"
            count_query += " AND [Category] = ?"
            params.append(selected_category)

        # 4. Pagination
        cursor.execute(count_query, params)
        total_items = cursor.fetchone()[0]
        total_pages = ceil(total_items / per_page)

        # 5. Fetch Data
        final_query = base_query + " ORDER BY [Item code] OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([offset, per_page])

        cursor.execute(final_query, params)
        master_items = [row_to_dict(cursor, row) for row in cursor.fetchall()]

        # 6. Process Items
        import json
        for item in master_items:
            # A. Calculate Total Inv
            current_total = 0
            for stage in all_db_stages:
                if stage in ['Not Started', 'FG']: continue
                qty = item.get(stage)
                if qty: current_total += int(qty)
            item['Total Inv'] = current_total

            # B. Calculate "Before Hydro" Group
            group_sum = 0
            breakdown = {}
            for sub in BEFORE_HYDRO_GROUP:
                q = item.get(sub, 0) or 0
                if q > 0:
                    group_sum += int(q)
                    breakdown[sub] = int(q)
            
            item['Before Hydro'] = group_sum
            item['Before_Hydro_Breakdown'] = json.dumps(breakdown)

        pagination = {
            'page': page, 'per_page': per_page, 'total': total_items,
            'total_pages': total_pages, 'has_prev': page > 1, 'has_next': page < total_pages,
            'prev_num': page - 1, 'next_num': page + 1
        }

    except Exception as e:
        flash(f"Error fetching WIP data: {e}", 'error')
        traceback.print_exc()
    finally:
        conn.close()

    return render_template('wip.html',
                           master_items=master_items,
                           stages=display_stages,
                           group_stages=BEFORE_HYDRO_GROUP, # Pass group order to frontend
                           type_options=type_options,
                           category_options=category_options,
                           selected_type=selected_type,
                           selected_category=selected_category,
                           search_query=search_query,
                           pagination=pagination,
                           current_per_page=per_page)

@app.route('/release_plan', methods=['POST'], endpoint='release_plan')
def release_plan():
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('production_planning'))

    try:
        cursor = conn.cursor()

        # 1. Sync 'Modified Release plan' from Production_pl TO 'Not Started' in master
        # FIX: Changed logic to REPLACE (=) instead of ADD (+)
        sync_sql = """
            IF EXISTS (SELECT * FROM sys.columns WHERE Name = N'Not Started' AND Object_ID = Object_ID(N'master'))
            BEGIN
                UPDATE m
                SET m.[Not Started] = p.[Modified Release plan]
                FROM master m
                INNER JOIN Production_pl p ON m.[Item code] = p.[Item code ]
                -- We update even if plan is 0, to effectively 'clear' the order if the plan changed to 0
            END
        """
        cursor.execute(sync_sql)

        # 2. Update 'Today Triggered' in Production_pl to match what was just released
        # This locks in the "Released Qty" for the day
        cursor.execute("UPDATE Production_pl SET [Today Triggered] = [Modified Release plan]")

        conn.commit()
        flash('Plan Released successfully! "Not Started" quantities have been updated (replaced).', 'success')

    except Exception as e:
        if conn: conn.rollback()
        flash(f'Error releasing plan: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()

    return redirect(url_for('production_planning'))

@app.route('/dashboard', endpoint='dashboard')
@login_required
def dashboard():
    current_view = request.args.get('view', 'overview')
    filter_type = request.args.get('type')
    filter_value = request.args.get('value')

    page = 1
    per_page = 17
    offset = (page - 1) * per_page
    pagination = None

    conn = get_db_connection()

    # --- 1. INITIALIZE DEFAULTS (Vital for preventing UndefinedError) ---
    stage_totals = {}
    top_plans = []
    vertical_overview = []
    category_overview = []
    total_wip_inventory = 0
    all_verticals_list = []
    daily_adherence = 0
    monthly_adherence = 0
    total_monthly_dispatch = 0
    monthly_trigger_adherence = 0  # <--- INITIALIZE HERE
    failure_reasons = FAILURE_REASONS
    show_failure_button = True
    current_stages = []

    # Handle DB Connection Failure
    if not conn:
        flash('Database connection failed.', 'error')
        # We must pass the default 'monthly_trigger_adherence=0' here too
        return render_template('dashboard.html', current_view='overview', stages=[], show_failure_button=False,
                               stage_totals={}, top_plans=[], pagination=None, vertical_overview=[],
                               category_overview=[],
                               total_monthly_dispatch=0, daily_adherence=0, monthly_adherence=0, 
                               monthly_trigger_adherence=0, # <--- PASS IT HERE
                               failure_reasons=[], all_verticals=[])

    try:
        cursor = conn.cursor()
        
        # --- FIX: Ensure 'Next Pending' column exists before querying ---
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'Next Pending' AND Object_ID = Object_ID(N'master'))
            ALTER TABLE master ADD [Next Pending] INT DEFAULT 0 WITH VALUES;
        """)
        conn.commit()

        current_stages = get_dynamic_stages(cursor, dashboard_only=True)

        # 1. ENSURE TABLES EXIST
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='failure_log' and xtype='U')
            CREATE TABLE failure_log (
                log_id INT IDENTITY(1,1) PRIMARY KEY,
                item_code NVARCHAR(255), failure_date DATE, failed_quantity INT, reason NVARCHAR(MAX), 
                logged_at DATETIME DEFAULT GETDATE(), CONSTRAINT UQ_failure_log UNIQUE (item_code, failure_date) 
            );
        """)
        conn.commit()

        # 2. Basic Totals
        cursor.execute("SELECT SUM(ISNULL([Total Inv], 0)) FROM master")
        total_wip_inventory = cursor.fetchone()[0] or 0

        cursor.execute("IF OBJECT_ID('dbo.Production_pl', 'U') IS NOT NULL SELECT 1 ELSE SELECT 0")
        if cursor.fetchone()[0] == 1:
            cursor.execute(
                "SELECT DISTINCT [Vertical] FROM Production_pl WHERE [Vertical] IS NOT NULL ORDER BY [Vertical]")
            all_verticals_list = [row[0] for row in cursor.fetchall()]
        else:
            all_verticals_list = []

        # 3. DAILY ADHERENCE & DISPATCH
        if all_verticals_list:
            cursor.execute("SELECT SUM(ISNULL([Pending], 0)) FROM master")
            total_daily_target = cursor.fetchone()[0] or 0
        else:
            total_daily_target = 0

        # Calculate Actual Dispatch from LOGS (2 AM Cutoff) - FROM FG TO DISPATCH
        sql_daily_actual = """
                    DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
                    DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
                    DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

                    SELECT SUM(quantity) FROM production_log 
                    WHERE from_stage = 'FG' AND to_stage = 'Dispatch' 
                    AND moved_at >= @ProductionDayStart AND moved_at < @ProductionDayEnd
                """
        cursor.execute(sql_daily_actual)
        total_daily_dispatched = cursor.fetchone()[0] or 0

        daily_adherence = (total_daily_dispatched / total_daily_target * 100) if total_daily_target > 0 else 0

        # 4. MONTHLY DISPATCH
        sql_monthly_dispatch = """
            SELECT SUM(pl.quantity) 
            FROM production_log pl
            INNER JOIN master m ON pl.item_code = m.[Item code]
            WHERE pl.to_stage IN ('Dispatch', 'Delivered') 
            AND MONTH(pl.moved_at) = MONTH(GETDATE()) 
            AND YEAR(pl.moved_at) = YEAR(GETDATE())
        """
        cursor.execute(sql_monthly_dispatch)
        total_monthly_dispatch = cursor.fetchone()[0] or 0
        
        # --- 5. CALCULATE MONTHLY TRIGGER ADHERENCE ---
        # Call the helper function defined in step 1 of previous solution
        monthly_trigger_adherence = calculate_monthly_trigger_adherence(conn)

        # --- VIEW LOGIC ---
        if current_view == 'details' and filter_type and filter_value:
            column_map = {'vertical': '[Vertical]', 'category': '[Category]'}
            target_column = column_map.get(filter_type)

            if not target_column:
                return redirect(url_for('dashboard'))

            sql_details = f"""
                DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
                DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
                DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

                WITH TodayDispatch AS (
                    SELECT item_code, SUM(quantity) as QtyDispatched
                    FROM production_log
                    WHERE from_stage = 'FG' AND to_stage = 'Dispatch'
                    AND moved_at >= @ProductionDayStart AND moved_at < @ProductionDayEnd
                    GROUP BY item_code
                )
                SELECT
                    ROW_NUMBER() OVER (ORDER BY p.[priority_weight] DESC, p.[S.No.] ASC) AS [S.No.],
                    p.[Item code ], p.[Type], p.[Category], p.[Model],

                    ISNULL(m.[Pending], 0) AS [Opening Trigger],
                    ISNULL(m.[PDI], 0) as [PDI], 
                    ISNULL(m.[Powder coating], 0) as [Powder coating], 
                    ISNULL(m.[Hydro Testing], 0) as [Hydro Testing],

                    ISNULL(td.QtyDispatched, 0) AS [Dispatched]
                FROM Production_pl p
                LEFT JOIN master m ON p.[Item code ] = m.[Item code]
                LEFT JOIN TodayDispatch td ON p.[Item code ] = td.item_code
                WHERE p.{target_column} = ?
                ORDER BY p.[priority_weight] DESC, p.[S.No.] ASC
            """
            cursor.execute(sql_details, filter_value)
            details_data = [row_to_dict(cursor, row) for row in cursor.fetchall()]

            return render_template('dashboard.html',
                                   current_view=current_view,
                                   filter_type=filter_type,
                                   filter_value=filter_value,
                                   details_data=details_data,
                                   stages=current_stages,
                                   all_verticals=all_verticals_list,
                                   daily_adherence=daily_adherence,
                                   total_monthly_dispatch=total_monthly_dispatch,
                                   monthly_trigger_adherence=monthly_trigger_adherence, # <--- PASS HERE
                                   show_failure_button=show_failure_button)

        else:
            # Overview
            if current_stages:
                sum_clauses = [f"SUM(ISNULL([{stage}], 0)) as [{stage}]" for stage in current_stages]
                cursor.execute(f"SELECT {', '.join(sum_clauses)} FROM master")
                stage_totals = row_to_dict(cursor, cursor.fetchone() or [])

            cursor.execute("SELECT COUNT(*) FROM Production_pl")
            total = cursor.fetchone()[0]

            if total > 0:
                total_pages = ceil(total / per_page)
                pagination = {'page': page, 'per_page': per_page, 'total': total, 'total_pages': total_pages,
                              'has_prev': page > 1, 'has_next': page < total_pages, 'prev_num': page - 1,
                              'next_num': page + 1}

                # --- TOP PLANS QUERY ---
                sql_top_plans = """
                                DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
                                DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
                                DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

                                WITH CurrentFailureReasons AS (
                                    SELECT item_code, MAX(reason) as reason 
                                    FROM failure_log WHERE failure_date = @CurrentProductionDate GROUP BY item_code
                                ),
                                ItemsReachedFG AS (
                                    SELECT item_code, SUM(quantity) as QtyReachedFG
                                    FROM production_log
                                    WHERE to_stage = 'FG' AND moved_at >= @ProductionDayStart AND moved_at < @ProductionDayEnd
                                    GROUP BY item_code
                                ),
                                TodayDispatch AS (
                                    SELECT item_code, SUM(quantity) as QtyDispatched
                                    FROM production_log
                                    WHERE from_stage = 'FG' AND to_stage = 'Dispatch'
                                    AND moved_at >= @ProductionDayStart AND moved_at < @ProductionDayEnd
                                    GROUP BY item_code
                                )
                                SELECT
                                    p.[S.No.], p.[Item code ], p.[Description], 

                                    ISNULL(m.[Pending], 0) AS [Opening Trigger],
                                    ISNULL(m.[Next Pending], 0) AS [Next Pending], 

                                    CASE 
                                        WHEN (ISNULL(m.[Pending], 0) - ISNULL(td.QtyDispatched, 0)) < 0 THEN 0
                                        ELSE (ISNULL(m.[Pending], 0) - ISNULL(td.QtyDispatched, 0))
                                    END AS [Pending],

                                    ISNULL(td.QtyDispatched, 0) AS [Dispatch],
                                    m.[FG],
                                    ISNULL(m.[Powder coating], 0) AS [Powder coating],
                                    ISNULL(m.[Hydro Testing], 0) AS [Hydro Testing],

                                    CASE WHEN ISNULL(m.[Pending], 0) > 0
                                        THEN (CAST(ISNULL(td.QtyDispatched, 0) AS FLOAT) * 100.0 / CAST(m.[Pending] AS FLOAT))
                                        ELSE 100.0 END AS ItemAdherencePercent,

                                    CASE 
                                        WHEN (ISNULL(m.[Pending], 0) - ISNULL(td.QtyDispatched, 0)) < 0 THEN 0
                                        ELSE (ISNULL(m.[Pending], 0) - ISNULL(td.QtyDispatched, 0))
                                    END AS FailureInTrigger,

                                    cfr.reason AS SavedReason
                                FROM Production_pl p
                                LEFT JOIN master m ON p.[Item code ] = m.[Item code]
                                LEFT JOIN CurrentFailureReasons cfr ON p.[Item code ] = cfr.item_code
                                LEFT JOIN ItemsReachedFG fg ON p.[Item code ] = fg.item_code
                                LEFT JOIN TodayDispatch td ON p.[Item code ] = td.item_code

                                ORDER BY 
                                    CASE WHEN ISNULL(m.[Pending], 0) > 0 THEN 1 ELSE 0 END DESC,
                                    p.[priority_weight] DESC, 
                                    p.[S.No.] ASC
                                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
                            """
                cursor.execute(sql_top_plans, (offset, per_page))
                top_plans = [row_to_dict(cursor, row) for row in cursor.fetchall()]

                # --- VERTICAL OVERVIEW ---
                sql_vertical = """
                    DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
                    DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
                    DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

                    WITH VerticalFG AS (
                        SELECT m.[Vertical], SUM(pl.quantity) as QtyReachedFG
                        FROM production_log pl JOIN master m ON pl.item_code = m.[Item code]
                        WHERE pl.to_stage = 'FG' AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
                        GROUP BY m.[Vertical]
                    ),
                    VerticalDispatch AS (
                        SELECT m.[Vertical], SUM(pl.quantity) as QtyDispatched
                        FROM production_log pl JOIN master m ON pl.item_code = m.[Item code]
                        WHERE pl.from_stage = 'FG' AND pl.to_stage = 'Dispatch'
                        AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
                        GROUP BY m.[Vertical]
                    )
                    SELECT 
                        p.[Vertical], 
                        SUM(ISNULL(m.[Pending], 0)) as [Opening Trigger],
                        
                        CASE 
                            WHEN (SUM(ISNULL(m.[Pending], 0)) - ISNULL(vd.QtyDispatched, 0)) < 0 THEN 0
                            ELSE (SUM(ISNULL(m.[Pending], 0)) - ISNULL(vd.QtyDispatched, 0))
                        END as [Pending],

                        ISNULL(vd.QtyDispatched, 0) as [Dispatch],
                        ISNULL(vfg.QtyReachedFG, 0) as [Achieved]
                    FROM Production_pl p 
                    LEFT JOIN master m ON p.[Item code ] = m.[Item code]
                    LEFT JOIN VerticalFG vfg ON p.[Vertical] = vfg.[Vertical]
                    LEFT JOIN VerticalDispatch vd ON p.[Vertical] = vd.[Vertical]
                    WHERE p.[Vertical] IS NOT NULL 
                    GROUP BY p.[Vertical], vfg.QtyReachedFG, vd.QtyDispatched 
                    ORDER BY p.[Vertical]
                """
                cursor.execute(sql_vertical)
                vertical_overview = [row_to_dict(cursor, row) for row in cursor.fetchall()]
                for item in vertical_overview:
                    trig = item.get('Opening Trigger', 0)
                    ach = item.get('Achieved', 0)
                    item['AdherencePercent'] = (ach * 100.0 / trig) if trig > 0 else 100.0

                # --- CATEGORY OVERVIEW ---
                sql_category = """
                    DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
                    DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
                    DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

                    WITH CategoryFG AS (
                        SELECT m.[Category], SUM(pl.quantity) as QtyReachedFG
                        FROM production_log pl JOIN master m ON pl.item_code = m.[Item code]
                        WHERE pl.to_stage = 'FG' AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
                        GROUP BY m.[Category]
                    ),
                    CategoryDispatch AS (
                        SELECT m.[Category], SUM(pl.quantity) as QtyDispatched
                        FROM production_log pl JOIN master m ON pl.item_code = m.[Item code]
                        WHERE pl.from_stage = 'FG' AND pl.to_stage = 'Dispatch'
                        AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
                        GROUP BY m.[Category]
                    )
                    SELECT 
                        p.[Category], 
                        SUM(ISNULL(m.[Pending], 0)) as [Opening Trigger],
                        
                        CASE 
                            WHEN (SUM(ISNULL(m.[Pending], 0)) - ISNULL(cd.QtyDispatched, 0)) < 0 THEN 0
                            ELSE (SUM(ISNULL(m.[Pending], 0)) - ISNULL(cd.QtyDispatched, 0))
                        END as [Pending],

                        ISNULL(cd.QtyDispatched, 0) as [Dispatch],
                        ISNULL(cfg.QtyReachedFG, 0) as [Achieved]
                    FROM Production_pl p 
                    LEFT JOIN master m ON p.[Item code ] = m.[Item code]
                    LEFT JOIN CategoryFG cfg ON p.[Category] = cfg.[Category]
                    LEFT JOIN CategoryDispatch cd ON p.[Category] = cd.[Category]
                    WHERE p.[Category] IS NOT NULL 
                    GROUP BY p.[Category], cfg.QtyReachedFG, cd.QtyDispatched 
                    ORDER BY p.[Category]
                """
                cursor.execute(sql_category)
                category_overview = [row_to_dict(cursor, row) for row in cursor.fetchall()]
                for item in category_overview:
                    trig = item.get('Opening Trigger', 0)
                    ach = item.get('Achieved', 0)
                    item['AdherencePercent'] = (ach * 100.0 / trig) if trig > 0 else 100.0

            return render_template('dashboard.html',
                                   current_view='overview',
                                   stage_totals=stage_totals, stages=current_stages,
                                   top_plans=top_plans,
                                   pagination=pagination,
                                   vertical_overview=vertical_overview,
                                   category_overview=category_overview,
                                   total_wip_inventory=total_wip_inventory,
                                   all_verticals=all_verticals_list,
                                   daily_adherence=daily_adherence,
                                   total_monthly_dispatch=total_monthly_dispatch,
                                   monthly_trigger_adherence=monthly_trigger_adherence, # <--- PASS HERE
                                   failure_reasons=failure_reasons,
                                   show_failure_button=show_failure_button)

    except Exception as e:
        flash(f"An error occurred: {e}", "error")
        traceback.print_exc()
        # FALLBACK RENDER: Must include 'monthly_trigger_adherence'
        return render_template('dashboard.html', current_view='overview', stages=[], show_failure_button=False,
                               stage_totals={}, top_plans=[], pagination=None, vertical_overview=[],
                               category_overview=[],
                               total_monthly_dispatch=0, daily_adherence=0, monthly_adherence=0, 
                               monthly_trigger_adherence=0, # <--- PASS HERE TO FIX THE ERROR
                               failure_reasons=[],
                               all_verticals=[])
    finally:
        if conn: conn.close()

def calculate_monthly_trigger_adherence(conn, item_codes=None, verticals=None, categories=None, types=None):
    """
    Calculates Monthly Adherence as the AVERAGE of individual item adherences.
    1. For each item, calculate Monthly Adherence = Total Dispatch / Total Target
       (Where Daily Target = Prev Day's Carryover)
    2. Cap each item's adherence at 100%.
    3. Return the average of all active items.
    """
    try:
        cursor = conn.cursor()
        
        today = date.today()
        start_of_month = today.replace(day=1)
        lookback_date = start_of_month - timedelta(days=7) 

        # --- 1. Build Filter SQL ---
        join_master = " INNER JOIN master m ON t.item_code = m.[Item code] "
        log_join_master = " INNER JOIN master m ON pl.item_code = m.[Item code] "
        
        where_clause = ""
        params = []

        if item_codes:
            seq = ','.join(['?'] * len(item_codes))
            where_clause += f" AND m.[Item code] IN ({seq})"
            params.extend(item_codes)
        if verticals:
            seq = ','.join(['?'] * len(verticals))
            where_clause += f" AND m.[Vertical] IN ({seq})"
            params.extend(verticals)
        if categories:
            seq = ','.join(['?'] * len(categories))
            where_clause += f" AND m.[Category] IN ({seq})"
            params.extend(categories)
        if types:
            seq = ','.join(['?'] * len(types))
            where_clause += f" AND m.[Type] IN ({seq})"
            params.extend(types)

        # --- 2. Fetch Data ---
        sql_history = f"""
            SELECT t.upload_date, t.item_code, t.pending_quantity
            FROM trigger_history t
            {join_master}
            WHERE t.upload_date >= ? {where_clause}
        """
        history_params = [lookback_date] + params
        cursor.execute(sql_history, history_params)
        
        history_data = defaultdict(dict)
        for row in cursor.fetchall():
            history_data[row.upload_date][row.item_code] = row.pending_quantity

        sql_log = f"""
            SELECT CAST(pl.moved_at AS DATE) as move_date, pl.item_code, SUM(pl.quantity) as qty
            FROM production_log pl
            {log_join_master}
            WHERE pl.moved_at >= ? AND pl.to_stage IN ('Dispatch', 'Delivered')
            {where_clause}
            GROUP BY CAST(pl.moved_at AS DATE), pl.item_code
        """
        log_params = [lookback_date] + params
        cursor.execute(sql_log, log_params)
        
        dispatch_data = defaultdict(dict)
        for row in cursor.fetchall():
            dispatch_data[row.move_date][row.item_code] = row.qty

        all_dates = sorted(set(history_data.keys()) | set(dispatch_data.keys()))
        all_items = set()
        for d in history_data: all_items.update(history_data[d].keys())
        for d in dispatch_data: all_items.update(dispatch_data[d].keys())

        if not all_dates or not all_items: return 0

        # --- 3. Calculate Per-Item Adherence ---
        item_adherence_percentages = []

        for item in all_items:
            # Build timeline for this specific item
            timeline = []
            for d in all_dates:
                timeline.append({
                    'date': d,
                    'ot': history_data.get(d, {}).get(item, 0),
                    'dispatch': dispatch_data.get(d, {}).get(item, 0)
                })

            item_total_dispatch = 0
            item_total_target = 0

            for i, current_day in enumerate(timeline):
                if current_day['date'] < start_of_month: continue

                item_total_dispatch += current_day['dispatch']

                # Target Calculation:
                # Target = Prev_OT - Prev_Dispatch (Carryover)
                # If we produce everything (Carryover <= 0), Target for today is 0.
                day_target = 0
                if i > 0:
                    prev = timeline[i-1]
                    carryover = prev['ot'] - prev['dispatch']
                    day_target = max(0, carryover)
                else:
                    day_target = current_day['ot']

                item_total_target += day_target

            # Calculate Item %
            if item_total_target > 0:
                p = (item_total_dispatch / item_total_target) * 100.0
                item_adherence_percentages.append(min(100.0, p)) # Cap at 100%
            elif item_total_dispatch > 0:
                # Target was 0 (clean slate), but we produced items (New triggers arrived & cleared same day)
                # This counts as 100% adherence to demand
                item_adherence_percentages.append(100.0)
            # Else: Target 0, Dispatch 0 -> Inactive item, ignore from average

        # --- 4. Average of Percentages ---
        if not item_adherence_percentages:
            return 0

        avg_adherence = sum(item_adherence_percentages) / len(item_adherence_percentages)
        return avg_adherence

    except Exception as e:
        print(f"Error calculating monthly adherence: {e}")
        traceback.print_exc()
        return 0

@app.route('/api/adherence/filtered', methods=['POST'])
def api_adherence_filtered():
    data = request.get_json()
    item_codes = data.get('item_code', [])
    verticals = data.get('vertical', [])
    categories = data.get('category', [])
    types = data.get('type', [])

    conn = get_db_connection()
    result_data = {
        'daily_adherence': 0,
        'monthly_trigger_adherence': 0 
    }
    
    if conn:
        try:
            # 1. Calculate Daily Adherence
            daily_data = calculate_adherence(
                conn, 
                item_codes=item_codes, 
                verticals=verticals, 
                categories=categories,
                types=types
            )
            result_data['daily_adherence'] = daily_data.get('daily_adherence', 0)

            # 2. Calculate Monthly Trigger Adherence (Filtered)
            monthly_val = calculate_monthly_trigger_adherence(
                conn,
                item_codes=item_codes,
                verticals=verticals,
                categories=categories,
                types=types
            )
            result_data['monthly_trigger_adherence'] = monthly_val

        finally:
            conn.close()
    
    return jsonify(result_data)

def calculate_adherence(conn, item_codes=None, verticals=None, categories=None, types=None):
    """
    Helper function: Calculates ONLY Daily Adherence.
    Monthly Adherence logic has been removed.
    """
    daily_adherence = 0

    # 1. Build Filter Logic
    filter_clause_master = ""  
    params = []

    if item_codes:
        seq = ','.join(['?'] * len(item_codes))
        filter_clause_master += f" AND m.[Item code] IN ({seq})"
        params.extend(item_codes)
    if verticals:
        seq = ','.join(['?'] * len(verticals))
        filter_clause_master += f" AND m.[Vertical] IN ({seq})"
        params.extend(verticals)
    if categories:
        seq = ','.join(['?'] * len(categories))
        filter_clause_master += f" AND m.[Category] IN ({seq})"
        params.extend(categories)
    if types:
        seq = ','.join(['?'] * len(types))
        filter_clause_master += f" AND m.[Type] IN ({seq})"
        params.extend(types)

    try:
        cursor = conn.cursor()

        # --- DAILY ADHERENCE CALCULATION ---
        sql_daily_target = f"SELECT SUM(ISNULL(m.[Pending], 0)) FROM master m WHERE 1=1 {filter_clause_master}"
        cursor.execute(sql_daily_target, params)
        total_daily_target = cursor.fetchone()[0] or 0

        sql_daily_actual = f"""
            DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
            DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
            DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

            SELECT SUM(pl.quantity) FROM production_log pl
            INNER JOIN master m ON pl.item_code = m.[Item code]
            WHERE pl.to_stage IN ('Dispatch', 'Delivered') 
            AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
            {filter_clause_master}
        """
        cursor.execute(sql_daily_actual, params)
        total_daily_dispatched = cursor.fetchone()[0] or 0

        if total_daily_target > 0:
            daily_adherence = (total_daily_dispatched / total_daily_target) * 100

    except Exception:
        traceback.print_exc()

    # Only return daily adherence
    return {'daily_adherence': daily_adherence}


@app.route('/dispatch_item', methods=['POST'])
def dispatch_item():
    # Support both JSON (from Dashboard Drag & Drop) and Form Data (from WIP Modal)
    if request.is_json:
        data = request.get_json()
        item_code = data.get('item_code')
        quantity = data.get('quantity')
    else:
        item_code = request.form.get('item_code')
        quantity = request.form.get('quantity')

    # Validate inputs
    try:
        quantity = int(quantity)
        if quantity <= 0:
            return jsonify({'status': 'error', 'message': "Quantity must be greater than 0."}), 400
    except (ValueError, TypeError):
        return jsonify({'status': 'error', 'message': "Invalid quantity."}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'status': 'error', 'message': "Database connection failed."}), 500

    try:
        cursor = conn.cursor()

        # 1. CHECK STOCK in FG
        cursor.execute("SELECT ISNULL([FG], 0) FROM master WHERE [Item code] = ?", (item_code,))
        row = cursor.fetchone()

        if not row:
            return jsonify({'status': 'error', 'message': f"Item {item_code} not found."}), 404

        current_fg_stock = row[0]

        if current_fg_stock < quantity:
            return jsonify({'status': 'error', 'message': f"Insufficient stock in FG. Available: {current_fg_stock}"}), 400

        # 2. UPDATE MASTER (Deduct FG)
        cursor.execute("UPDATE master SET [FG] = ISNULL([FG], 0) - ? WHERE [Item code] = ?", (quantity, item_code))

        # 3. UPDATE PRODUCTION_PL (Add to Dispatch)
        cursor.execute("UPDATE Production_pl SET [Dispatch] = ISNULL([Dispatch], 0) + ? WHERE [Item code ] = ?", (quantity, item_code))

        # 4. LOG THE DISPATCH
        cursor.execute("""
            INSERT INTO production_log (item_code, from_stage, to_stage, quantity, moved_at)
            VALUES (?, 'FG', 'Dispatch', ?, GETDATE())
        """, (item_code, quantity))

        conn.commit()
        return jsonify({'status': 'success', 'message': f"Successfully dispatched {quantity} of {item_code}."})

    except Exception as e:
        conn.rollback()
        print(f"Error in /dispatch_item: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/plan_page', endpoint='api_plan_page')
def api_plan_page():
    page = request.args.get('page', 1, type=int)
    per_page = 17
    offset = (page - 1) * per_page
    
    # --- NEW: Get Type Filter ---
    filter_type = request.args.get('type') 
    
    conn = get_db_connection()
    if not conn: return jsonify({'status': 'error', 'message': 'Database connection failed.'}), 500

    try:
        cursor = conn.cursor()
        
        # Build Filter Clause
        where_clause = ""
        params = []
        if filter_type:
            where_clause = " WHERE m.[Type] = ?"
            params.append(filter_type)

        # Count Total
        count_sql = f"SELECT COUNT(*) FROM Production_pl p LEFT JOIN master m ON p.[Item code ] = m.[Item code] {where_clause}"
        cursor.execute(count_sql, params)
        total = cursor.fetchone()[0]
        total_pages = ceil(total / per_page) if total > 0 else 1

        sql_top_plans = f"""
            DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
            DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
            DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

            WITH CurrentFailureReasons AS (
                SELECT item_code, MAX(reason) as reason 
                FROM failure_log WHERE failure_date = @CurrentProductionDate GROUP BY item_code
            ),
            TodayDispatch AS (
                SELECT item_code, SUM(quantity) as QtyDispatched
                FROM production_log
                WHERE to_stage IN ('Dispatch', 'Delivered') 
                AND moved_at >= @ProductionDayStart AND moved_at < @ProductionDayEnd
                GROUP BY item_code
            )
            SELECT
                p.[S.No.], p.[Item code ], p.[Description], 
                ISNULL(m.[Pending], 0) AS [Opening Trigger],
                ISNULL(m.[Next Pending], 0) AS [Next Pending],
                (ISNULL(m.[Pending], 0) - ISNULL(td.QtyDispatched, 0)) AS [Pending],
                p.[Today Triggered] AS [Released Qty], 
                m.[FG], 
                ISNULL(td.QtyDispatched, 0) as [Dispatch],
                ISNULL(m.[Powder coating], 0) AS [Powder coating],
                ISNULL(m.[Hydro Testing], 0) AS [Hydro Testing],
                CASE WHEN ISNULL(m.[Pending], 0) > 0
                    THEN (CAST(ISNULL(td.QtyDispatched, 0) AS FLOAT) * 100.0 / CAST(m.[Pending] AS FLOAT))
                    ELSE 100.0 END AS ItemAdherencePercent,
                (ISNULL(m.[Pending], 0) - ISNULL(td.QtyDispatched, 0)) AS FailureInTrigger,
                cfr.reason AS SavedReason
            FROM Production_pl p
            LEFT JOIN master m ON p.[Item code ] = m.[Item code]
            LEFT JOIN CurrentFailureReasons cfr ON p.[Item code ] = cfr.item_code
            LEFT JOIN TodayDispatch td ON p.[Item code ] = td.item_code
            {where_clause} -- Apply Filter
            ORDER BY 
                CASE WHEN ISNULL(m.[Pending], 0) > 0 THEN 1 ELSE 0 END DESC,
                p.[priority_weight] DESC, 
                p.[S.No.] ASC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        # Add pagination params to the end
        params.extend([offset, per_page])
        
        cursor.execute(sql_top_plans, params)
        plans = [row_to_dict(cursor, row) for row in cursor.fetchall()]

        return jsonify({'status': 'success', 'plans': plans, 'total_pages': total_pages, 'current_page': page, 'per_page': per_page, 'show_failure_button': True})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        if conn: conn.close()


@app.route('/api/log_failure_reason', methods=['POST'])
def log_failure_reason():
    data = request.get_json()
    item_code = data.get('item_code')
    failed_quantity = data.get('failed_quantity')
    reason = data.get('reason')

    if not item_code or failed_quantity is None: # Allow empty reason to clear it
        return jsonify({'status': 'error', 'message': 'Missing item code or failed quantity.'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'status': 'error', 'message': 'Database connection failed.'}), 500

    try:
        cursor = conn.cursor()

        # Get current production date (using 2 AM cutoff)
        cursor.execute("SELECT CAST(DATEADD(hour, -2, GETDATE()) AS DATE)")
        current_production_date = cursor.fetchone()[0]

        # Use MERGE for UPSERT logic
        merge_sql = """
        MERGE failure_log AS target
        USING (SELECT ? AS item_code, ? AS failure_date, ? AS failed_quantity, ? AS reason) AS source
        ON (target.item_code = source.item_code AND target.failure_date = source.failure_date)
        WHEN MATCHED THEN
            UPDATE SET
                reason = source.reason,
                failed_quantity = source.failed_quantity, -- Update qty in case plan changes
                logged_at = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (item_code, failure_date, failed_quantity, reason)
            VALUES (source.item_code, source.failure_date, source.failed_quantity, source.reason);
        """
        cursor.execute(merge_sql, item_code, current_production_date, failed_quantity, reason if reason else None) # Store NULL if reason is empty string
        conn.commit()

        return jsonify({'status': 'success', 'message': 'Reason logged.'})

    except Exception as e:
        if conn: conn.rollback()
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': f'Error logging reason: {str(e)}'}), 500
    finally:
        if conn: conn.close()



# In app/routes.py
# REPLACE your existing 'attendance' function with this one

# In app/routes.py
# REPLACE your existing 'attendance' function with this one

@app.route('/attendance', methods=['GET'], endpoint='attendance')
@login_required  
def attendance():
    start_date_str = request.args.get('start_date')
    selected_stage = request.args.get('stage', 'All')

    # Use selected date or today's date
    if start_date_str:
        today = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    else:
        today = date.today()

    start_of_week = today - timedelta(days=today.weekday())
    week_days = [start_of_week + timedelta(days=i) for i in range(7)]
    prev_week_start, next_week_start = start_of_week - timedelta(days=7), start_of_week + timedelta(days=7)

    conn = get_db_connection()
    employees_list, attendance_data, calendar_exceptions, stage_filter_list = [], {}, {}, []
    manage_employees_list = []  # List for the manage employees tab
    roster_dict = {}  # NEW: To store temporary assignments

    if conn:
        try:
            cursor = conn.cursor()

            # --- Create daily_roster table if it doesn't exist ---
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='daily_roster' and xtype='U')
            CREATE TABLE daily_roster (
                roster_date DATE NOT NULL,
                employee_code NVARCHAR(50) NOT NULL,
                employee_name NVARCHAR(255),
                default_stage NVARCHAR(255),
                assigned_stage NVARCHAR(255),
                transfer_reason NVARCHAR(MAX),
                PRIMARY KEY (roster_date, employee_code)
            );
            """)
            conn.commit()

            # Get stage list for filters (from main employees table)
            cursor.execute("SELECT DISTINCT [Stage] FROM employees WHERE [Stage] IS NOT NULL ORDER BY [Stage]")
            stage_filter_list = [row[0] for row in cursor.fetchall()]

            # --- NEW: Get full employee list for "Manage Employees" tab ---
            cursor.execute("SELECT * FROM employees ORDER BY [Name of the Employee]")
            manage_employees_list = [row_to_dict(cursor, row) for row in cursor.fetchall()]

            # --- MODIFIED: Get full employee list for "Attendance View" tab, with filtering ---
            sql_employees = "SELECT * FROM employees"
            params = []
            if selected_stage != 'All':
                sql_employees += " WHERE [Stage] = ?"  # Filter by default stage
                params.append(selected_stage)
            sql_employees += " ORDER BY [Name of the Employee]"

            cursor.execute(sql_employees, params)
            employees_list = [row_to_dict(cursor, row) for row in cursor.fetchall()]  # This is the full list

            # Get calendar exceptions
            cursor.execute("SELECT holiday_date, day_type FROM holidays WHERE holiday_date BETWEEN ? AND ?",
                           week_days[0], week_days[-1])
            calendar_exceptions = {row.holiday_date: row.day_type for row in cursor.fetchall()}

            # Get attendance data for the week
            cursor.execute(
                "SELECT employee_code, attendance_date, status, shift FROM attendance_log WHERE attendance_date BETWEEN ? AND ?",
                week_days[0], week_days[-1])
            for row in cursor.fetchall():
                e_code, att_date, status, shift = row
                if e_code not in attendance_data: attendance_data[e_code] = {}
                if status == 'Present' and shift:
                    attendance_data[e_code][att_date] = f"Present-{shift}"
                else:
                    attendance_data[e_code][att_date] = status

            # --- NEW: Get this week's roster data ---
            cursor.execute("SELECT * FROM daily_roster WHERE roster_date BETWEEN ? AND ?",
                           (week_days[0], week_days[-1]))
            roster_data_raw = cursor.fetchall()
            roster_dict = {}  # Format: { ecode: { date: {assigned_stage: '...'} } }
            for row in roster_data_raw:
                row_dict = row_to_dict(cursor, row)
                ecode = row_dict['employee_code']
                r_date = row_dict['roster_date']
                if ecode not in roster_dict:
                    roster_dict[ecode] = {}
                roster_dict[ecode][r_date] = {
                    'assigned_stage': row_dict['assigned_stage'],
                    'default_stage': row_dict['default_stage']
                }
            # --- END NEW ---

        except Exception as e:
            flash(f'An error occurred: {e}', 'error')
            traceback.print_exc()
        finally:
            if conn: conn.close()

    return render_template('attendance.html',
                           employees=employees_list,  # Full employee list
                           manage_employees=manage_employees_list,  # Full employee list for manage tab
                           week_days=week_days,
                           attendance_data=attendance_data,
                           roster_dict=roster_dict,  # Pass the roster data
                           holidays=calendar_exceptions,
                           prev_week_start=prev_week_start.strftime('%Y-%m-%d'),
                           next_week_start=next_week_start.strftime('%Y-%m-%d'),
                           current_week_str=f"{start_of_week.strftime('%b %d')} - {week_days[-1].strftime('%b %d, %Y')}",
                           stage_filter_list=stage_filter_list,
                           selected_stage=selected_stage,
                           today=today,  # Pass the 'today' (viewed date) variable
                           date=date  # Pass the date module for comparison
                           )




# In app/routes.py

@app.route('/generate_plan', methods=['POST'], endpoint='generate_plan')
def generate_plan():
    # --- 1. SECURITY CHECK: Verify Triggers Uploaded ---
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('production_planning'))

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT status_value FROM app_status WHERE status_key = 'last_triggers_upload_timestamp'")
        row = cursor.fetchone()

        triggers_valid = False
        if row and row[0]:
            upload_time = pd.to_datetime(row[0])
            now = datetime.now()
            if now.hour < 2:
                current_prod_date = (now - timedelta(days=1)).date()
            else:
                current_prod_date = now.date()
            if upload_time.date() >= current_prod_date:
                triggers_valid = True

        if not triggers_valid:
            flash('Error: You must upload the Pending Triggers file for today before generating the plan.', 'error')
            return redirect(url_for('production_planning'))

        # --- 2. PROCEED WITH GENERATION ---
        ahp_weights = session.get('ahp_weights', DEFAULT_AHP_WEIGHTS.copy())

        # Ensure Columns Exist in Master/Production_pl
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'Daily Max Plan' AND Object_ID = Object_ID(N'master'))
                ALTER TABLE master ADD [Daily Max Plan] INT DEFAULT 0 WITH VALUES;
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'Max Inv' AND Object_ID = Object_ID(N'master'))
                ALTER TABLE master ADD [Max Inv] INT DEFAULT 0 WITH VALUES;
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'CalculatedTrigger' AND Object_ID = Object_ID(N'Production_pl'))
                ALTER TABLE Production_pl ADD [CalculatedTrigger] INT DEFAULT 0 WITH VALUES;
             -- Ensure Live Dispatch exists in Master
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'Live Dispatch' AND Object_ID = Object_ID(N'master'))
                ALTER TABLE master ADD [Live Dispatch] INT DEFAULT 0 WITH VALUES;
        """)
        conn.commit()

        # --- 3. CALCULATE PLAN (New Formula) ---

        # We need to sum up the WIP stages to get "Current WIP".
        # We assume stages are columns in master. We exclude 'Not Started' and 'FG' from WIP usually,
        # but based on your previous 'Calculated_Total_Inv' logic, we sum the active stages.
        # Ensure STAGES list is available or hardcoded for the SQL sum.
        # Default active stages for WIP calculation:
        wip_stages_sql = "ISNULL([Long seam], 0) + ISNULL([Dish fit up], 0) + ISNULL([Cirseam welding], 0) + ISNULL([Part assembly], 0) + ISNULL([Full welding], 0) + ISNULL([Hydro Testing], 0) + ISNULL([Powder coating], 0) + ISNULL([PDI], 0)"

        sql_query = f"""
        WITH CalculatedData AS (
            SELECT 
                [Item code],
                [Description],
                [Vertical],
                [Category],
                [Type],
                [Model],
                [RPL days to Delivery],
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

        # --- APPLY THE FORMULAS ---

        # 1. Available WIP Space = Max Inventory - Current WIP
        plan_df['Available_WIP_Space'] = plan_df['Max_Inventory'] - plan_df['Current_WIP']

        # 2. Total Potential = Available WIP Space + Pending Trigger
        plan_df['Total_Potential'] = plan_df['Available_WIP_Space'] + plan_df['Pending_Trigger']

        # 3. Actual Production = MAX(0, MIN(Total Potential, Available WIP space, Max Daily Plan))
        # We use apply to handle the row-wise MIN logic
        def calculate_actual(row):
            # min() of the three values
            val = min(row['Total_Potential'], row['Available_WIP_Space'], row['Max_Daily_Plan'])
            # max(0, val)
            return max(0, val)

        plan_df['Daily Suggested Release Plan'] = plan_df.apply(calculate_actual, axis=1).astype(int)

        # Note: "New Pending Trigger" is not stored in the DB during generation (that happens on release),
        # but the plan is now derived correctly.

        # --- 4. FINALIZE DATAFRAME FOR SAVING ---

        # Prepare columns for Production_pl table
        plan_df['Item code'] = plan_df['Item code'].astype(str).str.strip()

        # Set Modified Plan default to the Calculated Plan
        plan_df['Modified Release plan'] = plan_df['Daily Suggested Release Plan']

        # For Adherence/Dashboard: Set CalculatedTrigger to the Suggestion
        plan_df['CalculatedTrigger'] = plan_df['Daily Suggested Release Plan']

        # Apply AHP Priority Sorting
        prioritized_df = prioritize_plan_with_ahp(plan_df, ahp_weights)

        final_plan_to_save = prioritized_df.sort_values(by='priority_weight', ascending=False)
        final_plan_to_save.reset_index(drop=True, inplace=True)
        final_plan_to_save.insert(0, 'S.No.', final_plan_to_save.index + 1)

        # Initialize other required columns
        final_plan_to_save['Today Triggered'] = 0
        final_plan_to_save['Failure in Trigger'] = 0
        final_plan_to_save['Dispatch'] = final_plan_to_save['Current_Dispatch']

        # Rename for DB compatibility
        final_plan_to_save.rename(columns={'Item code': 'Item code ', 'Pending_Trigger': 'Pending'}, inplace=True)

        # Drop temp calculation columns
        cols_to_drop = ['Available_WIP_Space', 'Total_Potential', 'Max_Inventory', 'Max_Daily_Plan', 'Current_WIP',
                        'Current_Dispatch', 'type_score', 'rpl_score', 'category_score', 'qty_score']
        final_plan_to_save.drop(columns=cols_to_drop, inplace=True, errors='ignore')

        replace_table_with_df(final_plan_to_save, 'Production_pl', cursor)
        conn.commit()

        flash('Production Plan generated successfully using Capacity Constraints.', 'success')

    except Exception as e:
        if conn: conn.rollback()
        flash(f'Error generating plan: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()

    return redirect(url_for('production_planning'))



@app.route('/delete_triggers', methods=['POST'], endpoint='delete_triggers')
def delete_triggers():
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(request.referrer or url_for('production_planning'))

    try:
        cursor = conn.cursor()
        
        now = datetime.now()
        current_date = (now - timedelta(days=1)).date() if now.hour < 2 else now.date()

        # 1. Clear raw table
        cursor.execute("IF OBJECT_ID('dbo.pending_triggers', 'U') IS NOT NULL DELETE FROM pending_triggers")
        
        # 2. Reset master quantities
        cursor.execute("IF OBJECT_ID('dbo.master', 'U') IS NOT NULL UPDATE master SET [Pending] = 0, [Next Pending] = 0")
        
        # 3. Manual Reset: Remove from history (because the user is manually "undoing" today's upload)
        cursor.execute("DELETE FROM trigger_history WHERE upload_date = ?", current_date)
        
        # 4. Reset status
        cursor.execute("UPDATE app_status SET status_value = NULL WHERE status_key = 'last_triggers_upload_timestamp'")

        conn.commit()
        flash("Pending triggers removed and today's history entry cleared.", "success")
    except Exception as e:
        flash(f'Error: {e}', 'error')
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

    return redirect(request.referrer or url_for('production_planning'))


@app.route('/update_single_plan', methods=['POST'])
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
        # Note: Production_pl usually has "[Item code ]" with a space
        sql_plan = "UPDATE Production_pl SET [Modified Release plan] = ? WHERE [Item code ] LIKE ?"
        cursor.execute(sql_plan, (new_plan_value, item_code))

        # 2. Update Master Table (Not Started)
        # Note: master usually has "[Item code]" without a space
        # We only update if the "Not Started" column exists
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
        # Read table back
        updated_plan_df = pd.read_sql("SELECT * FROM Production_pl", conn)

        # Helper to normalize column names for pandas
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

@app.route('/move_item', methods=['POST'])
def move_item():
    # Get form data
    if request.is_json:
        data = request.get_json()
        item_code = data.get('item_code')
        from_stage = data.get('source_stage')
        to_stage = data.get('dest_stage')
        quantity = data.get('quantity')
    else:
        item_code = request.form.get('item_code')
        from_stage = request.form.get('from_stage') or request.form.get('source_stage')
        to_stage = request.form.get('to_stage') or request.form.get('dest_stage')
        quantity = request.form.get('quantity')

    # Validate inputs
    if not all([item_code, from_stage, to_stage, quantity]):
        return jsonify({'status': 'error', 'message': 'Missing required fields.'}), 400

    # --- HANDLE VIRTUAL GROUP DESTINATION ---
    # If moving items BACK to "Before Hydro", place them in the last stage of that group ("Full welding")
    if to_stage == 'Before Hydro':
        to_stage = 'Full welding'

    # --- SAFETY CHECK FOR VIRTUAL GROUP SOURCE ---
    # We cannot deduct from "Before Hydro" directly; the frontend must resolve this to a concrete sub-stage.
    if from_stage == 'Before Hydro':
        return jsonify({
            'status': 'error', 
            'message': 'System Error: Cannot move directly from "Before Hydro" group without a resolved sub-stage. Please refresh the page and try again.'
        }), 400

    try:
        quantity = int(quantity)
        if quantity <= 0:
            return jsonify({'status': 'error', 'message': 'Quantity must be greater than 0.'}), 400
    except (ValueError, TypeError):
        return jsonify({'status': 'error', 'message': 'Invalid quantity.'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'status': 'error', 'message': 'Database connection failed.'}), 500

    try:
        cursor = conn.cursor()

        # 1. CHECK STOCK
        # We wrap stage names in brackets to handle spaces safely
        cursor.execute(f"SELECT [{from_stage}] FROM master WHERE [Item code] = ?", (item_code,))
        row = cursor.fetchone()

        if not row:
            return jsonify({'status': 'error', 'message': f'Item {item_code} not found.'}), 404

        current_stock = row[0] or 0

        if current_stock < quantity:
            return jsonify(
                {'status': 'error', 'message': f'Insufficient stock in {from_stage}. Available: {current_stock}'}), 400

        # 2. PERFORM THE MOVE

        # A. Subtract from Source
        cursor.execute(f"UPDATE master SET [{from_stage}] = ISNULL([{from_stage}], 0) - ? WHERE [Item code] = ?",
                       (quantity, item_code))

        # B. Add to Destination (Update Inventory)
        # We update the master table for ANY stage except 'Dispatch' (which is an exit action)
        if to_stage != 'Dispatch':
            cursor.execute(f"UPDATE master SET [{to_stage}] = ISNULL([{to_stage}], 0) + ? WHERE [Item code] = ?",
                           (quantity, item_code))

        # 3. HANDLE DISPATCH COUNT
        if to_stage == 'Dispatch':
            cursor.execute("""
                UPDATE Production_pl 
                SET [Dispatch] = ISNULL([Dispatch], 0) + ? 
                WHERE [Item code ] = ?
            """, (quantity, item_code))

        # 4. LOG THE MOVEMENT
        cursor.execute("""
            INSERT INTO production_log (item_code, from_stage, to_stage, quantity, moved_at)
            VALUES (?, ?, ?, ?, GETDATE())
        """, (item_code, from_stage, to_stage, quantity))

        conn.commit()

        return jsonify({'status': 'success', 'message': f'Moved {quantity} items successfully.'})

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error in /move_item: {e}")
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        if conn: conn.close()


@app.route('/deliver_item', methods=['POST'], endpoint='deliver_item')
def deliver_item():
    data = request.get_json()
    item_code, quantity = data.get('item_code'), int(data.get('quantity', 0))
    if not all([item_code, quantity > 0]): return jsonify(
        {'status': 'error', 'message': 'Missing or invalid data.'}), 400

    conn = get_db_connection()
    if not conn: return jsonify({'status': 'error', 'message': 'DB connection failed.'}), 500

    try:
        cursor = conn.cursor()

        # Step 1: Update master table (FG and Live Dispatch)
        cursor.execute(
            "UPDATE master SET [FG] = [FG] - ?, [Live Dispatch] = [Live Dispatch] + ? WHERE [Item code ] = ? AND [FG] >= ?",
            quantity, quantity, item_code, quantity)

        # Check if the update was successful
        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({'status': 'error', 'message': 'Not enough quantity in FG to deliver.'}), 400

        # --- NEW: Step 2: Log the delivery in production_log ---
        # Using 'Delivered' as the to_stage to clearly mark it
        cursor.execute(
            "INSERT INTO production_log (item_code, quantity, from_stage, to_stage) VALUES (?, ?, ?, ?)",
            item_code, quantity, 'FG', 'Delivered'
        )
        # --- END NEW ---

        conn.commit()  # Commit both the update and the insert

        return jsonify({'status': 'success', 'redirect_url': url_for('wip_tracking')})

    except Exception as e:
        if conn: conn.rollback()
        traceback.print_exc()
        # Changed error message slightly for clarity
        return jsonify({'status': 'error', 'message': f'Error during delivery: {str(e)}'}), 500
    finally:
        if conn: conn.close()


@app.context_processor
def inject_upload_status():
    """
    Injects upload status variables into all templates
    for use in the settings modal.
    """
    conn = get_db_connection()
    table_exists = False
    last_triggers_upload = None
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM master")
            if cursor.fetchone()[0] > 0:
                table_exists = True

            cursor.execute("SELECT status_value FROM app_status WHERE status_key = 'last_triggers_upload_timestamp'")
            row = cursor.fetchone()
            if row and row[0]:
                last_triggers_upload = pd.to_datetime(row[0])
        except pyodbc.ProgrammingError:
            table_exists = False  # master table doesn't exist
        except Exception as e:
            print(f"Error in context processor: {e}")
        finally:
            conn.close()

    return dict(
        master_table_exists=table_exists,
        last_triggers_upload_time=last_triggers_upload
    )


@app.route('/flag_item', methods=['POST'], endpoint='flag_item')
def flag_item():
    conn = None
    try:
        data = request.get_json()
        print(f"DEBUG: Received Flag Request: {data}")  # DEBUG PRINT 1

        item_code = data.get('item_code')
        stage = data.get('stage')
        quantity = int(data.get('quantity', 0))
        remark = data.get('remark')

        if quantity <= 0:
            return jsonify({'status': 'error', 'message': 'Quantity must be > 0'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Check Stock
        # NOTE: We wrap stage in brackets [] to handle spaces in names like 'Long seam'
        check_sql = f"SELECT [{stage}] FROM master WHERE [Item code] = ?"
        print(f"DEBUG: Checking stock with SQL: {check_sql} for {item_code}")  # DEBUG PRINT 2

        cursor.execute(check_sql, item_code)
        row = cursor.fetchone()

        if not row:
            print("DEBUG: Item not found in Master table")
            return jsonify({'status': 'error', 'message': 'Item not found in Master.'}), 404

        current_qty = row[0] or 0
        print(f"DEBUG: Current Stock: {current_qty}, Requesting: {quantity}")  # DEBUG PRINT 3

        if current_qty < quantity:
            return jsonify(
                {'status': 'error', 'message': f'Cannot flag {quantity}. Only {current_qty} available.'}), 400

        # 2. Update Master (Deduct Qty)
        update_sql = f"UPDATE master SET [{stage}] = [{stage}] - ? WHERE [Item code] = ?"
        cursor.execute(update_sql, (quantity, item_code))
        print("DEBUG: Master table updated (Deduction)")  # DEBUG PRINT 4

        # 3. Insert into flagged_items
        insert_sql = """
            INSERT INTO flagged_items (item_code, stage, quantity, reason, flagged_at)
            VALUES (?, ?, ?, ?, GETDATE())
        """
        cursor.execute(insert_sql, (item_code, stage, quantity, remark))
        print("DEBUG: Inserted into flagged_items")  # DEBUG PRINT 5

        conn.commit()
        print("DEBUG: Transaction Committed Successfully")  # DEBUG PRINT 6

        return jsonify({'status': 'success', 'message': f'Flagged {quantity} items.'})

    except Exception as e:
        if conn: conn.rollback()
        print(f"CRITICAL ERROR in flag_item: {e}")  # LOOK FOR THIS IN TERMINAL
        # Return the actual error to the frontend alert so you can see it
        return jsonify({'status': 'error', 'message': f"Server Error: {str(e)}"}), 500
    finally:
        if conn: conn.close()

@app.route('/update_weights', methods=['POST'], endpoint='update_weights')
def update_weights():
    try:
        raw_weights = {'type': float(request.form.get('weight_type', 0)),
                       'rpl': float(request.form.get('weight_rpl', 0)),
                       'category': float(request.form.get('weight_category', 0)),
                       'modified_plan': float(request.form.get('weight_modified_plan', 0))}
        total_weight = sum(raw_weights.values())
        if total_weight == 0:
            flash('All weights cannot be zero. Please enter at least one positive value.', 'error')
            return redirect(url_for('production_planning'))
        session['ahp_weights'] = {key: value / total_weight for key, value in raw_weights.items()}
        flash('AHP weights have been updated and normalized successfully!', 'success')
    except ValueError:
        flash('Invalid input. Please enter valid numbers for all weight fields.', 'error')
    except Exception as e:
        flash(f'An unexpected error occurred: {e}', 'error')
        traceback.print_exc()
    return redirect(url_for('production_planning'))

@app.route('/transfer_worker', methods=['POST'], endpoint='transfer_worker')
def transfer_worker():
    pass

@app.route('/attendance/mark_single', methods=['POST'])
def mark_single_attendance():
    data = request.get_json()
    e_code = data.get('employee_code')
    date_str = data.get('date')
    value = data.get('status')  # e.g., "Present-1", "Absent", ""

    if not all([e_code, date_str]):
        return jsonify({'status': 'error', 'message': 'Missing data'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor()
        attendance_date = datetime.strptime(date_str, '%Y-%m-%d').date()

        # CASE 1: Value is empty (User selected "--") -> CLEAR RECORD
        if not value:
            # Delete from attendance_log
            cursor.execute("DELETE FROM attendance_log WHERE employee_code = ? AND attendance_date = ?", e_code, attendance_date)
            # Delete from daily_roster (not working)
            cursor.execute("DELETE FROM daily_roster WHERE employee_code = ? AND roster_date = ?", e_code, attendance_date)

        # CASE 2: Value is "Absent" -> SAVE ABSENT, REMOVE FROM ROSTER
        elif value == 'Absent':
            # UPSERT into attendance_log as 'Absent'
            upsert_log_sql = """
            MERGE attendance_log AS target
            USING (SELECT ? AS employee_code, ? AS attendance_date, 'Absent' AS status, NULL AS shift) AS source
            ON (target.employee_code = source.employee_code AND target.attendance_date = source.attendance_date)
            WHEN MATCHED THEN UPDATE SET status = source.status, shift = source.shift
            WHEN NOT MATCHED THEN INSERT (employee_code, attendance_date, status, shift)
            VALUES (source.employee_code, source.attendance_date, source.status, source.shift);"""
            cursor.execute(upsert_log_sql, e_code, attendance_date)

            # Remove from daily_roster since they are absent
            cursor.execute("DELETE FROM daily_roster WHERE employee_code = ? AND roster_date = ?", e_code, attendance_date)

        # CASE 3: Value is "Present" (e.g., "Present-1") -> SAVE PRESENT, ADD TO ROSTER
        else:
            status_val = None
            shift_val = None
            if '-' in value:
                parts = value.split('-')
                status_val = parts[0]  # "Present"
                shift_val = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            else:
                status_val = value  # Should be "Present"

            # 1. UPSERT into attendance_log
            upsert_log_sql = """
            MERGE attendance_log AS target
            USING (SELECT ? AS employee_code, ? AS attendance_date, ? AS status, ? AS shift) AS source
            ON (target.employee_code = source.employee_code AND target.attendance_date = source.attendance_date)
            WHEN MATCHED THEN UPDATE SET status = source.status, shift = source.shift
            WHEN NOT MATCHED THEN INSERT (employee_code, attendance_date, status, shift)
            VALUES (source.employee_code, source.attendance_date, source.status, source.shift);"""
            cursor.execute(upsert_log_sql, e_code, attendance_date, status_val, shift_val)

            # 2. MERGE into daily_roster (Add them to roster if they aren't there yet)
            # We assume if they are present, they are working their default stage unless transferred
            merge_roster_sql = """
            MERGE daily_roster AS target
            USING (
                SELECT [E.code], [Name of the Employee], [Stage]
                FROM employees
                WHERE [E.code] = ?
            ) AS source
            ON (target.employee_code = source.[E.code] AND target.roster_date = ?)
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (roster_date, employee_code, employee_name, default_stage, assigned_stage)
                VALUES (?, source.[E.code], source.[Name of the Employee], source.[Stage], source.[Stage]);
            """
            cursor.execute(merge_roster_sql, e_code, attendance_date, attendance_date)

        conn.commit()
        return jsonify({'status': 'success'})

    except Exception as e:
        if conn: conn.rollback()
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        if conn: conn.close()


@app.route('/attendance/working_day', methods=['POST'], endpoint='mark_working_day')
def mark_working_day():
    work_date_str = request.form.get('work_date')
    description = request.form.get('description', 'Working Day')
    start_date = request.form.get('start_date')
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('attendance', start_date=start_date))
    try:
        cursor = conn.cursor()
        # Use MERGE to insert a new record or update an existing one
        sql = """
        MERGE holidays AS target
        USING (SELECT ? AS holiday_date, ? AS description, 'Working Day' AS day_type) AS source
        ON (target.holiday_date = source.holiday_date)
        WHEN MATCHED THEN
            UPDATE SET day_type = source.day_type, description = source.description
        WHEN NOT MATCHED THEN
            INSERT (holiday_date, description, day_type) VALUES (source.holiday_date, source.description, source.day_type);
        """
        cursor.execute(sql, work_date_str, description)
        conn.commit()
        flash(f'Date {work_date_str} marked as a Working Day.', 'success')
    except Exception as e:
        flash(f'An error occurred: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()
    return redirect(url_for('attendance', start_date=start_date))

@app.route('/attendance/export', methods=['GET'], endpoint='export_attendance')
def export_attendance():
    conn = get_db_connection()
    year, month = request.args.get('year', datetime.now().year, type=int), request.args.get('month',
                                                                                            datetime.now().month,
                                                                                            type=int)
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('attendance'))
    try:
        df_employees = pd.read_sql(
            "SELECT [E.code], [Name of the Employee] FROM employees ORDER BY [Name of the Employee]", conn)
        df_log = pd.read_sql(
            "SELECT employee_code, attendance_date, status FROM attendance_log WHERE YEAR(attendance_date) = ? AND MONTH(attendance_date) = ?",
            conn, params=(year, month))
        df_report = df_employees if df_log.empty else pd.merge(df_employees, df_log.pivot(index='employee_code',
                                                                                          columns='attendance_date',
                                                                                          values='status').reset_index(),
                                                               left_on='E.code', right_on='employee_code',
                                                               how='left').drop(columns=['employee_code'],
                                                                                errors='ignore')
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_report.to_excel(writer, index=False, sheet_name='Attendance')
        output.seek(0)
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         as_attachment=True, download_name=f'attendance_{year}_{month:02d}.xlsx')
    except Exception as e:
        flash(f"Error exporting data: {e}", "error")
        traceback.print_exc()
        return redirect(url_for('attendance'))
    finally:
        if conn: conn.close()

@app.route('/add_employee', methods=['POST'], endpoint='add_employee')
def add_employee():
    e_code = request.form.get('e_code')
    name = request.form.get('name')
    company = request.form.get('company')
    doj_str = request.form.get('doj')
    stage = request.form.get('stage')

    # Removed 'default_shift' from check
    if not all([e_code, name, company, doj_str, stage]):
        flash('Error: All fields are required.', 'error')
        return redirect(url_for('attendance'))

    doj_date = datetime.strptime(doj_str, '%Y-%m-%d')
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('attendance'))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT [E.code] FROM employees WHERE [E.code] = ?", e_code)
        if cursor.fetchone():
            flash(f'Error: Employee with E.code "{e_code}" already exists.', 'error')
            return redirect(url_for('attendance'))

        # Removed [Default Shift] from SQL
        sql_insert = "INSERT INTO employees ([E.code], [Name of the Employee], [Company / Contract], [Stage], [DOJ]) VALUES (?, ?, ?, ?, ?)"
        # Removed default_shift from params
        params = (e_code, name, company, stage, doj_date)
        cursor.execute(sql_insert, params)
        conn.commit()
        flash(f'Successfully added employee: {name}', 'success')
    except Exception as e:
        flash(f'An unexpected error occurred: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()
    # Redirect to the manage view by default
    return redirect(url_for('attendance', view='manage'))

@app.route('/edit_employee', methods=['POST'], endpoint='edit_employee')
def edit_employee():
    # This is the original E.code used to find the record
    original_e_code = request.form.get('original_e_code')

    # These are the new values from the form
    new_e_code = request.form.get('e_code')
    name = request.form.get('name')
    company = request.form.get('company')
    doj_str = request.form.get('doj')
    stage = request.form.get('stage')

    if not all([original_e_code, new_e_code, name, company, doj_str, stage]):
        flash('Error: All fields are required to edit.', 'error')
        return redirect(url_for('attendance', view='manage'))

    doj_date = datetime.strptime(doj_str, '%Y-%m-%d')
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('attendance', view='manage'))
    try:
        cursor = conn.cursor()

        # Check if the new E.code already exists (and isn't the original e.code)
        if original_e_code != new_e_code:
            cursor.execute("SELECT [E.code] FROM employees WHERE [E.code] = ?", new_e_code)
            if cursor.fetchone():
                flash(f'Error: New E.code "{new_e_code}" already exists for another employee.', 'error')
                return redirect(url_for('attendance', view='manage'))

        # Run the UPDATE query
        sql_update = """
            UPDATE employees 
            SET [E.code] = ?, [Name of the Employee] = ?, [Company / Contract] = ?, [Stage] = ?, [DOJ] = ?
            WHERE [E.code] = ?
        """
        params = (new_e_code, name, company, stage, doj_date, original_e_code)
        cursor.execute(sql_update, params)
        conn.commit()

        flash(f'Successfully updated employee: {name}', 'success')
    except Exception as e:
        # --- THIS IS THE CORRECTED LINE ---
        flash(f'An unexpected error occurred during update: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()

    return redirect(url_for('attendance', view='manage'))

@app.route('/attendance/holiday', methods=['POST'], endpoint='add_holiday')
def add_holiday():
    holiday_date_str = request.form.get('holiday_date')
    description = request.form.get('description', 'Holiday')
    start_date = request.form.get('start_date')
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('attendance', start_date=start_date))
    try:
        cursor = conn.cursor()
        sql = """
        MERGE holidays AS target
        USING (SELECT ? AS holiday_date, ? AS description, 'Holiday' AS day_type) AS source
        ON (target.holiday_date = source.holiday_date)
        WHEN MATCHED THEN
            UPDATE SET day_type = source.day_type, description = source.description
        WHEN NOT MATCHED THEN
            INSERT (holiday_date, description, day_type) VALUES (source.holiday_date, source.description, source.day_type);
        """
        cursor.execute(sql, holiday_date_str, description)
        conn.commit()
        flash(f'Holiday on {holiday_date_str} added successfully.', 'success')
    except Exception as e:
        flash(f'An error occurred: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()
    return redirect(url_for('attendance', start_date=start_date))

@app.route('/remove_employee', methods=['POST'], endpoint='remove_employee')
def remove_employee():
    e_code, start_date = request.form.get('e_code'), request.form.get('start_date')
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('attendance'))
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM employees WHERE [E.code] = ?", e_code)
        conn.commit()
        if cursor.rowcount > 0:
            flash(f'Employee with E.code "{e_code}" has been removed.', 'success')
        else:
            flash(f'Could not find employee with E.code "{e_code}".', 'error')
    except Exception as e:
        flash(f'An error occurred while removing the employee: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()
    return redirect(url_for('attendance', start_date=start_date))


def create_and_insert_employee(e_code, name, company, stage, doj_date, default_shift):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        create_table_sql = """
        CREATE TABLE employees (
            [E.code] NVARCHAR(50) PRIMARY KEY, [Name of the Employee] NVARCHAR(255),
            [Company / Contract] NVARCHAR(255), [DOJ] DATETIME, [Stage] NVARCHAR(255),
            [Default Shift] INT
        );
        """
        cursor.execute(create_table_sql)
        sql_insert = "INSERT INTO employees ([E.code], [Name of the Employee], [Company / Contract], [Stage], [DOJ], [Default Shift]) VALUES (?, ?, ?, ?, ?, ?)"
        params = (e_code, name, company, stage, doj_date, default_shift)
        cursor.execute(sql_insert, params)
        conn.commit()
        flash(f'Employees table created and first employee ({name}) added!', 'success')
    except Exception as e:
        flash(f'Failed to create table and add employee: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()


# In app/routes.py

@app.route('/upload_triggers', methods=['POST'], endpoint='upload_triggers')
def upload_triggers():
    if 'file' not in request.files or request.files['file'].filename == '':
        flash('No file selected. Please choose a CSV file.', 'error')
        return redirect(request.referrer or url_for('production_planning'))

    file = request.files['file']
    if not file.filename.endswith('.csv'):
        flash('Invalid file type. Please upload a .csv file.', 'error')
        return redirect(request.referrer or url_for('production_planning'))

    filepath, conn = None, None
    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        df = pd.read_csv(filepath, low_memory=False)

        # 1. Find and standardize column names
        item_code_col_actual = next((col for col in df.columns if str(col).strip().upper() == 'ITEM CODE'), None)
        qty_col_actual = next((col for col in df.columns if str(col).strip().upper() == 'QTY'), None)

        if not item_code_col_actual or not qty_col_actual:
            flash("Upload failed: Required columns 'ITEM CODE' or 'QTY' not found in the CSV file.", 'error')
            return redirect(request.referrer or url_for('production_planning'))

        df.rename(columns={
            item_code_col_actual: 'Item code',
            qty_col_actual: 'Qty'
        }, inplace=True)

        item_code_col = 'Item code'
        qty_col = 'Qty'

        # 2. Clean the data
        df[item_code_col] = df[item_code_col].astype(str).str.strip().str.lstrip("'")
        df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
        
        due_dt_col = None
        for col_name in ['TRIGGER DT', 'DUE DT']:
            actual_col = next((c for c in df.columns if str(c).strip().upper() == col_name), None)
            if actual_col:
                # 'dayfirst=True' ensures 05-01-2026 is read as Jan 5th, not May 1st
                df[actual_col] = pd.to_datetime(df[actual_col], dayfirst=True, errors='coerce')
                if str(actual_col).strip().upper() == 'DUE DT':
                    due_dt_col = actual_col

        # --- LOGIC: Define Time Windows ---
        # Using date.today() allows seamless year transition (e.g. Dec 23 -> Jan 23)
        today_date = date.today()
        
        # Calculate 31 days into the future
        cutoff_date = today_date + timedelta(days=31)
        
        # Initialize DataFrames
        df_today = df.copy() 
        df_future = pd.DataFrame(columns=df.columns)

        if due_dt_col:
            # 1. Pending: Due Date is TODAY or Earlier (Overdue)
            df_today = df[df[due_dt_col].dt.date <= today_date]
            
            # 2. Next Pending: Due Date is TOMORROW -> NEXT 31 DAYS
            # Logic: (Date > Today) AND (Date <= Today + 31 Days)
            # This logic works correctly across year boundaries (e.g. Dec 2025 to Jan 2026)
            df_future = df[
                (df[due_dt_col].dt.date > today_date) & 
                (df[due_dt_col].dt.date <= cutoff_date)
            ]

        # Aggregate Quantities per Item
        agg_today = df_today.groupby(item_code_col)[qty_col].sum().reset_index()
        agg_future = df_future.groupby(item_code_col)[qty_col].sum().reset_index()

        conn = get_db_connection()
        if not conn: raise ConnectionError("Database connection failed.")
        cursor = conn.cursor()

        # ---------------------------------------------------------
        # HISTORY LOGGING (Keeps record of "Pending" for today)
        # ---------------------------------------------------------
        
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='trigger_history' and xtype='U')
            CREATE TABLE trigger_history (
                history_id INT IDENTITY(1,1) PRIMARY KEY,
                upload_date DATE NOT NULL,
                item_code NVARCHAR(255) NOT NULL,
                pending_quantity INT NOT NULL,
                created_at DATETIME DEFAULT GETDATE()
            );
        """)
        
        # Clear previous history for TODAY only (allows re-upload fix)
        cursor.execute("DELETE FROM trigger_history WHERE upload_date = ?", today_date)

        # Insert New Snapshot
        history_params = [
            (today_date, str(row[item_code_col]), row[qty_col]) 
            for index, row in agg_today.iterrows() 
            if row[qty_col] > 0
        ]
        
        if history_params:
            cursor.executemany("""
                INSERT INTO trigger_history (upload_date, item_code, pending_quantity) 
                VALUES (?, ?, ?)
            """, history_params)

        # ---------------------------------------------------------
        # UPDATE MASTER TABLE
        # ---------------------------------------------------------

        # Ensure 'Next Pending' column exists
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'Next Pending' AND Object_ID = Object_ID(N'master'))
                ALTER TABLE master ADD [Next Pending] INT DEFAULT 0 WITH VALUES;
        """)
        conn.commit()

        # Save full raw details to 'pending_triggers' table
        replace_table_with_df(df, 'pending_triggers', cursor)

        # Reset Master Columns (Pending & Next Pending) to 0 before update
        cursor.execute("UPDATE master SET [Pending] = 0, [Next Pending] = 0")

        # Update [Pending] (Today/Overdue)
        update_count = 0
        for index, row in agg_today.iterrows():
            cursor.execute("UPDATE master SET [Pending] = ? WHERE [Item code] = ?", row[qty_col], str(row[item_code_col]))
            update_count += cursor.rowcount

        # Update [Next Pending] (Tomorrow to 31 Days)
        for index, row in agg_future.iterrows():
            cursor.execute("UPDATE master SET [Next Pending] = ? WHERE [Item code] = ?", row[qty_col], str(row[item_code_col]))

        # Record Upload Timestamp
        upsert_sql = """
            MERGE app_status AS target
            USING (SELECT 'last_triggers_upload_timestamp' AS src_key, ? AS src_val) AS src 
            ON (target.status_key = src.src_key)
            WHEN MATCHED THEN 
                UPDATE SET status_value = src.src_val
            WHEN NOT MATCHED THEN 
                INSERT (status_key, status_value) VALUES (src.src_key, src.src_val);
        """
        now_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(upsert_sql, now_timestamp)

        conn.commit()
        
        flash(f'Triggers uploaded. Updated {update_count} items in Pending. "Next Pending" updated for the next 31 days.', 'success')

    except Exception as e:
        if conn: conn.rollback()
        flash(f'An error occurred during trigger upload: {e}', 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()
        if filepath and os.path.exists(filepath): os.remove(filepath)

    return redirect(request.referrer or url_for('production_planning'))
@app.route('/api/stage_totals')
def api_stage_totals():
    """
    API endpoint to return the current WIP stage totals AND Monthly Data.
    """
    stage_totals = {}
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            
            # 1. Fetch WIP Stage Totals
            current_stages = get_dynamic_stages(cursor, dashboard_only=True)
            if current_stages:
                sum_clauses = [f"SUM(ISNULL([{stage}], 0)) as [{stage}]" for stage in current_stages]
                sql_wip = f"SELECT {', '.join(sum_clauses)} FROM master"
                cursor.execute(sql_wip)
                row = cursor.fetchone()
                if row:
                    stage_totals = row_to_dict(cursor, row)
            
            # 2. Fetch Monthly Dispatch
            sql_monthly = """
                SELECT SUM(quantity) FROM production_log 
                WHERE to_stage IN ('Dispatch', 'Delivered') 
                AND MONTH(moved_at) = MONTH(GETDATE()) 
                AND YEAR(moved_at) = YEAR(GETDATE())
            """
            cursor.execute(sql_monthly)
            monthly_val = cursor.fetchone()[0] or 0
            stage_totals['Dispatch'] = monthly_val

            # 3. Calculate Monthly Trigger Adherence (NEW FORMULA)
            monthly_adherence_val = calculate_monthly_trigger_adherence(conn)
            stage_totals['MonthlyTriggerAdherence'] = monthly_adherence_val

        except Exception as e:
            print(f"API Error fetching stage totals: {e}")
            traceback.print_exc()
        finally:
            conn.close()
    return jsonify(stage_totals)


@app.route('/api/inventory/<item_code>')
def get_inventory_for_item(item_code):
    """
    API endpoint to return the inventory counts for a single item code.
    """
    inventory = {}
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            stage_columns = ', '.join([f"[{stage}]" for stage in STAGES]) + ', [Live Dispatch] as [Dispatch]'
            sql_query = f"SELECT {stage_columns} FROM master WHERE [Item code] = ?"
            cursor.execute(sql_query, item_code)
            row = cursor.fetchone()
            if row:
                inventory = row_to_dict(cursor, row)
        except Exception as e:
            print(f"API Error fetching single item inventory: {e}")
        finally:
            if conn: conn.close()
    return jsonify(inventory)

@app.route('/api/inventory/vertical/<vertical_name>')
def get_inventory_for_vertical(vertical_name):
    """
    API endpoint to return the SUM of inventory for a given Vertical.
    """
    inventory = {}
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            stage_columns_sum = ', '.join([f"SUM(ISNULL([{stage}], 0)) as [{stage}]" for stage in STAGES]) + ', SUM(ISNULL([Live Dispatch], 0)) as [Dispatch]'
            sql_query = f"SELECT {stage_columns_sum} FROM master WHERE [Vertical] = ?"
            cursor.execute(sql_query, vertical_name)
            row = cursor.fetchone()
            if row:
                inventory = row_to_dict(cursor, row)
        except Exception as e:
            print(f"API Error fetching vertical inventory: {e}")
        finally:
            if conn: conn.close()
    return jsonify(inventory)

@app.route('/api/inventory/category/<category_name>')
def get_inventory_for_category(category_name):
    """
    API endpoint to return the SUM of inventory for a given Category.
    """
    inventory = {}
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            stage_columns_sum = ', '.join([f"SUM(ISNULL([{stage}], 0)) as [{stage}]" for stage in STAGES]) + ', SUM(ISNULL([Live Dispatch], 0)) as [Dispatch]'
            sql_query = f"SELECT {stage_columns_sum} FROM master WHERE [Category] = ?"
            cursor.execute(sql_query, category_name)
            row = cursor.fetchone()
            if row:
                inventory = row_to_dict(cursor, row)
        except Exception as e:
            print(f"API Error fetching category inventory: {e}")
        finally:
            if conn: conn.close()
    return jsonify(inventory)

def calculate_adherence(conn, item_codes=None, verticals=None, categories=None, types=None):
    """
    Helper function: Calculates ONLY Daily Adherence.
    Monthly Adherence references removed.
    """
    daily_adherence = 0

    # 1. Build Filter Logic
    filter_clause_master = ""  
    params = []

    if item_codes:
        seq = ','.join(['?'] * len(item_codes))
        filter_clause_master += f" AND m.[Item code] IN ({seq})"
        params.extend(item_codes)
    if verticals:
        seq = ','.join(['?'] * len(verticals))
        filter_clause_master += f" AND m.[Vertical] IN ({seq})"
        params.extend(verticals)
    if categories:
        seq = ','.join(['?'] * len(categories))
        filter_clause_master += f" AND m.[Category] IN ({seq})"
        params.extend(categories)
    if types:
        seq = ','.join(['?'] * len(types))
        filter_clause_master += f" AND m.[Type] IN ({seq})"
        params.extend(types)

    try:
        cursor = conn.cursor()

        # --- DAILY ADHERENCE ---
        sql_daily_target = f"SELECT SUM(ISNULL(m.[Pending], 0)) FROM master m WHERE 1=1 {filter_clause_master}"
        cursor.execute(sql_daily_target, params)
        total_daily_target = cursor.fetchone()[0] or 0

        sql_daily_actual = f"""
            DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
            DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
            DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

            SELECT SUM(pl.quantity) FROM production_log pl
            INNER JOIN master m ON pl.item_code = m.[Item code]
            WHERE pl.to_stage IN ('Dispatch', 'Delivered') 
            AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
            {filter_clause_master}
        """
        cursor.execute(sql_daily_actual, params)
        total_daily_dispatched = cursor.fetchone()[0] or 0

        if total_daily_target > 0:
            daily_adherence = (total_daily_dispatched / total_daily_target) * 100

    except Exception:
        traceback.print_exc()

    # RETURN ONLY DAILY ADHERENCE
    return {'daily_adherence': daily_adherence}

@app.route('/api/adherence/totals')
def api_adherence_totals():
    conn = get_db_connection()
    data = {}
    if conn:
        try:
            data = calculate_adherence(conn)
        finally:
            conn.close()
    return jsonify(data)

@app.route('/api/adherence/item_code/<item_code>')
def api_adherence_item_code(item_code):
    conn = get_db_connection()
    data = {}
    if conn:
        try:
            # MODIFIED: Pass as a list
            data = calculate_adherence(conn, item_codes=[item_code])
        finally:
            conn.close()
    return jsonify(data)

@app.route('/api/adherence/vertical/<vertical_name>')
def api_adherence_vertical(vertical_name):
    conn = get_db_connection()
    data = {}
    if conn:
        try:
            # MODIFIED: Pass as a list
            data = calculate_adherence(conn, verticals=[vertical_name])
        finally:
            conn.close()
    return jsonify(data)

@app.route('/api/adherence/category/<category_name>')
def api_adherence_category(category_name):
    conn = get_db_connection()
    data = {}
    if conn:
        try:
            # MODIFIED: Pass as a list
            data = calculate_adherence(conn, categories=[category_name])
        finally:
            conn.close()
    return jsonify(data)


@app.route('/api/inventory/filtered', methods=['POST'])
def api_inventory_filtered():
    data = request.get_json()
    item_codes = data.get('item_code', [])
    verticals = data.get('vertical', [])
    categories = data.get('category', [])
    # --- NEW: Get Type ---
    types = data.get('type', [])

    inventory = {}
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            
            # --- 1. Build Filter Logic ---
            base_filter = ""
            params = []

            if item_codes:
                seq = ','.join(['?'] * len(item_codes))
                base_filter += f" AND [Item code] IN ({seq})"
                params.extend(item_codes)
            if verticals:
                seq = ','.join(['?'] * len(verticals))
                base_filter += f" AND [Vertical] IN ({seq})"
                params.extend(verticals)
            if categories:
                seq = ','.join(['?'] * len(categories))
                base_filter += f" AND [Category] IN ({seq})"
                params.extend(categories)
            # --- NEW: SQL for Type ---
            if types:
                seq = ','.join(['?'] * len(types))
                base_filter += f" AND [Type] IN ({seq})"
                params.extend(types)

            # --- 2. Get WIP Totals ---
            current_stages = get_dynamic_stages(cursor, dashboard_only=True)
            if not current_stages: current_stages = STAGES 

            stage_columns_sum = ', '.join([f"SUM(ISNULL([{stage}], 0)) as [{stage}]" for stage in current_stages])
            
            sql_wip = f"SELECT {stage_columns_sum} FROM master WHERE 1=1 {base_filter}"
            cursor.execute(sql_wip, params)
            row = cursor.fetchone()
            if row:
                inventory = row_to_dict(cursor, row)

            # --- 3. Get Monthly Dispatch ---
            sql_monthly = f"""
                SELECT SUM(pl.quantity) 
                FROM production_log pl
                JOIN master m ON pl.item_code = m.[Item code]
                WHERE pl.to_stage IN ('Dispatch', 'Delivered') 
                AND MONTH(pl.moved_at) = MONTH(GETDATE()) 
                AND YEAR(pl.moved_at) = YEAR(GETDATE())
                {base_filter.replace('[Item code]', 'm.[Item code]')
                            .replace('[Vertical]', 'm.[Vertical]')
                            .replace('[Category]', 'm.[Category]')
                            .replace('[Type]', 'm.[Type]')} 
            """
            cursor.execute(sql_monthly, params)
            monthly_val = cursor.fetchone()[0] or 0
            
            inventory['Dispatch'] = monthly_val

        except Exception as e:
            print(f"API Error fetching filtered inventory: {e}")
            traceback.print_exc()
        finally:
            if conn: conn.close()
    return jsonify(inventory)




@app.route('/items', endpoint='manage_items')
@login_required
def manage_items():
    page = request.args.get('page', 1, type=int)
    per_page = 20
    offset = (page - 1) * per_page
    search_query = request.args.get('search', '')

    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('dashboard'))

    items = []
    stages = []
    pagination = None

    try:
        cursor = conn.cursor()

        # 1. Fetch Dynamic Stages (to build table headers)
        cursor.execute("SELECT stage_name FROM manufacturing_stages ORDER BY display_order ASC")
        stages = [row[0] for row in cursor.fetchall()]

        # 2. Build Query
        # We use SELECT * to get all columns including dynamic stages
        base_query = "SELECT * FROM master WHERE 1=1"
        count_query = "SELECT COUNT(*) FROM master WHERE 1=1"
        params = []

        if search_query:
            filter_clause = " AND ([Item code] LIKE ? OR [Description] LIKE ?)"
            base_query += filter_clause
            count_query += filter_clause
            params.extend([f'%{search_query}%', f'%{search_query}%'])

        # 3. Pagination
        cursor.execute(count_query, params)
        total_items = cursor.fetchone()[0]
        total_pages = ceil(total_items / per_page)

        # 4. Fetch Data
        final_query = base_query + " ORDER BY [Item code] OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([offset, per_page])

        cursor.execute(final_query, params)
        items = [row_to_dict(cursor, row) for row in cursor.fetchall()]

        pagination = {
            'page': page,
            'per_page': per_page,
            'total': total_items,
            'total_pages': total_pages,
            'has_prev': page > 1,
            'has_next': page < total_pages,
            'prev_num': page - 1,
            'next_num': page + 1
        }

    except Exception as e:
        flash(f"Error fetching items: {e}", 'error')
        traceback.print_exc()
    finally:
        conn.close()

    return render_template('items.html', items=items, stages=stages, pagination=pagination, search_query=search_query)


@app.route('/add_stage', methods=['POST'], endpoint='add_stage')
def add_stage():
    stage_name = request.form.get('stage_name').strip()
    # Checkbox returns 'on' if checked, None if unchecked
    include_dashboard = 1 if request.form.get('include_dashboard') else 0

    conn = get_db_connection()
    if not conn: return redirect(url_for('manage_stages'))

    try:
        cursor = conn.cursor()

        # 1. Validation: Check duplicate name
        cursor.execute("SELECT 1 FROM manufacturing_stages WHERE stage_name = ?", stage_name)
        if cursor.fetchone():
            flash(f"Stage '{stage_name}' already exists.", 'error')
            return redirect(url_for('manage_stages'))

        # --- LOGIC FIX: Insert BEFORE 'FG' ---

        # A. Find the current order of 'FG'
        cursor.execute("SELECT display_order FROM manufacturing_stages WHERE stage_name = 'FG'")
        fg_row = cursor.fetchone()

        if fg_row:
            target_order = fg_row[0]
            # B. Shift 'FG' (and anything that might be after it) down by 1 to make space
            cursor.execute("UPDATE manufacturing_stages SET display_order = display_order + 1 WHERE display_order >= ?",
                           target_order)
        else:
            # Fallback: If FG is missing for some reason, append to end
            cursor.execute("SELECT ISNULL(MAX(display_order), 0) + 1 FROM manufacturing_stages")
            target_order = cursor.fetchone()[0]

        # 2. Add to List Table at the specific target_order
        cursor.execute(
            "INSERT INTO manufacturing_stages (stage_name, display_order, is_dashboard_stage) VALUES (?, ?, ?)",
            (stage_name, target_order, include_dashboard))

        # 3. SYNC: Add Column to 'master' table
        # We check if column exists first to avoid crashes
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'{stage_name}' AND Object_ID = Object_ID(N'master'))
            BEGIN
                ALTER TABLE master ADD [{stage_name}] INT NOT NULL DEFAULT 0 WITH VALUES;
            END
        """)

        conn.commit()
        flash(f"Stage '{stage_name}' added successfully.", 'success')

    except Exception as e:
        if conn: conn.rollback()
        flash(f"Error adding stage: {e}", 'error')
        print(f"Error in add_stage: {e}")  # Print to terminal for debugging
    finally:
        if conn: conn.close()

    return redirect(url_for('manage_stages'))


# In app/routes.py

@app.route('/edit_stage', methods=['POST'], endpoint='edit_stage')
def edit_stage():
    stage_id = request.form.get('stage_id')
    new_name = request.form.get('stage_name').strip()
    original_name = request.form.get('original_name').strip()
    # Checkbox logic: 1 if checked, 0 if not
    include_dashboard = 1 if request.form.get('include_dashboard') else 0

    conn = get_db_connection()
    if not conn: return redirect(url_for('manage_stages'))

    try:
        cursor = conn.cursor()

        # --- 1. PROTECTION CHECK ---
        # Prevent users from renaming system-critical stages
        if original_name in ['Not Started', 'FG']:
            flash(f"System stage '{original_name}' cannot be edited.", 'error')
            return redirect(url_for('manage_stages'))

        # --- 2. UPDATE LIST TABLE ---
        # Update the name and the dashboard preference flag
        cursor.execute("""
            UPDATE manufacturing_stages 
            SET stage_name = ?, is_dashboard_stage = ? 
            WHERE id = ?
        """, (new_name, include_dashboard, stage_id))

        # --- 3. SYNC COLUMN NAMES ---
        # Only perform expensive DB operations if the name actually changed
        if new_name != original_name:
            # We check 'master' and 'Production_pl' to keep them in sync
            for table in ['master', 'Production_pl']:
                # Check if the old column exists before trying to rename
                cursor.execute(
                    f"SELECT 1 FROM sys.columns WHERE Name = N'{original_name}' AND Object_ID = Object_ID(N'{table}')")
                if cursor.fetchone():
                    # Check if the NEW name already exists (to avoid collision errors)
                    cursor.execute(
                        f"SELECT 1 FROM sys.columns WHERE Name = N'{new_name}' AND Object_ID = Object_ID(N'{table}')")
                    if cursor.fetchone():
                        # If new name exists, we can't rename. Rollback logic or skip.
                        # For simplicity, we skip renaming in this edge case to prevent crash.
                        pass
                    else:
                        # Use sp_rename to rename the column safely
                        query = f"EXEC sp_rename '{table}.[{original_name}]', '{new_name}', 'COLUMN';"
                        cursor.execute(query)

        conn.commit()
        flash(f"Stage '{original_name}' updated successfully.", 'success')
    except Exception as e:
        flash(f"Error updating stage: {e}", 'error')
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

    return redirect(url_for('manage_stages'))


@app.route('/delete_stage', methods=['POST'], endpoint='delete_stage')
def delete_stage():
    stage_id = request.form.get('stage_id')
    stage_name = request.form.get('stage_name')

    conn = get_db_connection()
    if not conn: return redirect(url_for('manage_stages'))

    try:
        cursor = conn.cursor()

        # --- 1. PROTECTION CHECK ---
        if stage_name in ['Not Started', 'FG']:
            flash(f"System stage '{stage_name}' cannot be deleted.", 'error')
            return redirect(url_for('manage_stages'))

        # --- 2. REMOVE FROM LIST TABLE ---
        cursor.execute("DELETE FROM manufacturing_stages WHERE id = ?", stage_id)

        # --- 3. DROP COLUMN FROM MASTER ---
        # We must use dynamic SQL to find and drop the Default Constraint first,
        # otherwise the DROP COLUMN command will fail in SQL Server.
        drop_logic = f"""
            DECLARE @ConstraintName nvarchar(200);

            -- A. Find the constraint name for this specific column in 'master'
            SELECT @ConstraintName = Name 
            FROM sys.default_constraints 
            WHERE parent_object_id = OBJECT_ID('master') 
            AND parent_column_id = (SELECT column_id FROM sys.columns WHERE Name = N'{stage_name}' AND object_id = OBJECT_ID('master'));

            -- B. If a constraint exists, delete it
            IF @ConstraintName IS NOT NULL
            BEGIN
                EXEC('ALTER TABLE master DROP CONSTRAINT ' + @ConstraintName);
            END

            -- C. Now safely drop the column
            IF EXISTS (SELECT * FROM sys.columns WHERE Name = N'{stage_name}' AND Object_ID = Object_ID(N'master'))
            BEGIN
                ALTER TABLE master DROP COLUMN [{stage_name}];
            END
        """
        cursor.execute(drop_logic)

        conn.commit()
        flash(f"Stage '{stage_name}' deleted.", 'success')

    except Exception as e:
        flash(f"Error deleting stage: {e}", 'error')
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

    return redirect(url_for('manage_stages'))

# --- STAGE MANAGEMENT ROUTES ---

# --- ADD THESE MISSING FUNCTIONS ---

@app.route('/edit_item', methods=['POST'], endpoint='edit_item')
def edit_item():
    original_item_code = request.form.get('original_item_code')
    if not original_item_code:
        flash("Original Item Code missing.", "error")
        return redirect(url_for('manage_items'))

    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('manage_items'))

    try:
        cursor = conn.cursor()

        # Define mapping for standard fields (Form Name -> DB Column Name)
        field_map = {
            'item_code': 'Item code',
            'description': 'Description',
            'vertical': 'Vertical',
            'category': 'Category',
            'type': 'Type',
            'model': 'Model',
            'monthly_avg': 'MonthlyAvg',
            'daily_max': 'Daily Max Plan',
            'max_inv': 'Max Inv',
            'rpl_days': 'RPL days to Delivery'
        }

        update_clauses = []
        params = []

        # Iterate over all form data submitted
        for key, value in request.form.items():
            if key == 'original_item_code':
                continue

            db_column = None

            # Check if it is a standard field
            if key in field_map:
                db_column = field_map[key]
            else:
                # If not a standard field, assume it is a dynamic Stage Name (e.g., "Long seam")
                # We trust the form data here because the input names are generated from the DB stages
                db_column = key

            # Build the update clause
            if db_column:
                update_clauses.append(f"[{db_column}] = ?")

                # Handle empty strings for numeric fields (default to 0)
                # If it's a text field, keep it as empty string
                text_fields = ['item_code', 'description', 'vertical', 'category', 'type', 'model']

                if value == '' and key not in text_fields:
                    params.append(0)
                else:
                    params.append(value)

        # Add the WHERE clause parameter
        params.append(original_item_code)

        if update_clauses:
            sql = f"UPDATE master SET {', '.join(update_clauses)} WHERE [Item code] = ?"
            cursor.execute(sql, params)
            conn.commit()
            flash(f"Item updated successfully.", 'success')
        else:
            flash("No changes detected.", "info")

    except Exception as e:
        flash(f"Error updating item: {e}", 'error')
        traceback.print_exc()
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

    return redirect(url_for('manage_items'))


@app.route('/delete_item', methods=['POST'], endpoint='delete_item')
def delete_item():
    item_code = request.form.get('item_code')
    conn = get_db_connection()
    if not conn: return redirect(url_for('manage_items'))

    try:
        cursor = conn.cursor()

        # Optional: Delete from child tables first if you want to enforce clean up
        # cursor.execute("DELETE FROM Production_pl WHERE [Item code ] = ?", item_code)

        # Delete from master
        cursor.execute("DELETE FROM master WHERE [Item code] = ?", item_code)
        conn.commit()
        flash(f"Item '{item_code}' deleted.", 'success')
    except Exception as e:
        flash(f"Error deleting item: {e}", 'error')
    finally:
        conn.close()

    return redirect(url_for('manage_items'))

def get_ordered_stages(cursor):
    """Helper to fetch stages in defined order"""
    cursor.execute("SELECT stage_name FROM manufacturing_stages ORDER BY display_order ASC")
    return [row[0] for row in cursor.fetchall()]


@app.route('/add_item', methods=['POST'], endpoint='add_item')
def add_item():
    # Collect basic form data
    item_code = request.form.get('item_code')
    description = request.form.get('description')
    vertical = request.form.get('vertical')
    category = request.form.get('category')
    item_type = request.form.get('type')
    model = request.form.get('model')
    # Default numeric fields to 0 if empty
    monthly_avg = request.form.get('monthly_avg') or 0
    daily_max = request.form.get('daily_max') or 0
    max_inv = request.form.get('max_inv') or 0
    rpl_days = request.form.get('rpl_days') or 0

    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('manage_items'))

    try:
        cursor = conn.cursor()

        # 1. Validation: Check if Item Code already exists
        cursor.execute("SELECT 1 FROM master WHERE [Item code] = ?", item_code)
        if cursor.fetchone():
            flash(f"Error: Item code '{item_code}' already exists.", 'error')
            return redirect(url_for('manage_items'))

        # 2. Insert new item
        # Note: We initialize [TriggerAccumulator] to 0
        sql = """
            INSERT INTO master (
                [Item code], [Description], [Vertical], [Category], [Type], [Model], 
                [MonthlyAvg], [Daily Max Plan], [Max Inv], [RPL days to Delivery],
                [TriggerAccumulator]
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        """
        params = (
        item_code, description, vertical, category, item_type, model, monthly_avg, daily_max, max_inv, rpl_days)

        cursor.execute(sql, params)
        conn.commit()
        flash(f"Item '{item_code}' added successfully.", 'success')

    except Exception as e:
        if conn: conn.rollback()
        flash(f"Error adding item: {e}", 'error')
        # traceback.print_exc() # Uncomment for debugging
    finally:
        if conn: conn.close()

    return redirect(url_for('manage_items'))

@app.route('/stages', endpoint='manage_stages')
@login_required
def manage_stages():
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('dashboard'))

    stages = []
    try:
        cursor = conn.cursor()
        # FETCH extra column: is_dashboard_stage
        cursor.execute(
            "SELECT id, stage_name, display_order, is_dashboard_stage FROM manufacturing_stages ORDER BY display_order ASC")
        stages = [row_to_dict(cursor, row) for row in cursor.fetchall()]
    except Exception as e:
        flash(f"Error fetching stages: {e}", 'error')
    finally:
        conn.close()

    return render_template('stages.html', stages=stages)




@app.route('/reorder_stages', methods=['POST'], endpoint='reorder_stages')
def reorder_stages():
    # Expects JSON list of IDs in new order: [5, 2, 1, 3...]
    data = request.get_json()
    new_order_ids = data.get('order', [])

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        for index, stage_id in enumerate(new_order_ids):
            # Update display_order based on the index in the received list (1-based)
            cursor.execute("UPDATE manufacturing_stages SET display_order = ? WHERE id = ?", (index + 1, stage_id))
        conn.commit()
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        conn.close()


# In app/routes.py

def get_dynamic_stages(cursor, dashboard_only=False):
    """
    Fetches stages.
    If dashboard_only=True, returns only stages marked for dashboard.
    Otherwise, returns all stages.
    """
    try:
        if dashboard_only:
            cursor.execute(
                "SELECT stage_name FROM manufacturing_stages WHERE is_dashboard_stage = 1 ORDER BY display_order ASC")
        else:
            cursor.execute("SELECT stage_name FROM manufacturing_stages ORDER BY display_order ASC")

        return [row[0] for row in cursor.fetchall()]
    except Exception:
        return []


@app.route('/api/vertical_overview', endpoint='api_vertical_overview')
def api_vertical_overview():
    conn = get_db_connection()
    if not conn: return jsonify([])
    
    # --- NEW: Get Filter ---
    filter_type = request.args.get('type')

    try:
        cursor = conn.cursor()
        
        # Build Clause
        filter_clause = " AND p.[Vertical] IS NOT NULL"
        params = []
        if filter_type:
            filter_clause += " AND m.[Type] = ?"
            params.append(filter_type)

        sql_vertical = f"""
            DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
            DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
            DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

            WITH VerticalFG AS (
                SELECT m.[Vertical], SUM(pl.quantity) as QtyReachedFG
                FROM production_log pl JOIN master m ON pl.item_code = m.[Item code]
                WHERE pl.to_stage = 'FG' AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
                GROUP BY m.[Vertical]
            ),
            VerticalDispatch AS (
                SELECT m.[Vertical], SUM(pl.quantity) as QtyDispatched
                FROM production_log pl JOIN master m ON pl.item_code = m.[Item code]
                WHERE pl.from_stage = 'FG' AND pl.to_stage = 'Dispatch'
                AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
                GROUP BY m.[Vertical]
            )
            SELECT 
                p.[Vertical], 
                SUM(ISNULL(m.[Pending], 0)) as [Opening Trigger],
                CASE 
                    WHEN (SUM(ISNULL(m.[Pending], 0)) - ISNULL(vd.QtyDispatched, 0)) < 0 THEN 0
                    ELSE (SUM(ISNULL(m.[Pending], 0)) - ISNULL(vd.QtyDispatched, 0))
                END as [Pending],
                ISNULL(vd.QtyDispatched, 0) as [Dispatch],
                ISNULL(vfg.QtyReachedFG, 0) as [Achieved]
            FROM Production_pl p 
            LEFT JOIN master m ON p.[Item code ] = m.[Item code]
            LEFT JOIN VerticalFG vfg ON p.[Vertical] = vfg.[Vertical]
            LEFT JOIN VerticalDispatch vd ON p.[Vertical] = vd.[Vertical]
            WHERE 1=1 {filter_clause}
            GROUP BY p.[Vertical], vfg.QtyReachedFG, vd.QtyDispatched 
            ORDER BY p.[Vertical]
        """
        cursor.execute(sql_vertical, params)
        data = [row_to_dict(cursor, row) for row in cursor.fetchall()]

        for item in data:
            trig = item.get('Opening Trigger', 0)
            ach = item.get('Achieved', 0)
            item['AdherencePercent'] = (ach * 100.0 / trig) if trig > 0 else 100.0

        return jsonify(data)
    finally:
        conn.close()


@app.route('/api/category_overview', endpoint='api_category_overview')
def api_category_overview():
    conn = get_db_connection()
    if not conn: return jsonify([])

    # --- NEW: Get Filter ---
    filter_type = request.args.get('type')

    try:
        cursor = conn.cursor()
        
        # Build Clause
        filter_clause = " AND p.[Category] IS NOT NULL"
        params = []
        if filter_type:
            filter_clause += " AND m.[Type] = ?"
            params.append(filter_type)

        sql_category = f"""
            DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
            DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
            DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

            WITH CategoryFG AS (
                SELECT m.[Category], SUM(pl.quantity) as QtyReachedFG
                FROM production_log pl JOIN master m ON pl.item_code = m.[Item code]
                WHERE pl.to_stage = 'FG' AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
                GROUP BY m.[Category]
            ),
            CategoryDispatch AS (
                SELECT m.[Category], SUM(pl.quantity) as QtyDispatched
                FROM production_log pl JOIN master m ON pl.item_code = m.[Item code]
                WHERE pl.from_stage = 'FG' AND pl.to_stage = 'Dispatch'
                AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
                GROUP BY m.[Category]
            )
            SELECT 
                p.[Category], 
                SUM(ISNULL(m.[Pending], 0)) as [Opening Trigger],
                CASE 
                    WHEN (SUM(ISNULL(m.[Pending], 0)) - ISNULL(cd.QtyDispatched, 0)) < 0 THEN 0
                    ELSE (SUM(ISNULL(m.[Pending], 0)) - ISNULL(cd.QtyDispatched, 0))
                END as [Pending],
                ISNULL(cd.QtyDispatched, 0) as [Dispatch],
                ISNULL(cfg.QtyReachedFG, 0) as [Achieved]
            FROM Production_pl p 
            LEFT JOIN master m ON p.[Item code ] = m.[Item code]
            LEFT JOIN CategoryFG cfg ON p.[Category] = cfg.[Category]
            LEFT JOIN CategoryDispatch cd ON p.[Category] = cd.[Category]
            WHERE 1=1 {filter_clause}
            GROUP BY p.[Category], cfg.QtyReachedFG, cd.QtyDispatched
            ORDER BY p.[Category]
        """
        cursor.execute(sql_category, params)
        data = [row_to_dict(cursor, row) for row in cursor.fetchall()]

        for item in data:
            trig = item.get('Opening Trigger', 0)
            ach = item.get('Achieved', 0)
            item['AdherencePercent'] = (ach * 100.0 / trig) if trig > 0 else 100.0

        return jsonify(data)
    finally:
        conn.close()      
       
        
    
@app.before_request
def make_session_permanent():
    session.permanent = True # Enforce the 30-minute timeout defined in Config
    session.modified = True  # Reset the timer on every request
    
# In app/routes.py

import calendar

@app.route('/upload_dispatch', methods=['POST'], endpoint='upload_dispatch')
@login_required
def upload_dispatch():
    if 'file' not in request.files or request.files['file'].filename == '':
        flash('No file selected.', 'error')
        return redirect(request.referrer)

    file = request.files['file']
    if not file.filename.endswith(('.xlsx', '.xls')):
        flash('Invalid file type. Please upload an Excel file.', 'error')
        return redirect(request.referrer)

    filepath = None
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(request.referrer)

    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        # 1. Read Excel (First Sheet) using 'with' to ensure file is CLOSED afterwards
        target_month = None
        target_year = None
        df = None
        sheet_name = ""

        with pd.ExcelFile(filepath) as xls:
            sheet_name = xls.sheet_names[0] # Use first sheet automatically
            df = pd.read_excel(xls, sheet_name=sheet_name)

        # 2. Parse Month/Year from Sheet Name (Expected format: "Dec- 25", "Jan 24", etc.)
        import re
        date_match = re.search(r"([A-Za-z]{3})[\s-]*(\d{2})", sheet_name)
        
        if date_match:
            try:
                month_str = date_match.group(1)
                year_str = "20" + date_match.group(2) # Assume 20xx
                target_date_obj = datetime.strptime(f"{month_str}-{year_str}", "%b-%Y")
                target_month = target_date_obj.month
                target_year = target_date_obj.year
            except ValueError:
                pass
        
        # Fallback: If sheet name parsing fails, try to guess from the first valid date column header
        if not target_month:
            for col in df.columns:
                if isinstance(col, datetime):
                    target_month = col.month
                    target_year = col.year
                    break
        
        if not target_month or not target_year:
            flash(f"Could not detect Month/Year from sheet name '{sheet_name}'. Please rename sheet to format 'MMM- YY' (e.g., 'Dec- 25').", 'error')
            return redirect(request.referrer)

        print(f"DEBUG: Processing Dispatch for {target_month}/{target_year}")

        # 3. Clean & Process Data
        # Standardize columns
        df.columns = [str(c).strip() if isinstance(c, str) else c for c in df.columns]
        
        # Deduplicate columns
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

        # --- MODIFIED: Identify Item Code Column (Check for 'item code' OR 'spl') ---
        possible_headers = ['item code', 'spl']
        item_code_col = next((c for c in df.columns if str(c).lower().strip() in possible_headers), None)
        
        if not item_code_col:
            flash("Column 'Item code' or 'SPL' not found in Excel.", 'error')
            return redirect(request.referrer)

        # Identify Date Columns (Dynamic based on found item code col)
        info_cols = [str(item_code_col).lower(), 'description', 'vertical', 'model', 'category', 'type', 'total', 's.no']
        date_columns = [c for c in df.columns if str(c).lower() not in info_cols and not str(c).startswith('Unnamed')]

        log_records = []
        for index, row in df.iterrows():
            raw_code = row[item_code_col]
            if pd.isna(raw_code): continue
            item_code = str(raw_code).strip()

            for col in date_columns:
                raw_val = row[col]
                # Validate Quantity
                try:
                    qty = int(raw_val)
                    if qty <= 0: continue
                except (ValueError, TypeError):
                    continue

                # Validate Date
                moved_at = None
                if isinstance(col, (datetime, pd.Timestamp)):
                    moved_at = col
                elif isinstance(col, str):
                    try:
                        clean_col = col.split('.')[0].strip() # Handle duplicates
                        # Use the parsed year from sheet name
                        date_str = f"{clean_col}-{target_year}" 
                        moved_at = datetime.strptime(date_str, "%d-%b-%Y")
                    except ValueError:
                        continue
                
                if moved_at:
                    # Filter: Only keep records belonging to the Target Month/Year found in sheet name
                    if moved_at.month == target_month and moved_at.year == target_year:
                        log_records.append((item_code, qty, 'FG', 'Dispatch', moved_at))

        if not log_records:
            flash("No valid dispatch records found for the detected month.", 'warning')
            return redirect(request.referrer)

        cursor = conn.cursor()

        # 4. REPLACE DATA (Delete existing for this month, then Insert)
        
        delete_sql = """
            DELETE FROM production_log 
            WHERE to_stage = 'Dispatch' 
            AND MONTH(moved_at) = ? 
            AND YEAR(moved_at) = ?
        """
        cursor.execute(delete_sql, target_month, target_year)
        deleted_count = cursor.rowcount

        insert_sql = """
            INSERT INTO production_log (item_code, quantity, from_stage, to_stage, moved_at) 
            VALUES (?, ?, ?, ?, ?)
        """
        cursor.executemany(insert_sql, log_records)
        
        conn.commit()
        flash(f"Successfully uploaded {len(log_records)} records for {calendar.month_name[target_month]} {target_year}. (Replaced {deleted_count} existing records)", 'success')

    except Exception as e:
        if conn: conn.rollback()
        flash(f"Error processing file: {str(e)}", 'error')
        traceback.print_exc()
    finally:
        if conn: conn.close()
        # Ensure file is removed
        if filepath and os.path.exists(filepath): 
            try:
                os.remove(filepath)
            except PermissionError:
                print(f"Warning: Could not remove {filepath} - File still locked.")

    return redirect(request.referrer)