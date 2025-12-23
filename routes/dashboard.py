from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.database import get_db_connection, row_to_dict
import traceback
from math import ceil
from datetime import datetime, timedelta

# --- Initialize Blueprint ---
dashboard_bp = Blueprint('dashboard', __name__)

# --- PASTE YOUR FUNCTIONS BELOW THIS LINE ---
# Remember to change @app.route to @dashboard_bp.route

STAGES = [
    "Long seam", "Dish fit up", "Cirseam welding", "Part assembly",
    "Full welding", "Hydro Testing", "Powder coating", "PDI", "FG" # REVERTED to actual DB column name
]

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
     

def calculate_adherence(conn, item_codes=None, verticals=None, categories=None):
    """
    Helper function to calculate adherence based on DISPATCH vs TARGET.
    Daily Adherence = (Daily Dispatch / Opening Trigger) * 100
    Monthly Adherence = (Monthly Dispatch / Monthly Avg) * 100
    """
    daily_adherence = 0
    monthly_adherence = 0

    base_query_master = "FROM master m WHERE 1=1"
    filter_clause = ""
    params = []

    if item_codes:
        seq = ','.join(['?'] * len(item_codes))
        filter_clause = f" AND m.[Item code] IN ({seq})"
        params.extend(item_codes)
    elif verticals:
        seq = ','.join(['?'] * len(verticals))
        filter_clause = f" AND m.[Vertical] IN ({seq})"
        params.extend(verticals)
    elif categories:
        seq = ','.join(['?'] * len(categories))
        filter_clause = f" AND m.[Category] IN ({seq})"
        params.extend(categories)

    try:
        cursor = conn.cursor()

        # =================================================
        # 1. DAILY ADHERENCE (Dispatch / Opening Trigger)
        # =================================================

        # A. Get Target (Sum of Pending / Opening Trigger)
        # Note: In your logic, 'Pending' in master is the Opening Trigger for the day
        sql_daily_target = f"SELECT SUM(ISNULL(m.[Pending], 0)) {base_query_master} {filter_clause}"
        cursor.execute(sql_daily_target, params)
        total_daily_target = cursor.fetchone()[0] or 0

        # B. Get Actual (Sum of items moved to 'Dispatch' today)
        # Using 2 AM cutoff
        sql_daily_actual = f"""
            DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
            DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
            DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

            SELECT SUM(pl.quantity) 
            FROM production_log pl
            JOIN master m ON pl.item_code = m.[Item code]
            WHERE pl.to_stage = 'Dispatch' 
            AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
            {filter_clause}
        """
        cursor.execute(sql_daily_actual, params)
        total_daily_dispatched = cursor.fetchone()[0] or 0

        if total_daily_target > 0:
            daily_adherence = (total_daily_dispatched / total_daily_target) * 100
        else:
            # If target is 0, but we dispatched something, it's 100% (or bonus)
            daily_adherence = 100.0 if total_daily_dispatched > 0 else 0.0

        # =================================================
        # 2. MONTHLY ADHERENCE (Monthly Dispatch / Monthly Avg)
        # =================================================

        # A. Get Target (Sum of MonthlyAvg)
        sql_monthly_target = f"SELECT SUM(ISNULL(m.[MonthlyAvg], 0)) {base_query_master} {filter_clause}"
        cursor.execute(sql_monthly_target, params)
        total_monthly_avg = cursor.fetchone()[0] or 0

        # B. Get Actual (Sum of items moved to 'Dispatch' this month)
        sql_monthly_actual = f"""
            SELECT SUM(pl.quantity) 
            FROM production_log pl
            JOIN master m ON pl.item_code = m.[Item code]
            WHERE pl.to_stage = 'Dispatch' 
            AND MONTH(pl.moved_at) = MONTH(GETDATE()) 
            AND YEAR(pl.moved_at) = YEAR(GETDATE())
            {filter_clause}
        """
        cursor.execute(sql_monthly_actual, params)
        total_monthly_dispatched = cursor.fetchone()[0] or 0

        if total_monthly_avg > 0:
            monthly_adherence = (total_monthly_dispatched / total_monthly_avg) * 100
        else:
            monthly_adherence = 100.0 if total_monthly_dispatched > 0 else 0.0

    except Exception as e:
        print(f"Error calculating adherence: {e}")
        traceback.print_exc()

    return {'daily_adherence': daily_adherence, 'monthly_adherence': monthly_adherence}

@dashboard_bp.route('/dashboard', endpoint='dashboard')
def dashboard():
    current_view = request.args.get('view', 'overview')
    filter_type = request.args.get('type')
    filter_value = request.args.get('value')

    page = 1
    per_page = 17
    offset = (page - 1) * per_page
    pagination = None

    conn = get_db_connection()

    # Defaults
    stage_totals = {}
    top_plans = []
    vertical_overview = []
    category_overview = []
    total_wip_inventory = 0
    all_verticals_list = []
    daily_adherence = 0
    monthly_adherence = 0
    total_monthly_dispatch = 0
    failure_reasons = FAILURE_REASONS
    show_failure_button = True

    if not conn:
        flash('Database connection failed.', 'error')
        return render_template('dashboard.html', current_view='overview', stages=[], show_failure_button=False,
                               stage_totals={}, top_plans=[], pagination=None, vertical_overview=[],
                               category_overview=[],
                               total_monthly_dispatch=0, daily_adherence=0, monthly_adherence=0, failure_reasons=[],
                               all_verticals=[])

    try:
        cursor = conn.cursor()
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

        # Calculate Actual Dispatch from LOGS (2 AM Cutoff) - UPDATED to include 'Delivered'
        sql_daily_actual = """
                    DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
                    DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
                    DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

                    SELECT SUM(quantity) FROM production_log 
                    WHERE to_stage IN ('Dispatch', 'Delivered') AND moved_at >= @ProductionDayStart AND moved_at < @ProductionDayEnd
                """
        cursor.execute(sql_daily_actual)
        total_daily_dispatched = cursor.fetchone()[0] or 0

        daily_adherence = (total_daily_dispatched / total_daily_target * 100) if total_daily_target > 0 else (
            100.0 if total_daily_dispatched > 0 else 0)

        # 4. MONTHLY ADHERENCE
        cursor.execute("SELECT SUM(ISNULL([MonthlyAvg], 0)) FROM master")
        total_monthly_avg = (cursor.fetchone()[0] or 0)

        # UPDATED to include 'Delivered'
        sql_monthly_actual = """
                    SELECT SUM(quantity) FROM production_log 
                    WHERE to_stage IN ('Dispatch', 'Delivered') AND MONTH(moved_at) = MONTH(GETDATE()) AND YEAR(moved_at) = YEAR(GETDATE())
                """
        cursor.execute(sql_monthly_actual)
        total_monthly_dispatched = (cursor.fetchone()[0] or 0)

        monthly_adherence = (total_monthly_dispatched / total_monthly_avg * 100) if total_monthly_avg > 0 else (
            100.0 if total_monthly_dispatched > 0 else 0)

        total_monthly_dispatch = total_monthly_dispatched

        # --- VIEW LOGIC ---
        if current_view == 'details' and filter_type and filter_value:
            column_map = {'vertical': '[Vertical]', 'category': '[Category]'}
            target_column = column_map.get(filter_type)

            if not target_column:
                return redirect(url_for('dashboard'))

            # UPDATED CTE for Dispatch
            sql_details = f"""
                DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
                DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
                DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

                WITH TodayDispatch AS (
                    SELECT item_code, SUM(quantity) as QtyDispatched
                    FROM production_log
                    WHERE to_stage IN ('Dispatch', 'Delivered') 
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
                                   monthly_adherence=monthly_adherence,
                                   total_monthly_dispatch=total_monthly_dispatch,
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

                # --- TOP PLANS QUERY (UPDATED) ---
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
                                    WHERE to_stage IN ('Dispatch', 'Delivered') 
                                    AND moved_at >= @ProductionDayStart AND moved_at < @ProductionDayEnd
                                    GROUP BY item_code
                                )
                                SELECT
                                    p.[S.No.], p.[Item code ], p.[Description], 

                                    ISNULL(m.[Pending], 0) AS [Opening Trigger],

                                    (ISNULL(m.[Pending], 0) - ISNULL(td.QtyDispatched, 0)) AS [Pending],

                                    ISNULL(td.QtyDispatched, 0) AS [Dispatch],
                                    m.[FG],
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

                # --- VERTICAL OVERVIEW (UPDATED) ---
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
                        WHERE pl.to_stage IN ('Dispatch', 'Delivered') 
                        AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
                        GROUP BY m.[Vertical]
                    )
                    SELECT 
                        p.[Vertical], 
                        SUM(ISNULL(m.[Pending], 0)) as [Opening Trigger],
                        (SUM(ISNULL(m.[Pending], 0)) - ISNULL(vfg.QtyReachedFG, 0)) as [Pending],
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

                # --- CATEGORY OVERVIEW (UPDATED) ---
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
                        WHERE pl.to_stage IN ('Dispatch', 'Delivered') 
                        AND pl.moved_at >= @ProductionDayStart AND pl.moved_at < @ProductionDayEnd
                        GROUP BY m.[Category]
                    )
                    SELECT 
                        p.[Category], 
                        SUM(ISNULL(m.[Pending], 0)) as [Opening Trigger],
                        (SUM(ISNULL(m.[Pending], 0)) - ISNULL(cfg.QtyReachedFG, 0)) as [Pending],
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
                                   monthly_adherence=monthly_adherence,
                                   total_monthly_dispatch=total_monthly_dispatch,
                                   failure_reasons=failure_reasons,
                                   show_failure_button=show_failure_button)

    except Exception as e:
        flash(f"An error occurred: {e}", "error")
        traceback.print_exc()
        return render_template('dashboard.html', current_view='overview', stages=[], show_failure_button=False,
                               stage_totals={}, top_plans=[], pagination=None, vertical_overview=[],
                               category_overview=[],
                               total_monthly_dispatch=0, daily_adherence=0, monthly_adherence=0, failure_reasons=[],
                               all_verticals=[])
    finally:
        if conn: conn.close()

# --- SHARED HELPER FOR SYNCED DATA ---
def get_dashboard_stats(cursor, group_by=None):
    """
    Returns unified statistics for Dashboard.
    Syncs: Daily Plan (Production_pl), Master Data (Opening), and Real Log (Adherence).
    """
    # 1. Define Time Window (Today 2AM to Tomorrow 2AM)
    # Uses SQL params to prevent injection, logic embedded in query
    
    # 2. Base Query
    # We only look at items currently in the Production Plan (Production_pl)
    # Opening Trigger = master.[Pending] (The CSV upload)
    # Produced = Count from production_log (moved to FG or Dispatch today)
    
    group_select = f"p.[{group_by}]" if group_by else "'All'"
    group_clause = f"GROUP BY p.[{group_by}]" if group_by else ""
    order_clause = f"ORDER BY p.[{group_by}]" if group_by else ""

    sql = f"""
        DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
        DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
        DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

        -- 1. Calculate REAL Production (Items moved to FG today)
        WITH RealProduction AS (
            SELECT pl.item_code, SUM(pl.quantity) as QtyProduced
            FROM production_log pl
            WHERE pl.to_stage IN ('FG', 'Dispatch') -- We count FG or direct Dispatch as 'Production Done'
              AND pl.moved_at >= @ProductionDayStart 
              AND pl.moved_at < @ProductionDayEnd
            GROUP BY pl.item_code
        ),
        -- 2. Calculate REAL Dispatch (Items moved to Dispatch today)
        RealDispatch AS (
             SELECT pl.item_code, SUM(pl.quantity) as QtyDispatched
            FROM production_log pl
            WHERE pl.to_stage = 'Dispatch'
              AND pl.moved_at >= @ProductionDayStart 
              AND pl.moved_at < @ProductionDayEnd
            GROUP BY pl.item_code
        )
        SELECT 
            ISNULL(NULLIF({group_select}, ''), 'Unassigned') as [GroupKey],
            
            -- A. Opening Trigger (From CSV Upload in Master)
            SUM(ISNULL(m.[Pending], 0)) as [Opening Trigger],
            
            -- B. Actual Produced (Adherence Numerator)
            SUM(ISNULL(rp.QtyProduced, 0)) as [Produced],

            -- C. Actual Dispatched
            SUM(ISNULL(rd.QtyDispatched, 0)) as [Dispatch],
            
            -- D. Calculated Adherence %
            CASE 
                WHEN SUM(ISNULL(m.[Pending], 0)) = 0 THEN 0
                ELSE (CAST(SUM(ISNULL(rp.QtyProduced, 0)) AS FLOAT) / SUM(ISNULL(m.[Pending], 0))) * 100
            END as [AdherencePercent]

        FROM Production_pl p
        LEFT JOIN master m ON LTRIM(RTRIM(p.[Item code ])) = LTRIM(RTRIM(m.[Item code]))
        LEFT JOIN RealProduction rp ON LTRIM(RTRIM(p.[Item code ])) = LTRIM(RTRIM(rp.item_code))
        LEFT JOIN RealDispatch rd ON LTRIM(RTRIM(p.[Item code ])) = LTRIM(RTRIM(rd.item_code))
        
        {group_clause}
        {order_clause}
    """
    cursor.execute(sql)
    return [row_to_dict(cursor, row) for row in cursor.fetchall()]

@dashboard_bp.route('/api/stage_totals', endpoint='api_stage_totals')
def api_stage_totals():
    conn = get_db_connection()
    if not conn: return jsonify({})
    try:
        cursor = conn.cursor()
        
        # 1. Stage WIP Totals (Standard)
        stages = get_dynamic_stages(cursor)
        totals = {}
        for stage in stages:
            cursor.execute(f"SELECT SUM(ISNULL([{stage}], 0)) FROM master")
            totals[stage] = cursor.fetchone()[0] or 0
        
        # 2. Get Synced Dispatch Total
        stats = get_dashboard_stats(cursor, group_by=None) # Global stats
        if stats:
             totals['Dispatch'] = stats[0]['Dispatch'] # Use the synced calc
        else:
             totals['Dispatch'] = 0

        return jsonify(totals)
    finally:
        conn.close()


@dashboard_bp.route('/api/plan_page', endpoint='api_plan_page')
def api_plan_page():
    page = request.args.get('page', 1, type=int)
    per_page = 50
    offset = (page - 1) * per_page

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Count total plans
        cursor.execute("SELECT COUNT(*) FROM Production_pl")
        total_items = cursor.fetchone()[0]
        total_pages = ceil(total_items / per_page)

        # Get Item-Level Details using same Logic (CTE)
        # We join Production_pl to get the list, then calculate metrics per item
        sql = """
            DECLARE @CurrentProductionDate DATE = CAST(DATEADD(hour, -2, GETDATE()) AS DATE);
            DECLARE @ProductionDayStart DATETIME = DATEADD(hour, 2, CAST(@CurrentProductionDate AS DATETIME));
            DECLARE @ProductionDayEnd DATETIME = DATEADD(day, 1, @ProductionDayStart);

            WITH ItemProduction AS (
                SELECT pl.item_code, SUM(pl.quantity) as QtyProduced
                FROM production_log pl
                WHERE pl.to_stage IN ('FG', 'Dispatch') 
                  AND pl.moved_at >= @ProductionDayStart 
                  AND pl.moved_at < @ProductionDayEnd
                GROUP BY pl.item_code
            ),
            ItemDispatch AS (
                 SELECT pl.item_code, SUM(pl.quantity) as QtyDispatched
                FROM production_log pl
                WHERE pl.to_stage = 'Dispatch'
                  AND pl.moved_at >= @ProductionDayStart 
                  AND pl.moved_at < @ProductionDayEnd
                GROUP BY pl.item_code
            )
            SELECT 
                p.[Item code ] as [Item code ], 
                p.[Description], 
                
                -- Synced Metrics
                ISNULL(m.[Pending], 0) as [Opening Trigger],
                ISNULL(ip.QtyProduced, 0) as [Produced],
                ISNULL(id.QtyDispatched, 0) as [Dispatch],
                ISNULL(m.[FG], 0) as [FG],
                ISNULL(m.[Powder coating], 0) as [Powder coating],
                ISNULL(m.[Hydro Testing], 0) as [Hydro Testing],
                
                -- Failures
                ISNULL(p.[FailureInTrigger], 0) as [FailureInTrigger],
                p.[SavedReason]

            FROM Production_pl p
            LEFT JOIN master m ON LTRIM(RTRIM(p.[Item code ])) = LTRIM(RTRIM(m.[Item code]))
            LEFT JOIN ItemProduction ip ON LTRIM(RTRIM(p.[Item code ])) = LTRIM(RTRIM(ip.item_code))
            LEFT JOIN ItemDispatch id ON LTRIM(RTRIM(p.[Item code ])) = LTRIM(RTRIM(id.item_code))
            ORDER BY p.[priority_weight] DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        cursor.execute(sql, (offset, per_page))
        plans = [row_to_dict(cursor, row) for row in cursor.fetchall()]
        
        # Calculate Adherence and Pending per row in Python
        for p in plans:
             open_trig = p.get('Opening Trigger', 0)
             produced = p.get('Produced', 0)
             # Pending = Opening - Produced
             p['Pending'] = max(0, open_trig - produced)
             
             # Adherence %
             if open_trig > 0:
                 p['ItemAdherencePercent'] = (produced / open_trig) * 100.0
             else:
                 p['ItemAdherencePercent'] = 0.0

        return jsonify({
            'status': 'success',
            'plans': plans,
            'total_pages': total_pages,
            'current_page': page,
            'per_page': per_page,
            'show_failure_button': True
        })
    except Exception as e:
        print(f"Error Plan Page: {e}")
        return jsonify({'status': 'error', 'message': str(e)})
    finally:
        conn.close()

# --- REPLACEMENT FUNCTIONS FOR app/routes/dashboard.py ---
@dashboard_bp.route('/api/overview/vertical', endpoint='api_vertical_overview')
def api_vertical_overview():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        data = get_dashboard_stats(cursor, group_by='Vertical')
        # Remap [GroupKey] to [Vertical] for frontend
        for item in data:
            item['Vertical'] = item.pop('GroupKey')
            item['Pending'] = max(0, item['Opening Trigger'] - item['Produced']) # Calculated field
        return jsonify(data)
    except Exception as e:
        print(f"Error Vertical: {e}")
        return jsonify([])
    finally:
        conn.close()



@dashboard_bp.route('/api/overview/category', endpoint='api_category_overview')
def api_category_overview():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        data = get_dashboard_stats(cursor, group_by='Category')
        for item in data:
            item['Category'] = item.pop('GroupKey')
            item['Pending'] = max(0, item['Opening Trigger'] - item['Produced'])
        return jsonify(data)
    except Exception as e:
        print(f"Error Category: {e}")
        return jsonify([])
    finally:
        conn.close()    

@dashboard_bp.route('/api/inventory_filtered', methods=['POST'], endpoint='api_inventory_filtered')
def api_inventory_filtered():
    data = request.get_json()
    item_codes = data.get('item_code', [])
    verticals = data.get('vertical', [])
    categories = data.get('category', [])

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
            elif verticals:
                seq = ','.join(['?'] * len(verticals))
                base_filter += f" AND [Vertical] IN ({seq})"
                params.extend(verticals)
            elif categories:
                seq = ','.join(['?'] * len(categories))
                base_filter += f" AND [Category] IN ({seq})"
                params.extend(categories)

            # --- 2. Get WIP Totals from Master ---
            # We assume STAGES global variable is available or we fetch them
            # For robustness, we'll fetch dynamic stages here if possible, or use STAGES
            current_stages = get_dynamic_stages(cursor, dashboard_only=True)
            if not current_stages: current_stages = STAGES # Fallback

            stage_columns_sum = ', '.join([f"SUM(ISNULL([{stage}], 0)) as [{stage}]" for stage in current_stages])
            
            sql_wip = f"SELECT {stage_columns_sum} FROM master WHERE 1=1 {base_filter}"
            cursor.execute(sql_wip, params)
            row = cursor.fetchone()
            if row:
                inventory = row_to_dict(cursor, row)

            # --- 3. Get Monthly Dispatch from Logs (Filtered) ---
            # We need to join with Master to apply the same filters (Vertical/Category)
            sql_monthly = f"""
                SELECT SUM(pl.quantity) 
                FROM production_log pl
                JOIN master m ON pl.item_code = m.[Item code]
                WHERE pl.to_stage IN ('Dispatch', 'Delivered') 
                AND MONTH(pl.moved_at) = MONTH(GETDATE()) 
                AND YEAR(pl.moved_at) = YEAR(GETDATE())
                {base_filter.replace('[Item code]', 'm.[Item code]')
                            .replace('[Vertical]', 'm.[Vertical]')
                            .replace('[Category]', 'm.[Category]')}
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



@dashboard_bp.route('/api/adherence_filtered', methods=['POST'], endpoint='api_adherence_filtered')
def api_adherence_filtered():
    data = request.get_json()
    item_codes = data.get('item_code', [])
    verticals = data.get('vertical', [])
    categories = data.get('category', [])

    conn = get_db_connection()
    result_data = {}
    if conn:
        try:
            result_data = calculate_adherence(conn, item_codes=item_codes, verticals=verticals, categories=categories)
        finally:
            conn.close()
    return jsonify(result_data)
def calculate_adherence_metrics(cursor, filter_clause="", params=None):
    """
    Calculates Daily and Monthly Adherence.
    Formula: (Executed / (Executed + Failure)) * 100
    """
    if params is None: params = []
    
    # Daily Adherence
    # We sum up the 'Today Triggered' vs 'FailureInTrigger' from Production_pl
    sql_daily = f"""
        SELECT 
            SUM([Today Triggered]) as executed,
            SUM([FailureInTrigger]) as failed
        FROM Production_pl
        WHERE 1=1 {filter_clause}
    """
    cursor.execute(sql_daily, params)
    row = cursor.fetchone()
    daily_exec = row[0] or 0
    daily_fail = row[1] or 0
    daily_total = daily_exec + daily_fail
    daily_adherence = (daily_exec / daily_total * 100) if daily_total > 0 else 100.0

    # Monthly Adherence (Simple placeholder logic for now)
    monthly_adherence = daily_adherence 

    return daily_adherence, monthly_adherence

@dashboard_bp.route('/api/adherence_totals', endpoint='api_adherence_totals')
def api_adherence_totals():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        stats = get_dashboard_stats(cursor, group_by=None)
        
        if not stats:
            return jsonify({'daily_adherence': 0, 'monthly_adherence': 0})
        
        # Daily Adherence
        daily = stats[0]['AdherencePercent']
        
        # Monthly: For now, assuming same logic or accumulation. 
        # (To make it strictly cumulative, we'd need a wider date range query).
        # Returning daily for now to ensure at least this number is accurate.
        return jsonify({'daily_adherence': daily, 'monthly_adherence': daily})
        
    except Exception as e:
        print(f"Error Adherence: {e}")
        return jsonify({'daily_adherence': 0, 'monthly_adherence': 0})
    finally:
        conn.close()

@dashboard_bp.route('/log_failure_reason', methods=['POST'], endpoint='log_failure_reason')
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

