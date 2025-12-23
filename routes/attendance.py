from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file
from app.database import get_db_connection, row_to_dict
from datetime import datetime, date, timedelta
import traceback
import pandas as pd
import io

# --- Initialize Blueprint ---
attendance_bp = Blueprint('attendance', __name__)

# --- PASTE YOUR FUNCTIONS BELOW THIS LINE ---
# Remember to change @app.route to @attendance_bp.route

@attendance_bp.route('/attendance', methods=['GET'], endpoint='attendance')
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

@attendance_bp.route('/attendance/mark_single', methods=['POST'])
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

@attendance_bp.route('/add_employee', methods=['POST'], endpoint='add_employee')
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

@attendance_bp.route('/edit_employee', methods=['POST'], endpoint='edit_employee')
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

@attendance_bp.route('/add_holiday', methods=['POST'], endpoint='add_holiday')
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

@attendance_bp.route('/remove_employee', methods=['POST'], endpoint='remove_employee')
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

@attendance_bp.route('/mark_working_day', methods=['POST'], endpoint='mark_working_day')
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

@attendance_bp.route('/export_attendance', endpoint='export_attendance')
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
