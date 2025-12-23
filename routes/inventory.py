from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.database import get_db_connection, row_to_dict
import traceback
from math import ceil

# --- Initialize Blueprint ---
inventory_bp = Blueprint('inventory', __name__)

# --- PASTE YOUR FUNCTIONS BELOW THIS LINE ---
# Remember to change @app.route to @inventory_bp.route

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

@inventory_bp.route('/items', endpoint='manage_items')
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

@inventory_bp.route('/add_item', methods=['POST'], endpoint='add_item')
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

@inventory_bp.route('/edit_item', methods=['POST'], endpoint='edit_item')
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

@inventory_bp.route('/delete_item', methods=['POST'], endpoint='delete_item')
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

@inventory_bp.route('/stages', endpoint='manage_stages')
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


@inventory_bp.route('/add_stage', methods=['POST'], endpoint='add_stage')
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

@inventory_bp.route('/edit_stage', methods=['POST'], endpoint='edit_stage')
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

@inventory_bp.route('/delete_stage', methods=['POST'], endpoint='delete_stage')
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

@inventory_bp.route('/reorder_stages', methods=['POST'], endpoint='reorder_stages')
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

