from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.database import get_db_connection, row_to_dict
import traceback
from math import ceil

# --- Initialize Blueprint ---
wip_bp = Blueprint('wip', __name__)

# --- PASTE YOUR FUNCTIONS BELOW THIS LINE ---
# Remember to change @app.route to @wip_bp.route

@wip_bp.route('/wip_tracking', endpoint='wip_tracking')
def wip_tracking():
    # ... (existing setup code: page, search, filter vars) ...
    page = request.args.get('page', 1, type=int)
    per_page = 20
    offset = (page - 1) * per_page
    search_query = request.args.get('search', '')
    selected_type = request.args.get('type', '')
    selected_category = request.args.get('category', '')

    # Hardcoded Options
    type_options = ['Lean', 'Non Lean']
    category_options = ['Stranger', 'Runner', 'Repeater', 'Milk Run']

    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('dashboard'))

    master_items = []
    dynamic_stages = []
    pagination = None

    try:
        cursor = conn.cursor()

        # 1. FETCH DYNAMIC STAGES
        # We assume 'Not Started' and 'FG' might be in this list or separate columns
        cursor.execute("SELECT stage_name FROM manufacturing_stages ORDER BY display_order ASC")
        dynamic_stages = [row[0] for row in cursor.fetchall()]

        # 2. Build Query
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

        # 3. Pagination
        cursor.execute(count_query, params)
        total_items = cursor.fetchone()[0]
        total_pages = ceil(total_items / per_page)

        # 4. Fetch Data
        final_query = base_query + " ORDER BY [Item code] OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([offset, per_page])

        cursor.execute(final_query, params)
        master_items = [row_to_dict(cursor, row) for row in cursor.fetchall()]

        # 5. Calculate Total Inventory Dynamically (CORRECTED)
        for item in master_items:
            current_total = 0
            for stage in dynamic_stages:
                # SKIP 'Not Started' and 'FG' from the total sum
                if stage in ['Not Started', 'FG']:
                    continue

                qty = item.get(stage)
                if qty:
                    current_total += int(qty)
            item['Total Inv'] = current_total

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
        flash(f"Error fetching WIP data: {e}", 'error')
        traceback.print_exc()
    finally:
        conn.close()

    return render_template('wip.html',
                           master_items=master_items,
                           stages=dynamic_stages,
                           type_options=type_options,
                           category_options=category_options,
                           selected_type=selected_type,
                           selected_category=selected_category,
                           search_query=search_query,
                           pagination=pagination)

@wip_bp.route('/move_item', methods=['POST'])
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

        # 3. HANDLE DISPATCH COUNT (The Fix)
        # FIX: Only update Production_pl 'Dispatch' count if explicitly moving to 'Dispatch'.
        # Moving to 'FG' will NO LONGER increment this count.
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


@wip_bp.route('/dispatch_item', methods=['POST'])
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

@wip_bp.route('/flag_item', methods=['POST'], endpoint='flag_item')
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

@wip_bp.route('/deliver_item', methods=['POST'], endpoint='deliver_item')
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
