document.addEventListener('DOMContentLoaded', function() {
    
    // --- 1. Modal Logic ---
    const addModal = document.getElementById('add-stage-modal');
    const editModal = document.getElementById('edit-stage-modal');
    const addBtn = document.getElementById('show-add-modal-btn');

    // Specific close buttons
    const closeAddBtn = document.getElementById('close-add-modal');
    const closeEditBtn = document.getElementById('close-edit-modal');

    // Open Add Modal
    if (addBtn) {
        addBtn.addEventListener('click', function() {
            addModal.style.display = "block";
        });
    }

    // Close Logic
    if (closeAddBtn) {
        closeAddBtn.onclick = () => addModal.style.display = "none";
    }
    if (closeEditBtn) {
        closeEditBtn.onclick = () => editModal.style.display = "none";
    }

    // Close on outside click
    window.onclick = function(event) {
        if (event.target == addModal) addModal.style.display = "none";
        if (event.target == editModal) editModal.style.display = "none";
    }

    // --- 2. Global Edit Function ---
    window.openEditModal = function(id, name, isDashboard) {
        document.getElementById('edit_stage_id').value = id;
        document.getElementById('edit_original_name').value = name;
        document.getElementById('edit_stage_name').value = name;

        const chk = document.getElementById('edit_include_dashboard');
        if (isDashboard == 'True' || isDashboard == '1' || isDashboard === true) {
            chk.checked = true;
        } else {
            chk.checked = false;
        }

        editModal.style.display = "block";
    }

    // --- 3. Drag and Drop Logic (Fixed) ---
    const tbody = document.getElementById('sortable-rows');
    let draggedRow = null;

    if (tbody) {
        tbody.addEventListener('dragstart', function(e) {
            // FIX: Explicitly prevent dragging if row is not marked draggable
            if (!e.target.classList.contains('draggable-row')) {
                e.preventDefault();
                return false;
            }

            draggedRow = e.target;
            e.target.style.opacity = '0.5';
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/html', e.target.innerHTML); 
        });

        tbody.addEventListener('dragend', function(e) {
            if(e.target) e.target.style.opacity = '';
            saveNewOrder();
        });

        tbody.addEventListener('dragover', function(e) {
            e.preventDefault();
            
            const afterElement = getDragAfterElement(tbody, e.clientY);
            
            // FIX: Find the "FG" row (last locked row) to prevent dropping below it
            // We assume FG is the last row with class 'locked-row' usually
            const lockedRows = [...tbody.querySelectorAll('.locked-row')];
            const fgRow = lockedRows.length > 0 ? lockedRows[lockedRows.length - 1] : null;

            if (afterElement == null) {
                // If dragging to bottom, ensure we insert BEFORE FG, not append after
                if (fgRow) {
                    tbody.insertBefore(draggedRow, fgRow);
                } else {
                    tbody.appendChild(draggedRow);
                }
            } else {
                tbody.insertBefore(draggedRow, afterElement);
            }
        });
    }

    function getDragAfterElement(container, y) {
        // Only consider draggable rows for calculation, ignoring headers or locked rows
        const draggableElements = [...container.querySelectorAll('.draggable-row:not([style*="opacity: 0.5"])')];

        return draggableElements.reduce((closest, child) => {
            const box = child.getBoundingClientRect();
            const offset = y - box.top - box.height / 2;
            if (offset < 0 && offset > closest.offset) {
                return { offset: offset, element: child };
            } else {
                return closest;
            }
        }, { offset: Number.NEGATIVE_INFINITY }).element;
    }

    function saveNewOrder() {
        // Select ALL rows (draggable + locked) to get the true full order
        // This ensures the DB gets the correct index for every item
        const rows = document.querySelectorAll('#sortable-rows tr');
        const orderIds = Array.from(rows).map(row => row.getAttribute('data-id')).filter(id => id); // Filter out nulls

        fetch(stagesConfig.urls.reorderStages, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ order: orderIds })
        })
        .then(res => res.json())
        .then(data => {
            if(data.status === 'success') {
                // Update S.No visuals immediately
                document.querySelectorAll('#sortable-rows tr').forEach((row, index) => {
                    const snoCell = row.querySelector('.sno-cell');
                    if(snoCell) snoCell.textContent = index + 1;
                });
            } else {
                console.error('Save order failed', data);
            }
        })
        .catch(err => console.error('Error saving order:', err));
    }
});