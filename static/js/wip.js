document.addEventListener('DOMContentLoaded', function() {
    
    // --- 1. CONFIGURATION ---
    const config = window.wipConfig || {};
    const allStages = config.stages || [];
    const groupStages = config.groupStages || [];
    
    // --- 2. MODAL LOGIC ---
    const actionModal = document.getElementById('action-modal');
    
    // Click logic for table cells
    document.querySelectorAll('.clickable-cell').forEach(cell => {
        cell.addEventListener('click', function() {
            const itemCode = this.dataset.itemCode;
            const stage = this.dataset.stage;
            const quantity = parseInt(this.dataset.quantity);
            // Get breakdown if available (for grouped columns)
            const breakdown = this.dataset.breakdown ? JSON.parse(this.dataset.breakdown) : null;

            openActionModal(itemCode, stage, quantity, breakdown);
        });
    });

    function openActionModal(itemCode, stage, quantity, breakdown) {
        // Update Title & Subtitle
        document.getElementById('action-modal-title').textContent = `Actions for ${itemCode}`;
        document.getElementById('action-modal-subtitle').textContent = `${stage} | Available: ${quantity}`;

        // --- 1. Setup Move Form ---
        const moveForm = document.getElementById('move-form');
        const dispatchForm = document.getElementById('dispatch-form');
        
        const sourceDisplay = document.getElementById('source-stage-display');
        const sourceHidden = document.getElementById('source-stage-hidden');

        if (stage === 'FG') {
            // IF FG: Hide Move, Show Dispatch
            moveForm.style.display = 'none';
            dispatchForm.style.display = 'block';

            // Setup Dispatch Inputs
            document.getElementById('dispatch-item-code').value = itemCode;
            document.getElementById('dispatch-quantity').max = quantity;
            document.getElementById('dispatch-quantity').value = 1;
        } else {
            // IF NORMAL STAGE: Show Move, Hide Dispatch
            moveForm.style.display = 'block';
            dispatchForm.style.display = 'none';

            // Setup Item Code & Source Display
            document.getElementById('move-item-code').value = itemCode;
            sourceDisplay.value = stage; // User sees "Before Hydro"
            
            // --- AUTO-SELECT SOURCE LOGIC ---
            if (stage === 'Before Hydro' && breakdown) {
                // We need to deduct from a real column in the DB.
                // We pick the LATEST stage in the group that has stock.
                let bestSource = null;
                let maxQty = 0;

                // Iterate group in reverse to find the "furthest" item
                const reversedGroup = [...groupStages].reverse();
                for (const s of reversedGroup) {
                    if (breakdown[s] && breakdown[s] > 0) {
                        bestSource = s;
                        maxQty = breakdown[s];
                        break; 
                    }
                }

                if (bestSource) {
                    sourceHidden.value = bestSource; // Send real DB name (e.g. "Full welding")
                    document.getElementById('move-quantity').max = maxQty;
                    document.getElementById('move-quantity').value = 1;
                } else {
                    // Fallback (shouldn't happen if total > 0)
                    sourceHidden.value = stage;
                    document.getElementById('move-quantity').max = quantity;
                }
            } else {
                // Standard Single Stage
                sourceHidden.value = stage;
                document.getElementById('move-quantity').max = quantity;
                document.getElementById('move-quantity').value = 1;
            }

            // --- DESTINATION STAGE LOGIC (Allow Previous Stages) ---
            const destSelect = document.getElementById('dest-stage');
            destSelect.innerHTML = '';

            // Add ALL stages except the current one
            allStages.forEach(s => {
                if (s !== stage) {
                    const option = new Option(s, s);
                    destSelect.add(option);
                }
            });

            // Attempt to Auto-select the NEXT logical stage for convenience
            const currentIndex = allStages.indexOf(stage);
            if (currentIndex >= 0 && currentIndex < allStages.length - 1) {
                // Select the next one in the list
                destSelect.value = allStages[currentIndex + 1];
            } else if (destSelect.options.length > 0) {
                // Fallback to first option
                destSelect.selectedIndex = 0;
            }
        }

        // --- 2. Setup Flag Form (Always available) ---
        document.getElementById('flag-item-code').value = itemCode;
        document.getElementById('flag-stage').value = stage; 
        document.getElementById('flag-quantity').max = quantity;
        document.getElementById('flag-quantity').value = 1;
        document.getElementById('flag-remark').value = '';

        // Show Modal
        if (actionModal) actionModal.style.display = "block";
    }

    // --- 3. AJAX SUBMISSIONS ---

    // Move Form
    const moveForm = document.getElementById('move-form');
    if (moveForm) {
        moveForm.addEventListener('submit', function(e) {
            e.preventDefault();

            const data = {
                item_code: document.getElementById('move-item-code').value,
                source_stage: document.getElementById('source-stage-hidden').value, // Real DB Column
                dest_stage: document.getElementById('dest-stage').value,
                quantity: document.getElementById('move-quantity').value
            };

            fetch(config.urls.moveItem, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(data)
            })
            .then(response => response.json())
            .then(result => {
                if (result.status === 'success') {
                    window.location.reload();
                } else {
                    alert('Error: ' + result.message);
                }
            })
            .catch(error => {
                console.error('Error:', error);
                alert('An unexpected error occurred.');
            });
        });
    }

    // Flag Form
    const flagForm = document.getElementById('flag-form');
    if (flagForm) {
        flagForm.addEventListener('submit', function(e) {
            e.preventDefault();
            
            const data = {
                item_code: document.getElementById('flag-item-code').value,
                stage: document.getElementById('flag-stage').value,
                quantity: document.getElementById('flag-quantity').value,
                remark: document.getElementById('flag-remark').value
            };
            fetch(config.urls.flagItem, {
                method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data)
            })
            .then(response => response.json())
            .then(result => {
                if (result.status === 'success') {
                    document.getElementById('action-modal').style.display = "none";
                    alert("Issue logged.");
                    window.location.reload();
                } else { alert('Error: ' + result.message); }
            })
            .catch(error => { console.error('Error:', error); alert('An unexpected error occurred.'); });
        });
    }

    // Dispatch Form
    const dispatchForm = document.getElementById('dispatch-form');
    if (dispatchForm) {
        dispatchForm.addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            fetch(config.urls.dispatchItem, { method: 'POST', body: formData })
            .then(response => response.json())
            .then(result => {
                if (result.status === 'success') { window.location.reload(); } 
                else { alert('Error: ' + result.message); }
            })
            .catch(error => { console.error('Error:', error); alert('An unexpected error occurred.'); });
        });
    }

    // --- 4. CLOSE LOGIC ---
    document.querySelectorAll('.close-button').forEach(btn => {
        btn.onclick = () => { if(actionModal) actionModal.style.display = "none"; }
    });
    window.onclick = (e) => {
        if(e.target == actionModal) actionModal.style.display = "none";
    }
});