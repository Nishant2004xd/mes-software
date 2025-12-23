document.addEventListener('DOMContentLoaded', function() {
    
    // --- 1. Client-Side Filtering Logic ---

    window.filterTable = function() {
        const searchInput = document.getElementById('filter-search');
        // Get dropdowns if they exist, otherwise null
        const verticalSelect = document.getElementById('filter-vertical');
        const categorySelect = document.getElementById('filter-category');
        // Check navbar type filter if page specific one is missing
        const typeSelect = document.getElementById('filter-type') || document.getElementById('navbar-type-filter');

        if (!searchInput) return;

        const search = searchInput.value.toLowerCase();
        const vertical = verticalSelect ? verticalSelect.value : '';
        const category = categorySelect ? categorySelect.value : '';
        const type = typeSelect ? typeSelect.value : '';

        const rows = document.querySelectorAll('.plan-row');
        let visibleCount = 0;

        rows.forEach(row => {
            const rCode = (row.dataset.code || '').toLowerCase();
            const rDesc = (row.dataset.desc || '').toLowerCase();
            const rVert = row.dataset.vertical || '';
            const rCat = row.dataset.category || '';
            const rType = row.dataset.type || '';

            // Logic: If filter is empty, it's a match. Otherwise, check exact match (or includes for search)
            const matchSearch = (!search || rCode.includes(search) || rDesc.includes(search));
            const matchVert = (!vertical || rVert === vertical);
            const matchCat = (!category || rCat === category);
            const matchType = (!type || rType === type);

            if (matchSearch && matchVert && matchCat && matchType) {
                row.style.display = '';
                visibleCount++;
                // Update S.No for filtered view
                const snoCell = row.querySelector('.sno-cell');
                if (snoCell) snoCell.textContent = visibleCount;
            } else {
                row.style.display = 'none';
            }
        });

        // Show/Hide "No Results" message
        const noResults = document.getElementById('no-results-msg');
        if (noResults) {
            noResults.style.display = (visibleCount === 0) ? 'block' : 'none';
        }
    };

    window.clearFilters = function() {
        const ids = ['filter-search', 'filter-vertical', 'filter-category', 'filter-type'];
        ids.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.value = '';
        });
        filterTable(); // Re-run to show all
    };

    // --- 2. Plan Update Listener (AJAX) ---
    const planInputs = document.querySelectorAll('.plan-input');

    planInputs.forEach(input => {
        input.addEventListener('change', function(e) {
            const itemCode = this.name.replace('modified_plan_', '');
            const newPlanValue = this.value;

            // Visual feedback: Processing (Orange)
            this.style.borderColor = '#fdba74'; 
            this.style.backgroundColor = '#fff7ed';

            const postData = { item_code: itemCode, new_plan_value: newPlanValue };

            // Use the URL from the config object
            fetch(productionConfig.urls.updateSinglePlan, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(postData)
            })
            .then(response => response.json().then(data => ({ ok: response.ok, data })))
            .then(({ ok, data }) => {
                if (ok && data.status === 'success') {
                    // Success feedback: Green
                    this.style.borderColor = '#4ade80'; 
                    this.style.backgroundColor = '#f0fdf4';
                    
                    // Revert style after 1 second
                    setTimeout(() => {
                        this.style.borderColor = '';
                        this.style.backgroundColor = '';
                    }, 1000);
                } else {
                    // Error feedback: Red
                    this.style.borderColor = '#f87171'; 
                    this.style.backgroundColor = '#fef2f2';
                    alert('Error saving changes: ' + (data.message || 'Unknown error'));
                }
            })
            .catch(error => {
                this.style.borderColor = '#f87171';
                alert('Network error.');
                console.error(error);
            });
        });
    });
});