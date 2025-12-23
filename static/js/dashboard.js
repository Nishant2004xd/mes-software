document.addEventListener('DOMContentLoaded', function() {

    // --- GLOBAL VARIABLES ---
    let currentPage = 1;
    // READ FROM CONFIG OBJECT (Passed from HTML)
    const config = window.dashboardConfig || {};
    const stages = config.stages || [];
    const urls = config.urls || {};

    // DOM Elements
    const failureHeader = document.getElementById('failure-header-toggle');
    const planTableBody = document.getElementById('plan-table-body');
    const verticalTableBody = document.getElementById('vertical-table-body');
    const categoryTableBody = document.getElementById('category-table-body');
    
    // --- UPDATED: Get Filter from Navbar ---
    const typeFilterSelect = document.getElementById('navbar-type-filter');

    // Track selections for row-click filtering
    let currentSelections = {
        item_code: new Set(),
        vertical: new Set(),
        category: new Set()
    };

    // ---------------------------------------------------------
    // 1. DATA FETCHING (Real-Time Polling + Filtering)
    // ---------------------------------------------------------
    function refreshDashboardData() {
        if (!planTableBody) return;

        // Get Global Type Filter Value (Lean/Non Lean)
        const globalType = typeFilterSelect ? typeFilterSelect.value : '';

        // Prepare Filter Payload
        const payload = {
            item_code: Array.from(currentSelections.item_code),
            vertical: Array.from(currentSelections.vertical),
            category: Array.from(currentSelections.category),
            type: globalType ? [globalType] : [] 
        };

        // A. Update Top Cards (WIP & Dispatch)
        fetch(urls.inventoryFiltered, { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' }, 
            body: JSON.stringify(payload) 
        })
        .then(r => r.json())
        .then(updateCards)
        .catch(err => console.warn("Card update skipped:", err));

        // B. Update Adherence Cards
        fetch(urls.adherenceFiltered, { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' }, 
            body: JSON.stringify(payload) 
        })
        .then(r => r.json())
        .then(updateAdherenceCards)
        .catch(err => console.warn("Adherence update skipped:", err));

        // C. Update Tables (GET Requests)
        let queryParams = `?page=${currentPage}`;
        if (globalType) queryParams += `&type=${encodeURIComponent(globalType)}`;

        // 1. Daily Production Plan Table
        fetch(`${urls.planPage}${queryParams}`)
            .then(r => r.json())
            .then(data => {
                if (data.status === 'success') {
                    updatePlanTable(data.plans, data.per_page, data.current_page, data.show_failure_button);
                    updatePlanPagination(data.total_pages, data.current_page);
                    attachDragEvents();
                    restoreSelectionHighlights();
                }
            })
            .catch(console.error);

        // 2. Vertical Overview
        const overviewParams = globalType ? `?type=${encodeURIComponent(globalType)}` : '';

        fetch(`${urls.verticalOverview}${overviewParams}`)
            .then(r => r.json())
            .then(data => {
                updateOverviewTable(verticalTableBody, data, 'Vertical', 'vertical');
                renderOverviewChart('verticalChart', data, 'Vertical');
                restoreSelectionHighlights();
            })
            .catch(console.error);

        // 3. Category Overview
        fetch(`${urls.categoryOverview}${overviewParams}`)
            .then(r => r.json())
            .then(data => {
                updateOverviewTable(categoryTableBody, data, 'Category', 'category');
                renderOverviewChart('categoryChart', data, 'Category');
                restoreSelectionHighlights();
            })
            .catch(console.error);
    }

    // --- EVENT LISTENER: Navbar Type Filter ---
    if (typeFilterSelect) {
        typeFilterSelect.addEventListener('change', function() {
            currentPage = 1; 
            refreshDashboardData();
        });
    }

    // ---------------------------------------------------------
    // 2. INTERACTION LOGIC (Clicking Rows)
    // ---------------------------------------------------------

    function handleRowClick(event) {
        if (event.target.closest('a.button, button.reason-btn')) return;

        const row = event.target.closest('tr.clickable-row');
        if (!row) return;

        const filterType = row.dataset.filterType;
        const filterValue = row.dataset.filterValue;
        const isCtrlClick = event.ctrlKey || event.metaKey;

        if (filterType === 'item_code') { currentSelections.vertical.clear(); currentSelections.category.clear(); }
        if (filterType === 'vertical') { currentSelections.item_code.clear(); currentSelections.category.clear(); }
        if (filterType === 'category') { currentSelections.item_code.clear(); currentSelections.vertical.clear(); }

        if (isCtrlClick) {
            if (currentSelections[filterType].has(filterValue)) {
                currentSelections[filterType].delete(filterValue);
            } else {
                currentSelections[filterType].add(filterValue);
            }
        } else {
            const alreadySelected = currentSelections[filterType].has(filterValue) && currentSelections[filterType].size === 1;
            currentSelections[filterType].clear();
            if (!alreadySelected) {
                currentSelections[filterType].add(filterValue);
            }
        }

        restoreSelectionHighlights();
        refreshDashboardData();
    }

    function restoreSelectionHighlights() {
        document.querySelectorAll('tr.clickable-row').forEach(row => {
            const fType = row.dataset.filterType;
            const fVal = row.dataset.filterValue;
            if (currentSelections[fType] && currentSelections[fType].has(fVal)) {
                row.classList.add('selected-row');
            } else {
                row.classList.remove('selected-row');
            }
        });
    }

    if (planTableBody) planTableBody.addEventListener('click', handleRowClick);
    if (verticalTableBody) verticalTableBody.addEventListener('click', handleRowClick);
    if (categoryTableBody) categoryTableBody.addEventListener('click', handleRowClick);


    // ---------------------------------------------------------
    // 3. UI UPDATE FUNCTIONS (Render HTML)
    // ---------------------------------------------------------

// FILE: static/js/dashboard.js

// Find the existing updateCards function and modify it:

    function updateCards(data) {
        if (!data) return;
        let grandTotal = 0;
        
        // Update Standard WIP Stages
        stages.forEach(stage => {
            const count = parseInt(data[stage]) || 0;
            if (stage !== 'FG') grandTotal += count;
            const card = document.querySelector(`.stage-card[data-stage="${stage}"]`);
            if (card) card.querySelector('.count').textContent = count;
        });

        // Update Total Inv Card
        const totalCard = document.querySelector('.stage-card[data-stage="Total-Inv"] .count');
        if (totalCard) totalCard.textContent = grandTotal;
        
        // Update Monthly Dispatch Card
        const dispatchCard = document.querySelector('.stage-card.monthly-dispatch-card .count');
        if (dispatchCard && data['Dispatch'] !== undefined) {
            dispatchCard.textContent = parseInt(data['Dispatch']);
        }

        // --- NEW: Update Monthly Trigger Adherence Card ---
        const monthlyTriggerCard = document.querySelector('.stage-card[data-metric="monthly-trigger-adherence"]');
        if (monthlyTriggerCard && data['MonthlyTriggerAdherence'] !== undefined) {
            let val = parseFloat(data['MonthlyTriggerAdherence']) || 0;
            // Cap at 100% and Floor at 0% for visual sanity
            val = Math.max(0, Math.min(100, val));
            
            const countDiv = monthlyTriggerCard.querySelector('.count');
            countDiv.textContent = val.toFixed(1) + '%';

            // Optional: Dynamic coloring (Purple Scale)
            const hue = 270; // Purple
            const lightness = 95 - (val * 0.15); // Darker as percentage goes up
            monthlyTriggerCard.style.backgroundColor = `hsl(${hue}, 60%, ${Math.max(85, lightness)}%)`;
        }
    }

// FILE: static/js/dashboard.js

// Locate the updateAdherenceCards function and ensure it looks like this:

// FILE: static/js/dashboard.js

// Locate the existing updateAdherenceCards function and update the Monthly section:

    function updateAdherenceCards(data) {
        if (!data) return;

        // 1. Update Daily Adherence
        if (data.daily_adherence !== undefined) {
            updateSingleAdherenceCard('daily-adherence', data.daily_adherence);
        }

        // 2. Update Monthly Trigger Adherence
        if (data.monthly_trigger_adherence !== undefined) {
            const monthlyTriggerCard = document.querySelector('.stage-card[data-metric="monthly-trigger-adherence"]');
            if (monthlyTriggerCard) {
                let val = parseFloat(data.monthly_trigger_adherence) || 0;
                // Cap at 100% and Floor at 0%
                val = Math.max(0, Math.min(100, val));
                
                const countDiv = monthlyTriggerCard.querySelector('.count');
                if (countDiv) countDiv.textContent = val.toFixed(1) + '%';

                // --- CHANGED: Use Red-to-Green Logic (Same as Daily) ---
                // 0% = Hue 0 (Red), 100% = Hue 120 (Green)
                const hue = Math.round(val * 1.2); 
                
                monthlyTriggerCard.style.backgroundColor = `hsl(${hue}, 80%, 85%)`;
                monthlyTriggerCard.style.color = '#333';
            }
        }
    }

    // Cleaned up: Removed Num/Den logic
    function updateSingleAdherenceCard(metric, value) {
        const card = document.querySelector(`.stage-card[data-metric="${metric}"]`);
        if (!card) return;
        
        // 1. Update Percentage
        let percent = parseFloat(value) || 0;
        percent = Math.max(0, Math.min(100, percent));
        const hue = Math.round(percent * 1.2); 
        
        // Update the big number
        const countEl = card.querySelector('.count');
        if (countEl) countEl.textContent = `${percent.toFixed(1)}%`;
        
        // Update Background Color
        card.style.backgroundColor = `hsl(${hue}, 80%, 85%)`;

        // If there was any detail element from before, remove it to be safe
        const detailsEl = card.querySelector('.adherence-details');
        if (detailsEl) detailsEl.remove();
    }

    function updatePlanTable(plans, perPage, curPage, showFailureButton) {
         let html = '';
         if (!plans || plans.length === 0) {
             html = '<tr><td colspan="11" class="placeholder">No plan data available.</td></tr>';
         } else {
             plans.forEach((plan, index) => {
                 const serialNumber = (parseInt(curPage) - 1) * parseInt(perPage) + (index + 1);
                 
                 let percent = parseFloat(plan['ItemAdherencePercent']) || 0;
                 percent = Math.max(0, Math.min(100, percent));
                 const hue = Math.round(percent * 1.2);
                 const adherenceStyle = `background-color: hsl(${hue}, 80%, 85%); color: #333; font-weight: bold;`;
                 
                 const pendingQty = parseInt(plan['Pending']);
                 const pendingStyle = pendingQty > 0 ? `font-weight: bold; color: #d95319;` : `color: #166534;`;

                 let reasonHtml = '';
                 const failureQty = parseInt(plan['FailureInTrigger']);
                 if (failureQty > 0 && showFailureButton) {
                     const reason = plan['SavedReason'] || '';
                     const escapedReason = reason.replace(/"/g, '&quot;');
                     const hasReasonClass = reason ? ' has-reason' : '';
                     reasonHtml = `<button class="button reason-btn${hasReasonClass}"
                                     data-item-code="${plan['Item code ']}"
                                     data-failed-qty="${failureQty}"
                                     data-saved-reason="${escapedReason}"
                                     title="${reason || 'Log Reason'}">
                                     ${failureQty} Fail
                                    </button>`;
                 }

                 html += `
                 <tr class="clickable-row draggable-item" draggable="true"
                     data-item-code="${plan['Item code ']}"
                     data-filter-type="item_code" data-filter-value="${plan['Item code ']}">
                    <td>${serialNumber}</td>
                    <td class="align-left">${plan['Item code ']}</td>
                    <td class="align-left description-col">${plan['Description']}</td>
                    
                    <td style="${adherenceStyle}" title="Adherence: ${percent.toFixed(1)}%">
                        ${parseInt(plan['Opening Trigger'])}
                    </td>

                    <td style="color: #64748b; font-weight: 500;">
                        ${parseInt(plan['Next Pending'] || 0)}
                    </td>
                    
                    <td style="${pendingStyle}">${pendingQty}</td>
                    
                    <td>${parseInt(plan['Dispatch'])}</td>
                    <td>${parseInt(plan['FG'])}</td>
                    <td>${parseInt(plan['Powder coating'])}</td>
                    <td>${parseInt(plan['Hydro Testing'])}</td>
                    <td class="reason-cell">${reasonHtml}</td>
                 </tr>`;
             });
         }
         planTableBody.innerHTML = html;
         if (failureHeader && failureHeader.classList.contains('active')) {
             planTableBody.classList.add('show-failures');
         } else {
             planTableBody.classList.remove('show-failures');
         }
    }

    function updatePlanPagination(totalPages, curPage) {
        const paginationContainer = document.getElementById('plan-pagination');
        if(!paginationContainer) return;
        let html = '<nav aria-label="Page navigation"><ul class="pagination">';
        const prevDisabled = (curPage === 1) ? 'disabled' : '';
        html += `<li class="page-item ${prevDisabled}"><a class="page-link" href="#" data-page="${curPage - 1}">&laquo;</a></li>`;
        for (let i = 1; i <= totalPages; i++) {
            const active = (i === curPage) ? 'active' : '';
            html += `<li class="page-item ${active}"><a class="page-link" href="#" data-page="${i}">${i}</a></li>`;
        }
        const nextDisabled = (curPage === totalPages) ? 'disabled' : '';
        html += `<li class="page-item ${nextDisabled}"><a class="page-link" href="#" data-page="${curPage + 1}">&raquo;</a></li>`;
        html += `</ul></nav>`;
        paginationContainer.innerHTML = html;
    }

    function updateOverviewTable(tbody, data, labelKey, type) {
        if (!tbody) return;
        let html = '';
        let totalOpen = 0, totalPend = 0, totalDisp = 0;

        if (!data || data.length === 0) {
            html = '<tr><td colspan="5" class="placeholder">No data available.</td></tr>';
        } else {
            data.forEach(item => {
                let percent = parseFloat(item['AdherencePercent']) || 0;
                percent = Math.max(0, Math.min(100, percent));
                const hue = Math.round(percent * 1.2);
                const cellStyle = `background-color: hsl(${hue}, 80%, 85%); color: #333;`;

                const open = parseInt(item['Opening Trigger'] || 0);
                const pend = parseInt(item['Pending'] || 0);
                const disp = parseInt(item['Dispatch'] || 0);

                totalOpen += open;
                totalPend += pend;
                totalDisp += disp;

                html += `
                <tr class="clickable-row" data-filter-type="${type}" data-filter-value="${item[labelKey]}">
                    <td class="align-left">${item[labelKey]}</td>
                    <td style="${cellStyle}">${open}</td>
                    <td style="font-weight: bold;">${pend}</td>
                    <td>${disp}</td>
                    <td><a href="/dashboard?view=details&type=${type}&value=${encodeURIComponent(item[labelKey])}" class="button" style="padding: 1px 5px; font-size: 0.7rem;">Details</a></td>
                </tr>`;
            });
        }
        tbody.innerHTML = html;

        // Update Footer Totals
        const table = tbody.closest('table');
        if (table) {
            const footer = table.querySelector('tfoot');
            if (footer) {
                footer.innerHTML = `
                <tr class="total-row">
                    <td><strong>Total</strong></td>
                    <td>${totalOpen}</td>
                    <td>${totalPend}</td>
                    <td>${totalDisp}</td>
                    <td></td>
                </tr>`;
            }
        }
    }

    // --- Chart.js Rendering ---
    function renderOverviewChart(canvasId, data, labelKey) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return; 

        const labels = data ? data.map(d => d[labelKey]) : [];
        const pendingData = data ? data.map(d => d['Pending'] || 0) : [];
        const dispatchData = data ? data.map(d => d['Dispatch'] || 0) : [];

        if (window[canvasId + 'Instance']) {
            window[canvasId + 'Instance'].destroy();
        }

        window[canvasId + 'Instance'] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Pending',
                        data: pendingData,
                        backgroundColor: '#fbbf24'
                    },
                    {
                        label: 'Dispatch',
                        data: dispatchData,
                        backgroundColor: '#4ade80'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: { stacked: true },
                    y: { stacked: true, beginAtZero: true }
                }
            }
        });
    }

    // ---------------------------------------------------------
    // 4. COMMON EVENTS (Pagination, Modal, Drag)
    // ---------------------------------------------------------

    const planPagination = document.getElementById('plan-pagination');
    if (planPagination) {
        planPagination.addEventListener('click', function(e) {
            e.preventDefault();
            const link = e.target.closest('.page-link');
            if (!link || link.parentElement.classList.contains('disabled')) return;
            currentPage = parseInt(link.dataset.page);
            refreshDashboardData();
        });
    }

    if (failureHeader && planTableBody) {
        failureHeader.addEventListener('click', function() {
            planTableBody.classList.toggle('show-failures');
            this.classList.toggle('active');
            this.innerHTML = this.classList.contains('active') ? 'Failure ▴' : 'Failure ▾';
        });
    }

    // --- Failure Reason Modal Logic ---
    const reasonModal = document.getElementById('failure-reason-modal');
    const reasonForm = document.getElementById('failure-reason-form');
    const reasonSelect = document.getElementById('reason-modal-select');
    const reasonModalItemCodeHidden = document.getElementById('reason-modal-item-code-hidden');
    const reasonModalFailedQtyHidden = document.getElementById('reason-modal-failed-qty-hidden');
    const reasonModalCloseBtn = reasonModal ? reasonModal.querySelector('.close-button') : null;
    const reasonModalClearBtn = document.getElementById('reason-modal-clear-btn');

    if (planTableBody) {
        planTableBody.addEventListener('click', function(event) {
            const button = event.target.closest('button.reason-btn');
            if (button) {
                document.getElementById('reason-modal-item-code').textContent = button.dataset.itemCode;
                document.getElementById('reason-modal-failed-qty').textContent = button.dataset.failedQty;
                reasonModalItemCodeHidden.value = button.dataset.itemCode;
                reasonModalFailedQtyHidden.value = button.dataset.failedQty;
                reasonSelect.value = button.dataset.savedReason.replace(/&quot;/g, '"');
                if(reasonModal) reasonModal.style.display = 'block';
            }
        });
    }

    if (reasonForm) {
        reasonForm.addEventListener('submit', function(event) { event.preventDefault(); saveReason(reasonSelect.value); });
    }
    if (reasonModalClearBtn) {
        reasonModalClearBtn.addEventListener('click', function() { saveReason(""); });
    }

    function saveReason(reasonValue) {
        const postData = {
            item_code: reasonModalItemCodeHidden.value,
            failed_quantity: parseInt(reasonModalFailedQtyHidden.value),
            reason: reasonValue
        };
        fetch(urls.logFailureReason, {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(postData)
        }).then(r => r.json()).then(data => {
            if (data.status === 'success') {
                if(reasonModal) reasonModal.style.display = 'none';
                refreshDashboardData();
            } else { alert(data.message); }
        });
    }

    if(reasonModalCloseBtn) reasonModalCloseBtn.onclick = () => reasonModal.style.display = 'none';
    window.onclick = (e) => { if(e.target == reasonModal) reasonModal.style.display = 'none'; };

    // --- Drag & Drop Logic ---
    let draggedItemCode = null;
    function attachDragEvents() {
        document.querySelectorAll('.draggable-item').forEach(draggable => {
            draggable.addEventListener('dragstart', (e) => {
                draggedItemCode = draggable.getAttribute('data-item-code');
                draggable.classList.add('dragging');
                e.dataTransfer.effectAllowed = "move";
            });
            draggable.addEventListener('dragend', () => {
                draggable.classList.remove('dragging');
                draggedItemCode = null;
            });
        });
    }

    function attachDropEvents() {
        document.querySelectorAll('.drop-target').forEach(target => {
            target.addEventListener('dragover', (e) => { e.preventDefault(); e.currentTarget.classList.add('drag-over'); });
            target.addEventListener('dragleave', (e) => { e.currentTarget.classList.remove('drag-over'); });
            target.addEventListener('drop', handleDrop);
        });
    }

    function handleDrop(e) {
        e.preventDefault();
        e.currentTarget.classList.remove('drag-over');
        if (!draggedItemCode) return;
        let toStage = e.currentTarget.getAttribute('data-stage');
        if (e.currentTarget.classList.contains('monthly-dispatch-card')) toStage = 'Dispatch';
        
        const qty = prompt(`Moving ${draggedItemCode} to ${toStage}. Quantity:`, "1");
        if (qty && parseInt(qty) > 0) {
            const url = (toStage === 'Dispatch') ? urls.dispatchItem : urls.moveItem;
            const formData = new FormData();
            formData.append('item_code', draggedItemCode);
            formData.append('quantity', qty);
            if(toStage !== 'Dispatch') {
                formData.append('from_stage', 'FG');
                formData.append('to_stage', toStage);
            }
            fetch(url, { method: 'POST', body: formData }).then(() => refreshDashboardData());
        }
    }

    attachDropEvents();

    const fullscreenToggle = document.getElementById('fullscreen-toggle');
    if (fullscreenToggle) {
        const enterIcon = document.getElementById('fullscreen-enter-icon');
        const exitIcon = document.getElementById('fullscreen-exit-icon');
        fullscreenToggle.addEventListener('click', () => {
            if (!document.fullscreenElement) document.documentElement.requestFullscreen().catch(err => alert(err));
            else if (document.exitFullscreen) document.exitFullscreen();
        });
        document.addEventListener('fullscreenchange', () => {
            if (document.fullscreenElement) { enterIcon.style.display = 'none'; exitIcon.style.display = 'block'; }
            else { enterIcon.style.display = 'block'; exitIcon.style.display = 'none'; }
        });
    }

    // Init & Poll (Set to 10 seconds to avoid choking DB)
    refreshDashboardData();
    if (planTableBody) setInterval(refreshDashboardData, 10000); 
});