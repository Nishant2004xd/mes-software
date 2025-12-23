document.addEventListener('DOMContentLoaded', function() {
    
    // --- 1. View Toggle Logic (Tracker vs Manage) ---
    const toggleBtn = document.getElementById('toggle-view-btn');
    const toggleBtnText = document.getElementById('toggle-btn-text'); // Target the span
    const attendanceView = document.getElementById('attendance-view');
    const manageView = document.getElementById('manage-view');
    const pageTitle = document.getElementById('page-title');

    // Check URL param on load
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.get('view') === 'manage') {
        showManageView();
    } else {
        showTrackerView();
    }

    if (toggleBtn) {
        toggleBtn.addEventListener('click', function() {
            if (attendanceView.style.display === 'none') {
                showTrackerView();
                window.history.pushState({}, '', attendanceConfig.urls.attendanceBase);
            } else {
                showManageView();
            }
        });
    }

    function showTrackerView() {
        if(attendanceView) attendanceView.style.display = 'flex'; // Changed to flex for sticky footer
        if(manageView) manageView.style.display = 'none';
        if(toggleBtnText) toggleBtnText.textContent = 'Manage Employees';
        if(pageTitle) pageTitle.textContent = 'Worker Attendance';
    }

    function showManageView() {
        if(attendanceView) attendanceView.style.display = 'none';
        if(manageView) manageView.style.display = 'flex'; // Changed to flex for sticky footer
        if(toggleBtnText) toggleBtnText.textContent = 'Back to Tracker';
        if(pageTitle) pageTitle.textContent = 'Manage Employees';
    }

    // --- 2. Auto-Saving Attendance Logic ---
    const attendanceTable = document.querySelector('.attendance-table'); // Updated selector
    if (attendanceTable) {
        attendanceTable.addEventListener('change', function(event) {
            const selectElement = event.target;
            if (selectElement.tagName === 'SELECT' && selectElement.name.startsWith('status-')) {
                handleAttendanceChange(selectElement);
            }
        });
    }

    function handleAttendanceChange(selectElement) {
        const nameParts = selectElement.name.split('-');
        const postData = {
            employee_code: nameParts[1],
            date: nameParts.slice(2).join('-'), 
            status: selectElement.value
        };

        // Visual feedback (Yellow)
        selectElement.style.transition = 'background-color 0.2s';
        selectElement.style.backgroundColor = '#fef9c3'; 

        fetch(attendanceConfig.urls.markSingle, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(postData)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Success feedback (Green)
                selectElement.style.backgroundColor = '#dcfce7'; 
                selectElement.classList.remove('present', 'absent');
                
                if (postData.status.startsWith('Present')) {
                    selectElement.classList.add('present');
                } else if (postData.status === 'Absent') {
                    selectElement.classList.add('absent');
                }

                setTimeout(() => { selectElement.style.backgroundColor = ''; }, 1200);

                // Reload if status changed significantly (updates roster)
                if (postData.status.startsWith('Present') || postData.status === 'Absent' || postData.status === '') {
                     window.location.reload();
                }
            } else {
                alert('Error: ' + data.message);
                selectElement.style.backgroundColor = '#fee2e2'; // Red
            }
        })
        .catch(error => {
            console.error('Error:', error);
            selectElement.style.backgroundColor = '#fee2e2'; 
            alert('Network error saving attendance.');
        });
    }

    // --- 3. Modal Logic (Add Employee) ---
    const addModal = document.getElementById('add-employee-modal');
    const showAddModalBtn = document.getElementById('show-add-modal-btn');
    const addCloseBtn = addModal ? addModal.querySelector('.close-button') : null;

    if (showAddModalBtn && addModal) {
        showAddModalBtn.onclick = function() {
            document.getElementById('add-employee-form').reset();
            addModal.style.display = 'block';
        }
    }
    if (addCloseBtn) addCloseBtn.onclick = () => addModal.style.display = 'none';

    // --- 4. Modal Logic (Edit Employee) ---
    const editModal = document.getElementById('edit-employee-modal');
    const editCloseBtn = editModal ? editModal.querySelector('.close-button') : null;
    
    if (editCloseBtn) editCloseBtn.onclick = () => editModal.style.display = 'none';

    // Close on outside click
    window.onclick = function(event) {
        if (event.target == addModal) addModal.style.display = 'none';
        if (event.target == editModal) editModal.style.display = 'none';
    }
});

// --- 5. Global Function for "Edit" Button ---
window.populateEditModal = function(btn) {
    const editModal = document.getElementById('edit-employee-modal');
    if (!editModal) return;

    // Get data
    const ecode = btn.dataset.ecode;
    const title = btn.dataset.title;
    const name = btn.dataset.name;
    const company = btn.dataset.company;
    const doj = btn.dataset.doj;
    const stage = btn.dataset.stage;

    // Populate fields
    document.getElementById('edit_original_e_code').value = ecode;
    document.getElementById('edit_e_code').value = ecode;
    document.getElementById('edit_name').value = name;
    document.getElementById('edit_company').value = company;
    document.getElementById('edit_doj').value = doj;
    
    const titleSelect = document.getElementById('edit_title');
    if(titleSelect) titleSelect.value = title;

    const stageSelect = document.getElementById('edit_stage');
    if(stageSelect) stageSelect.value = stage;

    document.getElementById('edit-modal-title').textContent = `Edit Employee: ${title} ${name}`;
    
    editModal.style.display = 'block';
};