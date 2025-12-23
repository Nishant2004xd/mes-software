document.addEventListener('DOMContentLoaded', function() {
    
    // --- 1. Safely Load Stages from Python Config ---
    const allStages = window.itemsConfig ? window.itemsConfig.stages : [];

    // --- 2. Modal Logic ---
    const addModal = document.getElementById('add-item-modal');
    const editModal = document.getElementById('edit-item-modal');
    const wipModal = document.getElementById('wip-modal');
    const addBtn = document.getElementById('show-add-modal-btn');

    // Safe Event Listeners
    if (addBtn && addModal) {
        addBtn.onclick = () => addModal.style.display = "block";
    }

    document.querySelectorAll('.close-button').forEach(btn => {
        btn.onclick = function() {
            const modal = this.closest('.modal');
            if (modal) modal.style.display = "none";
        }
    });

    window.onclick = (event) => {
        if (addModal && event.target == addModal) addModal.style.display = "none";
        if (editModal && event.target == editModal) editModal.style.display = "none";
        if (wipModal && event.target == wipModal) wipModal.style.display = "none";
    }

    // --- 3. Edit Info Modal (Global Function) ---
    // Attached to window so onclick="..." in HTML can find it
    window.openEditModal = function(btn) {
        if (!editModal) return;

        // Set the original item code (hidden field)
        const hiddenInput = document.getElementById('edit_original_code');
        if (hiddenInput) hiddenInput.value = btn.dataset.code;

        // Helper function to safely set values
        const setValue = (id, val) => {
            const el = document.getElementById(id);
            if (el) el.value = val || '';
        };

        setValue('edit_code', btn.dataset.code);
        setValue('edit_desc', btn.dataset.desc);
        setValue('edit_vert', btn.dataset.vert);
        setValue('edit_cat', btn.dataset.cat);
        setValue('edit_type', btn.dataset.type);
        setValue('edit_model', btn.dataset.model);
        setValue('edit_monthly', btn.dataset.monthly);
        setValue('edit_daily', btn.dataset.daily);
        setValue('edit_maxinv', btn.dataset.maxinv);
        setValue('edit_rpl', btn.dataset.rpl);

        editModal.style.display = "block";
    }

    // --- 4. WIP Edit Modal (Global Function) ---
    window.openWipModal = function(btn) {
        if (!wipModal) return;

        // Set hidden item code
        const hiddenInput = document.getElementById('wip_original_code');
        if (hiddenInput) hiddenInput.value = btn.dataset.code;

        // Set Title text
        const subtitle = document.getElementById('wip-modal-subtitle');
        if (subtitle) subtitle.textContent = `Item: ${btn.dataset.code} - ${btn.dataset.desc}`;

        // Dynamic Stage Population
        allStages.forEach(stageName => {
            // Match the ID format used in HTML generation (spaces replaced by underscores)
            const stageKey = stageName.replace(/ /g, '_');
            const inputId = 'stage_input_' + stageKey;
            const el = document.getElementById(inputId);

            // Construct the data attribute name (e.g. data-stage-Long_seam)
            // Note: HTML data attributes are automatically lowercased by the browser
            // So data-stage-Long_seam becomes dataset.stageLong_seam (camelCase) or accessible via getAttribute
            const attrName = 'data-stage-' + stageKey;
            const val = btn.getAttribute(attrName);

            if (el) {
                el.value = val || 0;
            }
        });

        wipModal.style.display = "block";
    }
});