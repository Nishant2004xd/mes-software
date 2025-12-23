document.addEventListener('DOMContentLoaded', function () {
    // --- 1. Flash Banner Logic ---
    const banners = document.querySelectorAll('.banner');
    banners.forEach((banner, index) => {
        setTimeout(() => { banner.classList.add('show'); }, 100);
        setTimeout(() => {
            banner.classList.remove('show');
            setTimeout(() => { banner.remove(); }, 600);
        }, 5000);
    });

    // --- 2. Sidebar Logic ---
    const sidebarToggle = document.getElementById('sidebar-toggle');
    const body = document.body;

    function toggleSidebar() {
        body.classList.toggle('sb-open');
    }

    if (sidebarToggle) {
        sidebarToggle.addEventListener('click', toggleSidebar);
    }

    // --- 3. Settings Modal Logic ---
    const settingsModal = document.getElementById('settings-modal');
    const settingsBtn = document.getElementById('settings-btn');
    const sidebarSettingsBtn = document.getElementById('sidebar-settings-btn');

    const openSettings = function(e) {
        e.preventDefault();
        settingsModal.style.display = 'block';
        if (body.classList.contains('sb-open')) {
            toggleSidebar(); // Close sidebar if open
        }
    };

    if (settingsModal) {
        const settingsCloseBtn = settingsModal.querySelector('.close-button');

        if (settingsBtn) settingsBtn.addEventListener('click', openSettings);
        if (sidebarSettingsBtn) sidebarSettingsBtn.addEventListener('click', openSettings);

        if (settingsCloseBtn) {
            settingsCloseBtn.onclick = function() {
                settingsModal.style.display = 'none';
            }
        }

        window.addEventListener('click', function(event) {
            if (event.target == settingsModal) {
                settingsModal.style.display = 'none';
            }
        });
    }
});

// --- 4. File Input Helper (Global) ---
function updateFileName(input) {
    if (input.files && input.files.length > 0) {
        const label = input.nextElementSibling; // The <label> after the <input>
        const nameSpan = label.querySelector('.file-name');
        if (nameSpan) {
            nameSpan.textContent = input.files[0].name;
            nameSpan.style.color = '#0f172a'; // Darker text for visibility
        }
    }
}