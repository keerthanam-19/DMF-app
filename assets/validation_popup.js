// assets/validation_popup.js
// Native client-side interactivity for Enterprise DQ Validation Notifications

(function() {
    function initValidationPopup() {
        const popup = document.getElementById('validation-popup-container');
        if (!popup) {
            setTimeout(initValidationPopup, 200);
            return;
        }

        // 1. Observe class changes for auto-dismiss on success
        let dismissTimer = null;
        const observer = new MutationObserver(function() {
            if (popup.classList.contains('state-success')) {
                if (dismissTimer) clearTimeout(dismissTimer);
                dismissTimer = setTimeout(function() {
                    if (popup.classList.contains('state-success')) {
                        popup.style.opacity = '0';
                        popup.style.transform = 'translateX(40px)';
                        setTimeout(function() {
                            popup.style.display = 'none';
                            popup.style.opacity = '1';
                            popup.style.transform = 'none';
                        }, 400);
                    }
                }, 8500);
            }
        });
        observer.observe(popup, { attributes: true, attributeFilter: ['class'] });

        // 2. Global click interception for instant 0ms "VALIDATION IN PROGRESS" popup
        document.addEventListener('click', function(e) {
            const target = e.target;
            if (!target) return;

            // Check if user clicked any validation trigger
            const runBtn = target.closest('#run-all-checks-btn, #run-all-checks-btn-quick, [id*="dmf-rerun-btn"]');
            if (runBtn) {
                const badge = document.getElementById('popup-badge-text');
                const icon = document.getElementById('popup-icon-container');
                const title = document.getElementById('popup-title');
                const msg = document.getElementById('popup-message');
                const progress = document.getElementById('popup-progress-track');

                // Instant UI state transition
                popup.style.display = 'block';
                popup.style.opacity = '1';
                popup.style.transform = 'none';
                popup.className = 'dq-validation-popup state-in-progress';

                if (badge) badge.innerText = 'VALIDATION IN PROGRESS';
                if (icon) icon.innerText = '⚙';
                if (title) title.innerText = 'Validation In Progress...';
                if (msg) msg.innerText = 'Running Data Quality checks against Databricks Lakehouse tables. Please wait...';
                if (progress) progress.style.display = 'block';

                // Also immediately update inline feedback next to the button
                const inlineFeedback = document.getElementById('run-validation-feedback');
                if (inlineFeedback && (runBtn.id === 'run-all-checks-btn' || runBtn.id === 'run-all-checks-btn-quick')) {
                    inlineFeedback.innerHTML = '<div style="color: #0284c7; font-weight: 700; font-size: 12px; font-family: JetBrains Mono; padding: 8px 14px; background-color: #eff6ff; border-radius: 6px; border: 1px solid #bfdbfe; display: inline-block;">⚙ Running Data Quality checks across Databricks tables... Please wait...</div>';
                }
            }

            // Check if user clicked Dismiss (X)
            if (target.closest('#popup-close-btn')) {
                popup.style.display = 'none';
            }
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initValidationPopup);
    } else {
        initValidationPopup();
    }
})();
