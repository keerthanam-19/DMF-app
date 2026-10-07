
(function() {
    var clientside = window.dash_clientside = window.dash_clientside || {};
    var ns = clientside["_dashprivate_clientside_funcs"] = clientside["_dashprivate_clientside_funcs"] || {};
    ns["2f7ffc4cd9c91476c179a08f922969cda7f6b5099655dd8c17c62421f81829c2"] = 
    function(runAllClicks, headerRunClicks, runQuickClicks, rerunClicks, closeClicks, serverData) {
        var ctx = window.dash_clientside ? window.dash_clientside.callback_context : null;
        if (!ctx || !ctx.triggered || ctx.triggered.length === 0) {
            return [
                window.dash_clientside.no_update,
                window.dash_clientside.no_update,
                window.dash_clientside.no_update,
                window.dash_clientside.no_update,
                window.dash_clientside.no_update,
                window.dash_clientside.no_update,
                window.dash_clientside.no_update
            ];
        }

        var trig = ctx.triggered[0];
        var trigProp = trig.prop_id || "";
        var trigVal = trig.value;

        // If triggered by a button that has no clicks or 0 clicks, ignore
        if (trigProp.indexOf("validation-server-result-store") === -1) {
            if (!trigVal || trigVal === 0) {
                return [
                    window.dash_clientside.no_update,
                    window.dash_clientside.no_update,
                    window.dash_clientside.no_update,
                    window.dash_clientside.no_update,
                    window.dash_clientside.no_update,
                    window.dash_clientside.no_update,
                    window.dash_clientside.no_update
                ];
            }
        }

        // 1. User clicked Dismiss / Close Button (X)
        if (trigProp.indexOf("popup-close-btn") !== -1) {
            return [
                {"display": "none"},
                window.dash_clientside.no_update,
                window.dash_clientside.no_update,
                window.dash_clientside.no_update,
                window.dash_clientside.no_update,
                window.dash_clientside.no_update,
                {"display": "none"}
            ];
        }

        // 2. Server Response Returned (Success or Incomplete / Error)
        if (trigProp.indexOf("validation-server-result-store") !== -1 && serverData) {
            var status = (serverData.status || "SUCCESS").toUpperCase();
            var isSuccess = (status === "SUCCESS");
            var className = isSuccess ? "dq-validation-popup state-success" : "dq-validation-popup state-error";
            var badgeText = isSuccess ? "VALIDATION SUCCESSFUL" : "VALIDATION INCOMPLETE / ERROR";
            var icon = isSuccess ? "✓" : "⚠";
            var title = serverData.title || (isSuccess ? "Validation Completed Successfully" : "Validation Incomplete / Error");
            var msg = serverData.message || (isSuccess ? "All checks executed and recorded." : "Validation process encountered errors.");

            // Auto-dismiss after 8 seconds on success
            if (isSuccess) {
                setTimeout(function() {
                    var el = document.getElementById("validation-popup-container");
                    if (el && el.className.indexOf("state-success") !== -1) {
                        el.style.opacity = '0';
                        el.style.transform = 'translateX(40px)';
                        setTimeout(function() {
                            el.style.display = 'none';
                            el.style.opacity = '1';
                            el.style.transform = 'none';
                        }, 400);
                    }
                }, 8000);
            }

            return [
                {"display": "block", "opacity": "1", "transform": "none"},
                className,
                badgeText,
                icon,
                title,
                msg,
                {"display": "none"}
            ];
        }

        // 3. Any Run Button Clicked -> INSTANT 0ms "VALIDATION IN PROGRESS" POPUP
        var isRunTrigger = false;
        if (trigProp.indexOf("run-all-checks-btn") !== -1 && runAllClicks) isRunTrigger = true;
        if (trigProp.indexOf("header-run-validation-btn") !== -1 && headerRunClicks) isRunTrigger = true;
        if (trigProp.indexOf("run-all-checks-btn-quick") !== -1 && runQuickClicks) isRunTrigger = true;
        if (trigProp.indexOf("dmf-rerun-btn") !== -1) {
            if (Array.isArray(rerunClicks) && rerunClicks.some(function(c) { return c && c > 0; })) {
                isRunTrigger = true;
            } else if (trigVal && trigVal > 0) {
                isRunTrigger = true;
            }
        }

        if (isRunTrigger) {
            return [
                {"display": "block", "opacity": "1", "transform": "none"},
                "dq-validation-popup state-in-progress",
                "VALIDATION IN PROGRESS",
                "⚙",
                "Validation In Progress...",
                "Running Data Quality checks against Databricks Lakehouse tables. Please wait...",
                {"display": "block"}
            ];
        }

        return [
            window.dash_clientside.no_update,
            window.dash_clientside.no_update,
            window.dash_clientside.no_update,
            window.dash_clientside.no_update,
            window.dash_clientside.no_update,
            window.dash_clientside.no_update,
            window.dash_clientside.no_update
        ];
    }
    ;
})();
