/* GPManager shared UI helpers — accessible toast + confirm dialog.
   Framework-free. Motion gated through window.gpMotion (reduced-motion aware). */
(function () {
    "use strict";

    /* ---------- Toast ---------- */
    var toastHost = null;

    function ensureToastHost() {
        if (toastHost) { return toastHost; }
        toastHost = document.createElement("div");
        toastHost.className = "gp-toast-host";
        toastHost.setAttribute("aria-live", "polite");
        toastHost.setAttribute("aria-atomic", "false");
        document.body.appendChild(toastHost);
        return toastHost;
    }

    // gpToast(message, type?, opts?) — type: info|success|warning|danger
    function gpToast(message, type, opts) {
        opts = opts || {};
        var host = ensureToastHost();
        var t = document.createElement("div");
        t.className = "gp-toast gp-toast-" + (type || "info");
        t.setAttribute("role", type === "danger" ? "alert" : "status");
        t.textContent = message == null ? "" : String(message);
        host.appendChild(t);

        if (window.gpMotion) { window.gpMotion.enter(t, { y: 0, dur: 200 }); }

        var ttl = opts.ttl == null ? 4000 : opts.ttl;
        if (ttl > 0) {
            setTimeout(function () { dismiss(t); }, ttl);
        }
        t.addEventListener("click", function () { dismiss(t); });
        return t;
    }

    function dismiss(t) {
        if (!t || t.dataset.leaving) { return; }
        t.dataset.leaving = "1";
        var done = function () { if (t.parentNode) { t.parentNode.removeChild(t); } };
        if (t.animate && !(window.gpMotion && window.gpMotion.reduced)) {
            t.animate(
                [{ opacity: 1 }, { opacity: 0, transform: "translateX(16px)" }],
                { duration: 150, easing: "ease-in", fill: "both" }
            ).onfinish = done;
        } else {
            done();
        }
    }

    /* ---------- Confirm dialog ---------- */
    // gpConfirm(message, opts?) -> Promise<boolean>
    // opts: { title, confirmText, cancelText, danger }
    function gpConfirm(message, opts) {
        opts = opts || {};
        return new Promise(function (resolve) {
            var lastFocus = document.activeElement;

            var overlay = document.createElement("div");
            overlay.className = "gp-dialog-overlay";

            var panel = document.createElement("div");
            panel.className = "gp-dialog" + (opts.danger ? " gp-dialog-danger" : "");
            panel.setAttribute("role", "dialog");
            panel.setAttribute("aria-modal", "true");

            var titleId = "gpdlg-title-" + Date.now();
            var title = document.createElement("div");
            title.className = "gp-dialog-title";
            title.id = titleId;
            title.textContent = opts.title || "Подтвердите действие";
            panel.setAttribute("aria-labelledby", titleId);

            var body = document.createElement("div");
            body.className = "gp-dialog-body";
            body.textContent = message == null ? "" : String(message);

            var actions = document.createElement("div");
            actions.className = "gp-dialog-actions";

            var cancelBtn = document.createElement("button");
            cancelBtn.type = "button";
            cancelBtn.className = "btn btn-secondary";
            cancelBtn.textContent = opts.cancelText || "Отмена";

            var okBtn = document.createElement("button");
            okBtn.type = "button";
            okBtn.className = "btn " + (opts.danger ? "btn-danger" : "btn-primary");
            okBtn.textContent = opts.confirmText || "OK";

            actions.appendChild(cancelBtn);
            actions.appendChild(okBtn);
            panel.appendChild(title);
            panel.appendChild(body);
            panel.appendChild(actions);
            overlay.appendChild(panel);
            document.body.appendChild(overlay);

            if (window.gpMotion) { window.gpMotion.enter(panel, { y: 12, dur: 200 }); }

            function close(result) {
                document.removeEventListener("keydown", onKey, true);
                if (overlay.parentNode) { overlay.parentNode.removeChild(overlay); }
                if (lastFocus && lastFocus.focus) { lastFocus.focus(); }
                resolve(result);
            }

            function onKey(e) {
                if (e.key === "Escape") { e.preventDefault(); close(false); return; }
                if (e.key === "Tab") {
                    // simple focus trap between the two buttons
                    var focusables = [cancelBtn, okBtn];
                    var idx = focusables.indexOf(document.activeElement);
                    e.preventDefault();
                    var next = e.shiftKey ? idx - 1 : idx + 1;
                    if (next < 0) { next = focusables.length - 1; }
                    if (next >= focusables.length) { next = 0; }
                    focusables[next].focus();
                }
            }

            cancelBtn.addEventListener("click", function () { close(false); });
            okBtn.addEventListener("click", function () { close(true); });
            overlay.addEventListener("click", function (e) {
                if (e.target === overlay) { close(false); }
            });
            document.addEventListener("keydown", onKey, true);

            // focus the safe default (cancel for danger, else confirm)
            (opts.danger ? cancelBtn : okBtn).focus();
        });
    }

    /* ---------- Delegated confirm-on-submit ---------- */
    // Any <form data-gp-confirm="message"> is gated through gpConfirm().
    var confirmedForm = null;
    document.addEventListener("submit", function (e) {
        var form = e.target;
        if (!form || !form.matches || !form.matches("form[data-gp-confirm]")) { return; }
        if (form === confirmedForm) { confirmedForm = null; return; } // already confirmed
        e.preventDefault();
        gpConfirm(form.getAttribute("data-gp-confirm"), {
            danger: true,
            confirmText: form.getAttribute("data-gp-confirm-ok") || "Удалить"
        }).then(function (ok) {
            if (ok) { confirmedForm = form; form.submit(); }
        });
    }, true);

    window.gpToast = gpToast;
    window.gpConfirm = gpConfirm;
})();
