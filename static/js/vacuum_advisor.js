/* Ассистент Vacuum/Analyze: рекомендации по статистике таблиц.
   Все значения в innerHTML — через advEsc. */

(function () {
    "use strict";

    var ADV = [];            // текущие рекомендации
    var ADV_CHECKED = {};    // idx -> bool

    function advEsc(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    }

    function fmtBytes(n) {
        n = Number(n) || 0;
        if (n >= 1e12) { return (n / 1e12).toFixed(1) + " ТБ"; }
        if (n >= 1e9) { return (n / 1e9).toFixed(1) + " ГБ"; }
        if (n >= 1e6) { return (n / 1e6).toFixed(1) + " МБ"; }
        if (n >= 1e3) { return (n / 1e3).toFixed(0) + " КБ"; }
        return n + " Б";
    }

    var ACTION_LABELS = {
        VACUUM: "VACUUM",
        VACUUM_FULL: "VACUUM FULL",
        ANALYZE: "ANALYZE",
        VACUUM_ANALYZE: "VACUUM ANALYZE",
        VACUUM_FREEZE: "VACUUM FREEZE",
    };

    function selectedCount() {
        var n = 0;
        ADV.forEach(function (_, i) { if (ADV_CHECKED[i]) { n += 1; } });
        return n;
    }

    function updateRunButton() {
        var btn = document.getElementById("advRun");
        if (!btn) { return; }
        var n = selectedCount();
        btn.disabled = n === 0;
        btn.textContent = "Запустить выбранные (" + n + ")";
    }

    function render() {
        var box = document.getElementById("advBody");

        if (!ADV.length) {
            box.innerHTML = '<div class="text-muted small">Рекомендаций нет — ' +
                "статистика таблиц в порядке.</div>";
            updateRunButton();
            return;
        }

        box.innerHTML = ADV.map(function (r, i) {
            return '<div class="adv-row sev-' + advEsc(r.severity) + '">' +
                '<input class="form-check-input adv-cb" type="checkbox" ' +
                'data-idx="' + i + '"' + (ADV_CHECKED[i] ? " checked" : "") + ">" +
                '<div class="adv-main">' +
                    '<div class="adv-top">' +
                        '<span class="adv-dot"></span>' +
                        '<span class="adv-tbl">' + advEsc(r.schema) + "." +
                            advEsc(r.table) + "</span>" +
                        '<span class="adv-act">' +
                            advEsc(ACTION_LABELS[r.action] || r.action) + "</span>" +
                        '<span class="adv-meta">' + fmtBytes(r.size_bytes) +
                            (r.n_dead_tup
                                ? " · мёртвых " + advEsc(r.n_dead_tup) +
                                  " (" + Math.round((r.dead_ratio || 0) * 100) + "%)"
                                : "") +
                        "</span>" +
                    "</div>" +
                    '<div class="adv-why">' + r.reasons.map(advEsc).join(" ") + "</div>" +
                    "<code>" + advEsc(r.command) + "</code>" +
                "</div>" +
            "</div>";
        }).join("");

        box.querySelectorAll(".adv-cb").forEach(function (cb) {
            cb.addEventListener("change", function () {
                ADV_CHECKED[parseInt(cb.getAttribute("data-idx"), 10)] = cb.checked;
                updateRunButton();
            });
        });

        updateRunButton();
    }

    window.advLoad = function () {
        var conn = document.getElementById("connection_id").value;
        var btn = document.getElementById("advLoadBtn");
        var box = document.getElementById("advBody");
        var sum = document.getElementById("advSummary");

        if (!conn) { return; }

        btn.disabled = true;
        btn.textContent = "Анализирую…";
        box.innerHTML = '<div class="text-muted small">Читаю статистику таблиц…</div>';
        sum.textContent = "";

        fetch("/api/vacuum/advisor?connection_id=" + encodeURIComponent(conn))
            .then(function (r) { return r.json(); })
            .then(function (d) {
                btn.disabled = false;
                btn.textContent = "Проанализировать";

                if (!d.ok) {
                    box.innerHTML = '<div class="text-danger small">' +
                        advEsc(d.message || "Ошибка") + "</div>";
                    return;
                }

                ADV = d.recommendations || [];
                ADV_CHECKED = {};
                ADV.forEach(function (_, i) { ADV_CHECKED[i] = true; });

                sum.textContent = "Просканировано таблиц: " + d.tables_scanned +
                    " · критичных: " + (d.counts ? d.counts.crit : 0) +
                    " · предупреждений: " + (d.counts ? d.counts.warn : 0) +
                    " · " + d.duration_seconds + " с";

                render();
            })
            .catch(function (e) {
                btn.disabled = false;
                btn.textContent = "Проанализировать";
                box.innerHTML = '<div class="text-danger small">' +
                    advEsc(String(e)) + "</div>";
            });
    };

    window.advRunSelected = function () {
        var conn = document.getElementById("connection_id").value;

        // одна задача: у каждой таблицы своя рекомендованная операция,
        // критичные — первыми (ADV уже отсортирован crit -> warn),
        // раннер исполняет строки по порядку
        var tables = [];

        ["crit", "warn"].forEach(function (sev) {
            ADV.forEach(function (r, i) {
                if (!ADV_CHECKED[i] || r.severity !== sev) { return; }
                tables.push({
                    schema: r.schema, table: r.table, action: r.action,
                });
            });
        });

        if (!tables.length) { return; }

        var btn = document.getElementById("advRun");
        btn.disabled = true;

        fetch("/api/vacuum/start", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                connection_id: conn,
                action: "VACUUM_ANALYZE",   // запас на таблицы без action
                workers: parseInt(
                    (document.getElementById("vacuumWorkers") || {}).value, 10
                ) || 1,
                tables: tables,
            }),
        }).then(function (r) { return r.json(); }).then(function (d) {
            btn.disabled = false;

            if (!d.ok) {
                if (typeof showVacuumMessage === "function") {
                    showVacuumMessage(d.message || "Ошибка запуска", "danger");
                }
                return;
            }

            if (typeof showVacuumMessage === "function") {
                showVacuumMessage(
                    "Ассистент запустил задачу #" + d.job_id +
                    ": " + tables.length + " таблиц, у каждой своя операция, " +
                    "критичные первыми.",
                    "success"
                );
            }

            currentVacuumJobId = d.job_id;
            if (typeof setVacuumButtonRunning === "function") {
                setVacuumButtonRunning(true);
            }
            if (typeof startVacuumPolling === "function") {
                startVacuumPolling();
            }
        }).catch(function (e) {
            btn.disabled = false;
            if (typeof showVacuumMessage === "function") {
                showVacuumMessage(String(e), "danger");
            }
        });
    };
})();
