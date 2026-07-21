/* Schedules page (spec §9). Все значения в innerHTML — через escapeHtml. */

function escapeHtml(value) {
    return String(value == null ? "" : value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}

async function api(url, method, body) {
    const opts = { method: method || "GET", headers: {} };
    if (body !== undefined) {
        opts.headers["Content-Type"] = "application/json";
        opts.body = JSON.stringify(body);
    }
    const res = await fetch(url, opts);
    return res.json();
}

/* ---------- form helpers ---------- */

function onJobTypeChange() {
    const type = document.getElementById("schJobType").value;
    document.getElementById("schDateWindow").style.display =
        type === "gpcopy_date" ? "" : "none";
    if (type === "gpcopy_date") previewWindow();
}

function setCron(expr) {
    document.getElementById("schCron").value = expr;
    previewCron();
}

function parseEndpoint(value) {
    if (value.startsWith("expr:")) return { expr: value.slice(5) };
    if (value.startsWith("n_days_ago:")) {
        return { preset: "n_days_ago", n: parseInt(value.split(":")[1], 10) };
    }
    return { preset: value };
}

function currentDateWindow() {
    return {
        column: document.getElementById("dwColumn").value.trim(),
        from: parseEndpoint(document.getElementById("dwFrom").value),
        to: parseEndpoint(document.getElementById("dwTo").value),
    };
}

let cronTimer = null;

function previewCron() {
    clearTimeout(cronTimer);
    cronTimer = setTimeout(async () => {
        const expr = document.getElementById("schCron").value.trim();
        const el = document.getElementById("cronPreview");
        if (!expr) { el.textContent = "—"; return; }
        const data = await api("/api/schedules/preview", "POST", { cron_expr: expr });
        el.textContent = data.ok
            ? "След. запуски: " + data.next_runs.join("  ·  ")
            : "Некорректное cron-выражение";
    }, 300);
}

async function previewWindow() {
    const el = document.getElementById("dwPreview");
    const dw = currentDateWindow();
    const data = await api("/api/schedules/preview", "POST", { date_window: dw });
    el.textContent = data.ok
        ? "Окно на сегодня: [" + data.date_from + " .. " + data.date_to + ")"
        : "Ошибка: " + data.message;
}

function parseTables(text) {
    return text.split("\n")
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => {
            const dot = line.indexOf(".");
            if (dot < 1) return null;
            return { schema: line.slice(0, dot), table: line.slice(dot + 1) };
        })
        .filter(Boolean);
}

async function createSchedule() {
    const errEl = document.getElementById("schError");
    errEl.textContent = "";

    const jobType = document.getElementById("schJobType").value;
    const config = {
        connection_id: parseInt(document.getElementById("schConnection").value, 10),
        tables: parseTables(document.getElementById("schTables").value),
    };

    if (jobType === "gpcopy_date") {
        const dw = currentDateWindow();
        if (!dw.column) { errEl.textContent = "Укажи date column"; return; }
        config.date_window = dw;
        config.selected_tables = config.tables;
    }

    const data = await api("/api/schedules", "POST", {
        name: document.getElementById("schName").value.trim() || "schedule",
        job_type: jobType,
        cron_expr: document.getElementById("schCron").value.trim(),
        overlap_policy: document.getElementById("schOverlap").value,
        max_retries: parseInt(document.getElementById("schRetries").value, 10) || 0,
        retry_delay_seconds: parseInt(document.getElementById("schRetryDelay").value, 10) || 0,
        config: config,
    });

    if (!data.ok) { errEl.textContent = data.message || "Ошибка создания"; return; }

    document.getElementById("schName").value = "";
    loadSchedules();
}

/* ---------- list ---------- */

function statusBadge(status) {
    const map = { done: "success", failed: "danger", skipped: "secondary", running: "info" };
    const cls = map[status] || "secondary";
    return '<span class="badge bg-' + cls + '">' + escapeHtml(status || "—") + "</span>";
}

async function loadSchedules() {
    const body = document.getElementById("schedulesBody");
    const data = await api("/api/schedules");

    if (!data.ok || !data.schedules.length) {
        body.innerHTML = '<tr><td colspan="7" class="text-muted p-3">Пока нет расписаний.</td></tr>';
        return;
    }

    body.innerHTML = data.schedules.map((s) => {
        const id = parseInt(s.id, 10);
        return "<tr>" +
            "<td>" + escapeHtml(s.name) + "</td>" +
            "<td><code>" + escapeHtml(s.job_type) + "</code></td>" +
            "<td><code>" + escapeHtml(s.cron_expr) + "</code></td>" +
            "<td class='small'>" + escapeHtml(s.next_run_at || "—") + "</td>" +
            "<td>" + statusBadge(s.last_status) + "</td>" +
            "<td>" +
                '<div class="form-check form-switch m-0">' +
                '<input class="form-check-input" type="checkbox" ' +
                (s.enabled ? "checked" : "") +
                ' onchange="toggleSchedule(' + id + ')"></div>' +
            "</td>" +
            "<td class='text-end text-nowrap'>" +
                '<button class="btn btn-sm btn-outline-primary me-1" onclick="runNow(' + id + ')">Run</button>' +
                '<button class="btn btn-sm btn-outline-secondary me-1" onclick="showRuns(' + id + ', this)">History</button>' +
                '<button class="btn btn-sm btn-outline-danger" onclick="deleteSchedule(' + id + ')">✕</button>' +
            "</td>" +
        "</tr>";
    }).join("");
}

async function toggleSchedule(id) {
    await api("/api/schedules/" + id + "/toggle", "POST");
    loadSchedules();
}

async function runNow(id) {
    const data = await api("/api/schedules/" + id + "/run-now", "POST");
    if (data.ok && !data.started) {
        alert("Не запущено: " + (data.reason || "overlap"));
    }
    setTimeout(loadSchedules, 500);
}

async function deleteSchedule(id) {
    if (!confirm("Удалить расписание #" + id + "?")) return;
    await api("/api/schedules/" + id, "DELETE");
    document.getElementById("runsCard").style.display = "none";
    loadSchedules();
}

async function showRuns(id) {
    const data = await api("/api/schedules/" + id + "/runs");
    const card = document.getElementById("runsCard");
    const body = document.getElementById("runsBody");
    document.getElementById("runsTitle").textContent = "#" + id;
    card.style.display = "";

    if (!data.ok || !data.runs.length) {
        body.innerHTML = '<tr><td colspan="6" class="text-muted p-3">Запусков ещё не было.</td></tr>';
        return;
    }

    body.innerHTML = data.runs.map((r) =>
        "<tr>" +
        "<td class='small'>" + escapeHtml(r.fired_at || "—") + "</td>" +
        "<td class='small'>" + escapeHtml(r.run_date || "—") + "</td>" +
        "<td>" + (r.job_id ? "#" + parseInt(r.job_id, 10) : "—") + "</td>" +
        "<td>" + statusBadge(r.status) + "</td>" +
        "<td>" + parseInt(r.attempt_no || 0, 10) + "</td>" +
        "<td class='small text-muted'>" + escapeHtml((r.error || "").slice(0, 120)) + "</td>" +
        "</tr>"
    ).join("");
}

/* ---------- init ---------- */

document.addEventListener("DOMContentLoaded", () => {
    loadSchedules();
    previewCron();
    onJobTypeChange();
});
