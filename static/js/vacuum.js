let currentVacuumJobId = null;
let vacuumPollTimer = null;

function showVacuumMessage(message, type = "info") {
    const box = document.getElementById("vacuumStatusBox");

    if (!box) {
        return;
    }

    box.className = `alert alert-${type}`;
    box.textContent = message;
}

function setVacuumButtonRunning(isRunning) {
    const btn = document.getElementById("vacuumActionButton");

    if (!btn) {
        return;
    }

    if (isRunning) {
        btn.className = "btn btn-danger w-100 mb-3";
        btn.textContent = "Остановить задачу";
    } else {
        btn.className = "btn btn-primary w-100 mb-3";
        btn.textContent = "Запустить Vacuum / Analyze";
    }
}

function getVacuumSelectedTables() {
    let selected = [];

    if (typeof getSelectedTables === "function") {
        selected = getSelectedTables();
    }

    return selected;
}

async function handleVacuumActionButton() {
    if (currentVacuumJobId) {
        await stopVacuumJob();
        return;
    }

    await startVacuumJob();
}

async function startVacuumJob() {
    const connectionSelect = document.getElementById("connection_id");
    const actionSelect = document.getElementById("vacuumAction");

    if (!connectionSelect) {
        showVacuumMessage("connection_id element not found", "danger");
        return;
    }

    const connectionId = connectionSelect.value;
    const action = actionSelect ? actionSelect.value : "VACUUM_ANALYZE";
    const tables = getVacuumSelectedTables();

    if (!connectionId) {
        showVacuumMessage("Выбери connection", "warning");
        return;
    }

    if (!tables || tables.length === 0) {
        showVacuumMessage("Выбери хотя бы одну таблицу", "warning");
        return;
    }

    showVacuumMessage(`Starting ${action}...`, "info");

    const response = await fetch("/api/vacuum/start", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            connection_id: connectionId,
            action: action,
            tables: tables,
        }),
    });

    const data = await response.json();

    if (!response.ok || !data.ok) {
        showVacuumMessage(data.message || "Failed to start job", "danger");
        return;
    }

    currentVacuumJobId = data.job_id;

    showVacuumMessage(
        `Job #${currentVacuumJobId} started. Action: ${data.action}. Tables: ${data.total_items}`,
        "success"
    );

    setVacuumButtonRunning(true);
    startVacuumPolling();
}

async function stopVacuumJob() {
    if (!currentVacuumJobId) {
        return;
    }

    showVacuumMessage(`Stop requested for job #${currentVacuumJobId}...`, "warning");

    const response = await fetch(`/api/jobs/${currentVacuumJobId}/stop`, {
        method: "POST",
    });

    const data = await response.json();

    if (!response.ok || !data.ok) {
        showVacuumMessage(data.message || "Failed to stop job", "danger");
        return;
    }

    showVacuumMessage(`Job #${currentVacuumJobId} stopping...`, "warning");
}

function startVacuumPolling() {
    if (vacuumPollTimer) {
        clearInterval(vacuumPollTimer);
    }

    loadVacuumStatus();

    vacuumPollTimer = setInterval(loadVacuumStatus, 2000);
}

function stopVacuumPolling() {
    if (vacuumPollTimer) {
        clearInterval(vacuumPollTimer);
        vacuumPollTimer = null;
    }
}

async function loadVacuumStatus() {
    if (!currentVacuumJobId) {
        return;
    }

    const response = await fetch(`/api/jobs/${currentVacuumJobId}/status`);
    const data = await response.json();

    if (!response.ok || !data.ok) {
        showVacuumMessage(data.message || "Failed to load status", "danger");
        stopVacuumPolling();
        setVacuumButtonRunning(false);
        currentVacuumJobId = null;
        return;
    }

    renderVacuumStatus(data);

    const jobStatus = String(data.job.status || "").toLowerCase();

    if (["done", "failed", "cancelled", "interrupted"].includes(jobStatus)) {
        stopVacuumPolling();
        setVacuumButtonRunning(false);

        if (jobStatus === "done") {
            showVacuumMessage(`Last job #${data.job.id} done.`, "success");
        } else if (jobStatus === "failed") {
            showVacuumMessage(
                `Last job #${data.job.id} failed: ${data.job.error_message || ""}`,
                "danger"
            );
        } else if (jobStatus === "cancelled") {
            showVacuumMessage(`Last job #${data.job.id} cancelled.`, "warning");
        } else {
            showVacuumMessage(`Last job #${data.job.id} interrupted.`, "warning");
        }

        currentVacuumJobId = null;
    }
}

function renderVacuumStatus(data) {
    const summary = data.summary || {};
    const items = data.items || [];

    const percent = Number(summary.percent || 0);

    const progressPercent = document.getElementById("vacuumProgressPercent");
    const progressBar = document.getElementById("vacuumProgressBar");

    if (progressPercent) {
        progressPercent.textContent = `${percent}%`;
    }

    if (progressBar) {
        progressBar.style.width = `${percent}%`;
        progressBar.textContent = `${percent}%`;
    }

    setText("vacuumTotal", summary.total || 0);
    setText("vacuumDone", summary.done || 0);
    setText("vacuumRunning", summary.running || 0);
    setText("vacuumFailed", summary.failed || 0);
    setText("vacuumSkipped", summary.skipped || 0);

    renderVacuumItems(items);
}

function setText(id, value) {
    const el = document.getElementById(id);

    if (el) {
        el.textContent = value;
    }
}

function renderVacuumItems(items) {
    const body = document.getElementById("vacuumItemsBody");

    if (!body) {
        return;
    }

    if (!items || items.length === 0) {
        body.innerHTML = `
            <tr>
                <td colspan="6" class="text-muted">Пока нет данных.</td>
            </tr>
        `;
        return;
    }

    body.innerHTML = items.map(item => {
        const status = String(item.status || "").toLowerCase();

        let badgeClass = "bg-secondary";

        if (status === "done") {
            badgeClass = "bg-success";
        } else if (status === "running") {
            badgeClass = "bg-primary";
        } else if (status === "failed") {
            badgeClass = "bg-danger";
        } else if (status === "skipped") {
            badgeClass = "bg-warning text-dark";
        }

        return `
            <tr>
                <td>${escapeHtml(item.schema_name || "")}</td>
                <td>${escapeHtml(item.table_name || "")}</td>
                <td>${escapeHtml(item.action || "")}</td>
                <td>
                    <span class="badge ${badgeClass}">
                        ${escapeHtml(item.status || "")}
                    </span>
                </td>
                <td>${escapeHtml(item.duration_seconds || "")}</td>
                <td class="text-danger small">${escapeHtml(item.error_message || "")}</td>
            </tr>
        `;
    }).join("");
}

function escapeHtml(value) {
    return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

window.handleVacuumActionButton = handleVacuumActionButton;