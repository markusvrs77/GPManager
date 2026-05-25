function getDashboardConnectionId() {
    const el =
        document.getElementById("connectionId") ||
        document.getElementById("dashboardConnectionId") ||
        document.getElementById("connection_id");

    if (!el) {
        return null;
    }

    return el.value;
}

function sessionLimitBadge(value) {
    const percent = Number(value || 0);

    if (percent >= 90) {
        return `<span class="badge bg-danger">${percent}%</span>`;
    }

    if (percent >= 75) {
        return `<span class="badge bg-warning text-dark">${percent}%</span>`;
    }

    return `<span class="badge bg-success">${percent}%</span>`;
}

async function loadSessionLimitsStats() {
    const connectionId = getDashboardConnectionId();

    const messageBox = document.getElementById("sessionLimitsMessage");
    const tbody = document.getElementById("sessionLimitsTableBody");

    if (!connectionId) {
        messageBox.className = "alert alert-warning mb-3";
        messageBox.textContent = "Выбери connection на Dashboard.";
        return;
    }

    messageBox.className = "alert alert-info mb-3";
    messageBox.textContent = "Загружаю статистику сессий...";

    tbody.innerHTML = `
        <tr>
            <td colspan="13" class="text-muted">Loading...</td>
        </tr>
    `;

    try {
        const response = await fetch(`/api/dashboard/session-limits?connection_id=${encodeURIComponent(connectionId)}`);
        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.message || "Failed to load session limits");
        }

        document.getElementById("slNodes").textContent = data.summary.nodes;
        document.getElementById("slTotal").textContent = data.summary.total_sessions;
        document.getElementById("slActive").textContent = data.summary.active_sessions;
        document.getElementById("slIdle").textContent = data.summary.idle_sessions;
        document.getElementById("slIdleTxn").textContent = data.summary.idle_in_transaction_sessions;

        if (!data.rows || data.rows.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="13" class="text-muted">Нет данных.</td>
                </tr>
            `;
        } else {
            tbody.innerHTML = data.rows.map(row => {
                const nodeClass =
                    row.node_type === "MASTER/COORDINATOR"
                        ? "table-primary"
                        : "";

                return `
                    <tr class="${nodeClass}">
                        <td>${row.node_type || ""}</td>
                        <td>${row.segment_id}</td>
                        <td>${row.hostname || ""}</td>
                        <td>${row.address || ""}</td>
                        <td>${row.port || ""}</td>
                        <td><strong>${row.total_sessions}</strong></td>
                        <td class="text-primary fw-bold">${row.active_sessions}</td>
                        <td>${row.idle_sessions}</td>
                        <td class="text-danger fw-bold">${row.idle_in_transaction_sessions}</td>
                        <td>${row.max_connections}</td>
                        <td>${row.superuser_reserved_connections}</td>
                        <td>${row.free_connections}</td>
                        <td>${sessionLimitBadge(row.used_percent)}</td>
                    </tr>
                `;
            }).join("");
        }

        messageBox.className = "alert alert-success mb-3";
        messageBox.textContent = "Session limits loaded.";

    } catch (err) {
        messageBox.className = "alert alert-danger mb-3";
        messageBox.textContent = err.message;

        tbody.innerHTML = `
            <tr>
                <td colspan="13" class="text-danger">${err.message}</td>
            </tr>
        `;
    }
}

document.addEventListener("DOMContentLoaded", function () {
    const connectionId = getDashboardConnectionId();

    if (connectionId) {
        loadSessionLimitsStats();
    }
});