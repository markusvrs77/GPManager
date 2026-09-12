let currentVacuumJobId = null;   // активная задача (кнопка «Остановить»)
let vacuumViewJobId = null;      // какой запуск показан в деталях
let vacuumPollTimer = null;
let vacuumRunsTimer = null;

const VACUUM_ACTIVE = ["running", "queued", "stopping"];
const VACUUM_BAD = ["failed", "error", "cancelled", "interrupted"];

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
            workers: parseInt(
                (document.getElementById("vacuumWorkers") || {}).value, 10
            ) || 1,
            tables: tables,
        }),
    });

    const data = await response.json();

    if (!response.ok || !data.ok) {
        showVacuumMessage(data.message || "Failed to start job", "danger");
        return;
    }

    currentVacuumJobId = data.job_id;
    vacuumViewJobId = data.job_id;

    showVacuumMessage(
        `Запуск #${currentVacuumJobId}: ${data.action}, таблиц — ${data.total_items}`,
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

    // задачу мог запустить ассистент — он ставит только currentVacuumJobId
    if (currentVacuumJobId) {
        vacuumViewJobId = currentVacuumJobId;
    }

    loadVacuumStatus();
    loadVacuumRuns();

    vacuumPollTimer = setInterval(loadVacuumStatus, 2000);
}

function stopVacuumPolling() {
    if (vacuumPollTimer) {
        clearInterval(vacuumPollTimer);
        vacuumPollTimer = null;
    }
}

async function loadVacuumStatus() {
    const jobId = vacuumViewJobId || currentVacuumJobId;

    if (!jobId) {
        return;
    }

    const response = await fetch(`/api/jobs/${jobId}/status`);
    const data = await response.json();

    if (!response.ok || !data.ok) {
        showVacuumMessage(data.message || "Не удалось прочитать статус", "danger");
        stopVacuumPolling();
        setVacuumButtonRunning(false);
        currentVacuumJobId = null;
        return;
    }

    renderVacuumStatus(data);
    setVacuumViewLabel(data.job);

    const jobStatus = String(data.job.status || "").toLowerCase();

    if (VACUUM_ACTIVE.includes(jobStatus)) {
        return;
    }

    // завершилась именно та задача, которую мы вели
    if (jobId === currentVacuumJobId) {
        stopVacuumPolling();
        setVacuumButtonRunning(false);
        currentVacuumJobId = null;

        if (jobStatus === "done") {
            showVacuumMessage(`Запуск #${data.job.id} завершён.`, "success");
        } else if (jobStatus === "failed") {
            showVacuumMessage(
                `Запуск #${data.job.id} упал: ${data.job.error_message || ""}`,
                "danger"
            );
        } else if (jobStatus === "cancelled") {
            showVacuumMessage(`Запуск #${data.job.id} остановлен.`, "warning");
        } else {
            showVacuumMessage(`Запуск #${data.job.id} прерван.`, "warning");
        }

        loadVacuumRuns();
    } else if (!currentVacuumJobId) {
        // смотрим историю — опрашивать нечего
        stopVacuumPolling();
    }
}

function setVacuumViewLabel(job) {
    const el = document.getElementById("vacViewLabel");

    if (!el || !job) {
        return;
    }

    el.textContent = "показан запуск #" + job.id + " · " +
        String(job.status || "") +
        (job.started_at ? " · " + job.started_at : "");
}

/* ---------------- история запусков ---------------- */

function selectVacuumRun(jobId) {
    vacuumViewJobId = jobId;
    loadVacuumStatus();
    loadVacuumRuns();
}

async function loadVacuumRuns(adopt) {
    const box = document.getElementById("vacRuns");

    if (!box) {
        return;
    }

    let data;

    try {
        const response = await fetch(
            "/api/jobs/recent?types=vacuum_analyze&limit=12"
        );

        data = await response.json();
    } catch (e) {
        box.innerHTML =
            '<div class="text-danger small">Не удалось прочитать историю.</div>';
        return;
    }

    if (!data || !data.ok) {
        return;
    }

    const jobs = data.jobs || [];
    const active = jobs.find(j =>
        VACUUM_ACTIVE.includes(String(j.status || "").toLowerCase())
    );

    // после перезагрузки страницы подхватываем идущую задачу, а если
    // ничего не идёт — показываем последний запуск
    if (adopt) {
        if (active) {
            currentVacuumJobId = active.id;
            vacuumViewJobId = active.id;
            setVacuumButtonRunning(true);
            showVacuumMessage(
                `Запуск #${active.id} продолжается — подхватил его после ` +
                "перезагрузки страницы.",
                "info"
            );
            startVacuumPolling();
        } else if (jobs.length) {
            vacuumViewJobId = jobs[0].id;
            loadVacuumStatus();
        }
    }

    renderVacuumRuns(jobs);

    clearTimeout(vacuumRunsTimer);
    vacuumRunsTimer = setTimeout(function () {
        loadVacuumRuns();
    }, active ? 5000 : 20000);
}

function renderVacuumRuns(jobs) {
    const box = document.getElementById("vacRuns");
    const note = document.getElementById("vacRunsNote");

    if (!box) {
        return;
    }

    if (!jobs.length) {
        box.innerHTML =
            '<div class="text-muted small">Пока не было ни одного запуска.</div>';

        if (note) {
            note.textContent = "";
        }

        return;
    }

    const html = jobs.map(job => {
        const status = String(job.status || "").toLowerCase();
        const running = VACUUM_ACTIVE.includes(status);
        const failed = VACUUM_BAD.includes(status);
        const dot = running ? "b" : (failed ? "r" : (status === "done" ? "g" : "i"));

        let percent = Math.max(0, Math.min(100,
            Math.round(job.progress_percent || 0)));

        if (status === "done") {
            percent = 100;
        }

        const barClass = status === "done" ? "done" : (failed ? "fail" : "");
        const action = String(job.action || "").replace(/_/g, " ");
        const meta = [job.started_at, job.source_name]
            .filter(Boolean).map(escapeHtml).join(" · ");
        const err = failed && job.error_message
            ? ' <span class="text-danger">· ' +
              escapeHtml(String(job.error_message).slice(0, 60)) + "</span>"
            : "";

        return `
            <div class="vr-row ${job.id === vacuumViewJobId ? "on" : ""}"
                 data-run="${job.id}" title="Показать детали запуска">
                <span class="vr-dot ${dot}"></span>
                <span class="vr-lab">#${job.id}
                    <span class="act">${escapeHtml(action)}</span>
                    <span class="meta">· ${job.done_items || 0}/${job.total_items || 0}
                        ${meta ? "· " + meta : ""}</span>${err}</span>
                <span class="vr-bar"><i class="${barClass}"
                    style="width: ${percent}%"></i></span>
                <span class="vr-st">${running ? percent + "%" : escapeHtml(status)}
                    ${running
                        ? `<button type="button" class="vr-stop"
                                   data-stop="${job.id}">стоп</button>`
                        : ""}</span>
            </div>
        `;
    }).join("");

    vacuumRepaint(box, function () { box.innerHTML = html; });

    if (note) {
        const activeCount = jobs.filter(j =>
            VACUUM_ACTIVE.includes(String(j.status || "").toLowerCase())
        ).length;

        note.textContent = (activeCount ? activeCount + " активных · " : "") +
            jobs.length + " в истории";
    }

    box.querySelectorAll("[data-run]").forEach(row => {
        row.onclick = function () {
            selectVacuumRun(parseInt(row.getAttribute("data-run"), 10));
        };
    });

    box.querySelectorAll("[data-stop]").forEach(btn => {
        btn.onclick = async function (ev) {
            ev.stopPropagation();

            const id = btn.getAttribute("data-stop");

            btn.disabled = true;

            const response = await fetch(`/api/jobs/${id}/stop`, {
                method: "POST",
            });
            const result = await response.json();

            showVacuumMessage(
                result.ok
                    ? `Запуск #${id} останавливается…`
                    : (result.message || "Не удалось остановить"),
                result.ok ? "warning" : "danger"
            );

            loadVacuumRuns();
        };
    });
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

// перерисовка списка не должна ронять прокрутку (опрос идёт каждые 2 с)
function vacuumRepaint(box, paint) {
    if (window.gpKeepScroll) {
        window.gpKeepScroll(box, paint);
    } else {
        paint();
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

    const html = items.map(item => {
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

    vacuumRepaint(body, function () { body.innerHTML = html; });
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
window.selectVacuumRun = selectVacuumRun;
window.loadVacuumRuns = loadVacuumRuns;

document.addEventListener("DOMContentLoaded", function () {
    const refresh = document.getElementById("vacRunsRefresh");

    if (refresh) {
        refresh.onclick = function () { loadVacuumRuns(); };
    }

    // adopt: если задача идёт — подхватываем её, иначе показываем последнюю
    loadVacuumRuns(true);
});