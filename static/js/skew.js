let currentJobId = null;
let currentPollTimer = null;
let currentJobIsActive = false;
let currentStopRequested = false;

let skewBarChart = null;
let skewStatusChart = null;
let segmentRowsChart = null;

const SKEW_JOB_STORAGE_KEY = "greenplum_reorganize_current_skew_job_id";

/* ============================================================
   Helpers
============================================================ */

function skewEl(id) {
    return document.getElementById(id);
}

function escapeHtml(value) {
    return String(value === null || value === undefined ? "" : value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function skewSetText(id, value) {
    const el = skewEl(id);
    if (el) {
        el.textContent = value;
    }
}

function skewSetHtml(id, value) {
    const el = skewEl(id);
    if (el) {
        el.innerHTML = value;
    }
}

function skewSetStatus(message, type = "info") {
    const box =
        skewEl("skewRunStatus") ||
        skewEl("maintenanceStatusBox") ||
        skewEl("jobStatusBox") ||
        skewEl("statusBox");

    if (!box) {
        console.log(message);
        return;
    }

    box.className = "alert alert-" + type;
    box.textContent = message || "";
}

function getSkewConnectionId() {
    const candidates = [
        "connectionSelect",
        "maintenanceConnectionId",
        "skewConnectionId",
        "sourceConnectionId",
        "connectionId",
        "objectConnectionId",
        "reorganizeConnectionId"
    ];

    for (const id of candidates) {
        const el = document.getElementById(id);

        if (el && el.value) {
            return el.value;
        }
    }

    const selects = document.querySelectorAll("select");

    for (const el of selects) {
        const id = (el.id || "").toLowerCase();
        const name = (el.name || "").toLowerCase();

        if (
            id.includes("connection") ||
            name.includes("connection") ||
            id.includes("conn") ||
            name.includes("conn")
        ) {
            if (el.value) {
                return el.value;
            }
        }
    }

    console.warn("Connection select not found. Available selects:",
        Array.from(selects).map(function (el) {
            return {
                id: el.id,
                name: el.name,
                value: el.value,
                text: el.options && el.selectedIndex >= 0
                    ? el.options[el.selectedIndex].text
                    : ""
            };
        })
    );

    return "";
}

function getSkewActionButton() {
    return skewEl("skewActionButton");
}

function getSelectedTablesForApi() {
    const selectors = [
        ".table-checkbox:checked",
        "#objectTree input[data-schema][data-table]:checked",
        "#skewObjectTree input[data-schema][data-table]:checked",
        "#maintenanceObjectTree input[data-schema][data-table]:checked",
        ".object-table-checkbox:checked",
        ".gpcopy-table-checkbox:checked"
    ];

    const result = [];
    const seen = new Set();

    document.querySelectorAll(selectors.join(",")).forEach(function (cb) {
        const schema = cb.dataset.schema || cb.getAttribute("data-schema");
        const table = cb.dataset.table || cb.getAttribute("data-table");

        if (!schema || !table) {
            return;
        }

        const key = schema + "." + table;

        if (seen.has(key)) {
            return;
        }

        seen.add(key);

        result.push({
            schema: schema,
            table: table,
            schema_name: schema,
            table_name: table,
            full_name: cb.value || key
        });
    });

    return result;
}

/* ============================================================
   Button state
============================================================ */

function handleSkewActionButton() {
    const button = getSkewActionButton();
    const mode = button ? (button.dataset.mode || "run") : "run";

    if (mode === "run") {
        startSkewJob();
        return;
    }

    if (mode === "stop") {
        stopCurrentJob();
        return;
    }
}

function setSkewButtonRunMode() {
    const button = getSkewActionButton();

    if (!button) {
        return;
    }

    button.dataset.mode = "run";
    button.textContent = "Run Skew Analysis";
    button.className = "btn btn-warning w-100";
    button.disabled = false;

    currentJobIsActive = false;
    currentStopRequested = false;
}

function setSkewButtonStopMode() {
    const button = getSkewActionButton();

    if (!button) {
        return;
    }

    button.dataset.mode = "stop";
    button.textContent = "Stop current job";
    button.className = "btn w-100 is-running";
    button.disabled = false;

    currentJobIsActive = true;
    currentStopRequested = false;
}

function setSkewButtonStoppingMode() {
    const button = getSkewActionButton();

    if (!button) {
        return;
    }

    button.dataset.mode = "stopping";
    button.textContent = "Stopping...";
    button.className = "btn w-100 is-stopping";
    button.disabled = true;

    currentStopRequested = true;
}

/* ============================================================
   Job storage
============================================================ */

function saveCurrentSkewJobId(jobId) {
    if (!jobId) {
        return;
    }

    localStorage.setItem(SKEW_JOB_STORAGE_KEY, String(jobId));
}

function getSavedSkewJobId() {
    return localStorage.getItem(SKEW_JOB_STORAGE_KEY);
}

function clearSavedSkewJobId() {
    localStorage.removeItem(SKEW_JOB_STORAGE_KEY);
}

/* ============================================================
   Restore state
============================================================ */

async function restoreSkewPageState() {
    const savedJobId = getSavedSkewJobId();

    if (savedJobId) {
        const restored = await restoreJobById(savedJobId);

        if (restored) {
            return;
        }
    }

    await restoreLatestSkewJob();

    if (!currentJobId) {
        skewSetStatus("Выбери таблицы и нажми Run Skew Analysis.", "info");
        setSkewButtonRunMode();
    }
}

async function restoreJobById(jobId) {
    try {
        const response = await fetch(`/api/jobs/${jobId}`);
        const data = await response.json();

        if (!data.ok || !data.job) {
            clearSavedSkewJobId();
            return false;
        }

        const job = data.job;

        currentJobId = job.id;
        saveCurrentSkewJobId(job.id);

        renderSkewJobProgress(job);
        await loadSkewJobItems(job.id);
        await loadJobCharts(job.id);
        applyJobStatusToUi(job);

        if (["queued", "running", "stopping"].includes(job.status)) {
            startPollingJob(job.id);
        }

        return true;

    } catch (e) {
        console.error(e);
        return false;
    }
}

async function restoreLatestSkewJob() {
    try {
        const response = await fetch("/api/jobs/latest/skew");
        const data = await response.json();

        if (!data.ok || !data.job) {
            return false;
        }

        const job = data.job;

        currentJobId = job.id;
        saveCurrentSkewJobId(job.id);

        renderSkewJobProgress(job);;
        await loadSkewJobItems(job.id);
        await loadJobCharts(job.id);
        applyJobStatusToUi(job);

        if (["queued", "running", "stopping"].includes(job.status)) {
            startPollingJob(job.id);
        }

        return true;

    } catch (e) {
        console.error(e);
        return false;
    }
}

function applyJobStatusToUi(job) {
    if (!job) {
        setSkewButtonRunMode();
        return;
    }

    if (job.status === "done") {
        setSkewButtonRunMode();
        skewSetStatus(`Last job #${job.id} done.`, "success");
        return;
    }

    if (job.status === "failed") {
        setSkewButtonRunMode();
        skewSetStatus(`Last job #${job.id} failed: ${job.error_message || ""}`, "danger");
        return;
    }

    if (job.status === "cancelled") {
        setSkewButtonRunMode();
        skewSetStatus(`Last job #${job.id} cancelled.`, "warning");
        return;
    }

    if (job.status === "interrupted") {
        setSkewButtonRunMode();
        skewSetStatus(
            `Last job #${job.id} interrupted: ${job.error_message || "Application was restarted"}`,
            "danger"
        );
        return;
    }

    if (job.status === "stopping") {
        setSkewButtonStoppingMode();
        skewSetStatus(`Job #${job.id} stopping...`, "warning");
        return;
    }

    if (job.status === "running" || job.status === "queued") {
        setSkewButtonStopMode();
        skewSetStatus(`Job #${job.id} status: ${job.status}`, "info");
        return;
    }

    setSkewButtonRunMode();
    skewSetStatus(`Job #${job.id} status: ${job.status}`, "info");
}

/* ============================================================
   Start / Stop / Poll
============================================================ */

async function startSkewJob() {
    const connectionId = getSkewConnectionId();
    const selectedTables = getSelectedTablesForApi();

    const rawOutput = skewEl("skewRawOutput");
    const jobItemsBody = skewEl("jobItemsBody");

    if (rawOutput) {
        rawOutput.textContent = "";
    }

    if (jobItemsBody) {
        jobItemsBody.innerHTML = "";
    }

    clearCharts();
    resetSummary();

    if (!connectionId) {
        skewSetStatus("Сначала выбери connection.", "danger");
        return;
    }

    if (selectedTables.length === 0) {
        skewSetStatus("Сначала выбери таблицы.", "danger");
        return;
    }

    skewSetStatus(`Starting skew job for ${selectedTables.length} tables...`, "info");

    resetSkewJobProgress();
    setSkewButtonStoppingMode();

    try {
        const response = await fetch("/api/skew/start", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                connection_id: Number(connectionId),
                tables: selectedTables
            })
        });

        const data = await response.json();

        if (!data.ok) {
            skewSetStatus(data.message || "Failed to start skew job.", "danger");

            if (rawOutput) {
                rawOutput.textContent = JSON.stringify(data, null, 2);
            }

            setSkewButtonRunMode();
            return;
        }

        currentJobId = data.job_id;
        saveCurrentSkewJobId(currentJobId);

        skewSetStatus(`Job #${currentJobId} started.`, "info");

        setSkewButtonStopMode();
        startPollingJob(currentJobId);

    } catch (e) {
        skewSetStatus("Error: " + e, "danger");
        setSkewButtonRunMode();
    }
}

function startPollingJob(jobId) {
    if (currentPollTimer) {
        clearInterval(currentPollTimer);
    }

    pollJob(jobId);

    currentPollTimer = setInterval(function () {
        pollJob(jobId);
    }, 2000);
}

async function pollJob(jobId) {
    try {
        const jobResponse = await fetch(`/api/jobs/${jobId}`);
        const jobData = await jobResponse.json();

        if (!jobData.ok) {
            return;
        }

        const job = jobData.job;

        currentJobId = job.id;
        saveCurrentSkewJobId(job.id);

        renderSkewJobProgress(job);;
        await loadSkewJobItems(job.id);
        await loadJobCharts(job.id);

        const rawOutput = skewEl("skewRawOutput");

        if (rawOutput) {
            rawOutput.textContent = JSON.stringify(job, null, 2);
        }

        applyJobStatusToUi(job);

        if (["done", "failed", "cancelled", "interrupted"].includes(job.status)) {
            if (currentPollTimer) {
                clearInterval(currentPollTimer);
                currentPollTimer = null;
            }
        }

    } catch (e) {
        console.error(e);
    }
}

async function stopCurrentJob() {
    if (!currentJobId) {
        skewSetStatus("Нет активного job для остановки.", "warning");
        setSkewButtonRunMode();
        return;
    }

    setSkewButtonStoppingMode();

    try {
        const response = await fetch(`/api/jobs/${currentJobId}/stop`, {
            method: "POST"
        });

        const data = await response.json();

        if (data.ok) {
            skewSetStatus(`Stop requested for job #${currentJobId}.`, "warning");
        } else {
            skewSetStatus(data.message || "Failed to stop job.", "danger");
            setSkewButtonStopMode();
        }

    } catch (e) {
        skewSetStatus("Error: " + e, "danger");
        setSkewButtonStopMode();
    }
}

/* ============================================================
   Job progress and items
============================================================ */
let skewLastResultMap = {};
function resetSkewJobProgress() {
    skewSetText("skewJobProgressText", "0%");
    skewSetText("skewJobTotal", "0");
    skewSetText("skewJobDone", "0");
    skewSetText("skewJobFailed", "0");
    skewSetText("skewJobSkipped", "0");

    const bar = skewEl("skewJobProgressBar");

    if (bar) {
        bar.style.width = "0%";
        bar.textContent = "0%";
    }

    const body = skewEl("skewJobItemsBody");

    if (body) {
        body.innerHTML = `
            <tr>
                <td colspan="5" class="text-muted">
                    Пока нет данных.
                </td>
            </tr>
        `;
    }
}


function renderSkewJobProgress(job) {
    const progress = Number(job.progress_percent || 0);

    skewSetText("skewJobProgressText", `${progress}%`);
    skewSetText("skewJobTotal", job.total_items || 0);
    skewSetText("skewJobDone", job.done_items || 0);
    skewSetText("skewJobFailed", job.failed_items || 0);
    skewSetText("skewJobSkipped", job.skipped_items || 0);

    const bar = skewEl("skewJobProgressBar");

    if (bar) {
        bar.style.width = `${progress}%`;
        bar.textContent = `${progress}%`;
    }
}


async function loadSkewJobItems(jobId) {
    try {
        const itemsResponse = await fetch(`/api/jobs/${jobId}/items`);
        const itemsData = await itemsResponse.json();

        if (itemsData.ok) {
            renderSkewJobItems(itemsData.items || [], skewLastResultMap);
        }

    } catch (e) {
        console.error(e);
    }
}

function renderSkewJobItems(items, resultMap = {}) {
    const body = skewEl("skewJobItemsBody");

    if (!body) {
        return;
    }

    body.innerHTML = "";

    if (!items || !items.length) {
        body.innerHTML = `
            <tr>
                <td colspan="11" class="text-muted">
                    Пока нет данных.
                </td>
            </tr>
        `;
        return;
    }

    items.forEach(function (item) {
        const schemaName = item.schema_name || item.schema || "";
        const tableName = item.table_name || item.table || "";
        const key = `${schemaName}.${tableName}`;
        const result = resultMap[key] || {};

        const tr = document.createElement("tr");

        tr.innerHTML = `
            <td>${escapeHtml(schemaName)}</td>
            <td>${escapeHtml(tableName)}</td>
            <td>${makeJobStatusBadge(item.status)}</td>

            <td>${escapeHtml(result.skew_ratio ?? "")}</td>
            <td>${escapeHtml(result.total_rows ?? "")}</td>
            <td>${escapeHtml(result.segment_count ?? "")}</td>
            <td>${escapeHtml(result.empty_segments ?? "")}</td>
            <td>${escapeHtml(result.max_rows ?? "")}</td>
            <td>${escapeHtml(result.min_rows ?? "")}</td>

            <td>${escapeHtml(item.duration_seconds || "")}</td>
            <td class="text-danger small">${escapeHtml(item.error_message || "")}</td>
        `;

        body.appendChild(tr);
    });

    if (window.gpMotion) {
        window.gpMotion.stagger(body.querySelectorAll("tr"), { step: 14, y: 6, dur: 200 });
    }
}


function makeJobStatusBadge(status) {
    if (status === "queued") {
        return `<span class="badge bg-secondary">queued</span>`;
    }

    if (status === "running") {
        return `<span class="badge bg-primary">running</span>`;
    }

    if (status === "done") {
        return `<span class="badge bg-success">done</span>`;
    }

    if (status === "failed") {
        return `<span class="badge bg-danger">failed</span>`;
    }

    if (status === "skipped") {
        return `<span class="badge bg-warning text-dark">skipped</span>`;
    }

    if (status === "interrupted") {
        return `<span class="badge bg-danger">interrupted</span>`;
    }

    if (status === "stopping") {
        return `<span class="badge bg-warning text-dark">stopping</span>`;
    }

    return `<span class="badge bg-dark">${escapeHtml(status || "")}</span>`;
}

/* ============================================================
   Summary
============================================================ */

function resetSummary() {
    skewSetText("summaryTotalTables", "0");
    skewSetText("summaryMaxSkew", "0");
    skewSetText("summaryAvgSkew", "0");
    skewSetText("summaryOk", "0");
    skewSetText("summaryWarning", "0");
    skewSetText("summaryCritical", "0");
    skewSetText("summaryEmpty", "0");
    skewSetText("summaryFailed", "0");
    skewSetText("summaryInterrupted", "0");

    skewSetText("skewTotalTables", "0");
    skewSetText("skewOkTables", "0");
    skewSetText("skewWarningTables", "0");
    skewSetText("skewCriticalTables", "0");
    skewSetText("skewEmptyTables", "0");

    skewSetText("skewProgressPercent", "0%");

    const bar = skewEl("skewProgressBar");

    if (bar) {
        bar.style.width = "0%";
        bar.textContent = "0%";
    }
}

function renderSummary(summary) {
    if (!summary) {
        resetSummary();
        return;
    }

    const counts = summary.status_counts || {};

    skewSetText("summaryTotalTables", summary.total_tables ?? 0);
    skewSetText("summaryMaxSkew", summary.max_skew ?? 0);
    skewSetText("summaryAvgSkew", summary.avg_skew ?? 0);
    skewSetText("summaryOk", counts.OK ?? 0);
    skewSetText("summaryWarning", counts.WARNING ?? 0);
    skewSetText("summaryCritical", counts.CRITICAL ?? 0);
    skewSetText("summaryEmpty", counts.EMPTY ?? 0);
    skewSetText("summaryFailed", counts.FAILED ?? 0);
    skewSetText("summaryInterrupted", counts.INTERRUPTED ?? 0);

    skewSetText("skewTotalTables", summary.total_tables ?? 0);
    skewSetText("skewOkTables", counts.OK ?? 0);
    skewSetText("skewWarningTables", counts.WARNING ?? 0);
    skewSetText("skewCriticalTables", counts.CRITICAL ?? 0);
    skewSetText("skewEmptyTables", counts.EMPTY ?? 0);
}

/* ============================================================
   Charts
============================================================ */

async function loadJobCharts(jobId) {
    try {
        const response = await fetch(`/api/jobs/${jobId}/skew-results`);
        const data = await response.json();

        if (!data.ok) {
            return;
        }

        const results = data.results || [];

        skewLastResultMap = {};

        results.forEach(function (r) {
            const key = `${r.schema_name}.${r.table_name}`;

            skewLastResultMap[key] = {
                status: r.status,
                skew_ratio: r.skew_ratio,
                total_rows: r.total_rows,
                segment_count: r.segment_count,
                empty_segments: r.empty_segments,
                max_rows: r.max_rows,
                min_rows: r.min_rows
            };
        });

        renderSummary(data.summary);
        renderSkewCharts(results, data.summary);

        // Перерисуем items, чтобы добавить skew metrics
        if (currentJobId) {
            await loadSkewJobItems(currentJobId);
        }

    } catch (e) {
        console.error(e);
    }
}

function renderSkewCharts(results, summary) {
    renderSkewBarChart(results || []);
    renderSkewStatusChart(summary || {});
}

function renderSkewBarChart(results) {
    const canvas = skewEl("skewBarChart");

    if (!canvas || typeof Chart === "undefined") {
        return;
    }

    const sorted = [...results]
        .sort(function (a, b) {
            return Number(b.skew_ratio || 0) - Number(a.skew_ratio || 0);
        })
        .slice(0, 20);

    const labels = sorted.map(function (r) {
        return `${r.schema_name}.${r.table_name}`;
    });

    const values = sorted.map(function (r) {
        return Number(r.skew_ratio || 0);
    });

    if (skewBarChart) {
        skewBarChart.destroy();
    }

    skewBarChart = new Chart(canvas, {
        type: "bar",
        data: {
            labels: labels,
            datasets: [
                {
                    label: "Skew ratio",
                    data: values,
                    backgroundColor: values.map(function (v) {
                        if (v >= 3.0) return "rgba(220, 53, 69, 0.7)";
                        if (v >= 1.5) return "rgba(255, 193, 7, 0.7)";
                        return "rgba(25, 135, 84, 0.7)";
                    }),
                    borderColor: values.map(function (v) {
                        if (v >= 3.0) return "rgba(220, 53, 69, 1)";
                        if (v >= 1.5) return "rgba(255, 193, 7, 1)";
                        return "rgba(25, 135, 84, 1)";
                    }),
                    borderWidth: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: "y",
            scales: {
                x: {
                    beginAtZero: true
                }
            },
            plugins: {
                legend: {
                    display: false
                }
            }
        }
    });
}

function renderSkewStatusChart(summary) {
    const canvas = skewEl("skewStatusChart");

    if (!canvas || typeof Chart === "undefined") {
        return;
    }

    const counts = summary.status_counts || {};

    const labels = ["OK", "WARNING", "CRITICAL", "EMPTY", "FAILED", "INTERRUPTED"];

    const data = [
        counts.OK || 0,
        counts.WARNING || 0,
        counts.CRITICAL || 0,
        counts.EMPTY || 0,
        counts.FAILED || 0,
        counts.INTERRUPTED || 0
    ];

    if (skewStatusChart) {
        skewStatusChart.destroy();
    }

    skewStatusChart = new Chart(canvas, {
        type: "doughnut",
        data: {
            labels: labels,
            datasets: [
                {
                    data: data,
                    backgroundColor: [
                        "rgba(25, 135, 84, 0.8)",
                        "rgba(255, 193, 7, 0.8)",
                        "rgba(220, 53, 69, 0.8)",
                        "rgba(108, 117, 125, 0.8)",
                        "rgba(33, 37, 41, 0.8)",
                        "rgba(111, 66, 193, 0.8)"
                    ],
                    borderColor: [
                        "rgba(25, 135, 84, 1)",
                        "rgba(255, 193, 7, 1)",
                        "rgba(220, 53, 69, 1)",
                        "rgba(108, 117, 125, 1)",
                        "rgba(33, 37, 41, 1)",
                        "rgba(111, 66, 193, 1)"
                    ],
                    borderWidth: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: "bottom"
                }
            }
        }
    });
}

function clearCharts() {
    if (skewBarChart) {
        skewBarChart.destroy();
        skewBarChart = null;
    }

    if (skewStatusChart) {
        skewStatusChart.destroy();
        skewStatusChart = null;
    }

    if (segmentRowsChart) {
        segmentRowsChart.destroy();
        segmentRowsChart = null;
    }

    hideSegmentDetail();
}

/* ============================================================
   Segment details
============================================================ */

async function loadSegmentDetail(resultId) {
    const card = skewEl("segmentDetailCard");

    try {
        const response = await fetch(`/api/skew-results/${resultId}/segments`);
        const data = await response.json();

        if (!data.ok) {
            alert(data.message || "Failed to load segment detail");
            return;
        }

        const result = data.result;
        const segments = data.segments || [];

        if (card) {
            card.classList.remove("d-none");
        }

        skewSetText("segmentDetailTitle", `${result.schema_name}.${result.table_name}`);
        skewSetHtml("segmentDetailStatus", makeSkewStatusBadge(result.status));
        skewSetText("segmentDetailTotalRows", result.total_rows || 0);
        skewSetText("segmentDetailSkewRatio", result.skew_ratio || 0);
        skewSetText("segmentDetailMaxRows", result.max_rows || 0);
        skewSetText("segmentDetailMinRows", result.min_rows || 0);
        skewSetText("segmentDetailEmptySegments", result.empty_segments || 0);

        renderSegmentRowsChart(segments);

        if (card) {
            card.scrollIntoView({
                behavior: "smooth",
                block: "start"
            });
        }

    } catch (e) {
        console.error(e);
        alert("Error: " + e);
    }
}

function hideSegmentDetail() {
    const card = skewEl("segmentDetailCard");

    if (card) {
        card.classList.add("d-none");
    }

    if (segmentRowsChart) {
        segmentRowsChart.destroy();
        segmentRowsChart = null;
    }
}

function renderSegmentRowsChart(segments) {
    const canvas = skewEl("segmentRowsChart");

    if (!canvas || typeof Chart === "undefined") {
        return;
    }

    const labels = segments.map(function (s) {
        return `seg ${s.gp_segment_id}`;
    });

    const values = segments.map(function (s) {
        return Number(s.row_count || 0);
    });

    if (segmentRowsChart) {
        segmentRowsChart.destroy();
    }

    segmentRowsChart = new Chart(canvas, {
        type: "bar",
        data: {
            labels: labels,
            datasets: [
                {
                    label: "Rows per segment",
                    data: values,
                    backgroundColor: values.map(function (v) {
                        const maxValue = Math.max(...values, 1);

                        if (v === 0) {
                            return "rgba(108, 117, 125, 0.7)";
                        }

                        if (v >= maxValue * 0.8) {
                            return "rgba(220, 53, 69, 0.7)";
                        }

                        return "rgba(13, 110, 253, 0.7)";
                    }),
                    borderColor: values.map(function (v) {
                        const maxValue = Math.max(...values, 1);

                        if (v === 0) {
                            return "rgba(108, 117, 125, 1)";
                        }

                        if (v >= maxValue * 0.8) {
                            return "rgba(220, 53, 69, 1)";
                        }

                        return "rgba(13, 110, 253, 1)";
                    }),
                    borderWidth: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true
                }
            },
            plugins: {
                legend: {
                    display: false
                }
            }
        }
    });
}

function makeSkewStatusBadge(status) {
    if (status === "OK") {
        return `<span class="badge bg-success">OK</span>`;
    }

    if (status === "WARNING") {
        return `<span class="badge bg-warning text-dark">WARNING</span>`;
    }

    if (status === "CRITICAL") {
        return `<span class="badge bg-danger">CRITICAL</span>`;
    }

    if (status === "EMPTY") {
        return `<span class="badge bg-secondary">EMPTY</span>`;
    }

    return `<span class="badge bg-dark">${escapeHtml(status || "")}</span>`;
}

/* ============================================================
   Init
============================================================ */
function exportSkewExcel() {
    if (!currentJobId) {
        alert("Нет Skew job для выгрузки. Сначала запусти Skew Analysis или дождись восстановления последнего job.");
        return;
    }

    window.location.href = `/api/jobs/${currentJobId}/skew-export.xlsx`;
}

window.exportSkewExcel = exportSkewExcel;
document.addEventListener("DOMContentLoaded", function () {
    setSkewButtonRunMode();
    resetSummary();
    restoreSkewPageState();
});

/* Exports for inline onclick */
window.handleSkewActionButton = handleSkewActionButton;
window.startSkewJob = startSkewJob;
window.stopCurrentJob = stopCurrentJob;
window.loadSegmentDetail = loadSegmentDetail;
window.hideSegmentDetail = hideSegmentDetail;