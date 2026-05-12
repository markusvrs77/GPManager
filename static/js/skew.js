let currentJobId = null;
let currentPollTimer = null;
let currentJobIsActive = false;
let currentStopRequested = false;

let skewBarChart = null;
let skewStatusChart = null;
let segmentRowsChart = null;

const SKEW_JOB_STORAGE_KEY = "greenplum_reorganize_current_skew_job_id";

function getSelectedTablesForApi() {
    return [...document.querySelectorAll(".table-checkbox:checked")]
        .map(cb => {
            return {
                schema: cb.dataset.schema,
                table: cb.dataset.table,
                full_name: cb.value
            };
        });
}

function handleSkewActionButton() {
    const button = document.getElementById("skewActionButton");
    const mode = button.dataset.mode || "run";

    if (mode === "run") {
        startSkewJob();
        return;
    }

    if (mode === "stop") {
        stopCurrentJob();
        return;
    }
}

function saveCurrentSkewJobId(jobId) {
    if (!jobId) return;
    localStorage.setItem(SKEW_JOB_STORAGE_KEY, String(jobId));
}

function getSavedSkewJobId() {
    return localStorage.getItem(SKEW_JOB_STORAGE_KEY);
}

function clearSavedSkewJobId() {
    localStorage.removeItem(SKEW_JOB_STORAGE_KEY);
}

function setSkewButtonRunMode() {
    const button = document.getElementById("skewActionButton");
    button.dataset.mode = "run";
    button.textContent = "Run Skew Analysis";
    button.className = "btn btn-warning w-100";
    button.disabled = false;

    currentJobIsActive = false;
    currentStopRequested = false;
}

function setSkewButtonStopMode() {
    const button = document.getElementById("skewActionButton");
    button.dataset.mode = "stop";
    button.textContent = "Stop current job";
    button.className = "btn w-100 is-running";
    button.disabled = false;

    currentJobIsActive = true;
    currentStopRequested = false;
}

function setSkewButtonStoppingMode() {
    const button = document.getElementById("skewActionButton");
    button.dataset.mode = "stopping";
    button.textContent = "Stopping...";
    button.className = "btn w-100 is-stopping";
    button.disabled = true;

    currentStopRequested = true;
}

async function restoreSkewPageState() {
    const statusBox = document.getElementById("skewRunStatus");

    let savedJobId = getSavedSkewJobId();

    if (savedJobId) {
        const restored = await restoreJobById(savedJobId);
        if (restored) {
            return;
        }
    }

    await restoreLatestSkewJob();

    if (!currentJobId) {
        statusBox.className = "alert alert-info";
        statusBox.textContent = "Выбери таблицы и нажми Run Skew Analysis.";
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

        renderJobProgress(job);
        await loadJobItems(job.id);
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

        renderJobProgress(job);
        await loadJobItems(job.id);
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
    const statusBox = document.getElementById("skewRunStatus");

    if (!job) {
        setSkewButtonRunMode();
        return;
    }

    if (job.status === "done") {
        setSkewButtonRunMode();
        statusBox.className = "alert alert-success";
        statusBox.textContent = `Last job #${job.id} done.`;
        return;
    }

    if (job.status === "failed") {
        setSkewButtonRunMode();
        statusBox.className = "alert alert-danger";
        statusBox.textContent = `Last job #${job.id} failed: ${job.error_message || ""}`;
        return;
    }

    if (job.status === "cancelled") {
        setSkewButtonRunMode();
        statusBox.className = "alert alert-warning";
        statusBox.textContent = `Last job #${job.id} cancelled.`;
        return;
    }
    
    if (job.status === "interrupted") {
        setSkewButtonRunMode();
        statusBox.className = "alert alert-danger";
        statusBox.textContent = `Last job #${job.id} interrupted: ${job.error_message || "Application was restarted"}`;
        return;
    }

    if (job.status === "stopping") {
        setSkewButtonStoppingMode();
        statusBox.className = "alert alert-warning";
        statusBox.textContent = `Job #${job.id} stopping...`;
        return;
    }

    if (job.status === "running" || job.status === "queued") {
        setSkewButtonStopMode();
        statusBox.className = "alert alert-info";
        statusBox.textContent = `Job #${job.id} status: ${job.status}`;
        return;
    }

    setSkewButtonRunMode();
    statusBox.className = "alert alert-info";
    statusBox.textContent = `Job #${job.id} status: ${job.status}`;
}

async function startSkewJob() {
    const connectionId = document.getElementById("connectionSelect").value;
    const selectedTables = getSelectedTablesForApi();

    const statusBox = document.getElementById("skewRunStatus");
    const rawOutput = document.getElementById("skewRawOutput");
    const jobItemsBody = document.getElementById("jobItemsBody");

    rawOutput.textContent = "";
    jobItemsBody.innerHTML = "";

    clearCharts();
    resetSummary();

    if (!connectionId) {
        statusBox.className = "alert alert-danger";
        statusBox.textContent = "Сначала выбери connection.";
        return;
    }

    if (selectedTables.length === 0) {
        statusBox.className = "alert alert-danger";
        statusBox.textContent = "Сначала выбери таблицы.";
        return;
    }

    statusBox.className = "alert alert-info";
    statusBox.textContent = `Starting skew job for ${selectedTables.length} tables...`;

    resetJobProgress();
    setSkewButtonStoppingMode();

    try {
        const response = await fetch("/api/skew/start", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                connection_id: connectionId,
                tables: selectedTables
            })
        });

        const data = await response.json();

        if (!data.ok) {
            statusBox.className = "alert alert-danger";
            statusBox.textContent = data.message || "Failed to start skew job.";
            rawOutput.textContent = JSON.stringify(data, null, 2);
            setSkewButtonRunMode();
            return;
        }

        currentJobId = data.job_id;
        saveCurrentSkewJobId(currentJobId);

        statusBox.className = "alert alert-info";
        statusBox.textContent = `Job #${currentJobId} started.`;

        setSkewButtonStopMode();
        startPollingJob(currentJobId);

    } catch (e) {
        statusBox.className = "alert alert-danger";
        statusBox.textContent = "Error: " + e;
        setSkewButtonRunMode();
    }
}

function startPollingJob(jobId) {
    if (currentPollTimer) {
        clearInterval(currentPollTimer);
    }

    pollJob(jobId);

    currentPollTimer = setInterval(() => {
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

        renderJobProgress(job);
        await loadJobItems(job.id);
        await loadJobCharts(job.id);

        const rawOutput = document.getElementById("skewRawOutput");
        rawOutput.textContent = JSON.stringify(job, null, 2);

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

async function loadJobItems(jobId) {
    try {
        const itemsResponse = await fetch(`/api/jobs/${jobId}/items`);
        const itemsData = await itemsResponse.json();

        if (itemsData.ok) {
            renderJobItems(itemsData.items);
        }
    } catch (e) {
        console.error(e);
    }
}

async function loadJobCharts(jobId) {
    try {
        const response = await fetch(`/api/jobs/${jobId}/skew-results`);
        const data = await response.json();

        if (!data.ok) {
            return;
        }

        renderSummary(data.summary);
        renderSkewCharts(data.results, data.summary);

    } catch (e) {
        console.error(e);
    }
}

async function stopCurrentJob() {
    const statusBox = document.getElementById("skewRunStatus");

    if (!currentJobId) {
        statusBox.className = "alert alert-warning";
        statusBox.textContent = "Нет активного job для остановки.";
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
            statusBox.className = "alert alert-warning";
            statusBox.textContent = `Stop requested for job #${currentJobId}.`;
        } else {
            statusBox.className = "alert alert-danger";
            statusBox.textContent = data.message || "Failed to stop job.";
            setSkewButtonStopMode();
        }

    } catch (e) {
        statusBox.className = "alert alert-danger";
        statusBox.textContent = "Error: " + e;
        setSkewButtonStopMode();
    }
}

function resetJobProgress() {
    document.getElementById("jobProgressText").textContent = "0%";

    const bar = document.getElementById("jobProgressBar");
    bar.style.width = "0%";
    bar.textContent = "0%";

    document.getElementById("jobTotal").textContent = "0";
    document.getElementById("jobDone").textContent = "0";
    document.getElementById("jobFailed").textContent = "0";
    document.getElementById("jobSkipped").textContent = "0";
}

function renderJobProgress(job) {
    const progress = Number(job.progress_percent || 0);

    document.getElementById("jobProgressText").textContent = `${progress}%`;

    const bar = document.getElementById("jobProgressBar");
    bar.style.width = `${progress}%`;
    bar.textContent = `${progress}%`;

    document.getElementById("jobTotal").textContent = job.total_items || 0;
    document.getElementById("jobDone").textContent = job.done_items || 0;
    document.getElementById("jobFailed").textContent = job.failed_items || 0;
    document.getElementById("jobSkipped").textContent = job.skipped_items || 0;
}

function renderJobItems(items) {
    const body = document.getElementById("jobItemsBody");
    body.innerHTML = "";

    items.forEach(item => {
        const tr = document.createElement("tr");

        tr.innerHTML = `
            <td>${escapeHtml(item.schema_name || "")}</td>
            <td>${escapeHtml(item.table_name || "")}</td>
            <td>${makeJobStatusBadge(item.status)}</td>
            <td>${item.duration_seconds || ""}</td>
            <td class="text-danger small">${escapeHtml(item.error_message || "")}</td>
        `;

        body.appendChild(tr);
    });
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

function resetSummary() {
    document.getElementById("summaryTotalTables").textContent = "0";
    document.getElementById("summaryMaxSkew").textContent = "0";
    document.getElementById("summaryAvgSkew").textContent = "0";
    document.getElementById("summaryOk").textContent = "0";
    document.getElementById("summaryWarning").textContent = "0";
    document.getElementById("summaryCritical").textContent = "0";
    document.getElementById("summaryEmpty").textContent = "0";
    document.getElementById("summaryFailed").textContent = "0";
    
    const interruptedEl = document.getElementById("summaryInterrupted");
    if (interruptedEl) {
        interruptedEl.textContent = "0";
    }
}

function renderSummary(summary) {
    if (!summary) {
        resetSummary();
        return;
    }

    const counts = summary.status_counts || {};

    document.getElementById("summaryTotalTables").textContent = summary.total_tables ?? 0;
    document.getElementById("summaryMaxSkew").textContent = summary.max_skew ?? 0;
    document.getElementById("summaryAvgSkew").textContent = summary.avg_skew ?? 0;
    document.getElementById("summaryOk").textContent = counts.OK ?? 0;
    document.getElementById("summaryWarning").textContent = counts.WARNING ?? 0;
    document.getElementById("summaryCritical").textContent = counts.CRITICAL ?? 0;
    document.getElementById("summaryEmpty").textContent = counts.EMPTY ?? 0;
    document.getElementById("summaryFailed").textContent = counts.FAILED ?? 0;

    const interruptedEl = document.getElementById("summaryInterrupted");
    if (interruptedEl) {
        interruptedEl.textContent = counts.INTERRUPTED ?? 0;
    }
}

function renderSkewCharts(results, summary) {
    renderSkewBarChart(results || []);
    renderSkewStatusChart(summary || {});
}

function renderSkewBarChart(results) {
    const canvas = document.getElementById("skewBarChart");
    if (!canvas) return;

    const sorted = [...results]
        .sort((a, b) => Number(b.skew_ratio || 0) - Number(a.skew_ratio || 0))
        .slice(0, 20);

    const labels = sorted.map(r => `${r.schema_name}.${r.table_name}`);
    const values = sorted.map(r => Number(r.skew_ratio || 0));

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
                    backgroundColor: values.map(v => {
                        if (v >= 3.0) return "rgba(220, 53, 69, 0.7)";
                        if (v >= 1.5) return "rgba(255, 193, 7, 0.7)";
                        return "rgba(25, 135, 84, 0.7)";
                    }),
                    borderColor: values.map(v => {
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
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `skew_ratio: ${context.raw}`;
                        }
                    }
                }
            }
        }
    });
}

function renderSkewStatusChart(summary) {
    const canvas = document.getElementById("skewStatusChart");
    if (!canvas) return;

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

async function loadSegmentDetail(resultId) {
    const card = document.getElementById("segmentDetailCard");

    try {
        const response = await fetch(`/api/skew-results/${resultId}/segments`);
        const data = await response.json();

        if (!data.ok) {
            alert(data.message || "Failed to load segment detail");
            return;
        }

        const result = data.result;
        const segments = data.segments || [];

        card.classList.remove("d-none");

        document.getElementById("segmentDetailTitle").textContent =
            `${result.schema_name}.${result.table_name}`;

        document.getElementById("segmentDetailStatus").innerHTML =
            makeSkewStatusBadge(result.status);

        document.getElementById("segmentDetailTotalRows").textContent =
            result.total_rows || 0;

        document.getElementById("segmentDetailSkewRatio").textContent =
            result.skew_ratio || 0;

        document.getElementById("segmentDetailMaxRows").textContent =
            result.max_rows || 0;

        document.getElementById("segmentDetailMinRows").textContent =
            result.min_rows || 0;

        document.getElementById("segmentDetailEmptySegments").textContent =
            result.empty_segments || 0;

        renderSegmentRowsChart(segments);

        card.scrollIntoView({
            behavior: "smooth",
            block: "start"
        });

    } catch (e) {
        console.error(e);
        alert("Error: " + e);
    }
}

function hideSegmentDetail() {
    const card = document.getElementById("segmentDetailCard");

    if (card) {
        card.classList.add("d-none");
    }

    if (segmentRowsChart) {
        segmentRowsChart.destroy();
        segmentRowsChart = null;
    }
}

function renderSegmentRowsChart(segments) {
    const canvas = document.getElementById("segmentRowsChart");
    if (!canvas) return;

    const labels = segments.map(s => `seg ${s.gp_segment_id}`);
    const values = segments.map(s => Number(s.row_count || 0));

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
                    backgroundColor: values.map(v => {
                        const maxValue = Math.max(...values, 1);

                        if (v === 0) {
                            return "rgba(108, 117, 125, 0.7)";
                        }

                        if (v >= maxValue * 0.8) {
                            return "rgba(220, 53, 69, 0.7)";
                        }

                        return "rgba(13, 110, 253, 0.7)";
                    }),
                    borderColor: values.map(v => {
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
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `rows: ${context.raw}`;
                        }
                    }
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

document.addEventListener("DOMContentLoaded", function () {
    setSkewButtonRunMode();
    resetSummary();
    restoreSkewPageState();
});
