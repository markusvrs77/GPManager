let currentReorganizeJobId = null;
let currentReorganizePollTimer = null;

let applyDistributionTimer = null;
let applyDistributionStartedAt = null;

const REORGANIZE_JOB_STORAGE_KEY = "greenplum_reorganize_current_reorganize_job_id";

function showReorganizeMessage(message, type = "info") {
    const alertBox = document.getElementById("reorganizeMessage");

    const bootstrapTypeMap = {
        "info": "info",
        "success": "success",
        "warning": "warning",
        "danger": "danger",
        "error": "danger"
    };

    const alertType = bootstrapTypeMap[type] || "info";

    if (alertBox) {
        alertBox.className = `alert alert-${alertType}`;
        alertBox.innerHTML = escapeHtml(message);
        alertBox.style.display = "block";
        return;
    }

    console.log(`[${alertType}] ${message}`);
}

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

function saveCurrentReorganizeJobId(jobId) {
    if (!jobId) return;
    localStorage.setItem(REORGANIZE_JOB_STORAGE_KEY, String(jobId));
}

function getSavedReorganizeJobId() {
    return localStorage.getItem(REORGANIZE_JOB_STORAGE_KEY);
}

function clearSavedReorganizeJobId() {
    localStorage.removeItem(REORGANIZE_JOB_STORAGE_KEY);
}

function handleReorganizeActionButton() {
    const button = document.getElementById("reorganizeActionButton");
    const mode = button.dataset.mode || "run";

    if (mode === "run") {
        startReorganizeJob();
        return;
    }

    if (mode === "stop") {
        stopCurrentReorganizeJob();
        return;
    }
}

function setReorganizeButtonRunMode() {
    const button = document.getElementById("reorganizeActionButton");
    button.dataset.mode = "run";
    button.textContent = "Run Reorganize";
    button.className = "btn btn-warning w-100 mb-3";
    button.disabled = false;
}

function setReorganizeButtonStopMode() {
    const button = document.getElementById("reorganizeActionButton");
    button.dataset.mode = "stop";
    button.textContent = "Stop current job";
    button.className = "btn btn-danger w-100 mb-3";
    button.disabled = false;
}

function setReorganizeButtonStoppingMode() {
    const button = document.getElementById("reorganizeActionButton");
    button.dataset.mode = "stopping";
    button.textContent = "Stopping...";
    button.className = "btn btn-secondary w-100 mb-3";
    button.disabled = true;
}

async function startReorganizeJob() {
    const connectionId = document.getElementById("connection_id").value;
    const selectedTables = getSelectedTablesForApi();

    const selectedSkewTables = document.querySelectorAll(".skew-problem-checkbox:checked");

    if (selectedSkewTables.length > 0) {
    	console.log("Selected skew detected tables:", selectedSkewTables.length);
    }

    const statusBox = document.getElementById("reorganizeRunStatus");
    const rawOutput = document.getElementById("reorganizeRawOutput");
    const jobItemsBody = document.getElementById("jobItemsBody");

    rawOutput.textContent = "";
    jobItemsBody.innerHTML = "";

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

    resetJobProgress();
    setReorganizeButtonStoppingMode();

    statusBox.className = "alert alert-info";
    statusBox.textContent = `Starting reorganize job for ${selectedTables.length} selected tables...`;

    try {
        const response = await fetch("/api/reorganize/start", {
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
            statusBox.textContent = data.message || "Failed to start reorganize job.";
            rawOutput.textContent = JSON.stringify(data, null, 2);
            setReorganizeButtonRunMode();
            return;
        }

        currentReorganizeJobId = data.job_id;
        saveCurrentReorganizeJobId(currentReorganizeJobId);

        statusBox.className = "alert alert-info";
        statusBox.textContent = `Job #${currentReorganizeJobId} started. Expanded tables: ${data.total_items}`;

        setReorganizeButtonStopMode();
        startPollingReorganizeJob(currentReorganizeJobId);

    } catch (e) {
        statusBox.className = "alert alert-danger";
        statusBox.textContent = "Error: " + e;
        setReorganizeButtonRunMode();
    }
}

function startPollingReorganizeJob(jobId) {
    if (currentReorganizePollTimer) {
        clearInterval(currentReorganizePollTimer);
    }

    pollReorganizeJob(jobId);

    currentReorganizePollTimer = setInterval(() => {
        pollReorganizeJob(jobId);
    }, 2000);
}

async function pollReorganizeJob(jobId) {
    try {
        const jobResponse = await fetch(`/api/jobs/${jobId}`);
        const jobData = await jobResponse.json();

        if (!jobData.ok) {
            return;
        }

        const job = jobData.job;

        currentReorganizeJobId = job.id;
        saveCurrentReorganizeJobId(job.id);

        renderJobProgress(job);
        await loadJobItems(job.id);

        document.getElementById("reorganizeRawOutput").textContent =
            JSON.stringify(job, null, 2);

        applyReorganizeJobStatusToUi(job);

        if (["done", "failed", "cancelled", "interrupted"].includes(job.status)) {
            if (currentReorganizePollTimer) {
                clearInterval(currentReorganizePollTimer);
                currentReorganizePollTimer = null;
            }
        }

    } catch (e) {
        console.error(e);
    }
}

async function loadJobItems(jobId) {
    try {
        const response = await fetch(`/api/jobs/${jobId}/items`);
        const data = await response.json();

        if (data.ok) {
            renderJobItems(data.items);
        }
    } catch (e) {
        console.error(e);
    }
}

function applyReorganizeJobStatusToUi(job) {
    const statusBox = document.getElementById("reorganizeRunStatus");

    if (job.status === "done") {
        setReorganizeButtonRunMode();
        statusBox.className = "alert alert-success";
        statusBox.textContent = `Last job #${job.id} done.`;
        return;
    }

    if (job.status === "failed") {
        setReorganizeButtonRunMode();
        statusBox.className = "alert alert-danger";
        statusBox.textContent = `Last job #${job.id} failed: ${job.error_message || ""}`;
        return;
    }

    if (job.status === "cancelled") {
        setReorganizeButtonRunMode();
        statusBox.className = "alert alert-warning";
        statusBox.textContent = `Last job #${job.id} cancelled.`;
        return;
    }

    if (job.status === "interrupted") {
        setReorganizeButtonRunMode();
        statusBox.className = "alert alert-danger";
        statusBox.textContent = `Last job #${job.id} interrupted: ${job.error_message || "Application was restarted"}`;
        return;
    }

    if (job.status === "stopping") {
        setReorganizeButtonStoppingMode();
        statusBox.className = "alert alert-warning";
        statusBox.textContent = `Job #${job.id} stopping...`;
        return;
    }

    if (job.status === "running" || job.status === "queued") {
        setReorganizeButtonStopMode();
        statusBox.className = "alert alert-info";
        statusBox.textContent = `Job #${job.id} status: ${job.status}`;
        return;
    }

    setReorganizeButtonRunMode();
    statusBox.className = "alert alert-info";
    statusBox.textContent = `Job #${job.id} status: ${job.status}`;
}

async function stopCurrentReorganizeJob() {
    const statusBox = document.getElementById("reorganizeRunStatus");

    if (!currentReorganizeJobId) {
        statusBox.className = "alert alert-warning";
        statusBox.textContent = "Нет активного job для остановки.";
        setReorganizeButtonRunMode();
        return;
    }

    setReorganizeButtonStoppingMode();

    try {
        const response = await fetch(`/api/jobs/${currentReorganizeJobId}/stop`, {
            method: "POST"
        });

        const data = await response.json();

        if (data.ok) {
            statusBox.className = "alert alert-warning";
            statusBox.textContent = `Stop requested for job #${currentReorganizeJobId}.`;
        } else {
            statusBox.className = "alert alert-danger";
            statusBox.textContent = data.message || "Failed to stop job.";
            setReorganizeButtonStopMode();
        }

    } catch (e) {
        statusBox.className = "alert alert-danger";
        statusBox.textContent = "Error: " + e;
        setReorganizeButtonStopMode();
    }
}

async function restoreReorganizePageState() {
    const savedJobId = getSavedReorganizeJobId();

    if (!savedJobId) {
        setReorganizeButtonRunMode();
        return;
    }

    try {
        const response = await fetch(`/api/jobs/${savedJobId}`);
        const data = await response.json();

        if (!data.ok || !data.job) {
            clearSavedReorganizeJobId();
            setReorganizeButtonRunMode();
            return;
        }

        const job = data.job;

        currentReorganizeJobId = job.id;

        renderJobProgress(job);
        await loadJobItems(job.id);
        applyReorganizeJobStatusToUi(job);

        if (["queued", "running", "stopping"].includes(job.status)) {
            startPollingReorganizeJob(job.id);
        }

    } catch (e) {
        console.error(e);
        setReorganizeButtonRunMode();
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

function selectAllSkewTables() {
    const checkboxes = document.querySelectorAll(".skew-problem-checkbox");

    checkboxes.forEach(cb => {
        cb.checked = true;
    });
}


document.addEventListener("DOMContentLoaded", function () {
    setReorganizeButtonRunMode();
    restoreReorganizePageState();
});

let lastDistributionRecommendation = null;

function getOneSelectedTable() {
    const checked = document.querySelectorAll(".table-checkbox:checked");

    if (checked.length === 0) {
        showReorganizeMessage("Выбери одну таблицу для recommendation.", "warning");
        return null;
    }

    if (checked.length > 1) {
        showReorganizeMessage("Для recommendation выбери только одну таблицу.", "warning");
        return null;
    }

    const cb = checked[0];

    return {
        schema_name: cb.dataset.schema,
        table_name: cb.dataset.table
    };
}


function loadDistributionRecommendation() {
    console.log("Get recommendation clicked");

    const connectionId = getSelectedConnectionId();

    const selected = getOneSelectedTable();

    if (!connectionId || !selected) {
        return;
    }

    const box = document.getElementById("distributionRecommendationBox");
    const applyBtn = document.getElementById("applyDistributionBtn");

    if (applyBtn) {
        applyBtn.disabled = true;
    }

    lastDistributionRecommendation = null;

    box.innerHTML = `
        <div class="alert alert-info">
            Анализирую unique/primary key для ${selected.schema_name}.${selected.table_name}...
        </div>
    `;

    fetch("/api/reorganize/recommendation", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            connection_id: connectionId,
            schema_name: selected.schema_name,
            table_name: selected.table_name
        })
    })
        .then(r => r.json())
        .then(data => {
            if (!data.ok) {
                box.innerHTML = `
                    <div class="alert alert-danger">
                        ${escapeHtml(data.message || "Recommendation failed")}
                    </div>
                    <pre>${escapeHtml(JSON.stringify(data, null, 2))}</pre>
                `;
                return;
            }

            lastDistributionRecommendation = data;

            let currentText = "";
            if (data.current_distribution) {
                currentText = `${data.current_distribution.type}`;

                if (data.current_distribution.columns && data.current_distribution.columns.length > 0) {
                    currentText += ` BY (${data.current_distribution.columns.join(", ")})`;
                }
            }

            let recommendedText = data.recommendation_type;

            if (data.recommended_columns && data.recommended_columns.length > 0) {
                recommendedText += ` BY (${data.recommended_columns.join(", ")})`;
            }

            let sameHtml = "";

            if (data.already_same) {
                sameHtml = `
                    <div class="alert alert-success mt-2">
                        Current distribution уже совпадает с recommendation.
                    </div>
                `;
            } else {
                sameHtml = `
                    <div class="alert alert-warning mt-2">
                        Можно переопределить distribution и сразу выполнить REORGANIZE.
                    </div>
                `;
            }

            box.innerHTML = `
                <div class="border rounded p-3 bg-light">
                    <div><b>Table:</b> ${escapeHtml(data.schema_name)}.${escapeHtml(data.table_name)}</div>
                    <div><b>Current distribution:</b> ${escapeHtml(currentText)}</div>
                    <div><b>Recommended:</b> ${escapeHtml(recommendedText)}</div>
                    <div><b>Reason:</b> ${escapeHtml(data.reason || "")}</div>
                    <div><b>Source index:</b> ${escapeHtml(data.source_index || "-")}</div>

                    ${sameHtml}

                    <div class="mt-2">
                        <b>SQL preview:</b>
                        <pre class="mt-1">${escapeHtml(data.recommended_sql_preview || "")}</pre>
                    </div>
                </div>
            `;

            if (applyBtn) {
                applyBtn.disabled = false;
            }
        })
        .catch(err => {
            box.innerHTML = `
                <div class="alert alert-danger">
                    ${escapeHtml(String(err))}
                </div>
            `;
        });
}


function applyRecommendedDistribution() {
    if (!lastDistributionRecommendation) {
        showReorganizeMessage("Сначала получи recommendation.", "warning");
        return;
    }


    const connectionId = getSelectedConnectionId();

    if (!connectionId) {
        showReorganizeMessage("Connection не выбран.", "warning");
        return;
    }

    const msg = `
Будет выполнено:
${lastDistributionRecommendation.recommended_sql_preview}

Продолжить?
`;

    if (!confirm(msg)) {
        return;
    }

    const box = document.getElementById("distributionRecommendationBox");
    const applyBtn = document.getElementById("applyDistributionBtn");

    if (applyBtn) {
        applyBtn.disabled = true;
    }

   
    startApplyDistributionAnimation();

    fetch("/api/reorganize/apply-distribution", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            connection_id: connectionId,
            schema_name: lastDistributionRecommendation.schema_name,
            table_name: lastDistributionRecommendation.table_name,
            distribution_type: lastDistributionRecommendation.recommendation_type,
            columns: lastDistributionRecommendation.recommended_columns || []
        })
    })
        .then(r => r.json())
        .then(data => {
		if (data.ok) {
		    stopApplyDistributionAnimation(
		        "success",
		        `Distribution changed and REORGANIZE completed. Duration: ${String(data.duration_sec)} sec`
		    );
		} else {
		    stopApplyDistributionAnimation(
		        "failed",
		        data.message || "Apply failed"
		    );

		    box.insertAdjacentHTML(
		        "beforeend",
		        `<pre>${escapeHtml(JSON.stringify(data, null, 2))}</pre>`
		    );
		}
            
        })
 	.catch(err => {
	    stopApplyDistributionAnimation(
	        "failed",
        	String(err)
	    );
	})    
        .finally(() => {
            if (applyBtn) {
                applyBtn.disabled = false;
            }
        });
}


function escapeHtml(value) {
    if (value === null || value === undefined) {
        return "";
    }

    return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function getSelectedConnectionId() {
    const possibleIds = [
        "connection_id",
        "connectionId",
        "connectionSelect",
        "reorganizeConnectionId",
        "connection"
    ];

    for (const id of possibleIds) {
        const el = document.getElementById(id);
        if (el && el.value) {
            return el.value;
        }
    }

    const select = document.querySelector("select[name='connection_id']");
    if (select && select.value) {
        return select.value;
    }

    const anyConnectionSelect = document.querySelector("select");
    if (anyConnectionSelect && anyConnectionSelect.value) {
        return anyConnectionSelect.value;
    }

    return null;
}

window.loadDistributionRecommendation = loadDistributionRecommendation;
window.applyRecommendedDistribution = applyRecommendedDistribution;
window.selectAllSkewTables = selectAllSkewTables;

document.addEventListener("DOMContentLoaded", function () {
    const btn = document.getElementById("getRecommendationBtn");

    if (btn) {
        btn.addEventListener("click", function () {
            loadDistributionRecommendation();
        });
    }
});

function startApplyDistributionAnimation() {
    const box = document.getElementById("distributionRecommendationBox");

    if (!box) {
        return;
    }

    applyDistributionStartedAt = Date.now();

    const runningHtml = `
        <div id="applyDistributionRunningBox" class="reorg-running-box">
            <div class="reorg-running-title">
                <span class="reorg-spinner"></span>
                Выполняю Apply distribution + REORGANIZE...
            </div>

            <div class="reorg-running-meta">
                Операция может занять время, особенно для больших таблиц.
                <br>
                Elapsed: <span id="applyDistributionElapsed">0</span> sec
            </div>

            <div class="reorg-pulse-bar"></div>
        </div>
    `;

    box.insertAdjacentHTML("beforeend", runningHtml);

    applyDistributionTimer = setInterval(function () {
        const elapsedEl = document.getElementById("applyDistributionElapsed");

        if (!elapsedEl || !applyDistributionStartedAt) {
            return;
        }

        const elapsedSec = Math.floor((Date.now() - applyDistributionStartedAt) / 1000);
        elapsedEl.textContent = String(elapsedSec);
    }, 1000);
}


function stopApplyDistributionAnimation(status, message) {
    if (applyDistributionTimer) {
        clearInterval(applyDistributionTimer);
        applyDistributionTimer = null;
    }

    const runningBox = document.getElementById("applyDistributionRunningBox");

    if (runningBox) {
        runningBox.remove();
    }

    const box = document.getElementById("distributionRecommendationBox");

    if (!box) {
        return;
    }

    let alertClass = "alert-info";

    if (status === "success") {
        alertClass = "alert-success";
    } else if (status === "failed") {
        alertClass = "alert-danger";
    }

    box.insertAdjacentHTML(
        "beforeend",
        `
        <div class="alert ${alertClass} mt-2">
            ${escapeHtml(message)}
        </div>
        `
    );
}
