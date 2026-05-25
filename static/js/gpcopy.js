let gpcopyCurrentJobId = null;
let gpcopyPollTimer = null;

let gpcopyTreeData = null;
let selectedGpcopyTables = [];
let gpcopyCollapsedSchemas = new Set();

function gpcopyEl(id) {
    return document.getElementById(id);
}

function gpcopyEscapeHtml(value) {
    return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function gpcopyCssEscape(value) {
    if (window.CSS && CSS.escape) {
        return CSS.escape(value);
    }

    return String(value).replace(/"/g, '\\"');
}

function gpcopyValue(id, defaultValue = "") {
    const el = gpcopyEl(id);
    return el ? el.value : defaultValue;
}

function gpcopyChecked(id) {
    const el = gpcopyEl(id);
    return !!(el && el.checked);
}

function gpcopyMakeKey(schemaName, tableName) {
    return `${schemaName}.${tableName}`;
}

function gpcopyIsSelected(schemaName, tableName) {
    const key = gpcopyMakeKey(schemaName, tableName);

    return selectedGpcopyTables.some(function (item) {
        return gpcopyMakeKey(item.schema, item.table) === key;
    });
}

function gpcopyAddSelectedTable(schemaName, tableName) {
    const key = gpcopyMakeKey(schemaName, tableName);

    const exists = selectedGpcopyTables.some(function (item) {
        return gpcopyMakeKey(item.schema, item.table) === key;
    });

    if (!exists) {
        selectedGpcopyTables.push({
            schema: schemaName,
            table: tableName,
            schema_name: schemaName,
            table_name: tableName
        });
    }
}

function gpcopyRemoveSelectedTable(schemaName, tableName) {
    const key = gpcopyMakeKey(schemaName, tableName);

    selectedGpcopyTables = selectedGpcopyTables.filter(function (item) {
        return gpcopyMakeKey(item.schema, item.table) !== key;
    });
}

function gpcopyUpdateSelectedCount() {
    const el = gpcopyEl("gpcopySelectedCount");

    if (el) {
        el.textContent = selectedGpcopyTables.length;
    }
}

function gpcopySetMainStatus(message, type = "info") {
    const box = gpcopyEl("gpcopyStatusBox");

    if (!box) {
        console.log(message);
        return;
    }

    box.className = `alert alert-${type} mt-3`;
    box.textContent = message;
}

function gpcopySetTreeStatus(message) {
    const box = gpcopyEl("gpcopyObjectTreeStatus");

    if (box) {
        box.textContent = message || "";
    }
}

function gpcopySetDateStatus(message, type = "info") {
    const box =
        gpcopyEl("gpcopyDateStatusBox") ||
        gpcopyEl("gpcopyDateMessage");

    if (!box) {
        gpcopySetMainStatus(message, type);
        return;
    }

    box.className = `alert alert-${type} mt-3`;
    box.textContent = message;
}

function getGpcopyConnectionIds() {
    const sourceEl =
        gpcopyEl("sourceConnectionId") ||
        gpcopyEl("gpcopySourceConnectionId") ||
        gpcopyEl("source_connection_id") ||
        gpcopyEl("connectionId");

    const destEl =
        gpcopyEl("destConnectionId") ||
        gpcopyEl("destinationConnectionId") ||
        gpcopyEl("gpcopyDestConnectionId") ||
        gpcopyEl("gpcopyDestinationConnectionId") ||
        gpcopyEl("dest_connection_id");

    return {
        sourceConnectionId: sourceEl ? sourceEl.value : "",
        destConnectionId: destEl ? destEl.value : ""
    };
}

function getSelectedGpcopyTables() {
    const result = [];
    const seen = new Set();

    selectedGpcopyTables.forEach(function (item) {
        const schema = item.schema || item.schema_name;
        const table = item.table || item.table_name;

        if (!schema || !table) {
            return;
        }

        const key = gpcopyMakeKey(schema, table);

        if (seen.has(key)) {
            return;
        }

        seen.add(key);

        result.push({
            schema: schema,
            table: table,
            schema_name: schema,
            table_name: table
        });
    });

    return result;
}

async function loadGpcopyObjectTree() {
    const ids = getGpcopyConnectionIds();
    const sourceConnectionId = ids.sourceConnectionId;

    const treeBox = gpcopyEl("gpcopyObjectTree");

    if (!sourceConnectionId) {
        gpcopySetTreeStatus("Выбери source connection.");
        if (treeBox) {
            treeBox.innerHTML = "";
        }
        return;
    }

    gpcopySetTreeStatus("Loading source database objects...");

    if (treeBox) {
        treeBox.innerHTML = "";
    }

    selectedGpcopyTables = [];
    gpcopyCollapsedSchemas = new Set();
    gpcopyUpdateSelectedCount();

    try {
        const response = await fetch(
            `/api/objects/tree?connection_id=${encodeURIComponent(sourceConnectionId)}`
        );

        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.message || "Failed to load object tree");
        }

        gpcopyTreeData = data.tree;

        renderGpcopyObjectTree();

        const schemas = data.tree.schemas || [];
        const tablesCount = schemas.reduce(function (sum, schemaObj) {
            return sum + ((schemaObj.tables || []).length);
        }, 0);

        gpcopySetTreeStatus(
            `Loaded database ${data.tree.database || ""}: schemas=${schemas.length}, tables=${tablesCount}`
        );

    } catch (e) {
        console.error(e);

        gpcopySetTreeStatus(e.message);

        if (treeBox) {
            treeBox.innerHTML = "";
        }
    }
}

function renderGpcopyObjectTree() {
    const treeBox = gpcopyEl("gpcopyObjectTree");

    if (!treeBox) {
        return;
    }

    if (!gpcopyTreeData || !Array.isArray(gpcopyTreeData.schemas)) {
        treeBox.innerHTML = `<div class="text-muted small">Нет данных.</div>`;
        gpcopyUpdateSelectedCount();
        return;
    }

    const searchInput = gpcopyEl("gpcopyObjectSearch");
    const searchText = searchInput ? searchInput.value.trim().toLowerCase() : "";

    let html = "";

    gpcopyTreeData.schemas.forEach(function (schemaObj) {
        const schemaName = schemaObj.schema;
        const allTables = schemaObj.tables || [];

        let visibleTables = allTables;

        if (searchText) {
            visibleTables = allTables.filter(function (tableObj) {
                const tableName = tableObj.table;
                const fullName = `${schemaName}.${tableName}`.toLowerCase();

                return (
                    schemaName.toLowerCase().includes(searchText) ||
                    tableName.toLowerCase().includes(searchText) ||
                    fullName.includes(searchText)
                );
            });
        }

        if (searchText && visibleTables.length === 0) {
            return;
        }

        const collapsed = gpcopyCollapsedSchemas.has(schemaName);

        const selectedInSchema = visibleTables.filter(function (tableObj) {
            return gpcopyIsSelected(schemaName, tableObj.table);
        }).length;

        const allVisibleSelected =
            visibleTables.length > 0 && selectedInSchema === visibleTables.length;

        const someVisibleSelected =
            selectedInSchema > 0 && selectedInSchema < visibleTables.length;

        html += `
            <div class="gpcopy-schema-node mb-1" data-schema="${gpcopyEscapeHtml(schemaName)}">
                <div class="d-flex align-items-center gap-1">
                    <input type="checkbox"
                           class="form-check-input gpcopy-schema-checkbox"
                           ${allVisibleSelected ? "checked" : ""}
                           data-schema="${gpcopyEscapeHtml(schemaName)}"
                           onchange="gpcopyToggleSchemaCheckbox('${gpcopyEscapeHtml(schemaName)}', this.checked)">

                    <button type="button"
                            class="btn btn-sm btn-link p-0 text-decoration-none"
                            onclick="gpcopyToggleSchema('${gpcopyEscapeHtml(schemaName)}')">
                        ${collapsed ? "▶" : "▼"}
                    </button>

                    <b onclick="gpcopyToggleSchema('${gpcopyEscapeHtml(schemaName)}')" style="cursor:pointer;">
                        ${gpcopyEscapeHtml(schemaName)}
                    </b>

                    <span class="text-muted small">(${visibleTables.length})</span>
                </div>
        `;

        if (!collapsed) {
            html += `<div class="gpcopy-schema-tables ms-4 mt-1">`;

            visibleTables.forEach(function (tableObj) {
                const tableName = tableObj.table;
                const relkind = tableObj.relkind === "p" ? "partitioned" : "table";
                const checked = gpcopyIsSelected(schemaName, tableName);

                html += `
                    <div class="gpcopy-table-node d-flex align-items-center gap-1 mb-1"
                         data-schema="${gpcopyEscapeHtml(schemaName)}"
                         data-table="${gpcopyEscapeHtml(tableName)}"
                         data-full-name="${gpcopyEscapeHtml(schemaName + "." + tableName)}">

                        <input type="checkbox"
                               class="form-check-input gpcopy-table-checkbox"
                               ${checked ? "checked" : ""}
                               data-schema="${gpcopyEscapeHtml(schemaName)}"
                               data-table="${gpcopyEscapeHtml(tableName)}"
                               onchange="gpcopyToggleTableCheckbox('${gpcopyEscapeHtml(schemaName)}', '${gpcopyEscapeHtml(tableName)}', this.checked)">

                        <span>${gpcopyEscapeHtml(tableName)}</span>
                        <span class="text-muted small ms-1">${gpcopyEscapeHtml(relkind)}</span>
                    </div>
                `;
            });

            html += `</div>`;
        }

        html += `</div>`;
    });

    if (!html) {
        html = `<div class="text-muted small">По фильтру ничего не найдено.</div>`;
    }

    treeBox.innerHTML = html;

    document.querySelectorAll(".gpcopy-schema-checkbox").forEach(function (checkbox) {
        const schemaName = checkbox.getAttribute("data-schema");
        const tables = getGpcopyVisibleTablesBySchema(schemaName);

        const selectedCount = tables.filter(function (tableObj) {
            return gpcopyIsSelected(schemaName, tableObj.table);
        }).length;

        checkbox.indeterminate = selectedCount > 0 && selectedCount < tables.length;
    });

    gpcopyUpdateSelectedCount();
}

function getGpcopyVisibleTablesBySchema(schemaName) {
    if (!gpcopyTreeData || !Array.isArray(gpcopyTreeData.schemas)) {
        return [];
    }

    const schemaObj = gpcopyTreeData.schemas.find(function (item) {
        return item.schema === schemaName;
    });

    if (!schemaObj) {
        return [];
    }

    const allTables = schemaObj.tables || [];

    const searchInput = gpcopyEl("gpcopyObjectSearch");
    const searchText = searchInput ? searchInput.value.trim().toLowerCase() : "";

    if (!searchText) {
        return allTables;
    }

    return allTables.filter(function (tableObj) {
        const tableName = tableObj.table;
        const fullName = `${schemaName}.${tableName}`.toLowerCase();

        return (
            schemaName.toLowerCase().includes(searchText) ||
            tableName.toLowerCase().includes(searchText) ||
            fullName.includes(searchText)
        );
    });
}

function gpcopyToggleSchema(schemaName) {
    if (gpcopyCollapsedSchemas.has(schemaName)) {
        gpcopyCollapsedSchemas.delete(schemaName);
    } else {
        gpcopyCollapsedSchemas.add(schemaName);
    }

    renderGpcopyObjectTree();
}

function gpcopyToggleTableCheckbox(schemaName, tableName, checked) {
    if (checked) {
        gpcopyAddSelectedTable(schemaName, tableName);
    } else {
        gpcopyRemoveSelectedTable(schemaName, tableName);
    }

    renderGpcopyObjectTree();
}

function gpcopyToggleSchemaCheckbox(schemaName, checked) {
    const tables = getGpcopyVisibleTablesBySchema(schemaName);

    tables.forEach(function (tableObj) {
        if (checked) {
            gpcopyAddSelectedTable(schemaName, tableObj.table);
        } else {
            gpcopyRemoveSelectedTable(schemaName, tableObj.table);
        }
    });

    renderGpcopyObjectTree();
}

function gpcopyFilterObjectTree() {
    renderGpcopyObjectTree();
}

function formatDateTimeForBackend(value) {
    if (!value) {
        return "";
    }

    if (value.includes("T")) {
        return value.replace("T", " ") + ":00";
    }

    return value;
}

async function loadGpcopyDateColumns() {
    const ids = getGpcopyConnectionIds();

    if (!ids.sourceConnectionId) {
        gpcopySetDateStatus("Выбери source connection.", "warning");
        return;
    }

    const selectedTables = getSelectedGpcopyTables();

    if (!selectedTables.length) {
        gpcopySetDateStatus("Выбери хотя бы одну таблицу.", "warning");
        return;
    }

    const firstTable = selectedTables[0];

    gpcopySetDateStatus(
        `Загружаю date/timestamp колонки из ${firstTable.schema}.${firstTable.table} ...`,
        "info"
    );

    try {
        const params = new URLSearchParams({
            connection_id: ids.sourceConnectionId,
            schema: firstTable.schema,
            table: firstTable.table
        });

        const response = await fetch(`/api/gpcopy/date-columns?${params.toString()}`);
        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.message || "Failed to load date columns");
        }

        const select = gpcopyEl("gpcopyDateColumn");

        if (!select) {
            throw new Error("gpcopyDateColumn element not found");
        }

        select.innerHTML = "";

        if (!data.columns || !data.columns.length) {
            select.innerHTML = `<option value="">Нет date/timestamp колонок</option>`;
            gpcopySetDateStatus("В выбранной таблице нет date/timestamp колонок.", "warning");
            return;
        }

        select.innerHTML = `<option value="">Выбери колонку</option>`;

        data.columns.forEach(function (col) {
            const option = document.createElement("option");
            option.value = col.column_name;
            option.textContent = `${col.column_name} (${col.data_type})`;
            select.appendChild(option);
        });

        gpcopySetDateStatus(
            "Колонки загружены. Если выбрано несколько таблиц, эта колонка должна существовать во всех выбранных таблицах.",
            "success"
        );

    } catch (e) {
        console.error(e);
        gpcopySetDateStatus(e.message || String(e), "danger");
    }
}

function buildGpcopyCommonPayload() {
    const ids = getGpcopyConnectionIds();
    const selectedTables = getSelectedGpcopyTables();

    const options = {
        truncate: gpcopyChecked("gpcopyTruncate"),
        drop: gpcopyChecked("gpcopyDrop"),
        append: gpcopyChecked("gpcopyAppend"),
        skip_existing: gpcopyChecked("gpcopySkipExisting"),
        analyze: gpcopyChecked("gpcopyAnalyze"),
        dry_run: gpcopyChecked("gpcopyDryRun"),
        validate_count: gpcopyChecked("gpcopyValidateCount")
    };

    return {
        source_connection_id: Number(ids.sourceConnectionId),
        dest_connection_id: Number(ids.destConnectionId),
        destination_connection_id: Number(ids.destConnectionId),

        tables: selectedTables,
        selected_tables: selectedTables,

        gpcopy_path: gpcopyValue(
            "gpcopyPath",
            "/usr/local/gpdb/greenplum-db/bin/gpcopy"
        ),
        jobs: Number(gpcopyValue("gpcopyJobs", "4")),
        on_segment_threshold: Number(gpcopyValue("gpcopyOnSegmentThreshold", "-1")),

        target_schema: gpcopyValue("gpcopyTargetSchema", "").trim(),
        target_mode: gpcopyValue("gpcopyTargetMode", "same_name"),

        options: options,

        truncate: options.truncate,
        drop: options.drop,
        append: options.append,
        skip_existing: options.skip_existing,
        analyze: options.analyze,
        dry_run: options.dry_run,
        validate_count: options.validate_count,

        extra_args: gpcopyValue("gpcopyExtraArgs", "")
    };
}

function buildGpcopyByDatePayload() {
    const payload = buildGpcopyCommonPayload();

    payload.date_column = gpcopyValue("gpcopyDateColumn", "");
    payload.date_filter_column = payload.date_column;

    payload.date_from = formatDateTimeForBackend(gpcopyValue("gpcopyDateFrom", ""));
    payload.date_to = formatDateTimeForBackend(gpcopyValue("gpcopyDateTo", ""));

    payload.mode = "date_filter";

    return payload;
}

async function previewGpcopyDateJson() {
    const payload = buildGpcopyByDatePayload();

    if (!payload.source_connection_id) {
        gpcopySetDateStatus("Выбери source connection.", "warning");
        return;
    }

    if (!payload.dest_connection_id && !payload.destination_connection_id) {
        gpcopySetDateStatus("Выбери destination connection.", "warning");
        return;
    }

    if (!payload.selected_tables.length) {
        gpcopySetDateStatus("Выбери хотя бы одну таблицу слева.", "warning");
        return;
    }

    if (!payload.date_column) {
        gpcopySetDateStatus("Выбери date column.", "warning");
        return;
    }

    if (!payload.date_from || !payload.date_to) {
        gpcopySetDateStatus("Укажи Date from и Date to.", "warning");
        return;
    }

    try {
        const response = await fetch("/api/gpcopy/preview-date-json", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(payload)
        });

        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.message || "Failed to preview JSON");
        }

        const resultJson =
            data.include_json ||
            data.json ||
            data.items ||
            [];

        const pre =
            gpcopyEl("gpcopyDateJsonPreview") ||
            gpcopyEl("gpcopyDatePreview");

        if (pre) {
            pre.style.display = "block";
            pre.textContent = JSON.stringify(resultJson, null, 2);
        } else {
            alert(JSON.stringify(resultJson, null, 2));
        }

        gpcopySetDateStatus("JSON preview готов.", "success");

    } catch (e) {
        console.error(e);
        gpcopySetDateStatus(e.message || String(e), "danger");
    }
}

async function startGpcopyByDate() {
    const payload = buildGpcopyByDatePayload();

    if (!payload.source_connection_id) {
        gpcopySetDateStatus("Выбери source connection.", "warning");
        return;
    }

    if (!payload.dest_connection_id && !payload.destination_connection_id) {
        gpcopySetDateStatus("Выбери destination connection.", "warning");
        return;
    }

    if (!payload.selected_tables.length) {
        gpcopySetDateStatus("Выбери хотя бы одну таблицу слева.", "warning");
        return;
    }

    if (!payload.date_column) {
        gpcopySetDateStatus("Выбери date column.", "warning");
        return;
    }

    if (!payload.date_from || !payload.date_to) {
        gpcopySetDateStatus("Укажи Date from и Date to.", "warning");
        return;
    }

    gpcopySetDateStatus("Запускаю GPCOPY by date filter...", "info");

    try {
        const response = await fetch("/api/gpcopy/start-date", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(payload)
        });

        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.message || "Failed to start GPCOPY by date");
        }

        gpcopyCurrentJobId = data.job_id;

        gpcopySetDateStatus(
            `GPCOPY by date запущен. Job #${data.job_id}`,
            "success"
        );

        renderGpcopyItemsPreview(payload.selected_tables, payload.target_schema);

        startGpcopyPolling(data.job_id);

    } catch (e) {
        console.error(e);
        gpcopySetDateStatus(e.message || String(e), "danger");
    }
}

async function startGpcopyJob() {
    const payload = buildGpcopyCommonPayload();

    if (!payload.source_connection_id) {
        gpcopySetMainStatus("Source connection не выбран.", "warning");
        return;
    }

    if (!payload.dest_connection_id && !payload.destination_connection_id) {
        gpcopySetMainStatus("Destination connection не выбран.", "warning");
        return;
    }

    if (!payload.selected_tables.length) {
        gpcopySetMainStatus("Не выбраны таблицы для gpcopy.", "warning");
        return;
    }

    gpcopySetMainStatus("Запускаю gpcopy...", "info");

    try {
        const response = await fetch("/api/gpcopy/start", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(payload)
        });

        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.message || JSON.stringify(data) || `HTTP ${response.status}`);
        }

        gpcopyCurrentJobId = data.job_id;

        gpcopySetMainStatus(
            `Job #${gpcopyCurrentJobId} started. Tables: ${payload.selected_tables.length}`,
            "success"
        );

        renderGpcopyItemsPreview(payload.selected_tables, payload.target_schema);

        startGpcopyPolling(gpcopyCurrentJobId);

    } catch (e) {
        console.error(e);
        gpcopySetMainStatus(e.message, "danger");
    }
}

function startGpcopyPolling(jobId) {
    gpcopyCurrentJobId = jobId;

    if (gpcopyPollTimer) {
        clearInterval(gpcopyPollTimer);
    }

    gpcopyPollTimer = setInterval(loadGpcopyJobStatus, 2000);
    loadGpcopyJobStatus();
}

function renderGpcopyItemsPreview(tables, targetSchema) {
    const body = gpcopyEl("gpcopyItemsBody");

    if (!body) {
        return;
    }

    body.innerHTML = tables.map(function (table) {
        const target = targetSchema
            ? `${targetSchema}.${table.table}`
            : `${table.schema}.${table.table}`;

        return `
            <tr>
                <td>${gpcopyEscapeHtml(table.schema)}</td>
                <td>${gpcopyEscapeHtml(table.table)}</td>
                <td>${gpcopyEscapeHtml(target)}</td>
                <td>GPCOPY</td>
                <td><span class="badge bg-secondary">queued</span></td>
                <td></td>
                <td></td>
            </tr>
        `;
    }).join("");

    gpcopySetSummary({
        total: tables.length,
        done: 0,
        running: 0,
        failed: 0,
        skipped: 0
    });

    gpcopySetProgress(0);
}

async function loadGpcopyJobStatus() {
    if (!gpcopyCurrentJobId) {
        return;
    }

    try {
        const response = await fetch(`/api/jobs/${gpcopyCurrentJobId}/status`);
        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.message || `HTTP ${response.status}`);
        }

        const job = data.job || {};
        const items = data.items || [];
        const summary = data.summary || {};

        const total = summary.total ?? items.length ?? 0;
        const done = summary.done ?? items.filter(x => x.status === "done").length;
        const running = summary.running ?? items.filter(x => x.status === "running").length;
        const failed = summary.failed ?? items.filter(x => x.status === "failed").length;
        const skipped = summary.skipped ?? items.filter(x => x.status === "skipped").length;

        gpcopySetSummary({
            total: total,
            done: done,
            running: running,
            failed: failed,
            skipped: skipped
        });

        const percent = total > 0
            ? Math.round(((done + failed + skipped) / total) * 100)
            : 0;

        gpcopySetProgress(percent);
        renderGpcopyItems(items);

        if (["done", "failed", "cancelled", "interrupted"].includes(job.status)) {
            if (gpcopyPollTimer) {
                clearInterval(gpcopyPollTimer);
                gpcopyPollTimer = null;
            }

            if (job.status === "done") {
                gpcopySetMainStatus(`Last job #${job.id} done.`, "success");
            } else {
                gpcopySetMainStatus(
                    `Last job #${job.id} ${job.status}: ${job.error_message || ""}`,
                    "danger"
                );
            }
        }

    } catch (e) {
        console.error(e);
        gpcopySetMainStatus(e.message, "danger");
    }
}

function gpcopySetSummary(summary) {
    const totalEl = gpcopyEl("gpcopyTotal");
    const doneEl = gpcopyEl("gpcopyDone");
    const runningEl = gpcopyEl("gpcopyRunning");
    const failedEl = gpcopyEl("gpcopyFailed");
    const skippedEl = gpcopyEl("gpcopySkipped");

    if (totalEl) totalEl.textContent = summary.total;
    if (doneEl) doneEl.textContent = summary.done;
    if (runningEl) runningEl.textContent = summary.running;
    if (failedEl) failedEl.textContent = summary.failed;
    if (skippedEl) skippedEl.textContent = summary.skipped;
}

function gpcopySetProgress(percent) {
    const bar = gpcopyEl("gpcopyProgressBar");
    const pct = gpcopyEl("gpcopyProgressPercent");

    if (bar) {
        bar.style.width = `${percent}%`;
        bar.textContent = `${percent}%`;
    }

    if (pct) {
        pct.textContent = `${percent}%`;
    }
}

function renderGpcopyItems(items) {
    const body = gpcopyEl("gpcopyItemsBody");

    if (!body) {
        return;
    }

    if (!items || !items.length) {
        body.innerHTML = `
            <tr>
                <td colspan="7" class="text-muted">Пока нет данных.</td>
            </tr>
        `;
        return;
    }

    body.innerHTML = items.map(function (item) {
        const status = item.status || "";

        const badgeClass =
            status === "done" ? "bg-success" :
            status === "failed" ? "bg-danger" :
            status === "running" ? "bg-primary" :
            status === "skipped" ? "bg-warning text-dark" :
            "bg-secondary";

        const target = item.target_schema && item.target_table
            ? `${item.target_schema}.${item.target_table}`
            : "";

        return `
            <tr>
                <td>${gpcopyEscapeHtml(item.schema_name || item.schema || "")}</td>
                <td>${gpcopyEscapeHtml(item.table_name || item.table || "")}</td>
                <td>${gpcopyEscapeHtml(target)}</td>
                <td>${gpcopyEscapeHtml(item.action || "GPCOPY")}</td>
                <td><span class="badge ${badgeClass}">${gpcopyEscapeHtml(status)}</span></td>
                <td>${gpcopyEscapeHtml(item.duration_seconds || item.duration || "")}</td>
                <td>${gpcopyEscapeHtml(item.error_message || "")}</td>
            </tr>
        `;
    }).join("");
}

function handleGpcopyRunButton() {
    startGpcopyJob();
}

window.loadGpcopyObjectTree = loadGpcopyObjectTree;
window.renderGpcopyObjectTree = renderGpcopyObjectTree;
window.gpcopyFilterObjectTree = gpcopyFilterObjectTree;
window.gpcopyToggleSchema = gpcopyToggleSchema;
window.gpcopyToggleTableCheckbox = gpcopyToggleTableCheckbox;
window.gpcopyToggleSchemaCheckbox = gpcopyToggleSchemaCheckbox;
window.loadGpcopyDateColumns = loadGpcopyDateColumns;
window.previewGpcopyDateJson = previewGpcopyDateJson;
window.startGpcopyByDate = startGpcopyByDate;
window.startGpcopyJob = startGpcopyJob;
window.handleGpcopyRunButton = handleGpcopyRunButton;