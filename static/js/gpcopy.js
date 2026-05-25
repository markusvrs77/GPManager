/*
  static/js/gpcopy.js
  Clean version for GPManager / GPCOPY UI

  Что исправлено:
  - фильтр таблиц работает сразу при вводе
  - схемы сворачиваются/разворачиваются
  - можно выбирать несколько таблиц
  - Prepare selected tables работает
  - для каждой таблицы можно указать свою date column
  - для каждой таблицы можно указать свой target
  - --append отправляется только если чекбокс gpcopyAppend выбран
*/

let gpcopyCurrentJobId = null;
let gpcopyPollTimer = null;

let gpcopyTreeData = null;
let selectedGpcopyTables = [];
let gpcopyCollapsedSchemas = new Set();

function gpcopyEl(id) {
    return document.getElementById(id);
}

function gpcopyEscapeHtml(value) {
    return String(value === null || value === undefined ? "" : value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function gpcopyEscapeJs(value) {
    return String(value === null || value === undefined ? "" : value)
        .replace(/\\/g, "\\\\")
        .replace(/'/g, "\\'")
        .replace(/\n/g, "\\n")
        .replace(/\r/g, "\\r");
}

function gpcopyValue(id, defaultValue) {
    const el = gpcopyEl(id);
    return el ? el.value : (defaultValue || "");
}

function gpcopyChecked(id) {
    const el = gpcopyEl(id);
    return !!(el && el.checked);
}

function gpcopyMakeKey(schemaName, tableName) {
    return `${schemaName}.${tableName}`;
}

function gpcopySetStatus(message, type) {
    const box =
        gpcopyEl("gpcopyStatusBox") ||
        gpcopyEl("gpcopyDateStatusBox") ||
        gpcopyEl("gpcopyDateMessage");

    if (!box) {
        console.log(message);
        return;
    }

    box.className = `alert alert-${type || "info"} mt-3`;
    box.textContent = message;
}

function gpcopySetTreeStatus(message) {
    const box = gpcopyEl("gpcopyObjectTreeStatus");
    if (box) {
        box.textContent = message || "";
    }
}

function gpcopySetDateStatus(message, type) {
    const box =
        gpcopyEl("gpcopyDateStatusBox") ||
        gpcopyEl("gpcopyDateMessage") ||
        gpcopyEl("gpcopyStatusBox");

    if (!box) {
        console.log(message);
        return;
    }

    box.className = `alert alert-${type || "info"} mt-3`;
    box.textContent = message;
}

function setGpcopyDateStatus(message, type) {
    gpcopySetDateStatus(message, type);
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

function gpcopyUpdateSelectedCount() {
    const el = gpcopyEl("gpcopySelectedCount");
    if (el) {
        el.textContent = selectedGpcopyTables.length;
    }
}

function gpcopyIsSelected(schemaName, tableName) {
    const key = gpcopyMakeKey(schemaName, tableName);

    return selectedGpcopyTables.some(function (item) {
        const schema = item.schema || item.schema_name;
        const table = item.table || item.table_name;
        return gpcopyMakeKey(schema, table) === key;
    });
}

function gpcopyAddSelectedTable(schemaName, tableName) {
    if (!gpcopyIsSelected(schemaName, tableName)) {
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
        const schema = item.schema || item.schema_name;
        const table = item.table || item.table_name;
        return gpcopyMakeKey(schema, table) !== key;
    });
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

function getGpcopySelectedTablesSafe() {
    const fromState = getSelectedGpcopyTables();

    if (fromState.length > 0) {
        return fromState;
    }

    const result = [];
    const seen = new Set();

    document.querySelectorAll("#gpcopyObjectTree .gpcopy-table-checkbox:checked").forEach(function (cb) {
        const schema = cb.dataset.schema;
        const table = cb.dataset.table;

        if (!schema || !table) {
            return;
        }

        const key = `${schema}.${table}`;

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
        const tableCount = schemas.reduce(function (sum, schemaObj) {
            return sum + ((schemaObj.tables || []).length);
        }, 0);

        gpcopySetTreeStatus(
            `Loaded database ${data.tree.database || ""}: schemas=${schemas.length}, tables=${tableCount}`
        );

    } catch (e) {
        console.error(e);
        gpcopySetTreeStatus(e.message || String(e));

        if (treeBox) {
            treeBox.innerHTML = "";
        }
    }
}

function getGpcopySearchText() {
    const searchInput = gpcopyEl("gpcopyObjectSearch");
    return searchInput ? searchInput.value.trim().toLowerCase() : "";
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
    const searchText = getGpcopySearchText();

    if (!searchText) {
        return allTables;
    }

    return allTables.filter(function (tableObj) {
        const tableName = tableObj.table || "";
        const fullName = `${schemaName}.${tableName}`.toLowerCase();

        return (
            schemaName.toLowerCase().includes(searchText) ||
            tableName.toLowerCase().includes(searchText) ||
            fullName.includes(searchText)
        );
    });
}

function renderGpcopyObjectTree() {
    const treeBox = gpcopyEl("gpcopyObjectTree");

    if (!treeBox) {
        return;
    }

    if (!gpcopyTreeData || !Array.isArray(gpcopyTreeData.schemas)) {
        treeBox.innerHTML = `
            <div class="text-muted small">
                Нет данных. Выбери source connection и нажми Load source objects.
            </div>
        `;
        gpcopyUpdateSelectedCount();
        return;
    }

    let html = "";

    gpcopyTreeData.schemas.forEach(function (schemaObj) {
        const schemaName = schemaObj.schema;
        const visibleTables = getGpcopyVisibleTablesBySchema(schemaName);

        if (visibleTables.length === 0) {
            return;
        }

        const collapsed = gpcopyCollapsedSchemas.has(schemaName);

        const selectedInSchema = visibleTables.filter(function (tableObj) {
            return gpcopyIsSelected(schemaName, tableObj.table);
        }).length;

        const allVisibleSelected =
            visibleTables.length > 0 && selectedInSchema === visibleTables.length;

        html += `
            <div class="gpcopy-schema-node mb-2" data-schema="${gpcopyEscapeHtml(schemaName)}">
                <div class="d-flex align-items-center gap-2">
                    <input type="checkbox"
                           class="form-check-input gpcopy-schema-checkbox"
                           ${allVisibleSelected ? "checked" : ""}
                           onchange="gpcopyToggleSchemaCheckbox('${gpcopyEscapeJs(schemaName)}', this.checked)">

                    <button type="button"
                            class="btn btn-sm btn-link p-0 text-decoration-none"
                            onclick="gpcopyToggleSchema('${gpcopyEscapeJs(schemaName)}')">
                        ${collapsed ? "▶" : "▼"}
                    </button>

                    <b onclick="gpcopyToggleSchema('${gpcopyEscapeJs(schemaName)}')" style="cursor:pointer;">
                        ${gpcopyEscapeHtml(schemaName)}
                    </b>

                    <span class="text-muted small">(${visibleTables.length})</span>
                </div>
        `;

        if (!collapsed) {
            html += `<div class="ms-4 mt-1">`;

            visibleTables.forEach(function (tableObj) {
                const tableName = tableObj.table;
                const checked = gpcopyIsSelected(schemaName, tableName);

                html += `
                    <div class="d-flex align-items-center gap-2 mb-1 gpcopy-table-row">
                        <input type="checkbox"
                               class="form-check-input gpcopy-table-checkbox"
                               data-schema="${gpcopyEscapeHtml(schemaName)}"
                               data-table="${gpcopyEscapeHtml(tableName)}"
                               ${checked ? "checked" : ""}
                               onchange="gpcopyToggleTableCheckbox('${gpcopyEscapeJs(schemaName)}', '${gpcopyEscapeJs(tableName)}', this.checked)">

                        <span>${gpcopyEscapeHtml(tableName)}</span>
                    </div>
                `;
            });

            html += `</div>`;
        }

        html += `</div>`;
    });

    if (!html) {
        html = `
            <div class="text-muted small">
                По фильтру ничего не найдено.
            </div>
        `;
    }

    treeBox.innerHTML = html;
    gpcopyUpdateSelectedCount();
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
    const visibleTables = getGpcopyVisibleTablesBySchema(schemaName);

    visibleTables.forEach(function (tableObj) {
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

function filterGpcopyObjectTree() {
    renderGpcopyObjectTree();
}

function gpcopySearchChanged() {
    renderGpcopyObjectTree();
}

function resetGpcopyTreeOnSourceChange() {
    const searchInput = gpcopyEl("gpcopyObjectSearch");

    if (searchInput) {
        searchInput.value = "";
    }

    const treeBox = gpcopyEl("gpcopyObjectTree");

    if (treeBox) {
        treeBox.innerHTML = "";
    }

    gpcopyTreeData = null;
    selectedGpcopyTables = [];
    gpcopyCollapsedSchemas = new Set();
    gpcopyUpdateSelectedCount();
    gpcopySetTreeStatus("");
}

function getGpcopyTargetSchema() {
    return (
        gpcopyValue("gpcopyTargetSchema") ||
        gpcopyValue("targetSchema") ||
        gpcopyValue("destSchema") ||
        ""
    ).trim();
}

function getGpcopyDateFrom() {
    return (
        gpcopyValue("gpcopyDateFrom") ||
        gpcopyValue("dateFrom") ||
        gpcopyValue("gpcopyFromDate") ||
        ""
    ).trim();
}

function getGpcopyDateTo() {
    return (
        gpcopyValue("gpcopyDateTo") ||
        gpcopyValue("dateTo") ||
        gpcopyValue("gpcopyToDate") ||
        ""
    ).trim();
}

function getGpcopyJobs() {
    const value =
        gpcopyValue("gpcopyJobs") ||
        gpcopyValue("jobs") ||
        "4";

    const n = parseInt(value, 10);

    if (!n || n < 1) {
        return 4;
    }

    return n;
}

function getGpcopyOnSegmentThreshold() {
    const value =
        gpcopyValue("gpcopyOnSegmentThreshold") ||
        gpcopyValue("onSegmentThreshold") ||
        "-1";

    const n = parseInt(value, 10);

    if (Number.isNaN(n)) {
        return -1;
    }

    return n;
}

function getGpcopyOperationMode() {
    if (gpcopyChecked("gpcopyAppend")) {
        return "append";
    }

    if (gpcopyChecked("gpcopyTruncate")) {
        return "truncate";
    }

    if (gpcopyChecked("gpcopyDrop")) {
        return "drop";
    }

    if (gpcopyChecked("gpcopySkipExisting")) {
        return "skip-existing";
    }

    return "";
}

function buildGpcopyCommonPayload() {
    const ids = getGpcopyConnectionIds();
    const selectedTables = getGpcopySelectedTablesSafe();

    const payload = {
        source_connection_id: ids.sourceConnectionId,
        dest_connection_id: ids.destConnectionId,
        destination_connection_id: ids.destConnectionId,

        selected_tables: selectedTables,
        tables: selectedTables,

        jobs: getGpcopyJobs(),
        on_segment_threshold: getGpcopyOnSegmentThreshold(),

        target_schema: getGpcopyTargetSchema(),

        append: gpcopyChecked("gpcopyAppend"),
        truncate: gpcopyChecked("gpcopyTruncate"),
        drop: gpcopyChecked("gpcopyDrop"),
        skip_existing: gpcopyChecked("gpcopySkipExisting"),
        no_ownership: gpcopyChecked("gpcopyNoOwnership"),
        analyze: gpcopyChecked("gpcopyAnalyze"),
        dry_run: gpcopyChecked("gpcopyDryRun"),

        mode: getGpcopyOperationMode()
    };

    return payload;
}

function getTargetForTable(schemaName, tableName, rowIndex) {
    const targetInput =
        gpcopyEl(`gpcopyTarget_${rowIndex}`) ||
        gpcopyEl(`gpcopyTarget_${schemaName}_${tableName}`);

    let target = targetInput ? targetInput.value.trim() : "";

    if (!target) {
        const targetSchema = getGpcopyTargetSchema();

        if (targetSchema) {
            target = `${targetSchema}.${tableName}`;
        } else {
            target = `${schemaName}.${tableName}`;
        }
    }

    return target;
}

function prepareGpcopyDateTables() {
    const tables = getGpcopySelectedTablesSafe();
    const body =
        gpcopyEl("gpcopyDateTablesBody") ||
        gpcopyEl("gpcopyDateBody");

    if (!body) {
        gpcopySetDateStatus("Не найден tbody gpcopyDateTablesBody в gpcopy.html", "danger");
        return;
    }

    if (!tables.length) {
        body.innerHTML = `
            <tr>
                <td colspan="6" class="text-muted">
                    Сначала выбери таблицы.
                </td>
            </tr>
        `;
        gpcopySetDateStatus("Не выбраны таблицы.", "warning");
        return;
    }

    const targetSchema = getGpcopyTargetSchema();

    body.innerHTML = tables.map(function (table, index) {
        const schemaName = table.schema || table.schema_name;
        const tableName = table.table || table.table_name;

        const defaultTarget = targetSchema
            ? `${targetSchema}.${tableName}`
            : `${schemaName}.${tableName}`;

        return `
            <tr data-index="${index}"
                data-schema="${gpcopyEscapeHtml(schemaName)}"
                data-table="${gpcopyEscapeHtml(tableName)}">

                <td>${gpcopyEscapeHtml(schemaName)}</td>

                <td>${gpcopyEscapeHtml(tableName)}</td>

                <td>
                    <input type="text"
                           class="form-control form-control-sm gpcopy-date-column"
                           id="gpcopyDateColumn_${index}"
                           placeholder="date_change$ / created_at / insert_date">
                </td>

                <td>
                    <input type="text"
                           class="form-control form-control-sm gpcopy-target-table"
                           id="gpcopyTarget_${index}"
                           value="${gpcopyEscapeHtml(defaultTarget)}"
                           placeholder="target_schema.target_table">
                </td>

                <td>
                    <button type="button"
                            class="btn btn-sm btn-outline-secondary"
                            onclick="loadGpcopyDateColumns(${index}, '${gpcopyEscapeJs(schemaName)}', '${gpcopyEscapeJs(tableName)}')">
                        Load columns
                    </button>
                </td>

                <td>
                    <span class="small text-muted" id="gpcopyColumnsHint_${index}">
                        manual or load
                    </span>
                </td>
            </tr>
        `;
    }).join("");

    gpcopySetDateStatus(
        `Подготовлено таблиц: ${tables.length}. Для каждой таблицы укажи свою date column.`,
        "info"
    );
}

async function loadGpcopyDateColumns(rowIndex, schemaName, tableName) {
    const ids = getGpcopyConnectionIds();

    if (!ids.sourceConnectionId) {
        gpcopySetDateStatus("Source connection не выбран.", "warning");
        return;
    }

    const hint = gpcopyEl(`gpcopyColumnsHint_${rowIndex}`);

    if (hint) {
        hint.textContent = "loading...";
    }

    const endpoints = [
        `/api/gpcopy/date-columns?connection_id=${encodeURIComponent(ids.sourceConnectionId)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`,
        `/api/table/date-columns?connection_id=${encodeURIComponent(ids.sourceConnectionId)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`,
        `/api/objects/columns?connection_id=${encodeURIComponent(ids.sourceConnectionId)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
    ];

    let lastError = null;

    for (const url of endpoints) {
        try {
            const response = await fetch(url);
            const data = await response.json();

            if (!response.ok || !data.ok) {
                lastError = data.message || `HTTP ${response.status}`;
                continue;
            }

            const columns =
                data.columns ||
                data.date_columns ||
                data.items ||
                [];

            const names = columns.map(function (col) {
                if (typeof col === "string") {
                    return col;
                }

                return col.column_name || col.name || col.column || "";
            }).filter(Boolean);

            if (!names.length) {
                lastError = "date/timestamp columns not found";
                continue;
            }

            renderDateColumnSelect(rowIndex, names);

            if (hint) {
                hint.textContent = `${names.length} columns`;
            }

            return;

        } catch (e) {
            lastError = e.message || String(e);
        }
    }

    if (hint) {
        hint.textContent = lastError || "failed";
    }

    gpcopySetDateStatus(
        `Не смог автоматически загрузить колонки для ${schemaName}.${tableName}. Можно вписать колонку вручную.`,
        "warning"
    );
}

function renderDateColumnSelect(rowIndex, columns) {
    const oldInput = gpcopyEl(`gpcopyDateColumn_${rowIndex}`);

    if (!oldInput) {
        return;
    }

    const currentValue = oldInput.value || "";

    const select = document.createElement("select");
    select.className = "form-select form-select-sm gpcopy-date-column";
    select.id = `gpcopyDateColumn_${rowIndex}`;

    const emptyOption = document.createElement("option");
    emptyOption.value = "";
    emptyOption.textContent = "-- choose column --";
    select.appendChild(emptyOption);

    columns.forEach(function (column) {
        const option = document.createElement("option");
        option.value = column;
        option.textContent = column;

        if (column === currentValue) {
            option.selected = true;
        }

        select.appendChild(option);
    });

    oldInput.replaceWith(select);
}

function collectGpcopyDateTables() {
    const rows = document.querySelectorAll(
        "#gpcopyDateTablesBody tr[data-index], #gpcopyDateBody tr[data-index]"
    );

    const result = [];

    rows.forEach(function (row) {
        const index = row.dataset.index;
        const schemaName = row.dataset.schema;
        const tableName = row.dataset.table;

        const dateColumnEl = gpcopyEl(`gpcopyDateColumn_${index}`);
        const targetEl = gpcopyEl(`gpcopyTarget_${index}`);

        const dateColumn = dateColumnEl ? dateColumnEl.value.trim() : "";
        const target = targetEl ? targetEl.value.trim() : "";

        if (!schemaName || !tableName) {
            return;
        }

        result.push({
            schema: schemaName,
            table: tableName,
            schema_name: schemaName,
            table_name: tableName,
            date_column: dateColumn,
            target: target
        });
    });

    return result;
}

function buildGpcopyDatePayload() {
    const ids = getGpcopyConnectionIds();

    const dateFrom = getGpcopyDateFrom();
    const dateTo = getGpcopyDateTo();

    const tables = collectGpcopyDateTables();

    return {
        source_connection_id: ids.sourceConnectionId,
        dest_connection_id: ids.destConnectionId,
        destination_connection_id: ids.destConnectionId,

        selected_tables: tables,
        tables: tables,

        date_from: dateFrom,
        date_to: dateTo,

        jobs: getGpcopyJobs(),
        on_segment_threshold: getGpcopyOnSegmentThreshold(),

        target_schema: getGpcopyTargetSchema(),

        append: gpcopyChecked("gpcopyAppend"),
        truncate: gpcopyChecked("gpcopyTruncate"),
        drop: gpcopyChecked("gpcopyDrop"),
        skip_existing: gpcopyChecked("gpcopySkipExisting"),
        no_ownership: gpcopyChecked("gpcopyNoOwnership"),
        analyze: gpcopyChecked("gpcopyAnalyze"),
        dry_run: gpcopyChecked("gpcopyDryRun"),

        mode: getGpcopyOperationMode()
    };
}

function validateGpcopyDatePayload(payload) {
    if (!payload.source_connection_id) {
        return "Source connection не выбран.";
    }

    if (!payload.dest_connection_id && !payload.destination_connection_id) {
        return "Destination connection не выбран.";
    }

    if (!payload.selected_tables || !payload.selected_tables.length) {
        return "Нет подготовленных таблиц. Нажми Prepare selected tables.";
    }

    if (!payload.date_from) {
        return "Укажи date from.";
    }

    if (!payload.date_to) {
        return "Укажи date to.";
    }

    for (const table of payload.selected_tables) {
        if (!table.date_column) {
            return `Для таблицы ${table.schema}.${table.table} не указана date column.`;
        }

        if (!table.target) {
            return `Для таблицы ${table.schema}.${table.table} не указан target.`;
        }
    }

    return "";
}

function buildGpcopyJsonPreview(payload) {
    const dateFrom = payload.date_from;
    const dateTo = payload.date_to;

    return payload.selected_tables.map(function (table) {
        const source = `${table.schema}.${table.table}`;
        const dest = table.target || source;
        const dateColumn = table.date_column;

        return {
            source: source,
            dest: dest,
            sql: `SELECT * FROM ${source} WHERE ${dateColumn} >= '${dateFrom}' AND ${dateColumn} < '${dateTo}'`
        };
    });
}

function previewGpcopyDateJson() {
    previewGpcopyDateJsonMulti();
}

function previewGpcopyDateJsonMulti() {
    const payload = buildGpcopyDatePayload();
    const error = validateGpcopyDatePayload(payload);

    if (error) {
        gpcopySetDateStatus(error, "warning");
        return;
    }

    const jsonPreview = buildGpcopyJsonPreview(payload);

    const box =
        gpcopyEl("gpcopyDateJsonPreview") ||
        gpcopyEl("gpcopyJsonPreview") ||
        gpcopyEl("gpcopyPreviewBox");

    if (box) {
        box.textContent = JSON.stringify(jsonPreview, null, 2);
    } else {
        console.log(JSON.stringify(jsonPreview, null, 2));
    }

    gpcopySetDateStatus("JSON preview готов.", "success");
}

async function startGpcopyByDate() {
    await startGpcopyByDateMulti();
}

async function startGpcopyByDateMulti() {
    const payload = buildGpcopyDatePayload();
    const error = validateGpcopyDatePayload(payload);

    if (error) {
        gpcopySetDateStatus(error, "warning");
        return;
    }

    gpcopySetDateStatus("Запускаю GPCOPY by date...", "info");

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
            throw new Error(data.message || JSON.stringify(data) || `HTTP ${response.status}`);
        }

        const jobId = data.job_id || data.jobId;
        gpcopyCurrentJobId = jobId;

        gpcopySetDateStatus(`GPCOPY by date запущен. Job #${jobId}`, "success");

        renderGpcopyItemsPreview(payload.selected_tables, payload.target_schema);
        startGpcopyPolling(jobId);

    } catch (e) {
        console.error(e);
        gpcopySetDateStatus(e.message || String(e), "danger");
    }
}

async function startGpcopyJob() {
    const payload = buildGpcopyCommonPayload();

    if (!payload.source_connection_id) {
        gpcopySetStatus("Source connection не выбран.", "warning");
        return;
    }

    if (!payload.dest_connection_id && !payload.destination_connection_id) {
        gpcopySetStatus("Destination connection не выбран.", "warning");
        return;
    }

    if (!payload.selected_tables.length) {
        gpcopySetStatus("Не выбраны таблицы для gpcopy.", "warning");
        return;
    }

    gpcopySetStatus("Запускаю gpcopy...", "info");

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

        gpcopyCurrentJobId = data.job_id || data.jobId;

        gpcopySetStatus(
            `Job #${gpcopyCurrentJobId} started. Tables: ${payload.selected_tables.length}`,
            "success"
        );

        renderGpcopyItemsPreview(payload.selected_tables, payload.target_schema);
        startGpcopyPolling(gpcopyCurrentJobId);

    } catch (e) {
        console.error(e);
        gpcopySetStatus(e.message || String(e), "danger");
    }
}

function handleGpcopyRunButton() {
    startGpcopyJob();
}

function startGpcopyPolling(jobId) {
    gpcopyCurrentJobId = jobId;

    if (!jobId) {
        return;
    }

    if (gpcopyPollTimer) {
        clearInterval(gpcopyPollTimer);
    }

    gpcopyPollTimer = setInterval(loadGpcopyJobStatus, 2000);
    loadGpcopyJobStatus();
}

function pollGpcopyJob(jobId) {
    startGpcopyPolling(jobId);
}

function renderGpcopyItemsPreview(tables, targetSchema) {
    const body = gpcopyEl("gpcopyItemsBody");

    if (!body) {
        return;
    }

    body.innerHTML = tables.map(function (table) {
        const schemaName = table.schema || table.schema_name || "";
        const tableName = table.table || table.table_name || "";

        const target = table.target
            ? table.target
            : (targetSchema ? `${targetSchema}.${tableName}` : `${schemaName}.${tableName}`);

        return `
            <tr>
                <td>${gpcopyEscapeHtml(schemaName)}</td>
                <td>${gpcopyEscapeHtml(tableName)}</td>
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

        const total = summary.total !== undefined ? summary.total : items.length;
        const done = summary.done !== undefined ? summary.done : items.filter(x => x.status === "done").length;
        const running = summary.running !== undefined ? summary.running : items.filter(x => x.status === "running").length;
        const failed = summary.failed !== undefined ? summary.failed : items.filter(x => x.status === "failed").length;
        const skipped = summary.skipped !== undefined ? summary.skipped : items.filter(x => x.status === "skipped").length;

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
                gpcopySetStatus(`Last job #${job.id} done.`, "success");
            } else {
                gpcopySetStatus(
                    `Last job #${job.id} ${job.status}: ${job.error_message || ""}`,
                    "danger"
                );
            }
        }

    } catch (e) {
        console.error(e);
        gpcopySetStatus(e.message || String(e), "danger");
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
            : (item.target || "");

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

document.addEventListener("DOMContentLoaded", function () {
    const searchInput = gpcopyEl("gpcopyObjectSearch");

    if (searchInput) {
        searchInput.addEventListener("input", gpcopySearchChanged);
    }

    const sourceSelect =
        gpcopyEl("sourceConnectionId") ||
        gpcopyEl("gpcopySourceConnectionId") ||
        gpcopyEl("source_connection_id") ||
        gpcopyEl("connectionId");

    if (sourceSelect) {
        sourceSelect.addEventListener("change", resetGpcopyTreeOnSourceChange);
    }

    gpcopyUpdateSelectedCount();
});

/* Export functions for onclick in gpcopy.html */
window.loadGpcopyObjectTree = loadGpcopyObjectTree;
window.renderGpcopyObjectTree = renderGpcopyObjectTree;

window.gpcopyFilterObjectTree = gpcopyFilterObjectTree;
window.filterGpcopyObjectTree = filterGpcopyObjectTree;
window.gpcopySearchChanged = gpcopySearchChanged;

window.gpcopyToggleSchema = gpcopyToggleSchema;
window.gpcopyToggleTableCheckbox = gpcopyToggleTableCheckbox;
window.gpcopyToggleSchemaCheckbox = gpcopyToggleSchemaCheckbox;

window.loadGpcopyDateColumns = loadGpcopyDateColumns;
window.prepareGpcopyDateTables = prepareGpcopyDateTables;

window.previewGpcopyDateJson = previewGpcopyDateJson;
window.previewGpcopyDateJsonMulti = previewGpcopyDateJsonMulti;

window.startGpcopyByDate = startGpcopyByDate;
window.startGpcopyByDateMulti = startGpcopyByDateMulti;

window.startGpcopyJob = startGpcopyJob;
window.handleGpcopyRunButton = handleGpcopyRunButton;

window.startGpcopyPolling = startGpcopyPolling;
window.pollGpcopyJob = pollGpcopyJob;

window.gpcopySetDateStatus = gpcopySetDateStatus;
window.setGpcopyDateStatus = setGpcopyDateStatus;