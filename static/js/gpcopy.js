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

function gpcopySetStatus(message, type = "info") {
    const statusBox =
        gpcopyEl("gpcopyStatusBox") ||
        gpcopyEl("gpcopyStatus") ||
        gpcopyEl("gpcopyJobStatus") ||
        gpcopyEl("gpcopyMainStatus") ||
        gpcopyEl("gpcopySyncStatusBox");

    if (!statusBox) {
        console.log("[gpcopy]", message);
        return;
    }

    statusBox.className = `alert alert-${type} py-2 small`;
    statusBox.textContent = message || "";
}

function gpcopySetDateStatus(message, type = "info") {
    const statusBox =
        gpcopyEl("gpcopyDateStatusBox") ||
        gpcopyEl("gpcopyDateStatus") ||
        gpcopyEl("gpcopyDateCopyStatus") ||
        gpcopyEl("gpcopyByDateStatus") ||
        gpcopyEl("dateCopyStatus") ||
        gpcopyEl("gpcopySyncStatusBox");

    if (!statusBox) {
        console.log("[gpcopy date]", message);
        return;
    }

    statusBox.className = `alert alert-${type} py-2 small`;
    statusBox.textContent = message || "";
}

/*
    Совместимость со старым кодом.
    Где-то в файле вызывается setGpcopyDateStatus,
    а где-то gpcopySetDateStatus.
*/
function setGpcopyDateStatus(message, type = "info") {
    return gpcopySetDateStatus(message, type);
}

function setGpcopyStatus(message, type = "info") {
    return gpcopySetStatus(message, type);
}

//function gpcopyEl(id) {
//    return document.getElementById(id);
//}

const GPCOPY_STORAGE_KEY = "gpmanager_gpcopy_state_v1";

const GPCOPY_UI_STORAGE_KEY = "gpmanager_gpcopy_ui_state_v1";
function loadGpcopyUiState() {
    try {
        const raw = localStorage.getItem(GPCOPY_UI_STORAGE_KEY);
        if (!raw) {
            return {};
        }

        return JSON.parse(raw) || {};
    } catch (e) {
        console.warn("Failed to load gpcopy UI state", e);
        return {};
    }
}

function saveGpcopyUiState(patch) {
    try {
        const current = loadGpcopyUiState();
        const next = Object.assign({}, current, patch || {});
        localStorage.setItem(GPCOPY_UI_STORAGE_KEY, JSON.stringify(next));
    } catch (e) {
        console.warn("Failed to save gpcopy UI state", e);
    }
}

function restoreGpcopyUiState() {
    const state = loadGpcopyUiState();

    const sourceSelect = gpcopyEl("sourceConnectionId");
    const destSelect = gpcopyEl("destConnectionId");
    const searchInput = gpcopyEl("gpcopyObjectSearch");
    const targetSchema = gpcopyEl("gpcopyTargetSchema");
    const targetMode = gpcopyEl("gpcopyTargetMode");

    if (sourceSelect && state.sourceConnectionId) {
        sourceSelect.value = state.sourceConnectionId;
    }

    if (destSelect && state.destConnectionId) {
        destSelect.value = state.destConnectionId;
    }

    if (searchInput && typeof state.objectSearch === "string") {
        searchInput.value = state.objectSearch;
    }

    if (targetSchema && typeof state.targetSchema === "string") {
        targetSchema.value = state.targetSchema;
    }

    if (targetMode && state.targetMode) {
        targetMode.value = state.targetMode;
    }
}

function bindGpcopyUiStateEvents() {
    const sourceSelect = gpcopyEl("sourceConnectionId");
    const destSelect = gpcopyEl("destConnectionId");
    const searchInput = gpcopyEl("gpcopyObjectSearch");
    const targetSchema = gpcopyEl("gpcopyTargetSchema");
    const targetMode = gpcopyEl("gpcopyTargetMode");

    if (sourceSelect) {
        sourceSelect.addEventListener("change", function () {
            saveGpcopyUiState({
                sourceConnectionId: sourceSelect.value
            });

            resetGpcopyTreeOnSourceChange();
        });
    }

    if (destSelect) {
        destSelect.addEventListener("change", function () {
            saveGpcopyUiState({
                destConnectionId: destSelect.value
            });
        });
    }

    if (searchInput) {
        searchInput.addEventListener("input", function () {
            saveGpcopyUiState({
                objectSearch: searchInput.value
            });

            gpcopySearchChanged();
        });
    }

    if (targetSchema) {
        targetSchema.addEventListener("input", function () {
            saveGpcopyUiState({
                targetSchema: targetSchema.value
            });
        });
    }

    if (targetMode) {
        targetMode.addEventListener("change", function () {
            saveGpcopyUiState({
                targetMode: targetMode.value
            });
        });
    }
}

function getGpcopyState() {
    try {
        const raw = localStorage.getItem(GPCOPY_STORAGE_KEY);
        if (!raw) {
            return {};
        }
        return JSON.parse(raw) || {};
    } catch (e) {
        console.warn("Cannot read gpcopy state:", e);
        return {};
    }
}

function saveGpcopyStatePatch(patch) {
    const state = getGpcopyState();
    const nextState = Object.assign({}, state, patch || {});

    try {
        localStorage.setItem(GPCOPY_STORAGE_KEY, JSON.stringify(nextState));
    } catch (e) {
        console.warn("Cannot save gpcopy state:", e);
    }
}

function gpcopyGetEl(id) {
    return document.getElementById(id);
}

function gpcopyGetValue(id, defaultValue = "") {
    const el = gpcopyGetEl(id);
    if (!el) {
        return defaultValue;
    }
    return el.value;
}

function gpcopySetValue(id, value) {
    const el = gpcopyGetEl(id);
    if (!el || value === undefined || value === null) {
        return;
    }
    el.value = value;
}

function gpcopyIsChecked(id) {
    const el = gpcopyGetEl(id);
    return !!(el && el.checked);
}

function gpcopySetChecked(id, checked) {
    const el = gpcopyGetEl(id);
    if (!el) {
        return;
    }
    el.checked = !!checked;
}

function gpcopyEscapeHtml(value) {
    return String(value === null || value === undefined ? "" : value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}
function gpcopyTableKey(schemaName, tableName) {
    return String(schemaName || "") + "." + String(tableName || "");
}

function isGpcopyTableSelected(schemaName, tableName) {
    const key = gpcopyTableKey(schemaName, tableName);

    return selectedGpcopyTables.some(function (item) {
        return gpcopyTableKey(item.schema, item.table) === key;
    });
}

function setGpcopyTableSelected(schemaName, tableName, checked) {
    const key = gpcopyTableKey(schemaName, tableName);

    selectedGpcopyTables = selectedGpcopyTables.filter(function (item) {
        return gpcopyTableKey(item.schema, item.table) !== key;
    });

    if (checked) {
        selectedGpcopyTables.push({
            schema: schemaName,
            table: tableName
        });
    }

    gpcopyUpdateSelectedCount();

    if (typeof saveGpcopySelectedTablesState === "function") {
        saveGpcopySelectedTablesState();
    }
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
    return String(schemaName || "") + "." + String(tableName || "");
}

function gpcopyIsSelected(schemaName, tableName) {
    const key = gpcopyMakeKey(schemaName, tableName);

    return selectedGpcopyTables.some(function (item) {
        const s = item.schema || item.schema_name;
        const t = item.table || item.table_name;
        return gpcopyMakeKey(s, t) === key;
    });
}

function gpcopyAddSelectedTable(schemaName, tableName) {
    if (!schemaName || !tableName) {
        return;
    }

    const key = gpcopyMakeKey(schemaName, tableName);

    const exists = selectedGpcopyTables.some(function (item) {
        const s = item.schema || item.schema_name;
        const t = item.table || item.table_name;
        return gpcopyMakeKey(s, t) === key;
    });

    if (!exists) {
        selectedGpcopyTables.push({
            schema: schemaName,
            table: tableName,
            schema_name: schemaName,
            table_name: tableName
        });
    }

    saveSelectedGpcopyTablesSafe();
}

function gpcopyRemoveSelectedTable(schemaName, tableName) {
    const key = gpcopyMakeKey(schemaName, tableName);

    selectedGpcopyTables = selectedGpcopyTables.filter(function (item) {
        const s = item.schema || item.schema_name;
        const t = item.table || item.table_name;
        return gpcopyMakeKey(s, t) !== key;
    });

    saveSelectedGpcopyTablesSafe();
}

function saveSelectedGpcopyTablesSafe() {
    try {
        localStorage.setItem(
            "gpmanager_gpcopy_selected_tables",
            JSON.stringify(selectedGpcopyTables || [])
        );
    } catch (e) {
        console.warn("Cannot save selected gpcopy tables:", e);
    }
}

function restoreSelectedGpcopyTablesSafe() {
    try {
        const raw = localStorage.getItem("gpmanager_gpcopy_selected_tables");
        if (!raw) {
            return;
        }

        const parsed = JSON.parse(raw);

        if (Array.isArray(parsed)) {
            selectedGpcopyTables = parsed.filter(function (item) {
                return (item.schema || item.schema_name) && (item.table || item.table_name);
            });
        }
    } catch (e) {
        console.warn("Cannot restore selected gpcopy tables:", e);
    }
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

function gpcopyUpdateSelectedCount() {
    const selectedCount = getSelectedGpcopyTables().length;

    const ids = [
        "gpcopySelectedCount",
        "selectedCount",
        "gpcopySelectedTablesCount"
    ];

    ids.forEach(function (id) {
        const el = gpcopyEl(id);
        if (el) {
            el.textContent = String(selectedCount);
        }
    });
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
        const tableName = tableObj.table || tableObj.table_name || "";
        const fullName = `${schemaName}.${tableName}`.toLowerCase();

        return (
            schemaName.toLowerCase().includes(searchText) ||
            tableName.toLowerCase().includes(searchText) ||
            fullName.includes(searchText)
        );
    });
}

async function loadGpcopyObjectTree() {
    const sourceConnectionSelect =
        gpcopyEl("sourceConnectionId") ||
        gpcopyEl("gpcopySourceConnectionId") ||
        gpcopyEl("source_connection_id") ||
        gpcopyEl("connectionId");

    const treeBox =
        gpcopyEl("gpcopyObjectTree") ||
        gpcopyEl("objectTree");

    if (!sourceConnectionSelect) {
        gpcopySetTreeStatus("Source connection select не найден.", "danger");
        return;
    }

    const connectionId = sourceConnectionSelect.value;

    if (!connectionId) {
        gpcopySetTreeStatus("Выбери Source connection.", "warning");
        return;
    }

    if (treeBox) {
        treeBox.innerHTML = `
            <div class="text-muted small">
                Loading source database objects...
            </div>
        `;
    }

    gpcopySetTreeStatus("Loading source database objects...", "info");

    try {
        const response = await fetch(
            `/api/objects/tree?connection_id=${encodeURIComponent(connectionId)}`
        );

        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.message || "Failed to load source object tree");
        }

        gpcopyTreeData = data.tree;

        restoreSelectedGpcopyTablesSafe();

        renderGpcopyObjectTree();

        const totalTables = countGpcopyTreeTables(gpcopyTreeData);

        gpcopySetTreeStatus(
            `Loaded source objects. Tables: ${totalTables}`,
            "success"
        );

        const searchInput = gpcopyEl("gpcopyObjectSearch");
        if (searchInput && searchInput.value) {
            renderGpcopyObjectTree();
        }

    } catch (e) {
        console.error(e);

        if (treeBox) {
            treeBox.innerHTML = `
                <div class="text-danger small">
                    Failed to load source object tree: ${gpcopyEscapeHtml(e.message)}
                </div>
            `;
        }

        gpcopySetTreeStatus(
            `Failed to load source object tree: ${e.message}`,
            "danger"
        );
    }
}

function countGpcopyTreeTables(treeData) {
    if (!treeData || !Array.isArray(treeData.schemas)) {
        return 0;
    }

    let total = 0;

    treeData.schemas.forEach(function (schema) {
        total += (schema.tables || []).length;
    });

    return total;
}

function gpcopySetTreeStatus(message, type) {
    const statusBox =
        gpcopyEl("gpcopyObjectTreeStatus") ||
        gpcopyEl("objectTreeStatus") ||
        gpcopyEl("gpcopyTreeStatus");

    if (!statusBox) {
        return;
    }

    const alertType = type || "info";

    statusBox.className = `alert alert-${alertType} py-2 small`;
    statusBox.textContent = message || "";
}


function gpcopySetDateStatus(message, type) {
    const statusBox =
        gpcopyEl("gpcopyDateStatus") ||
        gpcopyEl("gpcopyDateCopyStatus") ||
        gpcopyEl("gpcopyByDateStatus") ||
        gpcopyEl("dateCopyStatus");

    if (!statusBox) {
        console.warn("gpcopy date status box not found:", message);
        return;
    }

    const alertType = type || "info";

    statusBox.className = `alert alert-${alertType} py-2 small`;
    statusBox.textContent = message || "";
}


function renderGpcopyObjectTree() {
    const treeBox = gpcopyEl("gpcopyObjectTree") || gpcopyEl("objectTree");

    if (!treeBox) {
        console.error("gpcopyObjectTree element not found");
        return;
    }

    if (!gpcopyTreeData || !Array.isArray(gpcopyTreeData.schemas)) {
        treeBox.innerHTML = `
            <div class="text-muted small">
                Source object tree is empty. Нажми Load Source Objects.
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
            const tableName = tableObj.table || tableObj.table_name;
            return gpcopyIsSelected(schemaName, tableName);
        }).length;

        const allVisibleSelected =
            visibleTables.length > 0 && selectedInSchema === visibleTables.length;

        const someVisibleSelected =
            selectedInSchema > 0 && selectedInSchema < visibleTables.length;

        html += `
            <div class="gpcopy-schema-node mb-2" data-schema="${gpcopyEscapeHtml(schemaName)}">
                <div class="d-flex align-items-center gap-2">
                    <input type="checkbox"
                           class="form-check-input gpcopy-schema-checkbox"
                           data-schema="${gpcopyEscapeHtml(schemaName)}"
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
            html += `<div class="gpcopy-schema-tables ms-4 mt-1">`;

            visibleTables.forEach(function (tableObj) {
                const tableName = tableObj.table || tableObj.table_name;
                const relkind = tableObj.relkind === "p" ? "partitioned" : "table";
                const checked = gpcopyIsSelected(schemaName, tableName);

                html += `
                    <label class="gpcopy-table-node d-flex align-items-center gap-2 mb-1"
                           data-schema="${gpcopyEscapeHtml(schemaName)}"
                           data-table="${gpcopyEscapeHtml(tableName)}"
                           data-full-name="${gpcopyEscapeHtml(schemaName + "." + tableName)}"
                           style="cursor:pointer;">

                        <input type="checkbox"
                               class="form-check-input gpcopy-table-checkbox"
                               data-schema="${gpcopyEscapeHtml(schemaName)}"
                               data-table="${gpcopyEscapeHtml(tableName)}"
                               ${checked ? "checked" : ""}
                               onchange="gpcopyToggleTableCheckbox('${gpcopyEscapeJs(schemaName)}', '${gpcopyEscapeJs(tableName)}', this.checked)">

                        <span>${gpcopyEscapeHtml(tableName)}</span>
                        <span class="text-muted small ms-1">${gpcopyEscapeHtml(relkind)}</span>
                    </label>
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

    document.querySelectorAll(".gpcopy-schema-checkbox").forEach(function (checkbox) {
        const schemaName = checkbox.dataset.schema;
        const tables = getGpcopyVisibleTablesBySchema(schemaName);

        const selectedCount = tables.filter(function (tableObj) {
            const tableName = tableObj.table || tableObj.table_name;
            return gpcopyIsSelected(schemaName, tableName);
        }).length;

        checkbox.checked = tables.length > 0 && selectedCount === tables.length;
        checkbox.indeterminate = selectedCount > 0 && selectedCount < tables.length;
    });

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

    const cb = document.querySelector(
        `.gpcopy-table-checkbox[data-schema="${CSS.escape(schemaName)}"][data-table="${CSS.escape(tableName)}"]`
    );

    if (cb) {
        cb.checked = !!checked;
    }

    updateGpcopySchemaCheckboxState(schemaName);
    gpcopyUpdateSelectedCount();
}

function updateGpcopySchemaCheckboxState(schemaName) {
    const schemaCheckbox = document.querySelector(
        `.gpcopy-schema-checkbox[data-schema="${CSS.escape(schemaName)}"]`
    );

    if (!schemaCheckbox) {
        return;
    }

    const tables = getGpcopyVisibleTablesBySchema(schemaName);

    const selectedCount = tables.filter(function (tableObj) {
        const tableName = tableObj.table || tableObj.table_name;
        return gpcopyIsSelected(schemaName, tableName);
    }).length;

    schemaCheckbox.checked = tables.length > 0 && selectedCount === tables.length;
    schemaCheckbox.indeterminate = selectedCount > 0 && selectedCount < tables.length;
}

function gpcopyToggleSchemaCheckbox(schemaName, checked) {
    const tables = getGpcopyVisibleTablesBySchema(schemaName);

    tables.forEach(function (tableObj) {
        const tableName = tableObj.table || tableObj.table_name;

        if (checked) {
            gpcopyAddSelectedTable(schemaName, tableName);
        } else {
            gpcopyRemoveSelectedTable(schemaName, tableName);
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
    const sourceSelect = gpcopyEl("sourceConnectionId");

    if (sourceSelect) {
        saveGpcopyUiState({
            sourceConnectionId: sourceSelect.value
        });
    }

    const treeBox = gpcopyEl("gpcopyObjectTree");

    if (treeBox) {
        treeBox.innerHTML = "";
    }

    gpcopyTreeData = null;
    selectedGpcopyTables = [];
    gpcopyCollapsedSchemas = new Set();

    gpcopyUpdateSelectedCount();

    gpcopySetTreeStatus(
        "Source connection changed. Нажми Load source database objects.",
        "info"
    );
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

function getGpcopyConnectionIds() {
    const sourceSelect =
        gpcopyEl("sourceConnectionId") ||
        gpcopyEl("gpcopySourceConnectionId") ||
        gpcopyEl("source_connection_id") ||
        gpcopyEl("connectionId");

    const destSelect =
        gpcopyEl("destConnectionId") ||
        gpcopyEl("gpcopyDestConnectionId") ||
        gpcopyEl("dest_connection_id") ||
        gpcopyEl("targetConnectionId");

    const sourceConnectionId = sourceSelect ? sourceSelect.value : "";
    const destConnectionId = destSelect ? destSelect.value : "";

    return {
        source_connection_id: sourceConnectionId,
        dest_connection_id: destConnectionId,
        sourceConnectionId: sourceConnectionId,
        destConnectionId: destConnectionId
    };
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
    `/api/gpcopy/date-columns?connection_id=${encodeURIComponent(ids.sourceConnectionId)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
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

function normalizeGpcopyDateForSql(value) {
    if (!value) {
        return "";
    }

    value = value.trim();

    // Если уже формат YYYY-MM-DD HH:MI или YYYY-MM-DD HH:MI:SS
    if (/^\d{4}-\d{2}-\d{2}/.test(value)) {
        if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$/.test(value)) {
            return value + ":00";
        }
        return value;
    }

    // Формат DD.MM.YYYY HH:MI
    const m = value.match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})$/);

    if (m) {
        const dd = m[1];
        const mm = m[2];
        const yyyy = m[3];
        const hh = m[4];
        const mi = m[5];

        return `${yyyy}-${mm}-${dd} ${hh}:${mi}:00`;
    }

    // Формат DD.MM.YYYY HH:MI:SS
    const m2 = value.match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$/);

    if (m2) {
        const dd = m2[1];
        const mm = m2[2];
        const yyyy = m2[3];
        const hh = m2[4];
        const mi = m2[5];
        const ss = m2[6];

        return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
    }

    return value;
}

function buildGpcopyDatePayload() {
    const ids = getGpcopyConnectionIds();

    const dateFromRaw = getGpcopyDateFrom();
    const dateToRaw = getGpcopyDateTo();

    const dateFrom = normalizeGpcopyDateForSql(dateFromRaw);
    const dateTo = normalizeGpcopyDateForSql(dateToRaw);

    const tables = collectGpcopyDateTables();

    const tableConfigs = tables.map(function (table) {
        const schemaName = table.schema || table.schema_name;
        const tableName = table.table || table.table_name;
        const sourceFullName = `${schemaName}.${tableName}`;

        const target = table.target || sourceFullName;
        const dateColumn = table.date_column;

        return {
            schema: schemaName,
            table: tableName,
            schema_name: schemaName,
            table_name: tableName,

            source: sourceFullName,
            dest: target,
            target: target,

            date_column: dateColumn,

            sql: `SELECT * FROM ${sourceFullName} WHERE ${dateColumn} >= '${dateFrom}' AND ${dateColumn} < '${dateTo}'`
        };
    });

    return {
        source_connection_id: ids.sourceConnectionId,
        dest_connection_id: ids.destConnectionId,
        destination_connection_id: ids.destConnectionId,

        // важно: backend ждёт именно table_configs
        table_configs: tableConfigs,

        // оставляем совместимость
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

    if (!payload.table_configs || !payload.table_configs.length) {
        return "Нет подготовленных таблиц. Нажми Prepare selected tables.";
    }

    if (!payload.date_from) {
        return "Укажи date from.";
    }

    if (!payload.date_to) {
        return "Укажи date to.";
    }

    for (const table of payload.table_configs) {
        if (!table.date_column) {
            return `Для таблицы ${table.schema}.${table.table} не указана date column.`;
        }

        if (!table.dest && !table.target) {
            return `Для таблицы ${table.schema}.${table.table} не указан target.`;
        }

        if (!table.sql) {
            return `Для таблицы ${table.schema}.${table.table} не сформировался SQL.`;
        }
    }

    return "";
}

function buildGpcopyJsonPreview(payload) {
    return payload.table_configs.map(function (table) {
        return {
            source: table.source,
            dest: table.dest || table.target,
            sql: table.sql
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

        renderGpcopyItemsPreview(payload.table_configs, payload.target_schema);
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

        renderGpcopyItemsPreview(payload.table_configs, payload.target_schema);
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

function saveGpcopyFormState() {
    saveGpcopyStatePatch({
        sourceConnectionId: gpcopyGetValue("gpcopySourceConnectionId"),
        destConnectionId: gpcopyGetValue("gpcopyDestConnectionId"),

        tableFilter: gpcopyGetValue("gpcopyObjectFilter"),

        targetSchema: gpcopyGetValue("gpcopyTargetSchema"),
        targetTable: gpcopyGetValue("gpcopyTargetTable"),

        dateFrom: gpcopyGetValue("gpcopyDateFrom"),
        dateTo: gpcopyGetValue("gpcopyDateTo"),

        append: gpcopyIsChecked("gpcopyAppend"),
        truncate: gpcopyIsChecked("gpcopyTruncate"),
        drop: gpcopyIsChecked("gpcopyDrop"),
        skipExisting: gpcopyIsChecked("gpcopySkipExisting"),
        noOwnership: gpcopyIsChecked("gpcopyNoOwnership"),
        analyze: gpcopyIsChecked("gpcopyAnalyze"),
        dryRun: gpcopyIsChecked("gpcopyDryRun"),

        jobs: gpcopyGetValue("gpcopyJobs"),
        onSegmentThreshold: gpcopyGetValue("gpcopyOnSegmentThreshold")
    });
}

function restoreGpcopyFormState() {
    const state = getGpcopyState();

    gpcopySetValue("gpcopySourceConnectionId", state.sourceConnectionId);
    gpcopySetValue("gpcopyDestConnectionId", state.destConnectionId);

    gpcopySetValue("gpcopyObjectFilter", state.tableFilter);

    gpcopySetValue("gpcopyTargetSchema", state.targetSchema);
    gpcopySetValue("gpcopyTargetTable", state.targetTable);

    gpcopySetValue("gpcopyDateFrom", state.dateFrom);
    gpcopySetValue("gpcopyDateTo", state.dateTo);

    gpcopySetChecked("gpcopyAppend", state.append);
    gpcopySetChecked("gpcopyTruncate", state.truncate);
    gpcopySetChecked("gpcopyDrop", state.drop);
    gpcopySetChecked("gpcopySkipExisting", state.skipExisting);
    gpcopySetChecked("gpcopyNoOwnership", state.noOwnership);
    gpcopySetChecked("gpcopyAnalyze", state.analyze);
    gpcopySetChecked("gpcopyDryRun", state.dryRun);

    gpcopySetValue("gpcopyJobs", state.jobs);
    gpcopySetValue("gpcopyOnSegmentThreshold", state.onSegmentThreshold);
}

function getSavedExpandedSchemas() {
    const state = getGpcopyState();
    return state.expandedSchemas || {};
}

function saveSchemaExpanded(schemaName, expanded) {
    const state = getGpcopyState();
    const expandedSchemas = state.expandedSchemas || {};

    expandedSchemas[schemaName] = !!expanded;

    saveGpcopyStatePatch({
        expandedSchemas: expandedSchemas
    });
}

function getSavedSelectedTables() {
    const state = getGpcopyState();
    return state.selectedTables || {};
}

function saveSelectedGpcopyTables() {
    const selected = {};

    document.querySelectorAll(".gpcopy-table-checkbox").forEach(function (cb) {
        const schemaName = cb.dataset.schema || "";
        const tableName = cb.dataset.table || "";

        if (!schemaName || !tableName) {
            return;
        }

        const key = schemaName + "." + tableName;

        if (cb.checked) {
            selected[key] = true;
        }
    });

    saveGpcopyStatePatch({
        selectedTables: selected
    });
}

function restoreSelectedGpcopyTables() {
    const selected = getSavedSelectedTables();

    document.querySelectorAll(".gpcopy-table-checkbox").forEach(function (cb) {
        const schemaName = cb.dataset.schema || "";
        const tableName = cb.dataset.table || "";
        const key = schemaName + "." + tableName;

        cb.checked = !!selected[key];
    });

    if (typeof updateGpcopySelectedCount === "function") {
        updateGpcopySelectedCount();
    }
}

function restoreGpcopyTreeUiState() {
    const state = getGpcopyState();
    const filterValue = state.tableFilter || "";

    if (filterValue) {
        gpcopySetValue("gpcopyObjectFilter", filterValue);

        if (typeof filterGpcopyObjectTree === "function") {
            filterGpcopyObjectTree();
        }
    }

    const expandedSchemas = getSavedExpandedSchemas();

    document.querySelectorAll("[data-gpcopy-schema-body]").forEach(function (body) {
        const schemaName = body.dataset.gpcopySchemaBody;

        if (expandedSchemas[schemaName] === true) {
            body.style.display = "";
        } else if (expandedSchemas[schemaName] === false) {
            body.style.display = "none";
        }
    });

    restoreSelectedGpcopyTables();
    bindGpcopyTreePersistEvents();
}

function bindGpcopyTreePersistEvents() {
    document.querySelectorAll(".gpcopy-table-checkbox").forEach(function (cb) {
        cb.addEventListener("change", function () {
            saveSelectedGpcopyTables();
            saveGpcopyFormState();
        });
    });

    document.querySelectorAll("[data-gpcopy-schema-toggle]").forEach(function (btn) {
        btn.addEventListener("click", function () {
            const schemaName = btn.dataset.gpcopySchemaToggle;
            const body = document.querySelector('[data-gpcopy-schema-body="' + cssEscapeValue(schemaName) + '"]');

            if (!body) {
                return;
            }

            const isHidden = body.style.display === "none";
            body.style.display = isHidden ? "" : "none";

            saveSchemaExpanded(schemaName, isHidden);
        });
    });
}

function cssEscapeValue(value) {
    if (window.CSS && CSS.escape) {
        return CSS.escape(value);
    }

    return String(value).replace(/"/g, '\\"');
}

document.addEventListener("DOMContentLoaded", function () {
    restoreGpcopyUiState();
    bindGpcopyUiStateEvents();

    gpcopyUpdateSelectedCount();

    const searchInput = gpcopyEl("gpcopyObjectSearch");

    if (searchInput && searchInput.value) {
        renderGpcopyObjectTree();
    }
});

function prepareGpcopySyncTables() {
    const tables = getGpcopySelectedTablesSafe();
    const body = document.getElementById("gpcopySyncTablesBody");

    if (!body) {
        alert("gpcopySyncTablesBody not found");
        return;
    }

    if (!tables.length) {
        body.innerHTML = `
            <tr>
                <td colspan="5" class="text-warning">
                    Сначала выбери таблицы слева.
                </td>
            </tr>
        `;
        return;
    }

    const targetSchema = getGpcopyTargetSchema ? getGpcopyTargetSchema() : "";

    body.innerHTML = tables.map(function (t, index) {
        const schema = t.schema || t.schema_name;
        const table = t.table || t.table_name;

        const sourceFull = `${schema}.${table}`;
        const targetFull = targetSchema ? `${targetSchema}.${table}` : sourceFull;

        return `
            <tr data-index="${index}"
                data-schema="${gpcopyEscapeHtml(schema)}"
                data-table="${gpcopyEscapeHtml(table)}">

                <td>${gpcopyEscapeHtml(sourceFull)}</td>

                <td>
                    <input class="form-control form-control-sm gpcopy-sync-target"
                           id="gpcopySyncTarget_${index}"
                           value="${gpcopyEscapeHtml(targetFull)}">
                </td>

                <td>
                    <input class="form-control form-control-sm gpcopy-sync-key-columns"
                           id="gpcopySyncKeys_${index}"
                           placeholder="id или cli_code,trn_date">
                </td>

                <td>
                    <input class="form-control form-control-sm gpcopy-sync-compare-columns"
                           id="gpcopySyncCompare_${index}"
                           placeholder="* или amount,status,updated_at"
                           value="*">
                </td>

                <td class="text-center">
                    <input type="checkbox"
                           class="form-check-input gpcopy-sync-delete-missing"
                           id="gpcopySyncDelete_${index}">
                </td>
            </tr>
        `;
    }).join("");

    setGpcopySyncStatus(`Подготовлено таблиц: ${tables.length}. Укажи key columns.`, "info");
}

function collectGpcopySyncConfigs() {
    const rows = document.querySelectorAll("#gpcopySyncTablesBody tr[data-index]");
    const configs = [];

    rows.forEach(function (row) {
        const index = row.dataset.index;
        const schema = row.dataset.schema;
        const table = row.dataset.table;

        const target = document.getElementById(`gpcopySyncTarget_${index}`)?.value.trim();
        const keyColumnsRaw = document.getElementById(`gpcopySyncKeys_${index}`)?.value.trim();
        const compareColumnsRaw = document.getElementById(`gpcopySyncCompare_${index}`)?.value.trim();
        const deleteMissing = document.getElementById(`gpcopySyncDelete_${index}`)?.checked === true;

        const keyColumns = keyColumnsRaw
            ? keyColumnsRaw.split(",").map(x => x.trim()).filter(Boolean)
            : [];

        const compareColumns = compareColumnsRaw && compareColumnsRaw !== "*"
            ? compareColumnsRaw.split(",").map(x => x.trim()).filter(Boolean)
            : ["*"];

        configs.push({
            schema: schema,
            table: table,
            source: `${schema}.${table}`,
            target: target,
            key_columns: keyColumns,
            compare_columns: compareColumns,
            delete_missing: deleteMissing
        });
    });

    return configs;
}

function buildGpcopySyncPayload() {
    const ids = getGpcopyConnectionIds();
    const tableConfigs = collectGpcopySyncConfigs();

    for (const cfg of tableConfigs) {
        if (!cfg.key_columns.length) {
            throw new Error(`Для таблицы ${cfg.source} не указан key columns`);
        }

        if (!cfg.target) {
            throw new Error(`Для таблицы ${cfg.source} не указан target`);
        }
    }

    return {
        source_connection_id: Number(ids.sourceConnectionId || ids.source_connection_id),
        dest_connection_id: Number(ids.destConnectionId || ids.dest_connection_id),
        table_configs: tableConfigs,
        gpcopy_path: gpcopyValue("gpcopyPath", "/usr/local/gpdb/greenplum-db/bin/gpcopy"),
        jobs: Number(gpcopyValue("gpcopyJobs", "4")),
        dry_run: false
    };
}

function setGpcopySyncStatus(message, type) {
    const box = document.getElementById("gpcopySyncStatusBox");

    if (!box) {
        console.log(message);
        return;
    }

    box.className = `alert alert-${type || "info"} mt-3`;
    box.textContent = message;
}

async function previewGpcopySyncDiff() {
    try {
        const payload = buildGpcopySyncPayload();

        const response = await fetch("/api/gpcopy/sync/preview", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(payload)
        });

        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.message || "Preview failed");
        }

        setGpcopySyncStatus(
            `Preview готов. Insert=${data.total_insert}, Update=${data.total_update}, Delete=${data.total_delete}`,
            "success"
        );

        console.log("sync preview", data);

    } catch (e) {
        console.error(e);
        setGpcopySyncStatus(e.message, "danger");
    }
}

async function startGpcopySyncApply() {
    try {
        const payload = buildGpcopySyncPayload();

        if (!confirm("Внимание! На TEST будут выполнены INSERT/UPDATE/DELETE. Продолжить?")) {
            return;
        }

        const response = await fetch("/api/gpcopy/sync/apply", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(payload)
        });

        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.message || "Sync apply failed");
        }

        setGpcopySyncStatus(`Sync запущен. Job #${data.job_id}`, "success");

        if (typeof startGpcopyPolling === "function") {
            startGpcopyPolling(data.job_id);
        }

    } catch (e) {
        console.error(e);
        setGpcopySyncStatus(e.message, "danger");
    }
}

window.prepareGpcopySyncTables = prepareGpcopySyncTables;
window.previewGpcopySyncDiff = previewGpcopySyncDiff;
window.startGpcopySyncApply = startGpcopySyncApply;

/* Export functions for onclick in gpcopy.html */
window.loadGpcopyObjectTree = loadGpcopyObjectTree;
window.renderGpcopyObjectTree = renderGpcopyObjectTree;
window.countGpcopyTreeTables = countGpcopyTreeTables;
window.gpcopySetTreeStatus = gpcopySetTreeStatus;

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

window.getGpcopyConnectionIds = getGpcopyConnectionIds;

window.gpcopySetStatus = gpcopySetStatus;
window.gpcopySetDateStatus = gpcopySetDateStatus;
window.setGpcopyDateStatus = setGpcopyDateStatus;
window.setGpcopyStatus = setGpcopyStatus;

/* ============================================================
 * gpcopy v2: increment / partition-diff + schedule
 * ============================================================ */

function gpcopyV2Escape(value) {
    return String(value == null ? "" : value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}

function gpcopyV2ModeChange() {
    const mode = document.getElementById("gpcopyV2Mode").value;
    document.getElementById("gpcopyV2WatermarkBox").style.display =
        mode === "increment" ? "" : "none";
}

function gpcopyV2BuildPayload() {
    const ids = getGpcopyConnectionIds();
    const selected = getGpcopySelectedTablesSafe();
    const watermark = (document.getElementById("gpcopyV2Watermark").value || "").trim();
    const mode = document.getElementById("gpcopyV2Mode").value;

    const tables = selected.map(function (t) {
        const row = {
            schema: t.schema || t.schema_name,
            table: t.table || t.table_name,
        };
        if (mode === "increment") {
            row.watermark_column = watermark;
        }
        return row;
    });

    return {
        mode: mode,
        source_connection_id: ids.sourceConnectionId,
        dest_connection_id: ids.destConnectionId,
        tables: tables,
        jobs: getGpcopyJobs(),
    };
}

function gpcopyV2SetResult(html) {
    document.getElementById("gpcopyV2Result").innerHTML = html;
}

async function gpcopyV2Preview() {
    const p = gpcopyV2BuildPayload();

    if (!p.tables.length) {
        gpcopyV2SetResult('<span class="text-danger">Выбери таблицы слева.</span>');
        return;
    }

    try {
        if (p.mode === "increment") {
            const res = await fetch("/api/gpcopy/increment/preview", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(p),
            });
            const data = await res.json();

            if (!data.ok) {
                gpcopyV2SetResult('<span class="text-danger">' + gpcopyV2Escape(data.message) + "</span>");
                return;
            }

            gpcopyV2SetResult(
                data.tables.map(function (t) {
                    return "<div><code>" + gpcopyV2Escape(t.schema + "." + t.table) +
                        "</code> watermark=<b>" + gpcopyV2Escape(t.watermark === null ? "нет (полная догрузка)" : t.watermark) +
                        "</b><br><span class='text-muted'>" + gpcopyV2Escape(t.sql) + "</span></div>";
                }).join("")
            );
        } else {
            const first = p.tables[0];
            const res = await fetch("/api/gpcopy/partition-diff/preview", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    source_connection_id: p.source_connection_id,
                    dest_connection_id: p.dest_connection_id,
                    schema: first.schema,
                    table: first.table,
                }),
            });
            const data = await res.json();

            if (!data.ok) {
                gpcopyV2SetResult('<span class="text-danger">' + gpcopyV2Escape(data.message) + "</span>");
                return;
            }

            gpcopyV2SetResult(
                "<div class='mb-1'>Партиций к копированию: <b>" + data.to_copy.length + "</b> из " + data.partitions.length + "</div>" +
                data.partitions.map(function (r) {
                    const cls = r.action === "skip" ? "text-muted" : "text-warning";
                    return "<div class='" + cls + "'><code>" + gpcopyV2Escape(r.partition) +
                        "</code> src=" + gpcopyV2Escape(r.src_count) +
                        " dest=" + gpcopyV2Escape(r.dest_count === null ? "—" : r.dest_count) +
                        " → " + gpcopyV2Escape(r.action) + "</div>";
                }).join("")
            );
        }
    } catch (e) {
        gpcopyV2SetResult('<span class="text-danger">' + gpcopyV2Escape(e) + "</span>");
    }
}

async function gpcopyV2Start() {
    const p = gpcopyV2BuildPayload();

    if (!p.tables.length) {
        gpcopyV2SetResult('<span class="text-danger">Выбери таблицы слева.</span>');
        return;
    }

    const url = p.mode === "increment"
        ? "/api/gpcopy/increment/start"
        : "/api/gpcopy/partition-diff/start";

    const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(p),
    });
    const data = await res.json();

    gpcopyV2SetResult(
        data.ok
            ? '<span class="text-success">Job #' + parseInt(data.job_id, 10) + " запущен.</span>"
            : '<span class="text-danger">' + gpcopyV2Escape(data.message) + "</span>"
    );
}

async function gpcopyV2Schedule() {
    const p = gpcopyV2BuildPayload();
    const out = document.getElementById("gpcopyV2SchedResult");

    if (!p.tables.length) {
        out.textContent = "Выбери таблицы слева.";
        return;
    }

    const jobType = p.mode === "increment" ? "gpcopy_increment" : "gpcopy_partition_diff";

    const res = await fetch("/api/schedules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            name: (document.getElementById("gpcopyV2SchedName").value || "").trim() || ("gpcopy-" + p.mode),
            job_type: jobType,
            cron_expr: (document.getElementById("gpcopyV2SchedCron").value || "").trim(),
            config: p,
        }),
    });
    const data = await res.json();

    out.textContent = data.ok
        ? "Расписание #" + parseInt(data.id, 10) + " создано — см. страницу Schedules."
        : "Ошибка: " + (data.message || "");
}

window.gpcopyV2ModeChange = gpcopyV2ModeChange;
window.gpcopyV2Preview = gpcopyV2Preview;
window.gpcopyV2Start = gpcopyV2Start;
window.gpcopyV2Schedule = gpcopyV2Schedule;
