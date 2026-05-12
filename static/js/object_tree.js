let currentTree = null;

async function loadObjectTree() {
    function getObjectTreeConnectionId() {
        const possibleIds = [
            "connection_id",
            "connectionId",
            "connectionSelect",
            "reorganizeConnectionId",
            "skewConnectionId",
            "connection"
        ];

        for (const id of possibleIds) {
            const el = document.getElementById(id);
            if (el && el.value) {
                return el.value;
            }
        }

        const selectByName = document.querySelector("select[name='connection_id']");
        if (selectByName && selectByName.value) {
            return selectByName.value;
        }

        return null;
    }

    const connectionId = getObjectTreeConnectionId();
    const treeContainer = document.getElementById("objectTree");
    const status = document.getElementById("objectTreeStatus");

    if (!connectionId) {
        if (status) {
            status.textContent = "Connection не выбран.";
        } else {
            alert("Connection не выбран или select connection_id не найден.");
        }
        return;
    }

    if (!treeContainer) {
        alert("Не найден блок id='objectTree' в HTML.");
        return;
    }

    treeContainer.innerHTML = "";
    currentTree = null;
    updateSelectedCount();

    if (status) {
        status.textContent = "Loading objects...";
    }

    try {
        const response = await fetch(`/api/objects/tree?connection_id=${encodeURIComponent(connectionId)}`);
        const data = await response.json();

        if (!data.ok) {
            if (status) {
                status.textContent = "Error: " + (data.message || "Failed to load objects");
            }
            return;
        }

        currentTree = data.tree;
        renderObjectTree(currentTree);

        const schemaCount = currentTree.schemas.length;
        let tableCount = 0;

        currentTree.schemas.forEach(s => {
            tableCount += s.tables.length;
        });

        if (status) {
            status.textContent = `Loaded database ${currentTree.database}: schemas=${schemaCount}, tables=${tableCount}`;
        }
    } catch (e) {
        if (status) {
            status.textContent = "Error: " + e;
        } else {
            console.error(e);
        }
    }
}

function renderObjectTree(tree) {
    const treeContainer = document.getElementById("objectTree");

    const dbNode = document.createElement("div");
    dbNode.className = "tree-node db-node";
    dbNode.dataset.name = tree.database.toLowerCase();

    const dbLine = document.createElement("div");
    dbLine.className = "tree-line";

    dbLine.innerHTML = `
        <input type="checkbox" class="db-checkbox">
        <span class="toggle">▾</span>
        <span>${escapeHtml(tree.database)}</span>
    `;

    const dbChildren = document.createElement("div");
    dbChildren.className = "tree-children";

    tree.schemas.forEach(schema => {
        const schemaNode = document.createElement("div");
        schemaNode.className = "tree-node schema-node";
        schemaNode.dataset.name = schema.schema.toLowerCase();

        const schemaLine = document.createElement("div");
        schemaLine.className = "tree-line";

        schemaLine.innerHTML = `
            <input type="checkbox" class="schema-checkbox">
            <span class="toggle">▾</span>
            <span>${escapeHtml(schema.schema)}</span>
            <span class="text-muted small">(${schema.tables.length})</span>
        `;

        const schemaChildren = document.createElement("div");
        schemaChildren.className = "tree-children";

        schema.tables.forEach(table => {
            const tableNode = document.createElement("div");
            tableNode.className = "tree-node table-node table-item";
            tableNode.dataset.name = `${schema.schema}.${table.table}`.toLowerCase();

            const relkindLabel = table.relkind === "p" ? "partitioned" : "table";

            tableNode.innerHTML = `
                <div class="tree-line">
                    <input
                        type="checkbox"
                        class="table-checkbox"
                        value="${escapeHtml(table.full_name)}"
                        data-schema="${escapeHtml(schema.schema)}"
                        data-table="${escapeHtml(table.table)}"
                    >
                    <span>${escapeHtml(table.table)}</span>
                    <span class="relkind-badge">${relkindLabel}</span>
                </div>
            `;

            schemaChildren.appendChild(tableNode);
        });

        schemaNode.appendChild(schemaLine);
        schemaNode.appendChild(schemaChildren);
        dbChildren.appendChild(schemaNode);
    });

    dbNode.appendChild(dbLine);
    dbNode.appendChild(dbChildren);

    treeContainer.innerHTML = "";
    treeContainer.appendChild(dbNode);

    initObjectTreeEvents();
}

function initObjectTreeEvents() {
    document.querySelectorAll(".toggle").forEach(toggle => {
        toggle.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();

            const node = this.closest(".tree-node");
            node.classList.toggle("collapsed");

            this.textContent = node.classList.contains("collapsed") ? "▸" : "▾";
        });
    });

    document.querySelectorAll(".db-checkbox").forEach(dbCheckbox => {
        dbCheckbox.addEventListener("change", function () {
            const dbNode = this.closest(".db-node");
            const checked = this.checked;

            dbNode.querySelectorAll("input[type='checkbox']").forEach(cb => {
                cb.checked = checked;
                cb.indeterminate = false;
            });

            updateSelectedCount();
        });
    });

    document.querySelectorAll(".schema-checkbox").forEach(schemaCheckbox => {
        schemaCheckbox.addEventListener("change", function () {
            const schemaNode = this.closest(".schema-node");
            const checked = this.checked;

            schemaNode.querySelectorAll(".table-checkbox").forEach(cb => {
                cb.checked = checked;
            });

            updateParentCheckboxes();
            updateSelectedCount();
        });
    });

    document.querySelectorAll(".table-checkbox").forEach(tableCheckbox => {
        tableCheckbox.addEventListener("change", function () {
            updateParentCheckboxes();
            updateSelectedCount();
        });
    });
}

function updateParentCheckboxes() {
    document.querySelectorAll(".schema-node").forEach(schemaNode => {
        const schemaCheckbox = schemaNode.querySelector(".schema-checkbox");
        const tableCheckboxes = schemaNode.querySelectorAll(".table-checkbox");

        const total = tableCheckboxes.length;
        const checked = [...tableCheckboxes].filter(cb => cb.checked).length;

        if (checked === 0) {
            schemaCheckbox.checked = false;
            schemaCheckbox.indeterminate = false;
        } else if (checked === total) {
            schemaCheckbox.checked = true;
            schemaCheckbox.indeterminate = false;
        } else {
            schemaCheckbox.checked = false;
            schemaCheckbox.indeterminate = true;
        }
    });

    document.querySelectorAll(".db-node").forEach(dbNode => {
        const dbCheckbox = dbNode.querySelector(".db-checkbox");
        const tableCheckboxes = dbNode.querySelectorAll(".table-checkbox");

        const total = tableCheckboxes.length;
        const checked = [...tableCheckboxes].filter(cb => cb.checked).length;

        if (checked === 0) {
            dbCheckbox.checked = false;
            dbCheckbox.indeterminate = false;
        } else if (checked === total) {
            dbCheckbox.checked = true;
            dbCheckbox.indeterminate = false;
        } else {
            dbCheckbox.checked = false;
            dbCheckbox.indeterminate = true;
        }
    });
}

function getSelectedTables() {
    return [...document.querySelectorAll(".table-checkbox:checked")]
        .map(cb => cb.value);
}

function showSelectedTables() {
    const tables = getSelectedTables();
    const output = document.getElementById("selectedTablesOutput");

    if (output) {
        output.textContent = JSON.stringify(tables, null, 2);
    }

    updateSelectedCount();
}

function updateSelectedCount() {
    const count = getSelectedTables().length;
    const el = document.getElementById("selectedCount");

    if (el) {
        el.textContent = count;
    }
}

function expandAllTree() {
    document.querySelectorAll(".tree-node").forEach(node => {
        node.classList.remove("collapsed");
    });

    document.querySelectorAll(".toggle").forEach(toggle => {
        toggle.textContent = "▾";
    });
}

function collapseAllTree() {
    document.querySelectorAll(".schema-node").forEach(node => {
        node.classList.add("collapsed");
    });

    document.querySelectorAll(".schema-node > .tree-line .toggle").forEach(toggle => {
        toggle.textContent = "▸";
    });
}

function selectAllTree() {
    document.querySelectorAll("#objectTree input[type='checkbox']").forEach(cb => {
        cb.checked = true;
        cb.indeterminate = false;
    });

    updateSelectedCount();
}

function unselectAllTree() {
    document.querySelectorAll("#objectTree input[type='checkbox']").forEach(cb => {
        cb.checked = false;
        cb.indeterminate = false;
    });

    updateSelectedCount();
}

function filterObjectTree() {
    const searchEl = document.getElementById("objectSearch");

    if (!searchEl) {
        return;
    }

    const search = searchEl.value.toLowerCase().trim();

    if (!search) {
        document.querySelectorAll(".tree-node").forEach(node => {
            node.classList.remove("hidden-by-search");
        });
        return;
    }

    document.querySelectorAll(".schema-node").forEach(schemaNode => {
        const schemaName = schemaNode.dataset.name || "";
        let schemaMatches = schemaName.includes(search);
        let hasVisibleTable = false;

        schemaNode.querySelectorAll(".table-node").forEach(tableNode => {
            const tableName = tableNode.dataset.name || "";
            const tableMatches = tableName.includes(search);

            if (schemaMatches || tableMatches) {
                tableNode.classList.remove("hidden-by-search");
                hasVisibleTable = true;
            } else {
                tableNode.classList.add("hidden-by-search");
            }
        });

        if (schemaMatches || hasVisibleTable) {
            schemaNode.classList.remove("hidden-by-search");
            schemaNode.classList.remove("collapsed");

            const toggle = schemaNode.querySelector(".toggle");
            if (toggle) {
                toggle.textContent = "▾";
            }
        } else {
            schemaNode.classList.add("hidden-by-search");
        }
    });
}

function escapeHtml(value) {
    if (value === null || value === undefined) {
        return "";
    }

    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}
