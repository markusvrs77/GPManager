/* Массовый выбор/настройка для 10k+ таблиц (каталог, маска, список, наборы).
 * Работает поверх состояния gpcopy.js: selectedGpcopyTables +
 * saveSelectedGpcopyTablesSafe() + gpcopyUpdateSelectedCount().
 * Все подстановки в innerHTML — через bulkEsc(). */

function bulkEsc(v) {
    return String(v == null ? "" : v)
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function bulkConnId() {
    return parseInt(document.getElementById("sourceConnectionId").value, 10);
}

async function bulkApi(url, method, body) {
    const opts = { method: method || "GET", headers: {} };
    if (body !== undefined) {
        opts.headers["Content-Type"] = "application/json";
        opts.body = JSON.stringify(body);
    }
    return (await fetch(url, opts)).json();
}

/* ---------- работа с выбором (общее состояние gpcopy.js) ---------- */

function bulkSelectedKeySet() {
    return new Set(selectedGpcopyTables.map(t =>
        ((t.schema || t.schema_name) + "." + (t.table || t.table_name)).toLowerCase()
    ));
}

function bulkAddTables(tables) {
    const seen = bulkSelectedKeySet();
    let added = 0;

    tables.forEach(function (t) {
        const key = (t.schema + "." + t.table).toLowerCase();
        if (!seen.has(key)) {
            seen.add(key);
            selectedGpcopyTables.push({
                schema: t.schema, table: t.table,
                schema_name: t.schema, table_name: t.table,
            });
            added++;
        }
    });

    saveSelectedGpcopyTablesSafe();
    gpcopyUpdateSelectedCount();
    bulkRenderSummary();
    return added;
}

function bulkClearSelection() {
    selectedGpcopyTables.length = 0;
    saveSelectedGpcopyTablesSafe();
    gpcopyUpdateSelectedCount();
    bulkRenderSummary();
}

function bulkRenderSummary() {
    const el = document.getElementById("gpcopySelectedSummary");
    if (!el) return;

    const bySchema = {};
    selectedGpcopyTables.forEach(function (t) {
        const s = t.schema || t.schema_name;
        bySchema[s] = (bySchema[s] || 0) + 1;
    });

    const schemas = Object.keys(bySchema).sort();
    el.innerHTML = schemas.length
        ? schemas.map(s =>
            "<span class='badge bg-secondary me-1'>" + bulkEsc(s) + ": " + bySchema[s] + "</span>"
          ).join("")
        : "<span class='text-muted'>ничего не выбрано</span>";
}

/* ---------- каталог: автозагрузка + схемы + поиск ---------- */

let bulkCatalogSchemas = [];

async function catalogLoad(force) {
    const info = document.getElementById("gpcopyCatalogInfo");
    info.textContent = "Загрузка каталога…";

    const data = await bulkApi(
        "/api/catalog?connection_id=" + bulkConnId() + (force ? "&force=1" : "")
    );

    if (!data.ok) {
        info.textContent = "Каталог недоступен: " + data.message;
        return;
    }

    bulkCatalogSchemas = data.schemas;
    const age = Math.round((Date.now() / 1000 - data.cached_at) / 60);
    info.textContent =
        data.total + " таблиц в " + data.schemas.length + " схемах · кэш " +
        (age < 1 ? "свежий" : age + " мин");

    document.getElementById("gpcopyCatalogSchemas").innerHTML =
        data.schemas.map(s =>
            "<div class='d-flex justify-content-between align-items-center py-1'>" +
            "<span class='small'><code>" + bulkEsc(s.schema) + "</code> " +
            "<span class='text-muted'>(" + s.total + ")</span></span>" +
            "<button class='btn btn-sm btn-outline-secondary py-0' " +
            "onclick=\"bulkSelectSchema('" + bulkEsc(s.schema) + "')\">выбрать все</button>" +
            "</div>"
        ).join("");

    bulkRenderSummary();
}

async function catalogRefresh() {
    await catalogLoad(true);
}

async function bulkSelectSchema(schema) {
    const data = await bulkApi("/api/catalog/expand-mask", "POST", {
        connection_id: bulkConnId(), mask: schema + ".*",
    });
    if (data.ok) {
        const added = bulkAddTables(data.tables);
        bulkStatus("Схема " + schema + ": добавлено " + added + " (совпало " + data.count + ")");
    }
}

let bulkSearchTimer = null;

function bulkSearchInput() {
    clearTimeout(bulkSearchTimer);
    bulkSearchTimer = setTimeout(async () => {
        const q = document.getElementById("gpcopyCatalogSearch").value.trim();
        const box = document.getElementById("gpcopyCatalogResults");

        if (q.length < 2) { box.innerHTML = ""; return; }

        const data = await bulkApi(
            "/api/catalog/search?connection_id=" + bulkConnId() + "&q=" + encodeURIComponent(q)
        );

        if (!data.ok) { box.innerHTML = ""; return; }

        const selected = bulkSelectedKeySet();
        box.innerHTML = data.tables.slice(0, 50).map(t => {
            const full = t.schema + "." + t.table;
            const isSel = selected.has(full.toLowerCase());
            return "<div class='d-flex justify-content-between align-items-center py-1 border-bottom'>" +
                "<code class='small'>" + bulkEsc(full) + "</code>" +
                (isSel
                    ? "<span class='badge bg-success'>выбрана</span>"
                    : "<button class='btn btn-sm btn-outline-primary py-0' " +
                      "onclick=\"bulkAddTables([{schema:'" + bulkEsc(t.schema) +
                      "',table:'" + bulkEsc(t.table) + "'}]);bulkSearchInput()\">+</button>") +
                "</div>";
        }).join("") +
        (data.tables.length > 50
            ? "<div class='small text-muted py-1'>…и ещё " + (data.tables.length - 50) + " — уточни запрос или используй маску</div>"
            : "");
    }, 300);
}

/* ---------- маска и вставка списка ---------- */

async function bulkSelectByMask() {
    const mask = document.getElementById("gpcopyMaskInput").value.trim();
    if (!mask) return;

    const data = await bulkApi("/api/catalog/expand-mask", "POST", {
        connection_id: bulkConnId(), mask: mask,
    });

    if (!data.ok) { bulkStatus("Ошибка: " + data.message); return; }
    if (!data.count) { bulkStatus("По маске ничего не найдено"); return; }

    if (!confirm("Маска «" + mask + "»: совпало " + data.count + " таблиц. Добавить в выбор?")) return;

    const added = bulkAddTables(data.tables);
    bulkStatus("Добавлено " + added + " из " + data.count + " (остальные уже были выбраны)");
}

async function bulkAddPastedList() {
    const text = document.getElementById("gpcopyPasteList").value;
    if (!text.trim()) return;

    const data = await bulkApi("/api/catalog/resolve-list", "POST", {
        connection_id: bulkConnId(), text: text,
    });

    if (!data.ok) { bulkStatus("Ошибка: " + data.message); return; }

    const added = bulkAddTables(data.valid);
    let msg = "Из списка добавлено " + added + " таблиц.";

    if (data.invalid.length) {
        msg += " Не найдено " + data.invalid.length + ": " +
            data.invalid.slice(0, 5).join(", ") + (data.invalid.length > 5 ? "…" : "");
    }
    bulkStatus(msg);
}

function bulkStatus(text) {
    const el = document.getElementById("gpcopyBulkStatus");
    el.textContent = text;
}

/* ---------- table sets ---------- */

async function bulkLoadSets() {
    const data = await bulkApi("/api/table-sets?connection_id=" + bulkConnId());
    const sel = document.getElementById("gpcopyTableSets");

    sel.innerHTML = "<option value=''>— наборы таблиц —</option>" +
        (data.ok ? data.sets.map(s =>
            "<option value='" + parseInt(s.id, 10) + "'>" +
            bulkEsc(s.name) + " (" + s.tables.length + ")</option>"
        ).join("") : "");
}

async function bulkApplySet() {
    const id = document.getElementById("gpcopyTableSets").value;
    if (!id) return;

    const data = await bulkApi("/api/table-sets/" + id);
    if (!data.ok) return;

    const added = bulkAddTables(data.set.tables);

    const rules = data.set.rules || {};
    if (rules.date_priority) {
        const el = document.getElementById("gpcopyDateRule");
        if (el) el.value = rules.date_priority.join(", ");
    }

    bulkStatus("Набор «" + data.set.name + "»: добавлено " + added + " таблиц");
}

async function bulkSaveSet() {
    const name = (document.getElementById("gpcopySetName").value || "").trim();
    if (!name) { bulkStatus("Укажи имя набора"); return; }
    if (!selectedGpcopyTables.length) { bulkStatus("Нечего сохранять — выбор пуст"); return; }

    const ruleEl = document.getElementById("gpcopyDateRule");
    const rules = {};
    if (ruleEl && ruleEl.value.trim()) {
        rules.date_priority = ruleEl.value.split(",").map(x => x.trim()).filter(Boolean);
    }

    const data = await bulkApi("/api/table-sets", "POST", {
        name: name,
        connection_id: bulkConnId(),
        tables: selectedGpcopyTables.map(t => ({
            schema: t.schema || t.schema_name, table: t.table || t.table_name,
        })),
        rules: rules,
    });

    if (data.ok) {
        bulkStatus("Набор «" + name + "» сохранён (#" + data.id + ")");
        document.getElementById("gpcopySetName").value = "";
        bulkLoadSets();
    } else {
        bulkStatus("Ошибка: " + data.message);
    }
}

async function bulkDeleteSet() {
    const sel = document.getElementById("gpcopyTableSets");
    if (!sel.value) return;
    if (!confirm("Удалить набор «" + sel.options[sel.selectedIndex].text + "»?")) return;

    await bulkApi("/api/table-sets/" + sel.value, "DELETE");
    bulkLoadSets();
}

/* ---------- bulk-правило дат: резолв + запуск by date ---------- */

async function bulkRunByDateRule() {
    const box = document.getElementById("gpcopyDateRuleResult");
    const priority = (document.getElementById("gpcopyDateRule").value || "")
        .split(",").map(x => x.trim()).filter(Boolean);

    if (!priority.length) { box.textContent = "Укажи приоритет колонок (через запятую)"; return; }
    if (!selectedGpcopyTables.length) { box.textContent = "Выбор пуст"; return; }

    const dateFrom = document.getElementById("gpcopyDateFrom").value.trim();
    const dateTo = document.getElementById("gpcopyDateTo").value.trim();
    if (!dateFrom || !dateTo) { box.textContent = "Укажи Date from / Date to"; return; }

    const tables = selectedGpcopyTables.map(t => ({
        schema: t.schema || t.schema_name, table: t.table || t.table_name,
    }));

    box.textContent = "Резолвлю колонки для " + tables.length + " таблиц…";

    const res = await bulkApi("/api/catalog/resolve-columns", "POST", {
        connection_id: bulkConnId(), tables: tables, priority: priority,
        fallback_any_date: true,
    });

    if (!res.ok) { box.textContent = "Ошибка: " + res.message; return; }

    const viaFallback = res.resolved.filter(r => r.via === "fallback_date").length;
    let msg = "Колонка найдена у " + res.resolved.length + " таблиц" +
        (viaFallback ? " (из них " + viaFallback + " — по фолбэку на любую date-колонку)" : "");
    if (res.missing.length) {
        msg += "; НЕ найдена у " + res.missing.length + ": " +
            res.missing.slice(0, 5).map(m => m.schema + "." + m.table).join(", ") +
            (res.missing.length > 5 ? "…" : "") + " — они будут пропущены";
    }

    if (!confirm(msg + ".\nЗапустить GPCOPY by date для " + res.resolved.length + " таблиц?")) {
        box.textContent = msg;
        return;
    }

    const ids = getGpcopyConnectionIds();
    const payload = {
        source_connection_id: ids.sourceConnectionId,
        dest_connection_id: ids.destConnectionId,
        destination_connection_id: ids.destConnectionId,
        mode: "date_filter",
        date_from: dateFrom,
        date_to: dateTo,
        jobs: getGpcopyJobs(),
        table_configs: res.resolved.map(r => ({
            source_schema: r.schema,
            source_table: r.table,
            date_column: r.column,
            date_from: dateFrom,
            date_to: dateTo,
        })),
        selected_tables: res.resolved.map(r => ({schema: r.schema, table: r.table})),
        tables: res.resolved.map(r => ({schema: r.schema, table: r.table})),
    };

    const started = await bulkApi("/api/gpcopy/start-date", "POST", payload);
    box.textContent = started.ok
        ? "Job #" + parseInt(started.job_id, 10) + " запущен для " + res.resolved.length + " таблиц."
        : "Ошибка запуска: " + (started.message || "");
}

/* ---------- PK autofill для sync ---------- */

async function bulkFillSyncKeys() {
    const rows = document.querySelectorAll("#gpcopySyncTablesBody tr[data-index]");
    if (!rows.length) { setGpcopySyncStatus("Сначала Prepare selected tables.", "warning"); return; }

    const tables = [...rows].map(r => ({schema: r.dataset.schema, table: r.dataset.table}));

    // Иерархия: PK -> уникальный индекс -> (кнопкой) вычисление по данным.
    const data = await bulkApi("/api/catalog/resolve-keys", "POST", {
        connection_id: bulkConnId(), tables: tables,
    });

    if (!data.ok) { setGpcopySyncStatus("Ключи: " + data.message, "danger"); return; }

    const keyMap = {};
    data.resolved.forEach(k => { keyMap[k.schema + "." + k.table] = k; });

    let viaPk = 0, viaUnique = 0;
    rows.forEach(function (row) {
        const input = document.getElementById("gpcopySyncKeys_" + row.dataset.index);
        const info = keyMap[row.dataset.schema + "." + row.dataset.table];
        if (input && info && !input.value.trim()) {
            input.value = info.columns.join(",");
            if (info.source === "pk") viaPk++; else viaUnique++;
        }
    });

    setGpcopySyncStatus(
        "Ключи: PK — " + viaPk + ", уникальный индекс — " + viaUnique +
        (data.unresolved.length
            ? ". Без индексов: " + data.unresolved.length +
              " — нажми «Вычислить ключ (по данным)»."
            : "."),
        "info"
    );
}

async function bulkComputeMissingKeys() {
    const rows = [...document.querySelectorAll("#gpcopySyncTablesBody tr[data-index]")]
        .filter(r => {
            const input = document.getElementById("gpcopySyncKeys_" + r.dataset.index);
            return input && !input.value.trim();
        });

    if (!rows.length) { setGpcopySyncStatus("Нет строк без ключа.", "info"); return; }

    const batch = rows.slice(0, 5); // тяжёлые запросы — считаем по 5 за раз
    let found = 0;

    for (const row of batch) {
        const full = row.dataset.schema + "." + row.dataset.table;
        setGpcopySyncStatus("Вычисляю уникальность: " + full + "…", "info");

        const data = await bulkApi("/api/catalog/compute-unique", "POST", {
            connection_id: bulkConnId(),
            schema: row.dataset.schema,
            table: row.dataset.table,
        });

        if (data.ok && data.column) {
            document.getElementById("gpcopySyncKeys_" + row.dataset.index).value = data.column;
            found++;
        }
    }

    setGpcopySyncStatus(
        "Вычислено по данным: " + found + " из " + batch.length +
        (rows.length > batch.length ? " (осталось " + (rows.length - batch.length) + " — нажми ещё раз)" : ""),
        found ? "info" : "warning"
    );
}

/* ---------- init ---------- */

document.addEventListener("DOMContentLoaded", function () {
    const src = document.getElementById("sourceConnectionId");

    if (src && src.value) {
        catalogLoad(false);
        bulkLoadSets();
    }

    if (src) {
        src.addEventListener("change", function () {
            catalogLoad(false);
            bulkLoadSets();
        });
    }

    bulkRenderSummary();
});
