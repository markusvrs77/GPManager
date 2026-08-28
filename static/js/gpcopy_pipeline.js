/* ============================================================
   gpcopy pipeline («конвейер задачи») — логика страницы /gpcopy.
   Выбор таблиц счётчиками (без рендера тысяч строк), 5 режимов,
   умный подбор колонок/ключей, запуск сейчас или по расписанию.
   ============================================================ */
(function () {
    "use strict";

    var $ = function (id) { return document.getElementById(id); };

    function esc(s) {
        return String(s == null ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    }

    function toast(msg, type) {
        if (window.gpToast) { window.gpToast(msg, type || "info"); }
    }

    function api(url, method, body, signal) {
        var opts = { method: method || "GET" };
        if (body !== undefined) {
            opts.headers = { "Content-Type": "application/json" };
            opts.body = JSON.stringify(body);
        }
        if (signal) { opts.signal = signal; }
        return fetch(url, opts).then(function (r) { return r.json(); });
    }

    function fmtN(n) { return Number(n || 0).toLocaleString("ru-RU"); }
    function fmtBytes(b) {
        b = Number(b || 0);
        if (b >= 1024 * 1024 * 1024) { return (b / (1024 * 1024 * 1024)).toFixed(1) + " ГБ"; }
        if (b >= 1024 * 1024) { return (b / (1024 * 1024)).toFixed(1) + " МБ"; }
        if (b >= 1024) { return (b / 1024).toFixed(0) + " КБ"; }
        return b + " Б";
    }

    /* ---------------- state ---------------- */

    var state = {
        mode: "full",
        incStrategy: "watermark",  // watermark | key
        when: "now",
        sel: new Set(),            // "schema.table"
        catalog: null,             // {total, schemas:[{schema,count}], cached_at}
        incResolved: null,         // [{schema,table,column,via}]
        incOverrides: {},          // "schema.table" -> колонка, заданная вручную
        incExcluded: {},           // "schema.table" -> true (исключена из запуска)
        incWatermarks: {},         // "schema.table" -> значение watermark в dest (или null)
        incFilter: "",
        incEditing: null,          // строка с открытым инлайн-редактором колонки
        dateResolved: null,
        syncKeys: {},              // "schema.table" -> {columns:[], source}
        syncUnresolved: [],
        keyEditing: null,          // строка с открытым инлайн-редактором ключа
        keyFilter: "",             // фильтр по списку ключей
        keyLimit: 300,             // сколько строк списка ключей отрисовано
        partDiff: null,            // результат превью diff партиций
        partChecked: {},           // "schema.partition" -> true (грузить)
        partView: "all",           // all | diff | same
        partLimit: 300,            // строк отрендерено (дорендер при прокрутке)
        openRun: null,             // id запуска с раскрытой шторкой деталей
        sets: [],
        expandedSchema: null,      // раскрытая схема в модалке
        schemaTables: {},          // schema -> [{table,kind,partitions,parent}] (кэш)
        schemaFilter: "",
        schemaView: "all",         // all | parents | plain
        schemaLimit: 300,          // сколько строк отрендерено (infinite scroll)
        expandedParent: null,      // родитель, чьи партиции раскрыты
    };

    function modeName() {
        if (state.mode === "inc") {
            return state.incStrategy === "key"
                ? "инкремент по ключу (sync)"
                : "инкремент по watermark";
        }
        return {
            full: "полное копирование",
            date: "срез по датам",
            part: "только отличающиеся партиции",
        }[state.mode];
    }

    function jobType() {
        if (state.mode === "inc") {
            return state.incStrategy === "key" ? "gpcopy_sync" : "gpcopy_increment";
        }
        return {
            full: "gpcopy",
            date: "gpcopy_date",
            part: "gpcopy_partition_diff",
        }[state.mode];
    }

    var RUN_LABELS = {
        gpcopy: "полное",
        gpcopy_date: "по датам",
        gpcopy_increment: "инкремент",
        gpcopy_partition_diff: "партиции",
        gpcopy_sync: "sync",
        copy_pipe: "полное (COPY)",
    };

    function srcId() { return parseInt($("gppSrc").value, 10); }
    function dstId() { return parseInt($("gppDst").value, 10); }

    function selTables() {
        var out = [];
        state.sel.forEach(function (key) {
            var dot = key.indexOf(".");
            out.push({ schema: key.slice(0, dot), table: key.slice(dot + 1) });
        });
        return out;
    }

    function schemaCounts() {
        var by = {};
        state.sel.forEach(function (key) {
            var s = key.slice(0, key.indexOf("."));
            by[s] = (by[s] || 0) + 1;
        });
        return by;
    }

    function invalidateResolutions() {
        state.incResolved = null;
        state.incWatermarks = {};
        state.incEditing = null;
        state.dateResolved = null;
        state.syncKeys = {};
        state.syncUnresolved = [];
        state.keyEditing = null;
        state.partDiff = null;
        state.partChecked = {};
        var incList = $("gppIncList");
        if (incList) { incList.innerHTML = ""; }
        var keyList = $("gppKeyList");
        if (keyList) { keyList.innerHTML = ""; }
        var partList = $("gppPartPreview");
        if (partList) { partList.innerHTML = ""; }
    }

    /* ---------------- persistence ---------------- */

    function persistKey() { return "gpp_sel_" + srcId(); }

    function saveSel() {
        try {
            var raw = JSON.stringify(Array.from(state.sel));
            if (raw.length < 2000000) { localStorage.setItem(persistKey(), raw); }
        } catch (e) { /* quota — не критично */ }
    }

    function loadSel() {
        state.sel = new Set();
        try {
            var raw = localStorage.getItem(persistKey());
            if (raw) { JSON.parse(raw).forEach(function (k) { state.sel.add(k); }); }
        } catch (e) { /* ignore */ }
    }

    /* ---------------- render: selection ---------------- */

    function renderSelection(opts) {
        var by = schemaCounts();
        var schemas = Object.keys(by).sort();
        var total = state.sel.size;

        $("gppSelCount").textContent = total
            ? "— " + fmtN(total) + " выбрано"
            : "— ничего не выбрано";

        var html = "";
        schemas.forEach(function (s) {
            html += '<span class="gpp-chip"><b>' + esc(s) + "</b> · " + fmtN(by[s]) +
                ' <button class="x" data-schema="' + esc(s) +
                '" title="Убрать схему">✕</button></span>';
        });
        $("gppSelChips").innerHTML = html;

        $("gppSelChips").querySelectorAll(".x").forEach(function (btn) {
            btn.onclick = function () {
                var s = btn.getAttribute("data-schema");
                Array.from(state.sel).forEach(function (k) {
                    if (k.slice(0, k.indexOf(".")) === s) { state.sel.delete(k); }
                });
                onSelectionChanged();
            };
        });

        renderSummary();

        if (opts && opts.keepList) { updateModalCounters(); }
        else { renderModalCounts(); }
    }

    function onSelectionChanged(opts) {
        invalidateResolutions();
        saveSel();
        renderSelection(opts);
    }

    /* ---------------- catalog ---------------- */

    function loadCatalog(force) {
        $("gppSrcMeta").textContent = "загружаю каталог…";
        return api("/api/catalog?connection_id=" + srcId() + (force ? "&force=1" : ""))
            .then(function (d) {
                if (!d.ok) {
                    $("gppSrcMeta").innerHTML =
                        '<span style="color: var(--crit);">каталог: ' +
                        esc(d.message) + "</span>";
                    state.catalog = null;
                    return;
                }
                state.catalog = d;
                $("gppSrcMeta").innerHTML =
                    '<span class="ok">●</span> ' + fmtN(d.total) + " таблиц · " +
                    fmtN(d.schemas.length) + " схем";
                $("gppSelModalMeta").textContent =
                    "· " + fmtN(d.total) + " таблиц в источнике";
                renderModalSchemas();
            })
            .catch(function (e) {
                $("gppSrcMeta").textContent = "каталог: " + e;
            });
    }

    /* ---------------- modal: schemas ---------------- */

    function loadSchemaTables(schema) {
        return api("/api/catalog/schema-tables?connection_id=" + srcId() +
            "&schema=" + encodeURIComponent(schema))
            .then(function (d) {
                if (!d.ok) { toast(d.message, "error"); return null; }
                state.schemaTables[schema] = d.tables;
                renderModalSchemas();
                return d.tables;
            });
    }

    function ensureSchemaTables(schema) {
        return state.schemaTables[schema]
            ? Promise.resolve(state.schemaTables[schema])
            : loadSchemaTables(schema);
    }

    // Плоский список строк для рендера: главные таблицы по фильтрам,
    // после раскрытого родителя — его партиции.
    function visibleRows(schema) {
        var all = state.schemaTables[schema] || [];
        var f = (state.schemaFilter || "").toLowerCase();
        var view = state.schemaView;

        var rows = [];
        all.forEach(function (item) {
            if (item.kind === "partition") { return; } // партиции — только под родителем
            if (view === "parents" && item.kind !== "parent") { return; }
            if (view === "plain" && item.kind !== "regular") { return; }
            if (f && item.table.toLowerCase().indexOf(f) === -1) { return; }

            rows.push({ type: "main", item: item });

            if (item.kind === "parent" && state.expandedParent === item.table) {
                all.forEach(function (p) {
                    if (p.kind === "partition" && p.parent === item.table) {
                        rows.push({ type: "part", item: p });
                    }
                });
            }
        });
        return rows;
    }

    function rowHtml(schema, row) {
        var item = row.item;
        var key = schema + "." + item.table;
        var indent = row.type === "part" ? ' style="margin-left: 26px;"' : "";
        var badge = "";

        if (item.kind === "parent") {
            var open = state.expandedParent === item.table;
            badge = ' <span class="gpp-prt-badge" data-prt="' + esc(item.table) +
                '" title="Показать партиции">' + fmtN(item.partitions) +
                " прт " + (open ? "▾" : "▸") + "</span>";
        }

        return '<label class="gpp-tbl-row"' + indent +
            '><input type="checkbox" data-key="' + esc(key) + '"' +
            (state.sel.has(key) ? " checked" : "") + "> " + esc(item.table) +
            badge + "</label>";
    }

    function wireRows(container, schema) {
        container.querySelectorAll('input[type="checkbox"]:not([data-wired])')
            .forEach(function (cb) {
                cb.setAttribute("data-wired", "1");
                cb.onchange = function () {
                    var key = cb.getAttribute("data-key");
                    if (cb.checked) { state.sel.add(key); }
                    else { state.sel.delete(key); }
                    // список не перерисовываем: галочка держит себя сама,
                    // обновляем только счётчики — прокрутка остаётся на месте
                    onSelectionChanged({ keepList: true });
                };
            });
        container.querySelectorAll(".gpp-prt-badge:not([data-wired])")
            .forEach(function (b) {
                b.setAttribute("data-wired", "1");
                b.onclick = function (ev) {
                    ev.preventDefault();
                    ev.stopPropagation();
                    var t = b.getAttribute("data-prt");
                    state.expandedParent = state.expandedParent === t ? null : t;
                    renderModalSchemas();
                };
            });
    }

    var CHUNK = 300;

    function renderSchemaPanel(schema) {
        var all = state.schemaTables[schema];
        if (!all) { return '<div class="gpp-hint" style="margin: 0;">Загружаю таблицы…</div>'; }

        var parents = 0, plain = 0;
        all.forEach(function (i) {
            if (i.kind === "parent") { parents += 1; }
            if (i.kind === "regular") { plain += 1; }
        });

        var chip = function (view, label, n) {
            return '<button class="gpp-btn sm gpp-view' +
                (state.schemaView === view ? " primary" : "") +
                '" data-view="' + view + '">' + label + " · " + fmtN(n) + "</button>";
        };

        var rows = visibleRows(schema);
        var top = rows.slice(0, state.schemaLimit);

        return '<div class="gpp-chips" style="margin-bottom: 6px;">' +
            chip("all", "Все", parents + plain) +
            chip("parents", "С партициями", parents) +
            chip("plain", "Без партиций", plain) +
            "</div>" +
            '<input class="gpp-sc-filter" id="gppScFilter" ' +
            'placeholder="Фильтр…" value="' + esc(state.schemaFilter) + '">' +
            '<div class="gpp-tbl-list" id="gppTblList">' +
            top.map(function (r) { return rowHtml(schema, r); }).join("") +
            "</div>" +
            (!rows.length ? '<div class="gpp-hint">Ничего не найдено.</div>' : "") +
            (rows.length > top.length
                ? '<div class="gpp-hint" id="gppTblMore">Показано ' + fmtN(top.length) +
                  " из " + fmtN(rows.length) + " — прокрути список ниже.</div>"
                : "");
    }

    // перерисовка блока без потери прокрутки (иначе список прыгает наверх)
    function setHtml(box, html) {
        if (!box) { return; }

        if (window.gpKeepScroll) {
            window.gpKeepScroll(box, function () { box.innerHTML = html; });
        } else {
            box.innerHTML = html;
        }
    }

    // «выбрать все» по схеме — без партиций (родитель уже включает их данные)
    function wireSchemaButtons(scope) {
        scope.querySelectorAll("button[data-schema]").forEach(function (btn) {
            btn.onclick = function (ev) {
                ev.stopPropagation();
                var schema = btn.getAttribute("data-schema");
                if (btn.getAttribute("data-act") === "unsel") {
                    Array.from(state.sel).forEach(function (k) {
                        if (k.slice(0, k.indexOf(".")) === schema) { state.sel.delete(k); }
                    });
                    onSelectionChanged();
                    return;
                }
                btn.disabled = true;
                ensureSchemaTables(schema).then(function (tables) {
                    btn.disabled = false;
                    if (!tables) { return; }
                    tables.forEach(function (t) {
                        if (t.kind !== "partition") {
                            state.sel.add(schema + "." + t.table);
                        }
                    });
                    onSelectionChanged();
                });
            };
        });
    }

    function schemaActionHtml(schema, selN, total) {
        return (selN ? '<span class="selcnt">выбрано ' + fmtN(selN) + "</span> " : "") +
            '<button class="gpp-btn sm" data-schema="' + esc(schema) +
            '" data-act="' + (selN >= total ? "unsel" : "sel") + '">' +
            (selN >= total ? "снять" : "выбрать все") + "</button>";
    }

    // счётчики без перерисовки списка — чтобы галочка не роняла прокрутку
    function updateModalCounters() {
        var by = schemaCounts();

        $("gppSelTotal").textContent = fmtN(state.sel.size);
        $("gppSelTotalSchemas").textContent =
            "таблиц в " + fmtN(Object.keys(by).length) + " схемах";

        var box = $("gppSelSchemas");
        if (!box || !state.catalog) { return; }

        var totals = {};
        state.catalog.schemas.forEach(function (s) { totals[s.schema] = s.total; });

        box.querySelectorAll(".gpp-sc[data-open]").forEach(function (row) {
            var schema = row.getAttribute("data-open");
            var right = row.lastElementChild;
            if (!right) { return; }

            right.innerHTML = schemaActionHtml(
                schema, by[schema] || 0, totals[schema] || 0);
            wireSchemaButtons(right);
        });
    }

    function renderModalSchemas() {
        var box = $("gppSelSchemas");
        if (!state.catalog) { box.innerHTML = ""; return; }

        // перерисовка не должна ронять прокрутку списка
        if (window.gpKeepScroll) {
            window.gpKeepScroll(box, function () { paintModalSchemas(box); });
        } else {
            paintModalSchemas(box);
        }
    }

    function paintModalSchemas(box) {
        var by = schemaCounts();
        var html = "";
        state.catalog.schemas.forEach(function (s) {
            var selN = by[s.schema] || 0;
            var open = state.expandedSchema === s.schema;
            html += '<div class="gpp-scw">' +
                '<div class="gpp-sc' + (open ? " open" : "") + '" data-open="' +
                esc(s.schema) + '"><span><span class="chev">' + (open ? "▼" : "▶") +
                "</span><code>" + esc(s.schema) + "</code> " +
                '<span class="cnt">' + fmtN(s.total) + "</span></span><span>" +
                schemaActionHtml(s.schema, selN, s.total) + "</span></div>" +
                (open ? '<div class="gpp-sc-panel">' + renderSchemaPanel(s.schema) + "</div>" : "") +
                "</div>";
        });
        box.innerHTML = html;

        // клик по строке схемы — раскрыть/свернуть список таблиц
        box.querySelectorAll(".gpp-sc[data-open]").forEach(function (row) {
            row.onclick = function (ev) {
                if (ev.target.closest("button")) { return; }
                var schema = row.getAttribute("data-open");
                state.expandedSchema = state.expandedSchema === schema ? null : schema;
                state.schemaFilter = "";
                state.schemaView = "all";
                state.schemaLimit = CHUNK;
                state.expandedParent = null;
                if (state.expandedSchema && !state.schemaTables[schema]) {
                    loadSchemaTables(schema);
                }
                renderModalSchemas();
            };
        });

        wireSchemaButtons(box);

        // чипы «Все / С партициями / Без партиций»
        box.querySelectorAll(".gpp-view").forEach(function (b) {
            b.onclick = function () {
                state.schemaView = b.getAttribute("data-view");
                state.schemaLimit = CHUNK;
                renderModalSchemas();
            };
        });

        // фильтр внутри раскрытой схемы (перерисовываем, сохраняя фокус и курсор)
        var filter = $("gppScFilter");
        if (filter) {
            filter.oninput = function () {
                state.schemaFilter = filter.value;
                state.schemaLimit = CHUNK;
                renderModalSchemas();
                var el = $("gppScFilter");
                if (el) {
                    el.focus();
                    el.setSelectionRange(el.value.length, el.value.length);
                }
            };
        }

        // строки: чекбоксы + бейджи партиций; докрутка — дорендер следующего чанка
        var list = $("gppTblList");
        if (list) {
            wireRows(list, state.expandedSchema);
            list.onscroll = function () {
                if (list.scrollTop + list.clientHeight < list.scrollHeight - 80) { return; }
                var rows = visibleRows(state.expandedSchema);
                if (state.schemaLimit >= rows.length) { return; }

                var nextRows = rows.slice(state.schemaLimit, state.schemaLimit + CHUNK);
                state.schemaLimit += CHUNK;
                list.insertAdjacentHTML("beforeend", nextRows.map(function (r) {
                    return rowHtml(state.expandedSchema, r);
                }).join(""));
                wireRows(list, state.expandedSchema);

                var more = $("gppTblMore");
                if (more) {
                    var shown = Math.min(state.schemaLimit, rows.length);
                    more.textContent = shown >= rows.length
                        ? ""
                        : "Показано " + fmtN(shown) + " из " + fmtN(rows.length) +
                          " — прокрути список ниже.";
                }
            };
        }
    }

    function renderModalCounts() {
        var by = schemaCounts();
        $("gppSelTotal").textContent = fmtN(state.sel.size);
        $("gppSelTotalSchemas").textContent =
            "таблиц в " + fmtN(Object.keys(by).length) + " схемах";
        renderModalSchemas();
    }

    /* ---------------- modal: search / mask ---------------- */

    var searchTimer = null;

    function isMask(q) { return q.indexOf("*") !== -1 || q.indexOf("?") !== -1; }

    function doSearch() {
        var q = $("gppSelSearch").value.trim();
        var box = $("gppSelSearchResults");

        if (q.length < 2) { box.innerHTML = ""; return; }

        if (isMask(q)) {
            box.innerHTML = '<div class="gpp-hint">Маска — нажми Enter, чтобы выбрать все совпадения.</div>';
            return;
        }

        api("/api/catalog/search?connection_id=" + srcId() + "&q=" + encodeURIComponent(q))
            .then(function (d) {
                if (!d.ok) { box.innerHTML = ""; return; }
                var top = d.tables.slice(0, 50);
                var html = top.map(function (t) {
                    var key = t.schema + "." + t.table;
                    var inSel = state.sel.has(key);
                    return '<div class="hit' + (inSel ? " in" : "") + '" data-key="' + esc(key) +
                        '"><span>' + esc(key) + "</span><span>" +
                        (inSel ? "✓" : "+") + "</span></div>";
                }).join("");
                if (d.tables.length > top.length) {
                    html += '<div class="gpp-hint">…и ещё ' +
                        fmtN(d.tables.length - top.length) +
                        " — уточни запрос или нажми Enter, чтобы выбрать все " +
                        fmtN(d.tables.length) + "</div>";
                }
                box.innerHTML = html || '<div class="gpp-hint">Ничего не найдено.</div>';
                box.querySelectorAll(".hit").forEach(function (el) {
                    el.onclick = function () {
                        var key = el.getAttribute("data-key");
                        if (state.sel.has(key)) { state.sel.delete(key); }
                        else { state.sel.add(key); }
                        onSelectionChanged();
                        doSearch();
                    };
                });
            });
    }

    function selectAllFound() {
        var q = $("gppSelSearch").value.trim();
        if (q.length < 2) { return; }

        if (isMask(q)) {
            api("/api/catalog/expand-mask", "POST", { connection_id: srcId(), mask: q })
                .then(function (d) {
                    if (!d.ok) { toast(d.message, "error"); return; }
                    d.tables.forEach(function (t) { state.sel.add(t.schema + "." + t.table); });
                    toast("Маска: добавлено " + fmtN(d.count) + " таблиц", "success");
                    onSelectionChanged();
                });
            return;
        }

        api("/api/catalog/search?connection_id=" + srcId() + "&q=" + encodeURIComponent(q))
            .then(function (d) {
                if (!d.ok) { return; }
                d.tables.forEach(function (t) { state.sel.add(t.schema + "." + t.table); });
                toast("Добавлено " + fmtN(d.tables.length) + " таблиц", "success");
                onSelectionChanged();
                doSearch();
            });
    }

    /* ---------------- modal: paste list ---------------- */

    function applyPaste() {
        var text = $("gppSelPaste").value;
        if (!text.trim()) { return; }
        api("/api/catalog/resolve-list", "POST", { connection_id: srcId(), text: text })
            .then(function (d) {
                if (!d.ok) { $("gppSelPasteResult").textContent = d.message; return; }
                d.valid.forEach(function (t) { state.sel.add(t.schema + "." + t.table); });
                var msg = "Добавлено: " + fmtN(d.valid.length);
                if (d.invalid.length) {
                    msg += ' · <span class="warn">не найдено: ' + fmtN(d.invalid.length) +
                        " (" + d.invalid.slice(0, 3).map(esc).join(", ") +
                        (d.invalid.length > 3 ? "…" : "") + ")</span>";
                }
                $("gppSelPasteResult").innerHTML = msg;
                onSelectionChanged();
            });
    }

    /* ---------------- table sets ---------------- */

    function loadSets() {
        api("/api/table-sets?connection_id=" + srcId()).then(function (d) {
            if (!d.ok) { return; }
            state.sets = d.sets || [];
            var html = state.sets.map(function (s) {
                return '<span class="gpp-chip" style="cursor: pointer;" data-set="' + s.id +
                    '" title="Загрузить набор"><b>' + esc(s.name) + "</b> · " +
                    fmtN(s.tables_count != null ? s.tables_count : (s.tables || []).length) +
                    ' <button class="x" data-del="' + s.id +
                    '" title="Удалить набор">✕</button></span>';
            }).join("");
            $("gppSelSets").innerHTML = html || '<span class="gpp-hint">Пока нет наборов.</span>';

            $("gppSelSets").querySelectorAll("[data-set]").forEach(function (el) {
                el.onclick = function (ev) {
                    if (ev.target.hasAttribute("data-del")) { return; }
                    api("/api/table-sets/" + el.getAttribute("data-set")).then(function (d2) {
                        if (!d2.ok) { toast(d2.message, "error"); return; }
                        (d2.set.tables || []).forEach(function (t) {
                            state.sel.add(t.schema + "." + t.table);
                        });
                        toast("Набор «" + d2.set.name + "» загружен", "success");
                        onSelectionChanged();
                    });
                };
            });
            $("gppSelSets").querySelectorAll("[data-del]").forEach(function (btn) {
                btn.onclick = function (ev) {
                    ev.stopPropagation();
                    var id = btn.getAttribute("data-del");
                    var doDel = function () {
                        api("/api/table-sets/" + id, "DELETE").then(loadSets);
                    };
                    if (window.gpConfirm) {
                        window.gpConfirm("Удалить набор?").then(function (yes) {
                            if (yes) { doDel(); }
                        });
                    } else if (confirm("Удалить набор?")) { doDel(); }
                };
            });
        });
    }

    function saveSet(nameInputId) {
        var name = $(nameInputId).value.trim();
        if (!name) { toast("Укажи имя набора", "warning"); return; }
        if (!state.sel.size) { toast("Сначала выбери таблицы", "warning"); return; }
        api("/api/table-sets", "POST", {
            name: name,
            connection_id: srcId(),
            tables: selTables(),
        }).then(function (d) {
            if (!d.ok) { toast(d.message, "error"); return; }
            toast("Набор «" + name + "» сохранён (" + fmtN(state.sel.size) + ")", "success");
            $(nameInputId).value = "";
            loadSets();
        });
    }

    /* ---------------- smart resolution: inc / date columns ---------------- */

    function parsePriority(inputId) {
        return $(inputId).value.split(",")
            .map(function (s) { return s.trim(); })
            .filter(Boolean);
    }

    function checkColumns(priorityInputId, hintId, target) {
        var tables = selTables();
        var hint = $(hintId);

        if (!tables.length) {
            hint.innerHTML = '<span class="warn">Сначала выбери таблицы (шаг 1).</span>';
            return Promise.resolve(null);
        }
        var priority = parsePriority(priorityInputId);
        if (!priority.length) {
            hint.innerHTML = '<span class="warn">Укажи хотя бы одну колонку.</span>';
            return Promise.resolve(null);
        }

        hint.textContent = "Проверяю " + fmtN(tables.length) + " таблиц…";

        var ac = new AbortController();
        var op = opStart("Проверяю колонки у " + fmtN(tables.length) + " таблиц",
            function () { ac.abort(); });

        return api("/api/catalog/resolve-columns", "POST", {
            connection_id: srcId(),
            tables: tables,
            priority: priority,
            fallback_any_date: true,
        }, ac.signal).catch(function (e) {
            opEnd(op);
            if (op.cancelled) { hint.textContent = "Проверка отменена."; return null; }
            throw e;
        }).then(function (d) {
            opEnd(op);
            if (!d) { return null; }
            if (!d.ok) {
                hint.innerHTML = '<span class="bad">' + esc(d.message) + "</span>";
                return null;
            }

            var viaFallback = d.resolved.filter(function (r) {
                return r.via === "fallback_date";
            }).length;
            var msg = '<span class="good">✓ колонка найдётся у ' + fmtN(d.resolved.length) +
                " из " + fmtN(tables.length) + "</span>";
            if (viaFallback) {
                msg += " (из них " + fmtN(viaFallback) + " — по фолбэку на первую date-колонку)";
            }
            if (d.missing.length) {
                msg += ' · <span class="warn">' + fmtN(d.missing.length) +
                    " без колонки — будут пропущены (" +
                    d.missing.slice(0, 3).map(function (m) {
                        return esc(m.schema + "." + m.table);
                    }).join(", ") +
                    (d.missing.length > 3 ? "…" : "") + ")</span>";
            }
            hint.innerHTML = msg;
            state[target] = d.resolved;
            if (target === "incResolved") {
                renderIncList();
                renderIncSummaryHint();
            }
            return d;
        });
    }

    /* ---------------- increment: per-table watermark list ---------------- */

    var INC_BADGES = {
        priority: ["pk", "приоритет"],
        fallback_date: ["comp", "date-фолбэк"],
        manual: ["man", "вручную"],
    };

    function incRowsData() {
        var map = {};
        (state.incResolved || []).forEach(function (r) {
            map[r.schema + "." + r.table] = r;
        });
        return selTables().map(function (t) {
            var key = t.schema + "." + t.table;
            var ov = state.incOverrides[key];
            var res = map[key];
            return {
                key: key,
                schema: t.schema,
                table: t.table,
                column: ov || (res ? res.column : null),
                via: ov ? "manual" : (res ? res.via : null),
                excluded: !!state.incExcluded[key],
                wm: state.incWatermarks[key],
            };
        });
    }

    function renderIncList() {
        var box = $("gppIncList");
        if (!box || !state.incResolved) { return; }

        var rows = incRowsData();
        var f = (state.incFilter || "").toLowerCase();
        if (f) {
            rows = rows.filter(function (r) {
                return r.key.toLowerCase().indexOf(f) !== -1;
            });
        }
        // проблемные — наверх
        rows.sort(function (a, b) {
            if (!a.column !== !b.column) { return a.column ? 1 : -1; }
            return a.key < b.key ? -1 : 1;
        });

        var top = rows.slice(0, 200);
        setHtml(box, top.map(function (r) {
            var colHtml;
            if (state.incEditing === r.key) {
                colHtml = '<input class="colinput" id="gppIncColEdit" value="' +
                    esc(r.column || "") + '">';
            } else {
                colHtml = r.column
                    ? '<span class="cols">→ ' + esc(r.column) + "</span>"
                    : '<span class="cols" style="color: var(--crit);">колонка не найдена</span>';
                colHtml += ' <span class="edit" data-inc-e="' + esc(r.key) +
                    '" title="Задать колонку вручную">✎</span>';
            }

            var right = "";
            if (r.wm !== undefined) {
                right += '<span class="gpp-wm-chip">' +
                    (r.wm === null ? "пусто → полная догрузка" : "wm: " + esc(String(r.wm))) +
                    "</span> ";
            }
            var b = INC_BADGES[r.via];
            right += r.column
                ? '<span class="gpp-key-badge ' + b[0] + '">' + b[1] + "</span>"
                : '<span class="gpp-key-badge none">нет колонки</span>';

            return '<div class="gpp-key-row' + (r.excluded ? " off" : "") +
                '"><span><input type="checkbox" data-inc-x="' + esc(r.key) + '"' +
                (r.excluded ? "" : " checked") + ">" + esc(r.key) + " " + colHtml +
                "</span><span>" + right + "</span></div>";
        }).join("") +
        (rows.length > top.length
            ? '<div class="gpp-hint">…и ещё ' + fmtN(rows.length - top.length) +
              " — используй фильтр.</div>"
            : ""));

        $("gppIncFilter").style.display = incRowsData().length > 15 ? "" : "none";

        // чекбоксы включения
        box.querySelectorAll("input[data-inc-x]").forEach(function (cb) {
            cb.onchange = function () {
                var k = cb.getAttribute("data-inc-x");
                if (cb.checked) { delete state.incExcluded[k]; }
                else { state.incExcluded[k] = true; }
                renderIncList();
                renderIncSummaryHint();
            };
        });

        // инлайн-редактор колонки
        box.querySelectorAll(".edit[data-inc-e]").forEach(function (el) {
            el.onclick = function () {
                state.incEditing = el.getAttribute("data-inc-e");
                renderIncList();
                var inp = $("gppIncColEdit");
                if (inp) { inp.focus(); inp.select(); }
            };
        });

        var inp = $("gppIncColEdit");
        if (inp) {
            var apply = function () {
                var key = state.incEditing;
                var val = inp.value.trim();
                state.incEditing = null;
                if (!val) { renderIncList(); return; }

                var dot = key.indexOf(".");
                // проверяем, что колонка существует в этой таблице
                api("/api/catalog/resolve-columns", "POST", {
                    connection_id: srcId(),
                    tables: [{ schema: key.slice(0, dot), table: key.slice(dot + 1) }],
                    priority: [val],
                }).then(function (d) {
                    if (d.ok && d.resolved && d.resolved.length) {
                        state.incOverrides[key] = val;
                        delete state.incWatermarks[key];
                    } else {
                        toast("Колонки «" + val + "» нет в " + key, "error");
                    }
                    renderIncList();
                    renderIncSummaryHint();
                });
            };
            inp.onkeydown = function (e) {
                if (e.key === "Enter") { apply(); }
                if (e.key === "Escape") { state.incEditing = null; renderIncList(); }
            };
            inp.onblur = apply;
        }
    }

    function renderIncSummaryHint() {
        var rows = incRowsData();
        var run = rows.filter(function (r) { return !r.excluded && r.column; }).length;
        var noCol = rows.filter(function (r) { return !r.column; }).length;
        var off = rows.filter(function (r) { return r.excluded; }).length;

        var msg = 'К копированию: <span class="good">' + fmtN(run) + "</span> из " +
            fmtN(rows.length);
        if (noCol) { msg += ' · <span class="warn">без колонки: ' + fmtN(noCol) + "</span>"; }
        if (off) { msg += " · исключено: " + fmtN(off); }
        $("gppIncHint").innerHTML = msg;
    }

    function previewWatermarks() {
        if (!state.incResolved) {
            checkColumns("gppIncPriority", "gppIncHint", "incResolved")
                .then(function (d) { if (d) { previewWatermarks(); } });
            return;
        }
        var rows = incRowsData().filter(function (r) { return !r.excluded && r.column; });
        var CAPW = 50;
        var todo = rows.slice(0, CAPW);
        if (!todo.length) { toast("Нет таблиц с колонкой", "warning"); return; }

        var btn = $("gppIncWmPreview");
        btn.disabled = true;
        $("gppIncHint").textContent =
            "Читаю watermark в назначении (" + fmtN(todo.length) + " таблиц)…";

        var ac = new AbortController();
        var op = opStart("Watermark-превью: " + fmtN(todo.length) + " таблиц",
            function () { ac.abort(); });

        api("/api/gpcopy/increment/preview", "POST", {
            source_connection_id: srcId(),
            dest_connection_id: dstId(),
            tables: todo.map(function (r) {
                return { schema: r.schema, table: r.table, watermark_column: r.column };
            }),
        }, ac.signal).then(function (d) {
            opEnd(op);
            btn.disabled = false;
            if (!d.ok) { toast(d.message, "error"); renderIncSummaryHint(); return; }
            d.tables.forEach(function (t) {
                state.incWatermarks[t.schema + "." + t.table] = t.watermark;
            });
            renderIncList();
            var empty = d.tables.filter(function (t) { return t.watermark === null; }).length;
            renderIncSummaryHint();
            $("gppIncHint").innerHTML += " · превью: у " + fmtN(empty) +
                " watermark пуст (полная догрузка)" +
                (rows.length > CAPW ? " · показаны первые " + CAPW : "");
        }).catch(function () {
            opEnd(op);
            btn.disabled = false;
            if (op.cancelled) { $("gppIncHint").textContent = "Превью отменено."; return; }
            renderIncSummaryHint();
        });
    }

    /* ---------------- smart resolution: sync keys ---------------- */

    var KEY_BADGES = {
        pk: ["pk", "PK"],
        unique_index: ["uniq", "уник. индекс"],
        computed: ["comp", "вычислен"],
        sampled: ["comp", "по сэмплу"],
        manual: ["man", "вручную"],
    };

    function keyRowHtml(key, info) {
        var mid;
        if (state.keyEditing === key) {
            mid = ' <input class="colinput" id="gppKeyColEdit" style="width: 240px;" value="' +
                esc(info ? info.columns.join(", ") : "") +
                '" placeholder="col1, col2">';
        } else {
            mid = info
                ? ' <span class="cols">→ ' + esc(info.columns.join(", ")) + "</span>"
                : "";
            mid += ' <span class="edit" data-key-e="' + esc(key) +
                '" title="Задать ключевые колонки вручную (через запятую)">✎</span>';
        }

        var badge = info
            ? (function () {
                var b = KEY_BADGES[info.source] || ["", info.source];
                return '<span class="gpp-key-badge ' + b[0] + '">' + esc(b[1]) + "</span>";
            })()
            : '<span class="gpp-key-badge none">нет ключа</span>';

        return '<div class="gpp-key-row"><span>' + esc(key) + mid +
            "</span>" + badge + "</div>";
    }

    var KEY_CHUNK = 300;

    // весь список: проблемные наверх, дальше найденные ключи
    function keyRowsData() {
        var rows = (state.syncUnresolved || []).map(function (t) {
            return { key: t.schema + "." + t.table, info: null };
        });

        Object.keys(state.syncKeys).sort().forEach(function (k) {
            rows.push({ key: k, info: state.syncKeys[k] });
        });

        var f = (state.keyFilter || "").toLowerCase();

        return f
            ? rows.filter(function (r) {
                return r.key.toLowerCase().indexOf(f) !== -1;
            })
            : rows;
    }

    function keyMoreText(shown, total) {
        return shown >= total
            ? "Показаны все " + fmtN(total)
            : "Показано " + fmtN(shown) + " из " + fmtN(total) +
              " — прокрути список ниже";
    }

    function renderKeyList() {
        var box = $("gppKeyList");
        if (!box) { return; }

        var all = keyRowsData();
        var top = all.slice(0, state.keyLimit || KEY_CHUNK);
        var filter = $("gppKeyFilter");

        if (filter) {
            filter.style.display =
                (Object.keys(state.syncKeys).length +
                 (state.syncUnresolved || []).length) > 15 ? "" : "none";
        }

        setHtml(box, top.map(function (r) {
            return keyRowHtml(r.key, r.info);
        }).join("") || '<div class="gpp-hint">Ничего не найдено.</div>');

        var more = $("gppKeyMore");

        if (more) {
            more.textContent = all.length
                ? keyMoreText(top.length, all.length) : "";
        }

        // докрутили до низа — дорисовываем следующий кусок
        box.onscroll = function () {
            if (box.scrollTop + box.clientHeight < box.scrollHeight - 80) {
                return;
            }

            var shown = state.keyLimit || KEY_CHUNK;

            if (shown >= all.length) { return; }

            state.keyLimit = shown + KEY_CHUNK;
            box.insertAdjacentHTML("beforeend", all
                .slice(shown, state.keyLimit)
                .map(function (r) { return keyRowHtml(r.key, r.info); })
                .join(""));
            wireKeyRows(box);

            if (more) {
                more.textContent = keyMoreText(
                    Math.min(state.keyLimit, all.length), all.length);
            }
        };

        wireKeyRows(box);
    }

    function wireKeyRows(box) {
        box.querySelectorAll(".edit[data-key-e]:not([data-wired])")
            .forEach(function (el) {
                el.setAttribute("data-wired", "1");
                el.onclick = function () {
                    state.keyEditing = el.getAttribute("data-key-e");
                    renderKeyList();

                    var edit = $("gppKeyColEdit");

                    if (edit) { edit.focus(); edit.select(); }
                };
            });

        var inp = $("gppKeyColEdit");
        if (inp) {
            var apply = function () {
                var key = state.keyEditing;
                if (!key) { return; }
                state.keyEditing = null;
                var cols = inp.value.split(",")
                    .map(function (s) { return s.trim(); })
                    .filter(Boolean);
                if (!cols.length) { renderKeyList(); return; }

                var dot = key.indexOf(".");
                var tbl = { schema: key.slice(0, dot), table: key.slice(dot + 1) };

                // каждая колонка должна существовать в таблице
                Promise.all(cols.map(function (c) {
                    return api("/api/catalog/resolve-columns", "POST", {
                        connection_id: srcId(), tables: [tbl], priority: [c],
                    });
                })).then(function (results) {
                    var missing = cols.filter(function (_c, i) {
                        var d = results[i];
                        return !(d.ok && d.resolved && d.resolved.length);
                    });
                    if (missing.length) {
                        toast("Нет колонок в " + key + ": " + missing.join(", "), "error");
                    } else {
                        state.syncKeys[key] = { columns: cols, source: "manual" };
                        state.syncUnresolved = (state.syncUnresolved || [])
                            .filter(function (t) {
                                return t.schema + "." + t.table !== key;
                            });
                    }
                    renderKeyList();
                });
            };
            inp.onkeydown = function (e) {
                if (e.key === "Enter") { apply(); }
                if (e.key === "Escape") { state.keyEditing = null; renderKeyList(); }
            };
            inp.onblur = apply;
        }
    }

    function resolveSyncKeys() {
        var tables = selTables();
        var hint = $("gppSyncHint");
        if (!tables.length) {
            hint.innerHTML = '<span class="warn">Сначала выбери таблицы (шаг 1).</span>';
            return Promise.resolve(null);
        }

        hint.textContent = "Ищу ключи у " + fmtN(tables.length) + " таблиц…";

        var ac = new AbortController();
        var op = opStart("Ключи (PK + уник. индексы) у " + fmtN(tables.length) + " таблиц",
            function () { ac.abort(); });

        return api("/api/catalog/resolve-keys", "POST", {
            connection_id: srcId(),
            tables: tables,
        }, ac.signal).catch(function (e) {
            opEnd(op);
            if (op.cancelled) { hint.textContent = "Поиск ключей отменён."; return null; }
            throw e;
        }).then(function (d) {
            opEnd(op);
            if (!d) { return null; }
            if (!d.ok) {
                hint.innerHTML = '<span class="bad">' + esc(d.message) + "</span>";
                return null;
            }

            state.syncKeys = {};
            var viaPk = 0, viaUniq = 0;
            d.resolved.forEach(function (r) {
                state.syncKeys[r.schema + "." + r.table] =
                    { columns: r.columns, source: r.source };
                if (r.source === "pk") { viaPk += 1; } else { viaUniq += 1; }
            });
            state.syncUnresolved = d.unresolved;

            var msg = '<span class="good">✓ ключ найден у ' + fmtN(d.resolved.length) +
                " из " + fmtN(tables.length) + "</span> (PK: " + fmtN(viaPk) +
                ", уник. индекс: " + fmtN(viaUniq) + ")";
            if (d.unresolved.length) {
                msg += ' · <span class="warn">' + fmtN(d.unresolved.length) +
                    " без ключа</span> — нажми «Найти уникальную колонку (по данным)»";
            }
            hint.innerHTML = msg;
            renderKeyList();
            return d;
        });
    }

    // сколько таблиц проверять за заход: запросы тяжёлые, поэтому объём
    // выбирает пользователь, а «все» гоняем той же очередью
    function computeCap() {
        var sel = $("gppComputeCap");
        var raw = sel ? sel.value : "100";

        return raw === "all" ? 0 : (parseInt(raw, 10) || 100);
    }

    var COMPUTE_PARALLEL = 3;

    function computeMissingKeys() {
        var hint = $("gppSyncHint");
        var pending = state.syncUnresolved || [];

        if (!pending.length) {
            hint.innerHTML = "Нет таблиц без ключа. Сначала нажми «Ключи: PK + уник. индексы».";
            return;
        }

        var cap = computeCap();
        var todo = cap ? pending.slice(0, cap) : pending.slice();
        var found = 0, checked = 0, next = 0, active = 0, stopped = false;
        var controllers = [];

        $("gppSyncCompute").disabled = true;

        var op = opStart("Вычисляю уникальные колонки", function () {
            stopped = true;
            controllers.forEach(function (ac) {
                try { ac.abort(); } catch (e) { /* уже завершился */ }
            });
            $("gppSyncCompute").disabled = false;
            hint.innerHTML = "Вычисление отменено (проверено " +
                fmtN(checked) + " из " + fmtN(todo.length) + ").";
            renderKeyList();
        });

        function finish() {
            opEnd(op);
            $("gppSyncCompute").disabled = false;
            state.syncUnresolved = pending.filter(function (t) {
                return !state.syncKeys[t.schema + "." + t.table];
            });

            var msg = "Вычисление: проверено " + fmtN(checked) +
                ', <span class="good">ключ найден у ' + fmtN(found) +
                "</span>";

            if (state.syncUnresolved.length) {
                msg += ' · <span class="warn">без ключа: ' +
                    fmtN(state.syncUnresolved.length) +
                    " (будут пропущены)</span>";
            }

            var left = pending.length - todo.length;

            if (left > 0) {
                msg += " · осталось непроверенных: " + fmtN(left) +
                    ' <button class="gpp-btn sm" id="gppComputeMore">' +
                    "Проверить дальше</button>";
            }

            hint.innerHTML = msg;
            renderKeyList();

            if ($("gppComputeMore")) {
                $("gppComputeMore").onclick = computeMissingKeys;
            }
        }

        function pump() {
            if (stopped) { return; }

            if (next >= todo.length) {
                if (!active) { finish(); }
                return;
            }

            var t = todo[next];

            next += 1;
            active += 1;

            var ac = new AbortController();

            controllers.push(ac);

            api("/api/catalog/compute-unique", "POST", {
                connection_id: srcId(), schema: t.schema, table: t.table,
            }, ac.signal).then(function (d) {
                if (stopped) { return; }

                if (d && d.ok && d.column) {
                    state.syncKeys[t.schema + "." + t.table] = {
                        columns: [d.column],
                        source: d.confidence === "sample"
                            ? "sampled" : "computed",
                    };
                    found += 1;
                }
            }).catch(function () {
                /* таблицу проверить не смогли — идём дальше */
            }).then(function () {
                if (stopped) { return; }

                checked += 1;
                active -= 1;

                hint.textContent = "Пробую по данным: " + t.schema + "." +
                    t.table + " (" + fmtN(checked) + "/" +
                    fmtN(todo.length) + ")…";
                opProgress(op, Math.round(checked * 100 / todo.length),
                    fmtN(checked) + "/" + fmtN(todo.length) +
                    ", ключей найдено " + fmtN(found));

                pump();
            });
        }

        hint.textContent = "Проверяю по данным " + fmtN(todo.length) +
            " таблиц…";

        for (var i = 0; i < COMPUTE_PARALLEL; i++) { pump(); }
    }

    /* ---------------- partition diff preview ---------------- */

    function partKey(p) { return p.schema + "." + p.partition; }

    function partPreview() {
        var tables = selTables();
        var box = $("gppPartPreview");
        if (!tables.length) {
            box.innerHTML = '<div class="gpp-hint warn">Сначала выбери таблицы (шаг 1).</div>';
            return;
        }

        var exact = $("gppPartExact").checked;
        var btn = $("gppPartPreviewBtn");
        btn.disabled = true;
        box.innerHTML = '<div class="gpp-hint">' +
            (exact ? "Точный пересчёт COUNT(*) батчами…" : "Читаю статистику каталога…") +
            "</div>";

        var ac = new AbortController();
        var op = opStart(
            (exact ? "Diff партиций (точный COUNT)" : "Diff партиций (по статистике)") +
            ": " + fmtN(tables.length) + " таблиц",
            function () { ac.abort(); });

        api("/api/gpcopy/partition-diff/preview-bulk", "POST", {
            source_connection_id: srcId(), dest_connection_id: dstId(),
            tables: tables, exact: exact,
        }, ac.signal).then(function (d) {
            opEnd(op);
            btn.disabled = false;
            if (!d.ok) {
                box.innerHTML = '<div class="gpp-hint bad">' + esc(d.message) + "</div>";
                return;
            }
            d.tables.sort(function (a, b) { return b.to_copy - a.to_copy; });
            state.partDiff = d.tables;
            state.partView = "all";
            state.partLimit = PART_CHUNK;
            // автовычисление: отстающие от источника отмечены сразу
            state.partChecked = {};
            d.tables.forEach(function (t) {
                (t.detail || []).forEach(function (p) {
                    if (p.action !== "skip") { state.partChecked[partKey(p)] = true; }
                });
            });
            renderPartList();
        }).catch(function (e) {
            opEnd(op);
            btn.disabled = false;
            box.innerHTML = op.cancelled
                ? '<div class="gpp-hint">Diff отменён.</div>'
                : '<div class="gpp-hint bad">' + esc(String(e)) + "</div>";
        });
    }

    function partAutoOn() {
        var cb = $("gppPartAuto");
        return !cb || cb.checked;
    }

    function partCheckedList() {
        var out = [];
        (state.partDiff || []).forEach(function (t) {
            (t.detail || []).forEach(function (p) {
                if (state.partChecked[partKey(p)]) {
                    out.push({ schema: p.schema, table: p.partition });
                }
            });
        });
        return out;
    }

    var PART_CHUNK = 300;

    function partBadge(action) {
        if (action === "skip") {
            return '<span class="gpp-key-badge pk">совпадает</span>';
        }
        if (action === "copy_missing") {
            return '<span class="gpp-key-badge comp">нет в dest</span>';
        }
        return '<span class="gpp-key-badge comp">разл.</span>';
    }

    // Плоский список строк по текущему фильтру: заголовки таблиц + партиции.
    function partFlatItems() {
        var view = state.partView; // all | diff | same
        var items = [];
        (state.partDiff || []).forEach(function (t) {
            var parts = (t.detail || []).filter(function (p) {
                if (view === "diff") { return p.action !== "skip"; }
                if (view === "same") { return p.action === "skip"; }
                return true;
            });
            if (!parts.length && view !== "all") { return; }
            items.push({ type: "head", t: t });
            parts.forEach(function (p) { items.push({ type: "part", p: p }); });
        });
        return items;
    }

    function partItemHtml(item) {
        if (item.type === "head") {
            var t = item.t;
            return '<div class="gpp-key-row" style="background: var(--surface-2); font-weight: 650;">' +
                "<span>" + esc(t.schema + "." + t.table) +
                ' <span class="cols" style="font-weight: 400;">· партиций ' +
                fmtN(t.partitions) + "</span></span>" +
                (t.to_copy
                    ? '<span class="gpp-key-badge comp">отстают ' + fmtN(t.to_copy) + "</span>"
                    : '<span class="gpp-key-badge pk">совпадает</span>') +
                "</div>";
        }
        var p = item.p;
        var k = partKey(p);
        var counts = fmtN(p.src) + " → " + (p.dest === null ? "нет" : fmtN(p.dest));
        return '<div class="gpp-key-row" style="margin-left: 14px;"><span>' +
            '<input type="checkbox" data-part="' + esc(k) + '"' +
            (state.partChecked[k] ? " checked" : "") + "> " +
            esc(p.partition) +
            ' <span class="cols">· ' + counts + "</span></span>" +
            partBadge(p.action) + "</div>";
    }

    function updatePartCounter() {
        var el = $("gppPartCounter");
        if (!el) { return; }
        var checked = 0;
        Object.keys(state.partChecked).forEach(function (k) {
            if (state.partChecked[k]) { checked += 1; }
        });
        el.textContent = fmtN(checked);
    }

    function wirePartRows(container) {
        container.querySelectorAll("input[data-part]:not([data-wired])")
            .forEach(function (cb) {
                cb.setAttribute("data-wired", "1");
                cb.onchange = function () {
                    state.partChecked[cb.getAttribute("data-part")] = cb.checked;
                    updatePartCounter();
                };
            });
    }

    function renderPartList() {
        var box = $("gppPartPreview");
        var tables = state.partDiff || [];
        if (!tables.length) { box.innerHTML = ""; return; }

        var totalAll = 0, totalLag = 0;
        tables.forEach(function (t) {
            totalAll += (t.detail || []).length;
            totalLag += t.to_copy;
        });

        var chip = function (view, label, n) {
            return '<button class="gpp-btn sm gpp-pview' +
                (state.partView === view ? " primary" : "") +
                '" data-pview="' + view + '">' + label + " · " + fmtN(n) + "</button>";
        };

        var items = partFlatItems();
        var top = items.slice(0, state.partLimit);

        setHtml(box,
            '<div class="gpp-chips" style="margin-bottom: 6px;">' +
            chip("all", "Все партиции", totalAll) +
            chip("diff", "Различаются", totalLag) +
            chip("same", "Совпадают", totalAll - totalLag) +
            "</div>" +
            '<div class="gpp-hint" style="margin-top: 0;">Отмечено к загрузке: ' +
            '<b id="gppPartCounter"></b> ' +
            '<button class="gpp-btn sm" id="gppPartAll">Отметить отстающие</button> ' +
            '<button class="gpp-btn sm" id="gppPartNone">Снять все</button></div>' +
            '<div class="gpp-tbl-list" id="gppPartRows" style="max-height: 320px;">' +
            top.map(partItemHtml).join("") +
            "</div>" +
            (items.length > top.length
                ? '<div class="gpp-hint" id="gppPartMore">Показано ' + fmtN(top.length) +
                  " из " + fmtN(items.length) + " — прокрути список ниже.</div>"
                : ""));

        updatePartCounter();

        box.querySelectorAll(".gpp-pview").forEach(function (b) {
            b.onclick = function () {
                state.partView = b.getAttribute("data-pview");
                state.partLimit = PART_CHUNK;
                renderPartList();
            };
        });
        $("gppPartAll").onclick = function () {
            tables.forEach(function (t) {
                (t.detail || []).forEach(function (p) {
                    if (p.action !== "skip") { state.partChecked[partKey(p)] = true; }
                });
            });
            renderPartList();
        };
        $("gppPartNone").onclick = function () {
            state.partChecked = {};
            renderPartList();
        };

        var list = $("gppPartRows");
        wirePartRows(list);
        list.onscroll = function () {
            if (list.scrollTop + list.clientHeight < list.scrollHeight - 80) { return; }
            var all = partFlatItems();
            if (state.partLimit >= all.length) { return; }

            var next = all.slice(state.partLimit, state.partLimit + PART_CHUNK);
            state.partLimit += PART_CHUNK;
            list.insertAdjacentHTML("beforeend", next.map(partItemHtml).join(""));
            wirePartRows(list);

            var more = $("gppPartMore");
            if (more) {
                var shown = Math.min(state.partLimit, all.length);
                more.textContent = shown >= all.length
                    ? ""
                    : "Показано " + fmtN(shown) + " из " + fmtN(all.length) +
                      " — прокрути список ниже.";
            }
        };
    }

    /* ---------------- date window ---------------- */

    function pad(n) { return (n < 10 ? "0" : "") + n; }
    function iso(d) {
        return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate());
    }

    function dateRange() {
        var preset = $("gppDatePreset").value;
        var now = new Date();
        var today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        var d = 86400000;

        if (preset === "yesterday") { return [iso(new Date(+today - d)), iso(today)]; }
        if (preset === "today") { return [iso(today), iso(new Date(+today + d))]; }
        if (preset === "last7") {
            return [iso(new Date(+today - 6 * d)), iso(new Date(+today + d))];
        }
        if (preset === "last_month") {
            var first = new Date(now.getFullYear(), now.getMonth(), 1);
            var prev = new Date(now.getFullYear(), now.getMonth() - 1, 1);
            return [iso(prev), iso(first)];
        }
        return [$("gppDateFrom").value, $("gppDateTo").value];
    }

    function dateWindowSpec() {
        var preset = $("gppDatePreset").value;
        if (preset === "yesterday") {
            return { from: { preset: "yesterday" }, to: { preset: "yesterday" } };
        }
        if (preset === "today") {
            return { from: { preset: "today" }, to: { preset: "today" } };
        }
        if (preset === "last7") {
            return { from: { preset: "n_days_ago", n: 6 }, to: { preset: "today" } };
        }
        if (preset === "last_month") {
            return { from: { preset: "last_month" }, to: { preset: "last_month" } };
        }
        return null; // свой диапазон не планируется
    }

    /* ---------------- summary ---------------- */

    // расписание задаётся календарным выбором; cron живёт в скрытом поле
    function cronExpr() {
        return ($("gppSchedCron").value || "0 2 * * *").trim();
    }

    function renderSummary() {
        var by = schemaCounts();
        var schemas = Object.keys(by).sort();
        var srcName = $("gppSrc").selectedOptions[0]
            ? $("gppSrc").selectedOptions[0].textContent : "?";
        var dstName = $("gppDst").selectedOptions[0]
            ? $("gppDst").selectedOptions[0].textContent : "?";

        var whenTxt = state.when === "sched"
            ? "по расписанию — <b>" +
              esc(window.gpCronHuman ? gpCronHuman(cronExpr()) : cronExpr()) +
              "</b>"
            : "<b>прямо сейчас</b>";

        $("gppSummary").innerHTML = "Скопировать <b>" + fmtN(state.sel.size) +
            " таблиц</b>" +
            (schemas.length ? " (" + schemas.slice(0, 4).map(esc).join(", ") +
                (schemas.length > 4 ? "…" : "") + ")" : "") +
            " из <b>" + esc(srcName.trim()) + "</b> в <b>" + esc(dstName.trim()) +
            "</b> — <b>" + modeName() + "</b>, " + whenTxt + "." +
            // в режиме партиций важно понимать, откуда берётся список
            (state.mode === "part"
                ? (partAutoOn()
                    ? " Расхождение партиций считается в момент запуска."
                    : " Переливаются партиции, отмеченные в превью.")
                : "");

        $("gppGo").textContent =
            state.when === "sched" ? "🗓 Создать расписание" : "▶ Запустить";
    }

    /* ---------------- expert opts ---------------- */

    function expertOpts() {
        return {
            gpcopy_path: $("gppPath").value.trim() || undefined,
            jobs: parseInt($("gppJobs").value, 10) || 4,
            on_segment_threshold: parseInt($("gppThreshold").value, 10),
            extra_args: $("gppExtra").value.trim(),
        };
    }

    /* ---------------- launch ---------------- */

    function setMsg(text, cls) {
        var el = $("gppMsg");
        el.className = "gpp-msg" + (cls ? " " + cls : "");
        el.innerHTML = text;
    }

    function buildIncTables() {
        // watermark по резолву + ручные замены; исключённые и без колонки пропускаем
        return incRowsData()
            .filter(function (r) { return !r.excluded && r.column; })
            .map(function (r) {
                return { schema: r.schema, table: r.table,
                         watermark_column: r.column };
            });
    }

    function buildDateConfigs() {
        var byKey = {};
        (state.dateResolved || []).forEach(function (r) {
            byKey[r.schema + "." + r.table] = r.column;
        });
        return selTables()
            .filter(function (t) { return byKey[t.schema + "." + t.table]; })
            .map(function (t) {
                return { schema: t.schema, table: t.table,
                         date_column: byKey[t.schema + "." + t.table] };
            });
    }

    function buildSyncConfigs() {
        return selTables()
            .filter(function (t) { return state.syncKeys[t.schema + "." + t.table]; })
            .map(function (t) {
                return { schema: t.schema, table: t.table,
                         key_columns: state.syncKeys[t.schema + "." + t.table].columns };
            });
    }

    function launchNow() {
        var ex = expertOpts();
        var tables = selTables();

        if (state.mode === "full") {
            var body = {
                source_connection_id: srcId(), dest_connection_id: dstId(),
                tables: tables,
                gpcopy_path: ex.gpcopy_path, jobs: ex.jobs,
                on_segment_threshold: ex.on_segment_threshold,
                extra_args: ex.extra_args,
            };
            body[$("gppFullExisting").value] = true;
            return api("/api/gpcopy/start", "POST", body);
        }

        if (state.mode === "inc" && state.incStrategy === "watermark") {
            var pre = state.incResolved
                ? Promise.resolve(true)
                : checkColumns("gppIncPriority", "gppIncHint", "incResolved");
            return pre.then(function () {
                var incTables = buildIncTables();
                if (!incTables.length) {
                    return { ok: false, message: "Ни у одной таблицы нет watermark-колонки" };
                }
                return api("/api/gpcopy/increment/start", "POST", {
                    source_connection_id: srcId(), dest_connection_id: dstId(),
                    tables: incTables, gpcopy_path: ex.gpcopy_path, jobs: ex.jobs,
                });
            });
        }

        if (state.mode === "inc") { // по ключу (sync)
            var preS = Object.keys(state.syncKeys).length
                ? Promise.resolve(true)
                : resolveSyncKeys();
            return preS.then(function () {
                var cfgs = buildSyncConfigs();
                if (!cfgs.length) {
                    return { ok: false, message: "Ни у одной таблицы нет ключа" };
                }
                return api("/api/gpcopy/sync/apply", "POST", {
                    source_connection_id: srcId(), dest_connection_id: dstId(),
                    table_configs: cfgs, gpcopy_path: ex.gpcopy_path, jobs: ex.jobs,
                });
            });
        }

        if (state.mode === "date") {
            var range = dateRange();
            if (!range[0] || !range[1]) {
                return Promise.resolve({ ok: false, message: "Укажи диапазон дат" });
            }
            var preD = state.dateResolved
                ? Promise.resolve(true)
                : checkColumns("gppDatePriority", "gppDateHint", "dateResolved");
            return preD.then(function () {
                var cfgs = buildDateConfigs();
                if (!cfgs.length) {
                    return { ok: false, message: "Ни у одной таблицы нет колонки даты" };
                }
                return api("/api/gpcopy/start-date", "POST", {
                    source_connection_id: srcId(), dest_connection_id: dstId(),
                    date_from: range[0], date_to: range[1],
                    table_configs: cfgs,
                    gpcopy_path: ex.gpcopy_path, jobs: ex.jobs,
                });
            });
        }

        // part: расхождение считает сама задача на старте, если стоит
        // галочка (иначе грузим ровно отмеченные в превью партиции)
        var body = {
            source_connection_id: srcId(), dest_connection_id: dstId(),
            tables: tables, gpcopy_path: ex.gpcopy_path, jobs: ex.jobs,
            count_mode: $("gppPartExact").checked ? "exact" : "stats",
            recompute: partAutoOn(),
        };
        if (!partAutoOn() && state.partDiff) {
            var checkedParts = partCheckedList();
            if (!checkedParts.length) {
                return Promise.resolve({
                    ok: false,
                    message: "Не отмечена ни одна партиция — отметь в списке diff",
                });
            }
            body.partitions = checkedParts;
        }
        return api("/api/gpcopy/partition-diff/start", "POST", body);
    }

    function buildScheduleConfig() {
        var ex = expertOpts();
        var tables = selTables();
        var base = {
            source_connection_id: srcId(),
            dest_connection_id: dstId(),
            gpcopy_path: ex.gpcopy_path,
            jobs: ex.jobs,
        };

        if (state.mode === "full") {
            base.tables = tables;
            base.selected_tables = tables;
            base[$("gppFullExisting").value] = true;
            return Promise.resolve(base);
        }

        if (state.mode === "inc" && state.incStrategy === "watermark") {
            var pre = state.incResolved
                ? Promise.resolve(true)
                : checkColumns("gppIncPriority", "gppIncHint", "incResolved");
            return pre.then(function () {
                var incTables = buildIncTables();
                if (!incTables.length) { return null; }
                base.tables = incTables;
                return base;
            });
        }

        if (state.mode === "inc") { // по ключу (sync)
            var preS = Object.keys(state.syncKeys).length
                ? Promise.resolve(true)
                : resolveSyncKeys();
            return preS.then(function () {
                var cfgs = buildSyncConfigs();
                if (!cfgs.length) { return null; }
                base.tables = cfgs.map(function (c) {
                    return { schema: c.schema, table: c.table };
                });
                base.table_configs = cfgs;
                return base;
            });
        }

        if (state.mode === "date") {
            var dw = dateWindowSpec();
            if (!dw) { return Promise.resolve(null); }
            dw.column = parsePriority("gppDatePriority")[0];
            base.tables = tables;
            base.selected_tables = tables;
            base.date_window = dw;
            return Promise.resolve(base);
        }

        // part: в расписании список партиций не фиксируем — задача
        // считает расхождение в момент запуска
        base.tables = tables;
        base.count_mode = $("gppPartExact").checked ? "exact" : "stats";
        base.recompute = true;
        return Promise.resolve(base);
    }

    function createSchedule() {
        if (state.mode === "date" && !dateWindowSpec()) {
            setMsg("Свой диапазон дат нельзя запланировать — выбери пресет периода.", "err");
            return Promise.resolve(null);
        }

        return buildScheduleConfig().then(function (config) {
            if (!config) {
                setMsg("Не удалось собрать конфиг: нет таблиц с колонкой/ключом.", "err");
                return null;
            }
            var name = $("gppSchedName").value.trim() ||
                (jobType() + "-" + state.sel.size);
            return api("/api/schedules", "POST", {
                name: name,
                job_type: jobType(),
                cron_expr: cronExpr(),
                config: config,
            });
        });
    }

    function autoCreateOn() {
        var cb = $("gppAutoCreate");
        return !cb || cb.checked;
    }

    // перед запуском: сверить DDL, создать недостающее в приёмнике и
    // выкинуть объекты, которых нет в источнике
    function prepareTarget() {
        if (!autoCreateOn() || state.when === "sched") {
            return Promise.resolve(true);
        }

        setMsg("Сверяю DDL с приёмником…");

        return api("/api/gpcopy/precheck", "POST", {
            source_connection_id: srcId(),
            dest_connection_id: dstId(),
            tables: selTables(),
        }).then(function (d) {
            if (!d.ok) { return true; }   // предпроверка не должна блокировать

            ddlLast = d;
            ddlRender();

            var ghosts = ddlGhosts();

            ghosts.forEach(function (key) { state.sel.delete(key); });

            if (ghosts.length) {
                onSelectionChanged();
                toast("Пропускаю " + fmtN(ghosts.length) +
                      " объектов, которых нет в источнике: " +
                      ghosts.slice(0, 3).join(", ") +
                      (ghosts.length > 3 ? "…" : ""), "warning");
            }

            if (!state.sel.size) {
                setMsg("В источнике не осталось ни одного выбранного объекта.",
                       "err");
                return false;
            }

            var missing = ddlMissing().filter(function (t) {
                return ghosts.indexOf(t.schema + "." + t.table) === -1;
            });

            if (!missing.length) { return true; }

            setMsg("Создаю в приёмнике недостающие объекты (" +
                   fmtN(missing.length) + ")…");

            return createMissing(missing).then(function (r) {
                if (r.ok) {
                    toast("Создано в приёмнике: " + fmtN(r.created) +
                          (r.failed ? ", не удалось: " + fmtN(r.failed) : ""),
                          r.failed ? "warning" : "success");
                }

                return true;
            }).catch(function () { return true; });
        }).catch(function () { return true; });
    }

    function go() {
        if (!state.sel.size) { setMsg("Сначала выбери таблицы (шаг 1).", "err"); return; }
        if (isNaN(srcId()) || isNaN(dstId())) { setMsg("Выбери подключения.", "err"); return; }
        if (srcId() === dstId()) { setMsg("Источник и назначение совпадают.", "err"); return; }

        $("gppGo").disabled = true;
        setMsg("Запускаю…");

        var p = prepareTarget().then(function (goOn) {
            if (!goOn) { return null; }

            return state.when === "sched" ? createSchedule() : launchNow();
        });

        p.then(function (d) {
            $("gppGo").disabled = false;
            if (!d) { return; }
            if (!d.ok) { setMsg("Ошибка: " + esc(d.message || "неизвестно"), "err"); return; }
            if (state.when === "sched") {
                setMsg('✓ Расписание создано — <a href="/schedules">открыть Schedules</a>', "ok");
                toast("Расписание создано", "success");
            } else {
                setMsg("✓ Задача #" + d.job_id + " запущена", "ok");
                toast("Задача #" + d.job_id + " запущена", "success");
                loadRuns();
            }
        }).catch(function (e) {
            $("gppGo").disabled = false;
            setMsg("Ошибка: " + esc(String(e)), "err");
        });
    }

    /* ---------------- schedule preview ---------------- */

    var cronTimer = null;

    function previewCron() {
        clearTimeout(cronTimer);
        cronTimer = setTimeout(function () {
            var expr = cronExpr();
            if (!expr) { $("gppSchedNext").textContent = ""; return; }
            api("/api/schedules/preview", "POST", { cron_expr: expr }).then(function (d) {
                $("gppSchedNext").textContent = d.ok
                    ? "След. запуски: " + d.next_runs.slice(0, 3).join(" · ")
                    : "Некорректное cron-выражение";
            });
            renderSummary();
        }, 300);
    }

    /* ---------------- runs feed ---------------- */

    // лента запусков тоже своя у каждого тулкита: PG - только COPY-перенос
    var RUN_TYPES = (function () {
        var modes = document.getElementById("gppModes");
        var tk = (modes && modes.dataset && modes.dataset.toolkit) || "gp";

        return tk === "pg"
            ? "copy_pipe"
            : "gpcopy,gpcopy_date,gpcopy_increment,gpcopy_partition_diff," +
              "gpcopy_sync,copy_pipe";
    })();

    function progressRing(pct) {
        var r = 19;
        var c = 2 * Math.PI * r;
        var filled = (c * pct / 100).toFixed(1);
        return '<div class="gpp-ring"><svg viewBox="0 0 46 46">' +
            '<circle class="bg" cx="23" cy="23" r="' + r + '"></circle>' +
            '<circle class="fg" cx="23" cy="23" r="' + r +
            '" stroke-dasharray="' + filled + " " + c.toFixed(1) + '"></circle>' +
            '</svg><span class="pct">' + pct + "%</span></div>";
    }

    function indeterminateRing() {
        var r = 19;
        var c = 2 * Math.PI * r;
        return '<div class="gpp-ring ind"><svg viewBox="0 0 46 46">' +
            '<circle class="bg" cx="23" cy="23" r="' + r + '"></circle>' +
            '<circle class="fg" cx="23" cy="23" r="' + r +
            '" stroke-dasharray="' + (c * 0.3).toFixed(1) + " " + c.toFixed(1) +
            '"></circle></svg></div>';
    }

    /* Индикатор локальных операций анализа (резолв колонок/ключей, diff…).
       gpcopy-переносы сюда не попадают — они в ленте «Запуски». */

    var currentOp = null;

    function opRender(pct, detail) {
        var box = $("gppActiveJobs");
        if (!box) { return; }
        if (!currentOp) { box.innerHTML = ""; return; }

        box.innerHTML = '<div class="gpp-active">' +
            (pct === null ? indeterminateRing() : progressRing(pct)) +
            "<span>" + esc(currentOp.label) +
            (detail ? ' <span class="what">' + esc(detail) + "</span>" : "") +
            "</span>" +
            '<button class="gpp-btn sm stop" id="gppOpCancel">⏹ Отмена</button></div>';

        $("gppOpCancel").onclick = function () {
            var op = currentOp;
            if (!op) { return; }
            op.cancelled = true;
            if (op.onCancel) { try { op.onCancel(); } catch (e) { /* aborted */ } }
            opEnd(op);
            toast("Операция отменена", "info");
        };
    }

    function opStart(label, onCancel) {
        currentOp = { label: label, cancelled: false, onCancel: onCancel };
        opRender(null, "");
        return currentOp;
    }

    function opProgress(op, pct, detail) {
        if (op === currentOp && !op.cancelled) { opRender(pct, detail); }
    }

    function opEnd(op) {
        if (op === currentOp) {
            currentOp = null;
            var box = $("gppActiveJobs");
            if (box) { box.innerHTML = ""; }
        }
    }

    function isActiveStatus(s) {
        return s === "running" || s === "queued" || s === "stopping";
    }

    function loadRuns() {
        api("/api/jobs/recent?types=" + RUN_TYPES + "&limit=8").then(function (d) {
            if (!d.ok) { return; }
            var box = $("gppRuns");
            if (!d.jobs.length) {
                box.innerHTML = '<div class="gpp-hint">Пока нет запусков.</div>';
                return;
            }

            // детали нужны активным (текущая таблица) и раскрытой шторке
            var need = d.jobs.filter(function (j) {
                return isActiveStatus(j.status) || j.id === state.openRun;
            });

            Promise.all(need.map(function (j) {
                return api("/api/jobs/" + j.id + "/status")
                    .catch(function () { return null; });
            })).then(function (sts) {
                var stById = {};
                need.forEach(function (j, i) {
                    if (sts[i] && sts[i].ok) { stById[j.id] = sts[i]; }
                });
                renderRuns(box, d.jobs, stById);

                var anyActive = d.jobs.some(function (j) {
                    return isActiveStatus(j.status);
                });
                clearTimeout(loadRuns._t);
                loadRuns._t = setTimeout(loadRuns, anyActive ? 5000 : 20000);
            });
        });
    }

    function renderRuns(box, jobs, stById) {
        // перерисовка идёт каждые несколько секунд — прокрутку шторки,
        // списка таблиц и всей страницы возвращаем на место (gpKeepScroll)
        setHtml(box, jobs.map(function (j) {
            var running = isActiveStatus(j.status);
            var failed = j.status === "failed" || j.status === "error" ||
                j.status === "cancelled" || j.status === "interrupted";
            var dot = running ? "b" : (failed ? "r" : (j.status === "done" ? "g" : "i"));
            var pct = Math.max(0, Math.min(100, Math.round(j.progress_percent || 0)));
            if (j.status === "done") { pct = 100; }
            var barCls = j.status === "done" ? "done" : (failed ? "fail" : "");
            var label = RUN_LABELS[j.job_type] || j.job_type;
            var open = state.openRun === j.id;

            // что сейчас копируется
            var what = "";
            var st = stById[j.id];
            if (running && st && st.items) {
                var cur = st.items.find(function (it) { return it.status === "running"; });
                if (cur) {
                    what = (cur.schema_name || "") + "." + (cur.table_name || "");
                }
            }

            var statusTxt = running ? (pct + "%") : esc(j.status);

            // кто источник и кто назначение («—», если подключение удалено)
            var route = "";
            if (j.dest_name) {
                route = esc(j.source_name || "—") + " → " + esc(j.dest_name);
            } else if (j.source_name) {
                route = esc(j.source_name);
            }

            var meta = (route ? route + " · " : "") + esc(j.started_at || "");
            if (failed && j.error_message) {
                meta += " · " + esc(String(j.error_message).slice(0, 60));
            }

            return '<div class="run" data-job="' + j.id + '" title="Подробности">' +
                '<span class="chev">' + (open ? "▾" : "▸") + "</span>" +
                '<span class="gpp-dot ' + dot + '"></span>' +
                '<span class="rlab">#' + j.id + " · " + esc(label) +
                (what ? ' <span class="what">' + esc(what) + "</span>" : "") +
                " · " + fmtN(j.done_items) + "/" + fmtN(j.total_items) + "</span>" +
                '<div class="gpp-bar"><i class="' + barCls + '" style="width: ' + pct +
                '%;"></i></div><span>' + statusTxt + "</span>" +
                '<span class="meta">' + meta + "</span></div>" +
                (open ? drawerHtml(j, st) : "");
        }).join(""));

        box.querySelectorAll(".run[data-job]").forEach(function (row) {
            row.onclick = function () {
                var id = parseInt(row.getAttribute("data-job"), 10);
                state.openRun = state.openRun === id ? null : id;
                loadRuns();
            };
        });

        box.querySelectorAll("button[data-run-retry]").forEach(function (btn) {
            btn.onclick = function (ev) {
                ev.stopPropagation();
                var id = parseInt(btn.getAttribute("data-run-retry"), 10);
                btn.disabled = true;
                api("/api/gpcopy/retry-failed", "POST", {
                    job_id: id,
                    // режим существующих таблиц из шага 2 действует
                    // и на дозагрузку упавших
                    existing_mode: $("gppFullExisting")
                        ? $("gppFullExisting").value : "truncate",
                })
                    .then(function (r) {
                        btn.disabled = false;
                        if (!r.ok) {
                            toast(r.message || "Не удалось запустить дозагрузку", "error");
                            return;
                        }
                        toast("Дозагрузка запущена: #" + r.job_id + " (" +
                            fmtN(r.total_items) + " партиций)", "success");
                        state.openRun = r.job_id;
                        loadRuns();
                    });
            };
        });

        box.querySelectorAll("button[data-run-log]").forEach(function (btn) {
            btn.onclick = function (ev) {
                ev.stopPropagation();

                var id = btn.getAttribute("data-run-log");
                var out = $("gppLogBox" + id);
                var hint = $("gppLogHint" + id);

                if (out && out.innerHTML) {   // повторный клик — свернуть
                    out.innerHTML = "";
                    if (hint) { hint.textContent = ""; }
                    return;
                }

                btn.disabled = true;

                api("/api/jobs/" + id + "/log").then(function (d) {
                    btn.disabled = false;

                    if (!d.ok) {
                        if (hint) { hint.textContent = d.message || "лога нет"; }
                        return;
                    }

                    if (hint) {
                        hint.textContent =
                            (d.truncated ? "последние 200 КБ · " : "") +
                            Math.round((d.size || 0) / 1024) + " КБ · " + d.path;
                    }

                    var cause = (d.cause || []).length
                        ? '<div class="gpp-err" style="margin-bottom: 6px;">' +
                          "<b>Причина из лога:</b><pre>" +
                          esc(d.cause.join("\n")) + "</pre></div>"
                        : "";

                    out.innerHTML = cause +
                        '<pre class="gpp-log-full">' + esc(d.text) + "</pre>";

                    // лог читают с конца — прокручиваем туда сразу
                    var pre = out.querySelector(".gpp-log-full");

                    if (pre) { pre.scrollTop = pre.scrollHeight; }
                }).catch(function (e) {
                    btn.disabled = false;
                    if (hint) { hint.textContent = String(e); }
                });
            };
        });

        box.querySelectorAll("button[data-run-stop]").forEach(function (btn) {
            btn.onclick = function (ev) {
                ev.stopPropagation();
                var id = btn.getAttribute("data-run-stop");
                var doStop = function () {
                    btn.disabled = true;
                    api("/api/jobs/" + id + "/stop", "POST").then(function (r) {
                        if (!r.ok) {
                            btn.disabled = false;
                            toast(r.message || "Не удалось остановить", "error");
                            return;
                        }
                        toast("Задача #" + id + " останавливается", "info");
                        loadRuns();
                    });
                };
                if (window.gpConfirm) {
                    window.gpConfirm("Остановить задачу #" + id + "?")
                        .then(function (yes) { if (yes) { doStop(); } });
                } else if (confirm("Остановить задачу #" + id + "?")) { doStop(); }
            };
        });
    }

    /* ---------------- run details modal ---------------- */

    var RUN_STATUS_BADGES = {
        done: ["pk", "done"],
        running: ["man", "running"],
        queued: ["man", "queued"],
        failed: ["none", "failed"],
        error: ["none", "error"],
        cancelled: ["none", "cancelled"],
        skipped: ["comp", "skipped"],
    };

    function runBadge(status) {
        var b = RUN_STATUS_BADGES[status] || ["", status];
        return '<span class="gpp-key-badge ' + b[0] + '">' + esc(b[1]) + "</span>";
    }

    function drawerHtml(j, st) {
        var s = (st && st.summary) || {};
        var items = ((st && st.items) || []).slice();

        var dRoute = "";
        if (j.dest_name) {
            dRoute = esc(j.source_name || "—") + " → " + esc(j.dest_name);
        } else if (j.source_name) {
            dRoute = esc(j.source_name);
        }

        var html = '<div class="meta-line">' + runBadge(j.status) + " " +
            (dRoute ? "<b>" + dRoute + "</b> · " : "") +
            esc(j.started_at || "") +
            (j.finished_at ? " → " + esc(j.finished_at) : "") +
            " · объектов: " + fmtN(s.total || j.total_items) +
            (s.done ? ' · <span class="good">done: ' + fmtN(s.done) + "</span>" : "") +
            (s.failed ? ' · <span style="color: var(--crit);">failed: ' +
                fmtN(s.failed) + "</span>" : "") +
            (s.skipped ? " · skipped: " + fmtN(s.skipped) : "") +
            "</div>";

        if (isActiveStatus(j.status)) {
            // мини-статус по строкам: сколько прямо сейчас в работе и готово
            var cnt = { running: 0, done: 0, failed: 0, rest: 0 };
            items.forEach(function (it) {
                if (cnt[it.status] !== undefined) { cnt[it.status] += 1; }
                else { cnt.rest += 1; }
            });

            html += '<div style="margin: 6px 0 10px; display: flex;' +
                ' align-items: center; gap: 10px; flex-wrap: wrap;">' +
                '<button class="gpp-btn sm stop" data-run-stop="' + j.id + '"' +
                (j.status === "stopping" ? " disabled" : "") + ">" +
                (j.status === "stopping" ? "останавливаю…" : "⏹ Остановить") +
                "</button>" +
                '<span class="cols">running: <b>' + fmtN(cnt.running) + "</b>" +
                ' · <span class="good">done: <b>' + fmtN(cnt.done) + "</b></span>" +
                (cnt.failed
                    ? ' · <span style="color: var(--crit);">failed: <b>' +
                      fmtN(cnt.failed) + "</b></span>"
                    : "") +
                " · в очереди: <b>" + fmtN(cnt.rest) + "</b></span>" +
                "</div>";
        }

        if (j.status === "failed" && j.job_type === "gpcopy") {
            html += '<div style="margin: 6px 0 10px;">' +
                '<button class="gpp-btn sm" data-run-retry="' + j.id + '">' +
                "⟳ Дозагрузить упавшие</button></div>";
        }

        if (j.error_message) {
            html += '<div class="gpp-err"><pre>' + esc(j.error_message) + "</pre></div>";
        }

        // сам gpcopy пишет причину падения в свой лог — даём его открыть
        html += '<div style="margin: 6px 0 10px; display: flex; gap: 8px;' +
            ' align-items: center; flex-wrap: wrap;">' +
            '<button class="gpp-btn sm" data-run-log="' + j.id + '">' +
            "\ud83d\udcc4 Полный лог</button>" +
            '<a class="gpp-btn sm" href="/api/jobs/' + j.id + '/log.txt"' +
            ' download>\u2b07 Скачать лог</a>' +
            '<span class="gpp-hint" style="margin: 0;" id="gppLogHint' +
            j.id + '"></span></div>' +
            '<div id="gppLogBox' + j.id + '"></div>';

        if (items.length) {
            // порядок: сейчас копируется -> упавшие -> в очереди -> готовые
            var order = { running: 0, failed: 1, done: 2, pending: 3,
                          queued: 3, skipped: 4 };
            items.sort(function (a, b) {
                var ao = order[a.status] !== undefined ? order[a.status] : 2;
                var bo = order[b.status] !== undefined ? order[b.status] : 2;
                return ao - bo;
            });

            // весь список: и очередь, и готовые — прокрутка внутри блока
            html += '<div class="gpp-key-list" style="max-height: 320px;">' +
                items.map(function (it) {
                    var name = (it.schema_name || "") + "." + (it.table_name || "");
                    var msg = it.message || it.error_message || "";
                    var size = Number(it.size_bytes || 0);
                    var moved = Number(it.bytes_done || 0);
                    var pTot = Number(it.parts_total || 0);
                    var pDone = Number(it.parts_done || 0);
                    var vol = "";
                    if (size > 0) {
                        if (it.status === "running") {
                            var p = Math.min(100, Math.round(moved / size * 100));
                            vol = ' <span class="cols">· ' + fmtBytes(moved) + " / " +
                                fmtBytes(size) + " (" + p + "%)</span>";
                        } else if (it.status === "done") {
                            vol = ' <span class="cols">· ' + fmtBytes(size) + "</span>";
                        }
                    } else if (pTot > 1 && it.status === "running") {
                        // свой индикатор таблицы: партиций готово / всего
                        // (у таблиц без партиций pTot == 1 — бар не показываем,
                        // их состояние видно по бейджу done/running)
                        var pp = Math.min(100, Math.round(pDone / pTot * 100));
                        vol = ' <span class="cols">· ' + fmtN(pDone) + "/" + fmtN(pTot) +
                            ' парт.</span> <span class="gpp-bar" style="display: inline-block;' +
                            ' width: 90px; vertical-align: middle;"><i style="width: ' +
                            pp + '%;"></i></span> <span class="cols">' + pp + "%</span>";
                    } else if (pTot > 1 && it.status === "done") {
                        vol = ' <span class="cols">· ' + fmtN(pTot) + " парт.</span>";
                    } else if (it.status === "running") {
                        // без партиций и байтов точного процента нет —
                        // показываем «в работе» бегущей полоской
                        vol = ' <span class="gpp-bar" style="display: inline-block;' +
                            ' width: 90px; vertical-align: middle; overflow: hidden;">' +
                            '<i class="ind"></i></span>' +
                            ' <span class="cols">копируется</span>';
                    }
                    return '<div class="gpp-key-row"><span>' + esc(name) + vol +
                        (msg ? ' <span class="cols err">· ' + esc(String(msg)) +
                            "</span>" : "") +
                        "</span>" + runBadge(it.status) + "</div>";
                }).join("") +
                "</div>";
        } else if (!j.error_message) {
            html += '<div class="gpp-hint">Деталей по таблицам нет.</div>';
        }

        return '<div class="gpp-run-drawer" onclick="event.stopPropagation()">' +
            html + "</div>";
    }

    /* ---------------- предпроверка DDL ---------------- */

    var ddlLast = null;   // последний результат precheck

    // колонка есть, но названа иначе (регистр, кавычки, латинские двойники)
    function ddlRenames() {
        return (ddlLast ? ddlLast.results : []).filter(function (r) {
            return (r.renames || []).length;
        }).map(function (r) {
            return { schema: r.schema, table: r.table, renames: r.renames };
        });
    }

    // расхождения, которые правятся только пересозданием таблицы
    function ddlBroken() {
        return (ddlLast ? ddlLast.results : []).filter(function (r) {
            return (r.type_diffs || []).length ||
                (r.extra_in_dest || []).length;
        }).map(function (r) {
            return { schema: r.schema, table: r.table };
        });
    }

    // отчёт об операции прямо в блоке — тост уезжает, а это остаётся
    function ddlReport(title, d, countKey) {
        var box = $("gppDdlReport");

        if (!box) { return; }

        var bad = (d.results || []).filter(function (r) { return !r.ok; });

        box.innerHTML = '<div class="gpp-hint" style="margin-top: 8px;">' +
            esc(title) + ": <b>" + fmtN(d[countKey] || 0) + "</b>" +
            (d.failed
                ? ' · <span style="color: var(--crit);">ошибок ' +
                  fmtN(d.failed) + "</span>"
                : " · без ошибок") + "</div>" +
            (bad.length
                ? '<div class="gpp-key-list" style="max-height: 180px;">' +
                  bad.map(function (r) {
                      return '<div class="gpp-key-row"><span>' +
                          esc(r.schema + "." + r.table) +
                          ' <span class="cols err">· ' + esc(r.error) +
                          "</span></span></div>";
                  }).join("") + "</div>"
                : "");
    }

    // объекты, которых нет в приёмнике — создаём по DDL источника
    function ddlMissing() {
        return (ddlLast ? ddlLast.results : []).filter(function (r) {
            return r.status === "no_dest";
        }).map(function (r) {
            return { schema: r.schema, table: r.table };
        });
    }

    // объектов нет в источнике — их нельзя отдавать gpcopy: он падает
    // целиком на первом же несуществующем имени
    function ddlGhosts() {
        return (ddlLast ? ddlLast.results : []).filter(function (r) {
            return r.status === "no_source";
        }).map(function (r) {
            return r.schema + "." + r.table;
        });
    }

    // создать недостающие объекты, следом — их зависимости
    function createMissing(tables) {
        return api("/api/gpcopy/create-tables", "POST", {
            source_connection_id: srcId(),
            dest_connection_id: dstId(),
            tables: tables,
        }).then(function (d) {
            if (!d.ok) { return d; }

            return api("/api/gpcopy/fix-deps", "POST", {
                source_connection_id: srcId(),
                dest_connection_id: dstId(),
                tables: tables,
            }).then(function (dep) {
                d.deps = dep && dep.ok ? dep.created : 0;
                return d;
            }).catch(function () { return d; });
        });
    }

    function ddlFixables() {
        return (ddlLast ? ddlLast.results : []).filter(function (r) {
            return r.missing_in_dest && r.missing_in_dest.length;
        });
    }

    function ddlRender() {
        var box = $("gppDdlBox");

        if (!ddlLast) { box.innerHTML = ""; return; }

        var s = ddlLast.summary || {};
        var bad = (ddlLast.results || []).filter(function (r) {
            return r.status !== "ok";
        });

        var html = '<div class="gpp-hint" style="margin-top: 0;">' +
            'DDL: совпадают <b class="good">' + fmtN(s.ok || 0) + "</b>" +
            " · расхождения <b" + (s.diff ? ' style="color: var(--crit);"' : "") +
            ">" + fmtN(s.diff || 0) + "</b>" +
            " · нет в приёмнике <b>" + fmtN(s.no_dest || 0) + "</b>" +
            (s.no_source ? " · нет в источнике <b>" + fmtN(s.no_source) + "</b>" : "") +
            "</div>";

        var missing = ddlMissing();

        if (missing.length) {
            html += '<button class="gpp-btn sm" id="gppDdlCreate">🏗 Создать ' +
                "недостающие объекты (" + fmtN(missing.length) + ")</button> ";
        }

        var renames = ddlRenames();

        if (renames.length) {
            var nRen = renames.reduce(function (a, r) {
                return a + r.renames.length;
            }, 0);
            html += '<button class="gpp-btn sm" id="gppDdlRename">\u270e ' +
                "Переименовать колонки (" + fmtN(nRen) + ")</button> ";
        }

        var broken = ddlBroken();

        if (broken.length) {
            html += '<button class="gpp-btn sm" id="gppDdlRecreate">\u267b ' +
                "Пересоздать таблицы (" + fmtN(broken.length) + ")</button> ";
        }

        var fixables = ddlFixables();

        if (fixables.length) {
            var nCols = fixables.reduce(function (a, r) {
                return a + r.missing_in_dest.length;
            }, 0);
            html += '<button class="gpp-btn sm" id="gppDdlFix">➕ Создать ' +
                "недостающие колонки (" + fmtN(nCols) + " в " +
                fmtN(fixables.length) + " табл.)</button>";
        }

        // зависимости, которых нет в приёмнике (функции/расширения/sequences)
        var deps = ddlLast.deps || [];

        if (deps.length) {
            var DEP_LABELS = { extension: "расширение", "function": "функция",
                               sequence: "sequence" };
            html += '<div class="gpp-hint" style="color: var(--crit); margin-top: 8px;">' +
                "Нет в приёмнике: " +
                esc(deps.map(function (d) {
                    return (DEP_LABELS[d.kind] || d.kind) + " " + d.name;
                }).join(", ")) + "</div>" +
                '<button class="gpp-btn sm" id="gppDdlDeps">🧩 Досоздать ' +
                "зависимости (" + fmtN(deps.length) + ")</button>";
        }

        if (bad.length) {
            html += '<div class="gpp-key-list" style="max-height: 240px; margin-top: 8px;">' +
                bad.map(function (r) {
                    var det = [];

                    if (r.status === "no_dest") {
                        det.push("нет в приёмнике — создам по DDL источника");
                    }
                    if (r.status === "no_source") {
                        det.push("нет в источнике (проверь имя)");
                    }
                    (r.renames || []).forEach(function (x) {
                        det.push("колонка названа иначе: «" + x.from +
                            "» → «" + x.to + "» (переименую)");
                    });
                    (r.missing_in_dest || []).forEach(function (c) {
                        det.push("нет колонки " + c.name + " (" + c.type + ")");
                    });
                    (r.type_diffs || []).forEach(function (d) {
                        det.push(d.column + ": " + d.src + " → " + d.dst +
                            " — тип разошёлся, лечится пересозданием");
                    });
                    if ((r.extra_in_dest || []).length) {
                        det.push("лишние в приёмнике: " + r.extra_in_dest.join(", "));
                    }

                    return '<div class="gpp-key-row"><span>' +
                        esc(r.schema + "." + r.table) +
                        ' <span class="cols err">· ' + esc(det.join(" · ")) +
                        "</span></span></div>";
                }).join("") + "</div>";
        } else {
            html += '<div class="gpp-hint">Все выбранные таблицы совпадают по колонкам.</div>';
        }

        html += '<div id="gppDdlReport"></div>';

        box.innerHTML = html;

        var renameBtn = $("gppDdlRename");

        if (renameBtn) {
            renameBtn.onclick = function () {
                renameBtn.disabled = true;
                renameBtn.textContent = "Переименовываю…";

                api("/api/gpcopy/rename-columns", "POST", {
                    dest_connection_id: dstId(),
                    tables: ddlRenames(),
                }).then(function (d) {
                    renameBtn.disabled = false;

                    if (!d.ok) {
                        toast(d.message || "Не удалось переименовать", "error");
                        return;
                    }

                    var report = { results: d.results, failed: d.failed,
                                   renamed: d.renamed };

                    ddlRun().then(function () {
                        ddlReport("Переименовано колонок", report, "renamed");
                    });
                }).catch(function (e) {
                    renameBtn.disabled = false;
                    toast(String(e), "error");
                });
            };
        }

        var recreateBtn = $("gppDdlRecreate");

        if (recreateBtn) {
            recreateBtn.onclick = function () {
                var tables = ddlBroken();
                var ask = window.gpConfirm
                    ? window.gpConfirm("Пересоздать " + tables.length +
                        " таблиц в приёмнике? Данные в них будут удалены " +
                        "(DROP + CREATE по DDL источника).",
                        { danger: true, confirmText: "Пересоздать" })
                    : Promise.resolve(window.confirm("Пересоздать таблицы? " +
                        "Данные в приёмнике будут удалены."));

                ask.then(function (yes) {
                    if (!yes) { return; }

                    recreateBtn.disabled = true;
                    recreateBtn.textContent = "Пересоздаю…";

                    api("/api/gpcopy/recreate-tables", "POST", {
                        source_connection_id: srcId(),
                        dest_connection_id: dstId(),
                        tables: tables,
                    }).then(function (d) {
                        recreateBtn.disabled = false;

                        if (!d.ok) {
                            toast(d.message || "Не удалось пересоздать",
                                  "error");
                            return;
                        }

                        var report = { results: d.results, failed: d.failed,
                                       created: d.created };

                        ddlRun().then(function () {
                            ddlReport("Пересоздано таблиц", report, "created");
                        });
                    }).catch(function (e) {
                        recreateBtn.disabled = false;
                        toast(String(e), "error");
                    });
                });
            };
        }

        var createBtn = $("gppDdlCreate");

        if (createBtn) {
            createBtn.onclick = function () {
                var tables = ddlMissing();

                createBtn.disabled = true;
                createBtn.textContent = "Создаю…";

                createMissing(tables).then(function (d) {
                    createBtn.disabled = false;

                    if (!d.ok) {
                        createBtn.textContent = "🏗 Создать недостающие объекты";
                        toast(d.message || "Не удалось создать", "error");
                        return;
                    }

                    toast("Создано объектов: " + fmtN(d.created) +
                        " (" + fmtN(d.statements) + " DDL)" +
                        (d.failed ? ", ошибок: " + fmtN(d.failed) : "") +
                        (d.deps ? ", зависимостей: " + fmtN(d.deps) : ""),
                        d.failed ? "warning" : "success");

                    (d.results || []).filter(function (r) {
                        return !r.ok;
                    }).slice(0, 3).forEach(function (r) {
                        toast(r.schema + "." + r.table + ": " + r.error,
                              "error");
                    });

                    var report = { results: d.results, failed: d.failed,
                                   created: d.created };

                    ddlRun().then(function () {
                        ddlReport("Создано объектов", report, "created");
                    });
                }).catch(function (e) {
                    createBtn.disabled = false;
                    createBtn.textContent = "🏗 Создать недостающие объекты";
                    toast(String(e), "error");
                });
            };
        }

        var fixBtn = $("gppDdlFix");

        if (fixBtn) {
            fixBtn.onclick = function () {
                var payload = ddlFixables().map(function (r) {
                    return { schema: r.schema, table: r.table,
                             columns: r.missing_in_dest };
                });
                var text = "Добавить недостающие колонки в " + payload.length +
                    " таблиц(ы) приёмника (ALTER TABLE ADD COLUMN)?";

                var go = function () {
                    fixBtn.disabled = true;
                    api("/api/gpcopy/add-columns", "POST", {
                        dest_connection_id: dstId(), tables: payload,
                    }).then(function (d) {
                        fixBtn.disabled = false;
                        toast(d.ok
                            ? "Добавлено колонок: " + d.added +
                              (d.failed ? ", ошибок: " + d.failed : "")
                            : (d.message || "Ошибка"),
                            d.ok && !d.failed ? "success" : "error");
                        ddlRun();   // перепроверить после правок
                    });
                };

                if (window.gpConfirm) {
                    window.gpConfirm(text).then(function (y) { if (y) { go(); } });
                } else if (confirm(text)) { go(); }
            };
        }

        var depsBtn = $("gppDdlDeps");

        if (depsBtn) {
            depsBtn.onclick = function () {
                var n = (ddlLast.deps || []).length;
                var text = "Досоздать в приёмнике " + n + " зависимост" +
                    (n === 1 ? "ь" : "и(ей)") +
                    "? Расширения — CREATE EXTENSION, функции переносятся " +
                    "из источника, sequences создаются заново.";

                var go = function () {
                    depsBtn.disabled = true;
                    api("/api/gpcopy/fix-deps", "POST", {
                        source_connection_id: srcId(),
                        dest_connection_id: dstId(),
                        tables: selTables(),
                    }).then(function (d) {
                        depsBtn.disabled = false;
                        toast(d.ok
                            ? "Создано: " + d.created +
                              (d.failed ? ", ошибок: " + d.failed : "")
                            : (d.message || "Ошибка"),
                            d.ok && !d.failed ? "success" : "error");

                        if (d.ok && d.failed) {
                            var bad = (d.results || []).filter(function (r) {
                                return !r.ok;
                            }).map(function (r) {
                                return r.name + ": " + r.error;
                            }).join("\n");
                            if (bad) { console.warn("fix-deps errors:\n" + bad); }
                        }

                        ddlRun();   // перепроверить после правок
                    });
                };

                if (window.gpConfirm) {
                    window.gpConfirm(text).then(function (y) { if (y) { go(); } });
                } else if (confirm(text)) { go(); }
            };
        }
    }

    function ddlRun() {
        var box = $("gppDdlBox");
        var tables = selTables();

        if (!tables.length) {
            box.innerHTML = '<div class="gpp-hint">Сначала выбери таблицы (шаг 1).</div>';
            return Promise.resolve();
        }

        var btn = $("gppDdlCheck");
        btn.disabled = true;
        btn.textContent = "Проверяю DDL…";
        box.innerHTML = '<div class="gpp-hint">Читаю колонки с обеих сторон…</div>';

        // возвращаем промис: после правок надо дождаться перепроверки
        return api("/api/gpcopy/precheck", "POST", {
            source_connection_id: srcId(), dest_connection_id: dstId(),
            tables: tables,
        }).then(function (d) {
            btn.disabled = false;
            btn.textContent = "⚖ Проверить DDL";

            if (!d.ok) {
                box.innerHTML = '<div class="gpp-hint" style="color: var(--crit);">' +
                    esc(d.message || "Ошибка") + "</div>";
                return;
            }

            ddlLast = d;
            ddlRender();
        }).catch(function (e) {
            btn.disabled = false;
            btn.textContent = "⚖ Проверить DDL";
            box.innerHTML = '<div class="gpp-hint" style="color: var(--crit);">' +
                esc(String(e)) + "</div>";
        });
    }

    if ($("gppDdlCheck")) { $("gppDdlCheck").onclick = ddlRun; }

    /* ---------------- fancy connection dropdown ---------------- */

    function fancySelect(sel) {
        var wrap = sel.parentElement; // .gpp-db

        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "gpp-dbbtn";

        var dd = document.createElement("div");
        dd.className = "gpp-dd";

        function optLabel(o) {
            var name = o.getAttribute("data-name") || o.textContent.trim();
            var db = o.getAttribute("data-db") || "";
            return { name: name, db: db };
        }

        function renderBtn() {
            var o = sel.selectedOptions[0];
            var l = o ? optLabel(o) : { name: "—", db: "" };
            btn.innerHTML = "<span>" + esc(l.name) +
                (l.db ? ' <span style="color: var(--text-muted); font-weight: 400;">· ' +
                    esc(l.db) + "</span>" : "") +
                '</span><span class="chev">▾</span>';
        }

        function close() {
            dd.classList.remove("show");
            btn.classList.remove("open");
        }

        function open() {
            dd.innerHTML = Array.from(sel.options).map(function (o) {
                var l = optLabel(o);
                var cur = o.value === sel.value;
                var host = o.getAttribute("data-host") || "";
                return '<div class="opt' + (cur ? " cur" : "") + '" data-v="' +
                    esc(o.value) + '"><span><span class="name">' + esc(l.name) +
                    "</span>" +
                    ((l.db || host) ? ' <span class="sub">' +
                        esc(l.db + (host ? " @ " + host : "")) + "</span>" : "") +
                    "</span>" + (cur ? '<span class="ck">✓</span>' : "") + "</div>";
            }).join("");

            dd.querySelectorAll(".opt").forEach(function (el) {
                el.onclick = function (ev) {
                    ev.stopPropagation();
                    if (sel.value !== el.getAttribute("data-v")) {
                        sel.value = el.getAttribute("data-v");
                        sel.dispatchEvent(new Event("change"));
                    }
                    close();
                    renderBtn();
                };
            });

            dd.classList.add("show");
            btn.classList.add("open");
        }

        btn.onclick = function (ev) {
            ev.stopPropagation();
            if (dd.classList.contains("show")) { close(); } else { open(); }
        };
        document.addEventListener("click", close);
        document.addEventListener("keydown", function (e) {
            if (e.key === "Escape") { close(); }
        });
        sel.addEventListener("change", renderBtn);

        sel.insertAdjacentElement("afterend", btn);
        wrap.appendChild(dd);
        renderBtn();
    }

    /* ---------------- wiring ---------------- */

    function init() {
        // mode cards
        document.querySelectorAll("#gppModes .gpp-mode").forEach(function (card) {
            card.onclick = function () {
                document.querySelectorAll("#gppModes .gpp-mode").forEach(function (c) {
                    c.classList.remove("sel");
                });
                card.classList.add("sel");
                state.mode = card.getAttribute("data-m");
                ["full", "inc", "date", "part"].forEach(function (m) {
                    $("gppP-" + m).classList.toggle("show", m === state.mode);
                });
                renderSummary();
            };
        });

        // стратегия инкремента: watermark | key
        document.querySelectorAll("#gppIncStrategy .opt").forEach(function (opt) {
            opt.onclick = function () {
                document.querySelectorAll("#gppIncStrategy .opt").forEach(function (o) {
                    o.classList.remove("sel");
                });
                opt.classList.add("sel");
                state.incStrategy = opt.getAttribute("data-s");
                $("gppIncWm").style.display =
                    state.incStrategy === "watermark" ? "" : "none";
                $("gppIncKey").style.display =
                    state.incStrategy === "key" ? "" : "none";
                renderSummary();
            };
        });

        // when
        document.querySelectorAll("#gppWhen .opt").forEach(function (opt) {
            opt.onclick = function () {
                document.querySelectorAll("#gppWhen .opt").forEach(function (o) {
                    o.classList.remove("sel");
                });
                opt.classList.add("sel");
                state.when = opt.getAttribute("data-w");
                $("gppP-sched").classList.toggle("show", state.when === "sched");
                if (state.when === "sched") { previewCron(); }
                renderSummary();
            };
        });

        // date preset custom fields
        $("gppDatePreset").onchange = function () {
            var custom = $("gppDatePreset").value === "custom";
            $("gppDateFromF").style.display = custom ? "" : "none";
            $("gppDateToF").style.display = custom ? "" : "none";
        };

        // календарный выбор расписания вместо cron-строки
        if (window.gpCronPicker && $("gppSchedPicker")) {
            gpCronPicker($("gppSchedPicker"), {
                value: $("gppSchedCron").value,
                onChange: function (cron) {
                    $("gppSchedCron").value = cron;
                    previewCron();
                    renderSummary();
                },
            });
        }

        // priorities invalidate resolutions
        $("gppIncPriority").oninput = function () { state.incResolved = null; };
        $("gppDatePriority").oninput = function () { state.dateResolved = null; };

        // smart checks
        $("gppIncCheck").onclick = function () {
            checkColumns("gppIncPriority", "gppIncHint", "incResolved");
        };
        $("gppIncWmPreview").onclick = previewWatermarks;
        if ($("gppKeyFilter")) {
            $("gppKeyFilter").oninput = function () {
                state.keyFilter = $("gppKeyFilter").value;
                state.keyLimit = KEY_CHUNK;
                renderKeyList();
            };
        }

        $("gppIncFilter").oninput = function () {
            state.incFilter = $("gppIncFilter").value;
            renderIncList();
        };
        $("gppDateCheck").onclick = function () {
            checkColumns("gppDatePriority", "gppDateHint", "dateResolved");
        };
        $("gppSyncKeys").onclick = resolveSyncKeys;
        $("gppSyncCompute").onclick = computeMissingKeys;
        $("gppPartPreviewBtn").onclick = partPreview;

        // selection modal
        $("gppOpenSel").onclick = function () {
            $("gppSelModal").classList.add("show");
            $("gppSelSearch").focus();
            if (!state.catalog) { loadCatalog(false); }
            loadSets();
        };
        $("gppSelCancel").onclick = $("gppSelDone").onclick = function () {
            $("gppSelModal").classList.remove("show");
        };
        $("gppSelModal").addEventListener("click", function (ev) {
            if (ev.target === $("gppSelModal")) {
                $("gppSelModal").classList.remove("show");
            }
        });
        document.addEventListener("keydown", function (ev) {
            if (ev.key === "Escape") {
                $("gppSelModal").classList.remove("show");
            }
        });

        $("gppSelSearch").oninput = function () {
            clearTimeout(searchTimer);
            searchTimer = setTimeout(doSearch, 300);
        };
        $("gppSelSearch").onkeydown = function (ev) {
            if (ev.key === "Enter") { ev.preventDefault(); selectAllFound(); }
        };

        $("gppSelPasteApply").onclick = applyPaste;
        $("gppSelSetSave").onclick = function () { saveSet("gppSelSetName"); };
        $("gppSaveSet").onclick = function () {
            var name = prompt("Имя набора:");
            if (!name) { return; }
            $("gppSelSetName").value = name;
            saveSet("gppSelSetName");
        };

        $("gppClearSel").onclick = function () {
            if (!state.sel.size) { return; }
            var doClear = function () { state.sel.clear(); onSelectionChanged(); };
            if (window.gpConfirm) {
                window.gpConfirm("Снять выбор со всех " + fmtN(state.sel.size) + " таблиц?")
                    .then(function (yes) { if (yes) { doClear(); } });
            } else if (confirm("Очистить выбор?")) { doClear(); }
        };

        // connections — запоминаем выбор в localStorage, чтобы добавление
        // нового коннектора не подменяло источник/назначение
        // у каждого тулкита свои подключения — и своя память выбора
        var TK = (($("gppModes") || {}).dataset || {}).toolkit || "gp";
        var SRC_KEY = "gpp-src-conn-" + TK;
        var DST_KEY = "gpp-dst-conn-" + TK;

        function hasOption(sel, val) {
            return Array.prototype.some.call(sel.options, function (o) {
                return o.value === String(val);
            });
        }
        function restoreConn(sel, key) {
            try {
                var saved = localStorage.getItem(key);
                if (saved && hasOption(sel, saved)) { sel.value = saved; return true; }
            } catch (e) {}
            return false;
        }
        function saveConn(key, val) {
            try { localStorage.setItem(key, String(val)); } catch (e) {}
        }

        $("gppSrc").onchange = function () {
            saveConn(SRC_KEY, $("gppSrc").value);
            state.catalog = null;
            invalidateResolutions();
            loadSel();
            renderSelection();
            loadCatalog(false);
            loadSets();
        };
        $("gppDst").onchange = function () {
            saveConn(DST_KEY, $("gppDst").value);
            renderSummary();
        };
        $("gppCatalogRefresh").onclick = function () { loadCatalog(true); };

        // восстановить прошлый выбор; иначе дефолт — назначение = второе
        restoreConn($("gppSrc"), SRC_KEY);
        if (!restoreConn($("gppDst"), DST_KEY)) {
            if ($("gppDst").options.length > 1) { $("gppDst").selectedIndex = 1; }
        }

        fancySelect($("gppSrc"));
        fancySelect($("gppDst"));

        $("gppGo").onclick = go;

        var partAuto = $("gppPartAuto");

        if (partAuto) {
            try {
                partAuto.checked =
                    localStorage.getItem("gpp-part-auto") !== "0";
            } catch (e) { /* приватный режим */ }

            partAuto.onchange = function () {
                try {
                    localStorage.setItem("gpp-part-auto",
                                         partAuto.checked ? "1" : "0");
                } catch (e) { /* приватный режим */ }
                renderSummary();
            };
        }

        var auto = $("gppAutoCreate");

        if (auto) {
            try {
                auto.checked = localStorage.getItem("gpp-auto-create") !== "0";
            } catch (e) { /* приватный режим */ }

            auto.onchange = function () {
                try {
                    localStorage.setItem("gpp-auto-create",
                                         auto.checked ? "1" : "0");
                } catch (e) { /* приватный режим */ }
            };
        }

        loadSel();
        renderSelection();
        loadCatalog(false);
        loadRuns();
        renderSummary();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
