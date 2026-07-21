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

    function api(url, method, body) {
        var opts = { method: method || "GET" };
        if (body !== undefined) {
            opts.headers = { "Content-Type": "application/json" };
            opts.body = JSON.stringify(body);
        }
        return fetch(url, opts).then(function (r) { return r.json(); });
    }

    function fmtN(n) { return Number(n || 0).toLocaleString("ru-RU"); }

    /* ---------------- state ---------------- */

    var state = {
        mode: "full",
        incStrategy: "watermark",  // watermark | key
        when: "now",
        sel: new Set(),            // "schema.table"
        catalog: null,             // {total, schemas:[{schema,count}], cached_at}
        incResolved: null,         // [{schema,table,column,via}]
        dateResolved: null,
        syncKeys: {},              // "schema.table" -> {columns:[], source}
        syncUnresolved: [],
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
        state.dateResolved = null;
        state.syncKeys = {};
        state.syncUnresolved = [];
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

    function renderSelection() {
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
        renderModalCounts();
    }

    function onSelectionChanged() {
        invalidateResolutions();
        saveSel();
        renderSelection();
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
                    onSelectionChanged();
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

    function renderModalSchemas() {
        var box = $("gppSelSchemas");
        if (!state.catalog) { box.innerHTML = ""; return; }

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
                (selN ? '<span class="selcnt">выбрано ' + fmtN(selN) + "</span> " : "") +
                '<button class="gpp-btn sm" data-schema="' + esc(s.schema) +
                '" data-act="' + (selN >= s.total ? "unsel" : "sel") + '">' +
                (selN >= s.total ? "снять" : "выбрать все") + "</button></span></div>" +
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

        // «выбрать все» по схеме — без партиций (родитель уже включает их данные)
        box.querySelectorAll("button[data-schema]").forEach(function (btn) {
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

        return api("/api/catalog/resolve-columns", "POST", {
            connection_id: srcId(),
            tables: tables,
            priority: priority,
            fallback_any_date: true,
        }).then(function (d) {
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
            return d;
        });
    }

    /* ---------------- smart resolution: sync keys ---------------- */

    var KEY_BADGES = {
        pk: ["pk", "PK"],
        unique_index: ["uniq", "уник. индекс"],
        computed: ["comp", "вычислен"],
    };

    function renderKeyList() {
        var box = $("gppKeyList");
        if (!box) { return; }

        var rows = [];
        var keys = Object.keys(state.syncKeys).sort();

        keys.slice(0, 100).forEach(function (k) {
            var info = state.syncKeys[k];
            var b = KEY_BADGES[info.source] || ["", info.source];
            rows.push('<div class="gpp-key-row"><span>' + esc(k) +
                ' <span class="cols">→ ' + esc(info.columns.join(", ")) +
                '</span></span><span class="gpp-key-badge ' + b[0] + '">' +
                esc(b[1]) + "</span></div>");
        });

        (state.syncUnresolved || []).slice(0, 100).forEach(function (t) {
            rows.push('<div class="gpp-key-row"><span>' +
                esc(t.schema + "." + t.table) +
                '</span><span class="gpp-key-badge none">нет ключа</span></div>');
        });

        var extra = keys.length + (state.syncUnresolved || []).length - rows.length;
        if (extra > 0) {
            rows.push('<div class="gpp-hint">…и ещё ' + fmtN(extra) + "</div>");
        }
        box.innerHTML = rows.join("");
    }

    function resolveSyncKeys() {
        var tables = selTables();
        var hint = $("gppSyncHint");
        if (!tables.length) {
            hint.innerHTML = '<span class="warn">Сначала выбери таблицы (шаг 1).</span>';
            return Promise.resolve(null);
        }

        hint.textContent = "Ищу ключи у " + fmtN(tables.length) + " таблиц…";

        return api("/api/catalog/resolve-keys", "POST", {
            connection_id: srcId(),
            tables: tables,
        }).then(function (d) {
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

    function computeMissingKeys() {
        var hint = $("gppSyncHint");
        var pending = state.syncUnresolved || [];

        if (!pending.length) {
            hint.innerHTML = "Нет таблиц без ключа. Сначала нажми «Ключи: PK + уник. индексы».";
            return;
        }

        var CAP = 20;
        var todo = pending.slice(0, CAP);
        var found = 0, checked = 0;

        $("gppSyncCompute").disabled = true;

        function next(i) {
            if (i >= todo.length) {
                $("gppSyncCompute").disabled = false;
                state.syncUnresolved = pending.filter(function (t) {
                    return !state.syncKeys[t.schema + "." + t.table];
                });
                var msg = "Вычисление: проверено " + fmtN(checked) +
                    ', <span class="good">ключ найден у ' + fmtN(found) + "</span>";
                if (state.syncUnresolved.length) {
                    msg += ' · <span class="warn">без ключа: ' +
                        fmtN(state.syncUnresolved.length) + " (будут пропущены)</span>";
                }
                if (pending.length > CAP) {
                    msg += " · за раз проверяем до " + CAP + " таблиц — запросы тяжёлые";
                }
                hint.innerHTML = msg;
                renderKeyList();
                return;
            }
            var t = todo[i];
            hint.textContent = "Пробую по данным: " + t.schema + "." + t.table +
                " (" + (i + 1) + "/" + todo.length + ")…";
            api("/api/catalog/compute-unique", "POST", {
                connection_id: srcId(), schema: t.schema, table: t.table,
            }).then(function (d) {
                checked += 1;
                if (d.ok && d.column) {
                    state.syncKeys[t.schema + "." + t.table] =
                        { columns: [d.column], source: "computed" };
                    found += 1;
                }
                next(i + 1);
            }).catch(function () { checked += 1; next(i + 1); });
        }
        next(0);
    }

    /* ---------------- partition diff preview ---------------- */

    function partPreview() {
        var tables = selTables();
        var box = $("gppPartPreview");
        if (!tables.length) {
            box.innerHTML = '<span class="warn">Сначала выбери таблицы (шаг 1).</span>';
            return;
        }
        if (tables.length > 5) {
            box.innerHTML = "Выбрано " + fmtN(tables.length) +
                " таблиц — превью показываем до 5. Diff по всем посчитается при запуске.";
            return;
        }

        box.textContent = "Считаю партиции…";
        var lines = [];

        function next(i) {
            if (i >= tables.length) { box.innerHTML = lines.join("<br>"); return; }
            var t = tables[i];
            api("/api/gpcopy/partition-diff/preview", "POST", {
                source_connection_id: srcId(), dest_connection_id: dstId(),
                schema: t.schema, table: t.table,
            }).then(function (d) {
                if (!d.ok) {
                    lines.push(esc(t.schema + "." + t.table) +
                        ': <span class="bad">' + esc(d.message) + "</span>");
                } else {
                    lines.push("<b>" + esc(t.schema + "." + t.table) + "</b>: партиций " +
                        fmtN(d.partitions.length) + ', перелить <span class="' +
                        (d.to_copy.length ? "warn" : "good") + '">' +
                        fmtN(d.to_copy.length) + "</span>");
                }
                next(i + 1);
            });
        }
        next(0);
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

    function cronExpr() {
        var p = $("gppSchedPreset").value;
        return p === "custom" ? $("gppSchedCron").value.trim() : p;
    }

    function renderSummary() {
        var by = schemaCounts();
        var schemas = Object.keys(by).sort();
        var srcName = $("gppSrc").selectedOptions[0]
            ? $("gppSrc").selectedOptions[0].textContent : "?";
        var dstName = $("gppDst").selectedOptions[0]
            ? $("gppDst").selectedOptions[0].textContent : "?";

        var whenTxt = state.when === "sched"
            ? "по расписанию <b>" + esc(cronExpr()) + "</b>"
            : "<b>прямо сейчас</b>";

        $("gppSummary").innerHTML = "Скопировать <b>" + fmtN(state.sel.size) +
            " таблиц</b>" +
            (schemas.length ? " (" + schemas.slice(0, 4).map(esc).join(", ") +
                (schemas.length > 4 ? "…" : "") + ")" : "") +
            " из <b>" + esc(srcName.trim()) + "</b> в <b>" + esc(dstName.trim()) +
            "</b> — <b>" + modeName() + "</b>, " + whenTxt + ".";

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
        // watermark по резолву; таблицы без колонки пропускаем
        var byKey = {};
        (state.incResolved || []).forEach(function (r) {
            byKey[r.schema + "." + r.table] = r.column;
        });
        return selTables()
            .filter(function (t) { return byKey[t.schema + "." + t.table]; })
            .map(function (t) {
                return { schema: t.schema, table: t.table,
                         watermark_column: byKey[t.schema + "." + t.table] };
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

        // part
        return api("/api/gpcopy/partition-diff/start", "POST", {
            source_connection_id: srcId(), dest_connection_id: dstId(),
            tables: tables, gpcopy_path: ex.gpcopy_path, jobs: ex.jobs,
        });
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

        // part
        base.tables = tables;
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

    function go() {
        if (!state.sel.size) { setMsg("Сначала выбери таблицы (шаг 1).", "err"); return; }
        if (isNaN(srcId()) || isNaN(dstId())) { setMsg("Выбери подключения.", "err"); return; }
        if (srcId() === dstId()) { setMsg("Источник и назначение совпадают.", "err"); return; }

        $("gppGo").disabled = true;
        setMsg("Запускаю…");

        var p = state.when === "sched" ? createSchedule() : launchNow();

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

    var RUN_TYPES = "gpcopy,gpcopy_date,gpcopy_increment,gpcopy_partition_diff,gpcopy_sync";

    function loadRuns() {
        api("/api/jobs/recent?types=" + RUN_TYPES + "&limit=8").then(function (d) {
            if (!d.ok) { return; }
            var box = $("gppRuns");
            if (!d.jobs.length) {
                box.innerHTML = '<div class="gpp-hint">Пока нет запусков.</div>';
                return;
            }
            box.innerHTML = d.jobs.map(function (j) {
                var running = j.status === "running" || j.status === "queued" ||
                    j.status === "stopping";
                var failed = j.status === "failed" || j.status === "error" ||
                    j.status === "cancelled";
                var dot = running ? "b" : (failed ? "r" : (j.status === "done" ? "g" : "i"));
                var pct = Math.max(0, Math.min(100, Math.round(j.progress_percent || 0)));
                if (j.status === "done") { pct = 100; }
                var barCls = j.status === "done" ? "done" : (failed ? "fail" : "");
                var label = RUN_LABELS[j.job_type] || j.job_type;
                var statusTxt = running ? (pct + "%") : esc(j.status);
                var meta = esc(j.started_at || "");
                if (failed && j.error_message) {
                    meta += " · " + esc(String(j.error_message).slice(0, 60));
                }
                return '<div class="run"><span class="gpp-dot ' + dot + '"></span>' +
                    "<span>#" + j.id + " · " + esc(label) + " · " +
                    fmtN(j.total_items) + " объектов</span>" +
                    '<div class="gpp-bar"><i class="' + barCls + '" style="width: ' + pct +
                    '%;"></i></div><span>' + statusTxt + "</span>" +
                    '<span class="meta">' + meta + "</span></div>";
            }).join("");

            var anyActive = d.jobs.some(function (j) {
                return j.status === "running" || j.status === "queued" ||
                    j.status === "stopping";
            });
            clearTimeout(loadRuns._t);
            loadRuns._t = setTimeout(loadRuns, anyActive ? 5000 : 20000);
        });
    }

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

        // sched preset custom cron
        $("gppSchedPreset").onchange = function () {
            $("gppSchedCronF").style.display =
                $("gppSchedPreset").value === "custom" ? "" : "none";
            previewCron();
        };
        $("gppSchedCron").oninput = previewCron;

        // priorities invalidate resolutions
        $("gppIncPriority").oninput = function () { state.incResolved = null; };
        $("gppDatePriority").oninput = function () { state.dateResolved = null; };

        // smart checks
        $("gppIncCheck").onclick = function () {
            checkColumns("gppIncPriority", "gppIncHint", "incResolved");
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
            if (ev.key === "Escape") { $("gppSelModal").classList.remove("show"); }
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

        // connections
        $("gppSrc").onchange = function () {
            state.catalog = null;
            invalidateResolutions();
            loadSel();
            renderSelection();
            loadCatalog(false);
            loadSets();
        };
        $("gppDst").onchange = renderSummary;
        $("gppCatalogRefresh").onclick = function () { loadCatalog(true); };

        // если подключений ≥ 2 — по умолчанию назначение второе
        if ($("gppDst").options.length > 1) { $("gppDst").selectedIndex = 1; }

        fancySelect($("gppSrc"));
        fancySelect($("gppDst"));

        $("gppGo").onclick = go;

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
