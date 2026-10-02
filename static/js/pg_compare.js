/* ============================================================
   Postgres Toolkit → «Синхронизация» → режим «Сравнение и разница».
   Дерево источника «схема → таблицы», фоновое сравнение pg_compare,
   выбор действия по таблицам и загрузка pg_diff_load.
   Мастер «Перенос» (gpcopy_pipeline.js) этот файл не трогает: только
   прячет его шаги, пока открыт свой режим, и читает #gppSrc / #gppDst.
   ============================================================ */
(function () {
    "use strict";

    var $ = function (id) { return document.getElementById(id); };

    var POLL_MS = 2500;
    // задача ещё идёт — продолжаем опрос
    var ACTIVE = { queued: 1, pending: 1, running: 1, stopping: 1 };

    var STATUS = {
        same: ["Совпадает", "ok"],
        differs: ["Отличается", "warn"],
        no_dest: ["Нет в приёмнике", "warn"],
        no_source: ["Нет в источнике", ""],
        structure_diff: ["Структура отличается", "bad"],
        duplicate_keys: ["Дубликаты ключа", "bad"],
        error: ["Ошибка", "bad"],
        cancelled: ["Отменено", ""]
    };
    var JOB_STATUS = {
        queued: "в очереди", pending: "в очереди", running: "идёт",
        stopping: "останавливается", done: "завершено", failed: "ошибка",
        cancelled: "остановлено", interrupted: "прервано"
    };
    var KEY_SOURCE = { pk: "PK", unique_index: "уник. индекс", sync_keys: "сохранённый ключ" };
    var ACTION_LABEL = {
        diff: "разница", full: "полная (TRUNCATE + INSERT)",
        create: "создать и залить", skip: "пропустить"
    };

    var st = {
        mode: "transfer",
        ctxSeq: 0,          // растёт при смене пары или режима: ответы старых кликов отбрасываем
        catalogFor: null,   // id источника, для которого построено дерево
        schemas: [],        // [{schema, total}]
        tables: {},         // schema -> [{table, kind}] без листьев партиций | {error}
        tablesAll: {},      // schema -> [{table, kind, parent}] как отдал каталог
        tablesReq: {},      // schema -> Promise загрузки
        open: {},           // schema -> раскрыта
        selSchemas: {},     // schema -> true («вся схема»)
        selTables: {},      // key -> {schema, table}
        query: "",
        hits: null,         // результаты поиска [{schema, table}] | {error}
        searchSeq: 0,
        searchTimer: null,
        pair: null,         // {src, dst} показанного сравнения
        cmpJob: null,
        cmpCurrent: null,
        cmpResults: [],
        cmpTimer: null,
        cmpSeq: 0,
        actions: {},        // key -> diff | full | create | skip
        loadJob: null,
        loadItems: [],
        loadExpected: {},   // key -> {action, insert, update, del, rows}
        loadDelete: false,
        loadTimer: null,
        loadSeq: 0          // жива только цепочка опроса с последним номером
    };

    /* ---------------- helpers ---------------- */

    function pgcmpEsc(s) {
        return String(s == null ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    }

    function pgcmpN(n) {
        return n == null || n === "" ? "—" : Number(n).toLocaleString("ru-RU");
    }

    function pgcmpKey(schema, table) { return JSON.stringify([schema, table]); }

    function pgcmpDate(s) {
        return s ? String(s).replace("T", " ").slice(0, 19) : "";
    }

    function pgcmpToast(msg, type) {
        if (window.gpToast) { window.gpToast(msg, type || "info"); }
    }

    function pgcmpMsg(id, text, kind) {
        var el = $(id);
        if (!el) { return; }
        el.className = "gpp-msg" + (kind ? " " + kind : "");
        el.textContent = text || "";
    }

    // ответ всегда объект {ok, ...}: не-JSON и сетевые ошибки — текстом
    function pgcmpApi(url, method, body) {
        var opts = { method: method || "GET", headers: { "Accept": "application/json" } };
        if (body !== undefined) {
            opts.headers["Content-Type"] = "application/json";
            opts.body = JSON.stringify(body);
        }
        return fetch(url, opts).then(function (r) {
            return r.text().then(function (text) {
                var data = null;
                try { data = JSON.parse(text); } catch (e) { data = null; }
                if (!data || typeof data !== "object") {
                    return { ok: false, httpStatus: r.status,
                        message: "Сервер ответил HTTP " + r.status };
                }
                data.httpStatus = r.status;
                if (!r.ok && data.ok !== false) { data.ok = false; }
                if (data.ok === false && !data.message) {
                    data.message = data.error || ("Сервер ответил HTTP " + r.status);
                }
                return data;
            });
        }, function (e) {
            return { ok: false, httpStatus: 0,
                message: "Нет связи с сервером: " + (e && e.message || e) };
        });
    }

    function pgcmpSrc() { var el = $("gppSrc"); return el ? parseInt(el.value, 10) || null : null; }
    function pgcmpDst() { var el = $("gppDst"); return el ? parseInt(el.value, 10) || null : null; }

    // имя подключения по его id — того самого, что уйдёт в запрос
    function pgcmpConnName(selectEl, connId) {
        var opts = selectEl ? selectEl.options : [];
        for (var i = 0; i < opts.length; i++) {
            if (parseInt(opts[i].value, 10) === connId) {
                return opts[i].getAttribute("data-name") || opts[i].textContent;
            }
        }
        return "#" + connId;
    }

    function pgcmpPairNames(src, dst) {
        return "из «" + pgcmpConnName($("gppSrc"), src) + "» в «" +
            pgcmpConnName($("gppDst"), dst) + "»";
    }

    // без своего диалога действие не выполняется: confirm() не используем
    function pgcmpConfirm(msgId, text, opts) {
        if (!window.gpConfirm) {
            pgcmpMsg(msgId, "Окно подтверждения недоступно — действие не выполнено. " +
                "Обновите страницу.", "err");
            return Promise.resolve(false);
        }
        return window.gpConfirm(text, opts);
    }

    // общие проверки пары перед любым запуском; текст ошибки или ""
    function pgcmpPairError() {
        var src = pgcmpSrc();
        var dst = pgcmpDst();
        if (!src || !dst) { return "Выберите источник и приёмник в шапке страницы."; }
        if (src === dst) { return "Источник и приёмник совпадают — выберите разные подключения."; }
        return "";
    }

    function pgcmpSelection() {
        var schemas = Object.keys(st.selSchemas).sort();
        var tables = Object.keys(st.selTables).map(function (k) {
            return { schema: st.selTables[k].schema, table: st.selTables[k].table };
        });
        return { schemas: schemas, tables: tables };
    }

    /* ---------------- режим ---------------- */

    function pgcmpSetMode(mode) {
        st.mode = mode === "compare" ? "compare" : "transfer";
        st.ctxSeq++;
        var compare = st.mode === "compare";

        var sw = $("pgcmpModeSwitch");
        if (sw) {
            Array.prototype.forEach.call(sw.querySelectorAll("[data-pgcmp-mode]"), function (b) {
                b.setAttribute("aria-pressed",
                    b.getAttribute("data-pgcmp-mode") === st.mode ? "true" : "false");
            });
        }

        $("pgcmpPanel").hidden = !compare;

        // шаги мастера «Перенос» прячем, ленту «Запуски» оставляем
        Array.prototype.forEach.call(document.querySelectorAll(".gpp > .gpp-step"), function (step) {
            if (step.id === "pgcmpPanel" || step.querySelector("#gppRuns")) { return; }
            step.classList.toggle("pgcmp-hide", compare);
        });

        if (compare) {
            pgcmpEnsureCatalog(false);
            pgcmpLoadLatest();
        } else {
            clearTimeout(st.cmpTimer);
            st.cmpSeq++;
        }
    }

    /* ---------------- дерево источника ---------------- */

    function pgcmpEnsureCatalog(force) {
        var src = pgcmpSrc();
        var tree = $("pgcmpTree");

        if (!src) {
            tree.innerHTML = '<div class="pgcmp-empty">Выберите источник в шапке страницы.</div>';
            return;
        }
        if (!force && st.catalogFor === src && st.schemas.length) {
            pgcmpRenderTree();
            return;
        }

        st.catalogFor = src;
        st.schemas = [];
        st.tables = {};
        st.tablesAll = {};
        st.tablesReq = {};
        st.open = {};
        st.selSchemas = {};
        st.selTables = {};
        st.hits = null;
        pgcmpRenderCount();
        tree.innerHTML = '<div class="pgcmp-empty">Загружаю каталог источника…</div>';

        pgcmpApi("/api/catalog?connection_id=" + src + (force ? "&force=1" : "")).then(function (d) {
            if (st.catalogFor !== src) { return; }
            if (!d.ok) {
                st.catalogFor = null;
                tree.innerHTML = '<div class="pgcmp-empty gpp-msg err">Каталог не загружен: ' +
                    pgcmpEsc(d.message) + "</div>";
                return;
            }
            st.schemas = d.schemas || [];
            pgcmpRenderTree();
        });
    }

    function pgcmpLoadTables(schema) {
        if (Array.isArray(st.tables[schema])) { return Promise.resolve(st.tables[schema]); }
        if (st.tablesReq[schema]) { return st.tablesReq[schema]; }

        var src = st.catalogFor;
        st.tablesReq[schema] = pgcmpApi("/api/catalog/schema-tables?connection_id=" + src +
            "&schema=" + encodeURIComponent(schema)).then(function (d) {
            delete st.tablesReq[schema];
            if (st.catalogFor !== src) { return null; }
            if (!d.ok) {
                st.tables[schema] = { error: d.message };
                return null;
            }
            // полный список с ролями нужен, чтобы отсеять листья выбранных родителей
            st.tablesAll[schema] = d.tables || [];
            // листья партиций сравниваются через родителя
            st.tables[schema] = st.tablesAll[schema].filter(function (t) {
                return t.kind !== "partition";
            });
            return st.tables[schema];
        });
        return st.tablesReq[schema];
    }

    function pgcmpTableRow(schema, table) {
        var covered = !!st.selSchemas[schema];
        var checked = covered || !!st.selTables[pgcmpKey(schema, table)];
        return '<label class="pgcmp-tbl' + (covered ? " covered" : "") + '"' +
            (covered ? ' title="Схема отмечена целиком"' : "") + ">" +
            '<input type="checkbox" data-act="table" data-s="' + pgcmpEsc(schema) +
            '" data-t="' + pgcmpEsc(table) + '"' + (checked ? " checked" : "") +
            (covered ? " disabled" : "") + "> " + pgcmpEsc(table) + "</label>";
    }

    function pgcmpSchemaRow(sc) {
        var schema = sc.schema;
        var open = !!st.open[schema];
        var html = '<div class="pgcmp-sc">' +
            '<button type="button" class="chev" data-act="toggle" data-s="' + pgcmpEsc(schema) +
            '" aria-expanded="' + (open ? "true" : "false") + '" aria-label="Таблицы схемы">' +
            (open ? "▾" : "▸") + "</button>" +
            '<label style="display:flex;align-items:center;gap:8px;margin:0;cursor:pointer;">' +
            '<input type="checkbox" data-act="schema" data-s="' + pgcmpEsc(schema) + '"' +
            (st.selSchemas[schema] ? " checked" : "") + "> <code>" + pgcmpEsc(schema) +
            "</code></label>" +
            '<span class="cnt">' + pgcmpN(sc.total) + " табл.</span></div>";

        if (open) {
            var t = st.tables[schema];
            html += '<div class="pgcmp-tbls">';
            if (Array.isArray(t)) {
                html += t.length
                    ? t.map(function (r) { return pgcmpTableRow(schema, r.table); }).join("")
                    : '<div class="pgcmp-empty">В схеме нет таблиц.</div>';
            } else if (t && t.error) {
                html += '<div class="gpp-msg err">' + pgcmpEsc(t.error) + "</div>";
            } else {
                html += '<div class="pgcmp-empty">Загружаю…</div>';
            }
            html += "</div>";
        }
        return html;
    }

    function pgcmpRenderTree() {
        var tree = $("pgcmpTree");
        var q = st.query.toLowerCase();
        var html = "";

        if (!st.schemas.length) {
            tree.innerHTML = '<div class="pgcmp-empty">В источнике нет таблиц.</div>';
            return;
        }

        if (q.length >= 2) {
            var schemas = st.schemas.filter(function (s) {
                return s.schema.toLowerCase().indexOf(q) >= 0;
            });
            html += schemas.map(pgcmpSchemaRow).join("");

            if (st.hits === null) {
                html += '<div class="pgcmp-empty">Ищу таблицы…</div>';
            } else if (st.hits.error) {
                html += '<div class="gpp-msg err">Поиск не удался: ' + pgcmpEsc(st.hits.error) + "</div>";
            } else if (st.hits.length) {
                html += '<div class="pgcmp-tbls" style="padding-left: 8px;">' +
                    st.hits.map(function (h) {
                        var covered = !!st.selSchemas[h.schema];
                        var checked = covered || !!st.selTables[pgcmpKey(h.schema, h.table)];
                        return '<label class="pgcmp-tbl' + (covered ? " covered" : "") + '">' +
                            '<input type="checkbox" data-act="table" data-s="' + pgcmpEsc(h.schema) +
                            '" data-t="' + pgcmpEsc(h.table) + '"' + (checked ? " checked" : "") +
                            (covered ? " disabled" : "") + "> " + pgcmpEsc(h.schema) + "." +
                            pgcmpEsc(h.table) + "</label>";
                    }).join("") + "</div>";
            }
            if (!schemas.length && st.hits && !st.hits.error && !st.hits.length) {
                html = '<div class="pgcmp-empty">Ничего не найдено.</div>';
            }
        } else {
            html = st.schemas.map(pgcmpSchemaRow).join("");
        }

        if (window.gpKeepScroll) {
            window.gpKeepScroll(tree, function () { tree.innerHTML = html; });
        } else {
            tree.innerHTML = html;
        }
    }

    function pgcmpRenderCount() {
        var nS = Object.keys(st.selSchemas).length;
        var nT = Object.keys(st.selTables).length;
        var parts = [];
        if (nS) { parts.push("схем целиком: " + nS); }
        if (nT) { parts.push("отдельных таблиц: " + nT); }
        $("pgcmpSelCount").textContent = parts.length ? "— " + parts.join(", ") : "— ничего не выбрано";
    }

    function pgcmpOnTreeChange(e) {
        var el = e.target;
        var act = el.getAttribute("data-act");
        var schema = el.getAttribute("data-s");

        if (act === "schema") {
            if (el.checked) {
                st.selSchemas[schema] = true;
                // таблицы отмеченной схемы и так войдут в сравнение
                Object.keys(st.selTables).forEach(function (k) {
                    if (st.selTables[k].schema === schema) { delete st.selTables[k]; }
                });
            } else {
                delete st.selSchemas[schema];
            }
        } else if (act === "table") {
            var table = el.getAttribute("data-t");
            var key = pgcmpKey(schema, table);
            if (el.checked) {
                st.selTables[key] = { schema: schema, table: table };
            } else {
                delete st.selTables[key];
            }
        } else {
            return;
        }
        pgcmpRenderCount();
        pgcmpRenderTree();
    }

    function pgcmpOnTreeClick(e) {
        var btn = e.target.closest ? e.target.closest('[data-act="toggle"]') : null;
        if (!btn) { return; }
        var schema = btn.getAttribute("data-s");
        st.open[schema] = !st.open[schema];
        pgcmpRenderTree();
        if (st.open[schema] && !Array.isArray(st.tables[schema])) {
            if (st.tables[schema] && st.tables[schema].error) { delete st.tables[schema]; }
            pgcmpLoadTables(schema).then(pgcmpRenderTree);
        }
    }

    function pgcmpOnSearch() {
        st.query = ($("pgcmpSearch").value || "").trim();
        clearTimeout(st.searchTimer);
        var seq = ++st.searchSeq;

        if (st.query.length < 2) {
            st.hits = null;
            pgcmpRenderTree();
            return;
        }
        st.hits = null;
        pgcmpRenderTree();

        st.searchTimer = setTimeout(function () {
            var src = st.catalogFor;
            if (!src) { return; }
            pgcmpApi("/api/catalog/search?connection_id=" + src + "&q=" +
                encodeURIComponent(st.query)).then(function (d) {
                if (seq !== st.searchSeq) { return; }
                st.hits = d.ok ? (d.tables || []) : { error: d.message };
                pgcmpRenderTree();
            });
        }, 300);
    }

    function pgcmpClearSelection() {
        st.selSchemas = {};
        st.selTables = {};
        pgcmpRenderCount();
        pgcmpRenderTree();
        pgcmpMsg("pgcmpMsg", "");
    }

    /* ---------------- сравнение ---------------- */

    function pgcmpStartCompare() {
        var err = pgcmpPairError();
        var sel = pgcmpSelection();
        if (!err && !sel.schemas.length && !sel.tables.length) {
            err = "Отметьте схемы целиком и/или отдельные таблицы.";
        }
        if (err) { pgcmpMsg("pgcmpMsg", err, "err"); return; }

        var src = pgcmpSrc();
        var dst = pgcmpDst();
        var ctx = st.ctxSeq;
        var btn = $("pgcmpCompareBtn");
        btn.disabled = true;
        pgcmpMsg("pgcmpMsg", "Запускаю сравнение…");

        pgcmpApi("/api/pg/compare/start", "POST", {
            source_connection_id: src,
            dest_connection_id: dst,
            schemas: sel.schemas,
            tables: sel.tables
        }).then(function (d) {
            btn.disabled = false;
            if (ctx !== st.ctxSeq) {
                // пара или режим сменились: ответ к текущему экрану не относится
                pgcmpMsg("pgcmpMsg", d.ok
                    ? "Сравнение #" + d.job_id + " запущено для прежней пары — оно видно в ленте «Запуски»."
                    : "", d.ok ? "" : undefined);
                return;
            }
            if (!d.ok) {
                pgcmpMsg("pgcmpMsg", "Сравнение не запущено: " + d.message, "err");
                return;
            }
            pgcmpMsg("pgcmpMsg", "Сравнение #" + d.job_id + " запущено" +
                (d.total_items ? ": таблиц — " + pgcmpN(d.total_items) : "") +
                ". Базы при этом не меняются.", "ok");
            st.pair = { src: src, dst: dst };
            st.cmpJob = { id: d.job_id, status: "running", total_items: d.total_items };
            st.cmpCurrent = null;
            st.cmpResults = [];
            st.actions = {};
            pgcmpRenderResults();
            pgcmpPollCompare(d.job_id);
        });
    }

    function pgcmpLoadLatest() {
        var src = pgcmpSrc();
        var dst = pgcmpDst();
        clearTimeout(st.cmpTimer);
        var seq = ++st.cmpSeq;

        st.cmpJob = null;
        st.cmpResults = [];
        st.cmpCurrent = null;
        st.actions = {};
        st.pair = null;

        if (!src || !dst) {
            pgcmpRenderResults("Выберите источник и приёмник в шапке страницы.");
            return;
        }
        $("pgcmpResults").innerHTML = '<div class="pgcmp-empty">Ищу последнее сравнение этой пары…</div>';

        pgcmpApi("/api/pg/compare/latest?source_connection_id=" + src +
            "&dest_connection_id=" + dst).then(function (d) {
            if (seq !== st.cmpSeq) { return; }
            st.pair = { src: src, dst: dst };
            if (!d.ok) {
                pgcmpRenderResults("", "Не удалось получить последнее сравнение: " + d.message);
                return;
            }
            if (!d.job) {
                pgcmpRenderResults("Эту пару подключений ещё не сравнивали. Отметьте схемы " +
                    "или таблицы выше и нажмите «Сравнить» — ни одна база при этом не меняется.");
                return;
            }
            pgcmpApplyCompare(d);
            if (ACTIVE[d.job.status]) { pgcmpSchedulePoll(d.job.id, seq); }
        });
    }

    function pgcmpSchedulePoll(jobId, seq) {
        clearTimeout(st.cmpTimer);
        st.cmpTimer = setTimeout(function () {
            if (seq === st.cmpSeq) { pgcmpPollCompare(jobId, seq); }
        }, POLL_MS);
    }

    function pgcmpPollCompare(jobId, seq) {
        if (seq === undefined) {
            clearTimeout(st.cmpTimer);
            seq = ++st.cmpSeq;
        }
        pgcmpApi("/api/pg/compare/results?job_id=" + jobId).then(function (d) {
            if (seq !== st.cmpSeq) { return; }
            if (!d.ok) {
                $("pgcmpProgress").innerHTML = '<div class="gpp-msg err">Результаты сравнения #' +
                    pgcmpEsc(jobId) + " не получены: " + pgcmpEsc(d.message) + "</div>";
                // временный сбой — пробуем ещё; задачи нет (404) — прекращаем
                if (d.httpStatus !== 404) { pgcmpSchedulePoll(jobId, seq); }
                return;
            }
            pgcmpApplyCompare(d);
            if (d.job && ACTIVE[d.job.status]) { pgcmpSchedulePoll(jobId, seq); }
        });
    }

    function pgcmpApplyCompare(d) {
        st.cmpJob = d.job;
        st.cmpCurrent = d.current || null;
        st.cmpResults = d.results || [];

        // действие по умолчанию: разница для отличающихся, остальное пропускаем;
        // «создать и залить» по умолчанию снята
        st.cmpResults.forEach(function (r) {
            var key = pgcmpKey(r.schema, r.table);
            if (st.actions[key] === undefined) {
                st.actions[key] = r.status === "differs" ? "diff" : "skip";
            }
        });
        pgcmpRenderResults();
    }

    function pgcmpStatusBadge(status) {
        var s = STATUS[status] || [status || "?", ""];
        return '<span class="pgcmp-st ' + s[1] + '">' + pgcmpEsc(s[0]) + "</span>";
    }

    function pgcmpKeyCell(r) {
        var cols = r.key_columns || [];
        if (!cols.length) {
            return r.status === "same" || r.status === "differs"
                ? '<span class="gpp-key-badge comp" title="Сравнение мультимножеств строк">без ключа · EXCEPT ALL</span>'
                : "—";
        }
        var src = KEY_SOURCE[r.key_source] || r.key_source || "";
        var cls = r.key_source === "pk" ? "pk" : (r.key_source === "unique_index" ? "uniq" : "man");
        return pgcmpEsc(cols.join(", ")) +
            (src ? ' <span class="gpp-key-badge ' + cls + '">' + pgcmpEsc(src) + "</span>" : "");
    }

    function pgcmpActionCell(r, i) {
        var key = pgcmpKey(r.schema, r.table);
        var cur = st.actions[key];

        if (r.status === "same" || r.status === "differs") {
            return '<select data-act="action" data-i="' + i + '" aria-label="Действие">' +
                ["diff", "full", "skip"].map(function (a) {
                    return '<option value="' + a + '"' + (cur === a ? " selected" : "") + ">" +
                        ACTION_LABEL[a] + "</option>";
                }).join("") + "</select>";
        }
        if (r.status === "no_dest") {
            return '<label class="gpp-auto" style="color: var(--text);">' +
                '<input type="checkbox" data-act="create" data-i="' + i + '"' +
                (cur === "create" ? " checked" : "") + "> создать и залить</label>";
        }
        if (r.status === "structure_diff") {
            return '<span class="msg">Загрузка недоступна: сверьте колонки кнопкой ' +
                "«Проверить DDL» в режиме «Перенос».</span>";
        }
        return '<span class="msg">—</span>';
    }

    function pgcmpRenderSummary() {
        var box = $("pgcmpSummary");
        if (!st.cmpResults.length) { box.classList.add("pgcmp-hide"); return; }

        var c = {};
        var ins = 0, upd = 0, del = 0;
        st.cmpResults.forEach(function (r) {
            c[r.status] = (c[r.status] || 0) + 1;
            ins += Number(r.to_insert || 0);
            upd += Number(r.to_update || 0);
            del += Number(r.to_delete || 0);
        });
        var parts = [
            "совпадает <b>" + pgcmpN(c.same || 0) + "</b>",
            "отличается <b>" + pgcmpN(c.differs || 0) + "</b>",
            "нет в приёмнике <b>" + pgcmpN(c.no_dest || 0) + "</b>",
            "нет в источнике <b>" + pgcmpN(c.no_source || 0) + "</b>"
        ];
        if (c.structure_diff) { parts.push("структура отличается <b>" + pgcmpN(c.structure_diff) + "</b>"); }
        if (c.duplicate_keys) { parts.push("дубликаты ключа <b>" + pgcmpN(c.duplicate_keys) + "</b>"); }
        parts.push("ошибки <b>" + pgcmpN((c.error || 0)) + "</b>");
        if (c.cancelled) { parts.push("отменено <b>" + pgcmpN(c.cancelled) + "</b>"); }

        box.innerHTML = "Таблиц: <b>" + pgcmpN(st.cmpResults.length) + "</b> · " + parts.join(" · ") +
            '<div style="margin-top: 4px;">Строк: добавить <b>' + pgcmpN(ins) +
            "</b> · изменить <b>" + pgcmpN(upd) + "</b> · удалить <b>" + pgcmpN(del) + "</b></div>";
        box.classList.remove("pgcmp-hide");
    }

    function pgcmpRenderProgress() {
        var box = $("pgcmpProgress");
        var job = st.cmpJob;
        if (!job || !ACTIVE[job.status]) { box.innerHTML = ""; return; }

        var total = Number(job.total_items || 0);
        var doneN = st.cmpResults.length;
        var pct = total ? Math.min(100, Math.round(doneN * 100 / total)) : 0;
        var cur = st.cmpCurrent
            ? "Сравнивается <b>" + pgcmpEsc(st.cmpCurrent.schema + "." + st.cmpCurrent.table) + "</b>"
            : (job.status === "stopping" ? "Останавливаю…" : "Готовлю сравнение…");

        box.innerHTML = '<div class="gpp-active">' +
            '<div class="what" style="flex: 1;">' + cur + ' <span class="mut" style="color: var(--text-muted);">· ' +
            pgcmpN(doneN) + " из " + pgcmpN(total) + "</span></div>" +
            '<div class="gpp-bar"><i class="' + (total ? "" : "ind") + '" style="width:' +
            (total ? pct : 38) + '%"></i></div>' +
            '<button type="button" class="gpp-btn sm stop" id="pgcmpCmpStop"' +
            (job.status === "stopping" ? " disabled" : "") + ">Стоп</button></div>";
    }

    function pgcmpRenderResults(emptyText, errText) {
        var job = st.cmpJob;
        var meta = $("pgcmpResMeta");
        var res = $("pgcmpResults");

        if (job) {
            var when = pgcmpDate(job.finished_at || job.started_at);
            meta.textContent = "— #" + job.id + (when ? " от " + when : "") +
                " · " + (JOB_STATUS[job.status] || job.status || "");
        } else {
            meta.textContent = "";
        }

        pgcmpRenderProgress();
        pgcmpRenderSummary();

        var hasMissing = st.cmpResults.some(function (r) { return r.status === "no_dest"; });
        $("pgcmpAllMissingRow").classList.toggle("pgcmp-hide", !hasMissing);
        $("pgcmpAllMissing").checked = hasMissing && st.cmpResults.every(function (r) {
            return r.status !== "no_dest" || st.actions[pgcmpKey(r.schema, r.table)] === "create";
        });

        if (errText) {
            res.innerHTML = '<div class="gpp-msg err">' + pgcmpEsc(errText) + "</div>";
        } else if (!job && emptyText) {
            res.innerHTML = '<div class="pgcmp-empty">' + pgcmpEsc(emptyText) + "</div>";
        } else if (!st.cmpResults.length && !(job && ACTIVE[job.status])) {
            res.innerHTML = '<div class="pgcmp-empty">' +
                (job && job.error_message ? '<span class="gpp-msg err">' + pgcmpEsc(job.error_message) + "</span>"
                    : "В этом сравнении нет результатов.") + "</div>";
        } else {
            var rows = st.cmpResults.map(function (r, i) {
                var errSt = r.status === "error" || r.status === "duplicate_keys";
                return "<tr><td class=\"name\">" + pgcmpEsc(r.schema) + "." + pgcmpEsc(r.table) +
                    (r.message ? '<div class="msg' + (errSt ? " err" : "") + '">' + pgcmpEsc(r.message) + "</div>" : "") +
                    "</td><td>" + pgcmpStatusBadge(r.status) + "</td>" +
                    "<td>" + pgcmpKeyCell(r) + "</td>" +
                    '<td class="num">' + pgcmpN(r.src_rows) + "</td>" +
                    '<td class="num">' + pgcmpN(r.dst_rows) + "</td>" +
                    '<td class="num">' + pgcmpN(r.to_insert) + "</td>" +
                    '<td class="num">' + pgcmpN(r.to_update) + "</td>" +
                    '<td class="num">' + pgcmpN(r.to_delete) + "</td>" +
                    "<td>" + pgcmpActionCell(r, i) + "</td></tr>";
            });
            if (st.cmpCurrent && job && ACTIVE[job.status]) {
                rows.push('<tr class="cur"><td class="name">' + pgcmpEsc(st.cmpCurrent.schema) + "." +
                    pgcmpEsc(st.cmpCurrent.table) + '</td><td><span class="pgcmp-st run">сравнивается…</span></td>' +
                    '<td colspan="7"></td></tr>');
            }
            var html = '<div class="pgcmp-tablewrap"><table class="pgcmp-table"><thead><tr>' +
                "<th>Таблица</th><th>Статус</th><th>Ключ</th><th>Источник</th><th>Приёмник</th>" +
                "<th>Добавить</th><th>Изменить</th><th>Удалить</th><th>Действие</th>" +
                "</tr></thead><tbody>" + rows.join("") + "</tbody></table></div>";
            if (window.gpKeepScroll) {
                window.gpKeepScroll(res, function () { res.innerHTML = html; });
            } else {
                res.innerHTML = html;
            }
        }

        pgcmpRenderLoadBox();
    }

    // строки, которые уйдут в загрузку
    function pgcmpLoadPlan() {
        return st.cmpResults.filter(function (r) {
            var a = st.actions[pgcmpKey(r.schema, r.table)];
            if (r.status === "no_dest") { return a === "create"; }
            return (r.status === "same" || r.status === "differs") && (a === "diff" || a === "full");
        });
    }

    function pgcmpRenderLoadBox() {
        var job = st.cmpJob;
        var loadable = st.cmpResults.some(function (r) {
            return r.status === "same" || r.status === "differs" || r.status === "no_dest";
        });
        var finished = job && !ACTIVE[job.status];
        $("pgcmpLoadBox").classList.toggle("pgcmp-hide", !(finished && loadable));

        var plan = pgcmpLoadPlan();
        var loadRunning = st.loadJob && ACTIVE[st.loadJob.status];
        $("pgcmpLoadBtn").disabled = !plan.length || !!loadRunning;
        $("pgcmpLoadHint").textContent = loadRunning
            ? "Дождитесь окончания текущей загрузки."
            : (plan.length ? "К загрузке: таблиц — " + pgcmpN(plan.length) + "."
                : "Выберите действие хотя бы для одной таблицы.");
    }

    function pgcmpOnResultsChange(e) {
        var el = e.target;
        var act = el.getAttribute("data-act");
        var r = st.cmpResults[parseInt(el.getAttribute("data-i"), 10)];
        if (!r) { return; }
        var key = pgcmpKey(r.schema, r.table);

        if (act === "action") {
            st.actions[key] = el.value;
        } else if (act === "create") {
            st.actions[key] = el.checked ? "create" : "skip";
        } else {
            return;
        }
        pgcmpRenderResults();
    }

    function pgcmpOnAllMissing() {
        var on = $("pgcmpAllMissing").checked;
        st.cmpResults.forEach(function (r) {
            if (r.status === "no_dest") { st.actions[pgcmpKey(r.schema, r.table)] = on ? "create" : "skip"; }
        });
        pgcmpRenderResults();
    }

    /* ---------------- стоп ---------------- */

    function pgcmpStop(jobId, what, msgId, after) {
        pgcmpConfirm(msgId, "Остановить " + what + " #" + jobId + "? Текущая таблица будет " +
            "откачена, необработанные — пропущены.", { danger: true, confirmText: "Остановить" }
        ).then(function (yes) {
            if (!yes) { return; }
            pgcmpApi("/api/jobs/" + jobId + "/stop", "POST").then(function (d) {
                if (!d.ok) {
                    pgcmpToast("Не удалось остановить #" + jobId + ": " + d.message, "error");
                    return;
                }
                pgcmpToast("Остановка #" + jobId + " запрошена", "info");
                after();
            });
        });
    }

    /* ---------------- загрузка ---------------- */

    function pgcmpStartLoad() {
        var job = st.cmpJob;
        if (!job || ACTIVE[job.status] || !st.pair) { return; }
        if (st.loadJob && ACTIVE[st.loadJob.status]) { return; }

        var plan = pgcmpLoadPlan();
        if (!plan.length) {
            pgcmpMsg("pgcmpLoadMsg", "Не выбрано ни одной таблицы: укажите «разница» или «полная», " +
                "либо отметьте «создать и залить».", "err");
            return;
        }

        var del = $("pgcmpDeleteMissing").checked;
        var n = { diff: 0, full: 0, create: 0 };
        var ins = 0, upd = 0, dels = 0, fullRows = 0;
        var expected = {};

        plan.forEach(function (r) {
            var key = pgcmpKey(r.schema, r.table);
            var a = st.actions[key];
            n[a]++;
            if (a === "diff") {
                ins += Number(r.to_insert || 0);
                upd += Number(r.to_update || 0);
                dels += Number(r.to_delete || 0);
                expected[key] = { action: a, insert: r.to_insert, update: r.to_update,
                    del: del ? r.to_delete : null };
            } else {
                fullRows += Number(r.src_rows || 0);
                expected[key] = { action: a, rows: r.src_rows };
            }
        });

        var pair = st.pair;
        var ctx = st.ctxSeq;
        var text = "Загрузка " + pgcmpPairNames(pair.src, pair.dst) + ", таблиц: " + plan.length + ". ";
        if (n.diff) {
            text += "Разница — " + n.diff + " табл.: добавить " + pgcmpN(ins) + ", изменить " + pgcmpN(upd) +
                (del ? ", удалить " + pgcmpN(dels) : "; удаление выключено") + ". ";
        }
        if (n.full) { text += "Полная (TRUNCATE + INSERT) — " + n.full + " табл. "; }
        if (n.create) { text += "Создать и залить — " + n.create + " табл. "; }
        if (n.full || n.create) { text += "Строк источника для полной заливки: " + pgcmpN(fullRows) + ". "; }
        text += "Источник только читается, каждая таблица меняется одной транзакцией.";

        var tables = plan.map(function (r) {
            return { schema: r.schema, table: r.table,
                action: st.actions[pgcmpKey(r.schema, r.table)] };
        });

        pgcmpConfirm("pgcmpLoadMsg", text, {
            title: "Загрузить в приёмник?",
            confirmText: "Загрузить",
            danger: del || n.full > 0
        }).then(function (yes) {
            if (!yes) { return; }
            if (ctx !== st.ctxSeq) {
                pgcmpMsg("pgcmpLoadMsg", "Пара подключений или режим сменились — загрузка не запущена.", "err");
                return;
            }
            pgcmpPostLoad({
                source_connection_id: pair.src,
                dest_connection_id: pair.dst,
                compare_job_id: job.id,
                delete_missing: del,
                tables: tables
            }, expected, ctx);
        });
    }

    function pgcmpStartFull() {
        var err = pgcmpPairError();
        var sel = pgcmpSelection();
        if (!err && !sel.schemas.length && !sel.tables.length) {
            err = "Отметьте схемы целиком и/или отдельные таблицы.";
        }
        if (err) { pgcmpMsg("pgcmpMsg", err, "err"); return; }
        if (st.loadJob && ACTIVE[st.loadJob.status]) {
            pgcmpMsg("pgcmpMsg", "Дождитесь окончания текущей загрузки #" + st.loadJob.id + ".", "err");
            return;
        }

        var src = pgcmpSrc();
        var dst = pgcmpDst();
        var ctx = st.ctxSeq;
        var btn = $("pgcmpFullBtn");
        btn.disabled = true;
        pgcmpMsg("pgcmpMsg", "Собираю список таблиц…");

        // каталог нужен и для схем целиком, и для схем отдельных таблиц:
        // по нему отсеиваются листья партиций выбранных родителей
        var need = {};
        sel.schemas.forEach(function (s) { need[s] = true; });
        sel.tables.forEach(function (t) { need[t.schema] = true; });
        var schemas = Object.keys(need);

        Promise.all(schemas.map(function (s) { return pgcmpLoadTables(s); })).then(function () {
            btn.disabled = false;
            if (ctx !== st.ctxSeq) { pgcmpMsg("pgcmpMsg", ""); return; }

            var bad = schemas.filter(function (s) { return !Array.isArray(st.tablesAll[s]); });
            if (bad.length) {
                pgcmpMsg("pgcmpMsg", "Не удалось получить таблицы схемы " + bad.join(", ") + ": " +
                    ((st.tables[bad[0]] && st.tables[bad[0]].error) || "каталог недоступен"), "err");
                return;
            }

            var chosen = {};
            var order = [];
            function add(schema, table) {
                var k = pgcmpKey(schema, table);
                if (chosen[k]) { return; }
                chosen[k] = { schema: schema, table: table };
                order.push(k);
            }
            sel.schemas.forEach(function (s) { st.tables[s].forEach(function (t) { add(s, t.table); }); });
            sel.tables.forEach(function (t) { add(t.schema, t.table); });

            // лист партиции, чей родитель тоже выбран, загрузится через родителя
            // (каталог отдаёт имя корня без схемы — корень ищем в схеме листа)
            var tables = [];
            order.forEach(function (k) {
                var t = chosen[k];
                var info = null;
                st.tablesAll[t.schema].forEach(function (r) { if (r.table === t.table) { info = r; } });
                if (info && info.kind === "partition" && info.parent &&
                    chosen[pgcmpKey(t.schema, info.parent)]) {
                    return;
                }
                tables.push({ schema: t.schema, table: t.table, action: "full" });
            });

            if (!tables.length) {
                pgcmpMsg("pgcmpMsg", "В выбранных схемах нет таблиц.", "err");
                return;
            }
            pgcmpMsg("pgcmpMsg", "");

            var expected = {};
            tables.forEach(function (t) { expected[pgcmpKey(t.schema, t.table)] = { action: "full", rows: null }; });

            pgcmpConfirm("pgcmpMsg", "Полная загрузка без сравнения " + pgcmpPairNames(src, dst) +
                ": TRUNCATE + INSERT для " + tables.length + " табл. Данные этих таблиц в приёмнике " +
                "будут заменены данными источника; каждая таблица — одной транзакцией. Таблицы, " +
                "которых нет в приёмнике, будут пропущены — создать их можно через «Сравнить» и " +
                "галку «создать и залить».", {
                title: "Полная загрузка (TRUNCATE + INSERT)?",
                confirmText: "TRUNCATE + INSERT",
                danger: true
            }).then(function (yes) {
                if (!yes) { return; }
                if (ctx !== st.ctxSeq) {
                    pgcmpMsg("pgcmpMsg", "Пара подключений или режим сменились — загрузка не запущена.", "err");
                    return;
                }
                pgcmpPostLoad({
                    source_connection_id: src,
                    dest_connection_id: dst,
                    delete_missing: false,
                    tables: tables
                }, expected, ctx);
            });
        });
    }

    function pgcmpPostLoad(body, expected, ctx) {
        $("pgcmpLoadBtn").disabled = true;
        pgcmpMsg("pgcmpLoadMsg", "Запускаю загрузку…");

        pgcmpApi("/api/pg/diff-load/start", "POST", body).then(function (d) {
            if (ctx !== st.ctxSeq) {
                // экран уже про другую пару: ответ не применяем
                pgcmpMsg("pgcmpLoadMsg", d.ok && d.job_id
                    ? "Загрузка #" + d.job_id + " запущена для прежней пары — она видна в ленте «Запуски»."
                    : "");
                pgcmpRenderLoadBox();
                return;
            }
            if (!d.ok || !d.job_id) {
                pgcmpMsg("pgcmpLoadMsg", "Загрузка не запущена: " + (d.message || "нет job_id в ответе"), "err");
                pgcmpRenderLoadBox();
                return;
            }
            pgcmpMsg("pgcmpLoadMsg", "Загрузка #" + d.job_id + " запущена.", "ok");
            st.loadExpected = expected;
            st.loadDelete = !!body.delete_missing;
            st.loadItems = [];
            st.loadSummary = null;
            st.loadJob = { id: d.job_id, status: "running" };
            pgcmpRenderLoad();
            pgcmpRenderLoadBox();
            pgcmpPollLoad();
        });
    }

    function pgcmpPollLoad() {
        clearTimeout(st.loadTimer);
        var job = st.loadJob;
        if (!job) { return; }
        var jobId = job.id;
        var seq = ++st.loadSeq;

        function next(ms) {
            clearTimeout(st.loadTimer);
            st.loadTimer = setTimeout(function () {
                if (seq === st.loadSeq) { pgcmpPollLoad(); }
            }, ms);
        }

        pgcmpApi("/api/jobs/" + jobId + "/status").then(function (d) {
            // ответ устаревшей цепочки (например, после «Стоп») не продолжает опрос
            if (seq !== st.loadSeq || !st.loadJob || st.loadJob.id !== jobId) { return; }
            if (!d.ok) {
                pgcmpMsg("pgcmpLoadMsg", "Состояние загрузки #" + jobId + " не получено: " + d.message, "err");
                if (d.httpStatus !== 404) { next(POLL_MS * 2); }
                return;
            }
            st.loadJob = d.job || st.loadJob;
            st.loadItems = d.items || [];
            st.loadSummary = d.summary || null;
            pgcmpRenderLoad();
            pgcmpRenderLoadBox();

            if (ACTIVE[st.loadJob.status]) {
                next(POLL_MS);
            } else if (st.loadJob.status === "done") {
                pgcmpMsg("pgcmpLoadMsg", "Загрузка #" + jobId + " завершена.", "ok");
            } else {
                pgcmpMsg("pgcmpLoadMsg", "Загрузка #" + jobId + ": " +
                    (JOB_STATUS[st.loadJob.status] || st.loadJob.status) +
                    (st.loadJob.error_message ? " — " + st.loadJob.error_message : "") + ".",
                    st.loadJob.status === "cancelled" ? "" : "err");
            }
        });
    }

    function pgcmpExpectedText(exp) {
        if (!exp) { return "—"; }
        if (exp.action === "diff") {
            return "добавить " + pgcmpN(exp.insert) + " · изменить " + pgcmpN(exp.update) +
                " · удалить " + (exp.del == null ? "выкл." : pgcmpN(exp.del));
        }
        var rows = exp.rows == null ? "все строки источника" : "строк " + pgcmpN(exp.rows);
        return (exp.action === "create" ? "создать + insert: " : "truncate + insert: ") + rows;
    }

    // у done в error_message итог (insert=N; update=M; delete=K | truncate+insert=N |
    // create+insert=N), у failed/skipped — причина
    function pgcmpItemDone(item) {
        var text = item.error_message || "";
        switch (item.status) {
        case "done":
            return '<span class="msg ok pgcmp-fact">' + pgcmpEsc(text || "готово") + "</span>";
        case "failed":
            return '<span class="pgcmp-st bad">ошибка</span><div class="msg err">' +
                pgcmpEsc(text || "причина не указана") + "</div>";
        case "skipped":
            return '<span class="pgcmp-st warn">пропущено</span>' +
                (text ? '<div class="msg warn">' + pgcmpEsc(text) + "</div>" : "");
        case "running": return '<span class="pgcmp-st run">идёт…</span>';
        default: return '<span class="msg">' + pgcmpEsc(JOB_STATUS[item.status] || item.status || "") + "</span>";
        }
    }

    function pgcmpRenderLoad() {
        var box = $("pgcmpLoad");
        var job = st.loadJob;
        if (!job) { box.innerHTML = ""; return; }

        var active = !!ACTIVE[job.status];
        var sum = st.loadSummary || {};
        var pct = Math.round(Number(sum.percent || job.progress_percent || 0));

        var head = '<div class="gpp-active" style="margin-top: 10px;">' +
            '<div class="what" style="flex: 1;">Загрузка <b>#' + pgcmpEsc(job.id) + "</b> · " +
            pgcmpEsc(JOB_STATUS[job.status] || job.status || "") +
            (sum.total ? ' <span style="color: var(--text-muted);">· ' + pgcmpN(sum.finished) +
                " из " + pgcmpN(sum.total) + "</span>" : "") + "</div>" +
            '<div class="gpp-bar"><i class="' + (job.status === "done" ? "done"
                : (job.status === "failed" ? "fail" : "")) + '" style="width:' +
            (job.status === "done" ? 100 : pct) + '%"></i></div>' +
            (active ? '<button type="button" class="gpp-btn sm stop" id="pgcmpLoadStop"' +
                (job.status === "stopping" ? " disabled" : "") + ">Стоп</button>" : "") + "</div>";

        var rows = st.loadItems.map(function (item) {
            var key = pgcmpKey(item.schema_name, item.table_name);
            var exp = st.loadExpected[key];
            var action = exp ? ACTION_LABEL[exp.action] : (item.action || "");
            return '<tr><td class="name">' + pgcmpEsc(item.schema_name) + "." + pgcmpEsc(item.table_name) +
                "</td><td>" + pgcmpEsc(action) + "</td><td>" + pgcmpEsc(pgcmpExpectedText(exp)) +
                "</td><td>" + pgcmpItemDone(item) + "</td></tr>";
        });

        box.innerHTML = head + (rows.length
            ? '<div class="pgcmp-tablewrap"><table class="pgcmp-table"><thead><tr>' +
              "<th>Таблица</th><th>Действие</th><th>Ожидалось</th><th>Сделано</th>" +
              "</tr></thead><tbody>" + rows.join("") + "</tbody></table></div>"
            : "");
    }

    /* ---------------- init ---------------- */

    function pgcmpOnPairChange(srcChanged) {
        st.ctxSeq++;
        if (st.mode !== "compare") {
            if (srcChanged) { st.catalogFor = null; }
            return;
        }
        if (srcChanged) { pgcmpEnsureCatalog(false); }
        pgcmpLoadLatest();
    }

    function pgcmpInit() {
        var panel = $("pgcmpPanel");
        var sw = $("pgcmpModeSwitch");
        if (!panel || !sw) { return; }

        sw.addEventListener("click", function (e) {
            var b = e.target.closest ? e.target.closest("[data-pgcmp-mode]") : null;
            if (b) { pgcmpSetMode(b.getAttribute("data-pgcmp-mode")); }
        });

        $("pgcmpTree").addEventListener("change", pgcmpOnTreeChange);
        $("pgcmpTree").addEventListener("click", pgcmpOnTreeClick);
        $("pgcmpSearch").addEventListener("input", pgcmpOnSearch);
        $("pgcmpCompareBtn").addEventListener("click", pgcmpStartCompare);
        $("pgcmpFullBtn").addEventListener("click", pgcmpStartFull);
        $("pgcmpClearBtn").addEventListener("click", pgcmpClearSelection);
        $("pgcmpResults").addEventListener("change", pgcmpOnResultsChange);
        $("pgcmpAllMissing").addEventListener("change", pgcmpOnAllMissing);
        $("pgcmpLoadBtn").addEventListener("click", pgcmpStartLoad);

        $("pgcmpProgress").addEventListener("click", function (e) {
            if (e.target.id !== "pgcmpCmpStop" || !st.cmpJob) { return; }
            var jobId = st.cmpJob.id;
            pgcmpStop(jobId, "сравнение", "pgcmpMsg", function () { pgcmpPollCompare(jobId); });
        });
        $("pgcmpLoad").addEventListener("click", function (e) {
            if (e.target.id !== "pgcmpLoadStop" || !st.loadJob) { return; }
            pgcmpStop(st.loadJob.id, "загрузку", "pgcmpLoadMsg", pgcmpPollLoad);
        });

        // addEventListener не мешает onchange, который ставит gpcopy_pipeline.js
        var src = $("gppSrc");
        var dst = $("gppDst");
        if (src) { src.addEventListener("change", function () { pgcmpOnPairChange(true); }); }
        if (dst) { dst.addEventListener("change", function () { pgcmpOnPairChange(false); }); }

        // режим по умолчанию — «Перенос»
        pgcmpSetMode("transfer");
    }

    // наружу ничего не выставляется: вся связь со страницей — через обработчики
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", pgcmpInit);
    } else {
        pgcmpInit();
    }
})();
