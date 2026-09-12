/* Вкладка «Консьюмер-группы»: лаг и сброс оффсетов. */
(function () {
    "use strict";

    var snapshot = null;    // последний показанный срез
    var openGroup = null;   // какая группа развёрнута
    var resetFor = null;    // для какой группы открыта панель сброса

    function $(id) { return document.getElementById(id); }

    function esc(s) {
        return String(s === null || s === undefined ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }

    function fmtN(n) {
        return Number(n || 0).toLocaleString("ru-RU");
    }

    function lagText(lag) {
        return lag === null || lag === undefined ? "—" : fmtN(lag);
    }

    function clusterId() {
        var sel = $("kgCluster");
        return sel && sel.value ? Number(sel.value) : null;
    }

    function api(url, options) {
        return fetch(url, options || {}).then(function (r) {
            return r.json().then(function (data) {
                return { status: r.status, data: data };
            });
        });
    }

    function toast(message, kind) {
        if (window.gpToast) { window.gpToast(message, kind); }
    }

    function showError(message) {
        var box = $("kgError");
        if (!message) { box.style.display = "none"; return; }
        box.textContent = message;
        box.style.display = "";
    }

    function groups() {
        return (snapshot && snapshot.groups) || [];
    }

    function byId(id) {
        return groups().filter(function (g) { return g.id === id; })[0];
    }

    function visible() {
        var needle = ($("kgFilter").value || "").trim().toLowerCase();
        var onlyLag = $("kgOnlyLag").checked;

        return groups().filter(function (g) {
            if (onlyLag && !g.lag) { return false; }
            if (needle && String(g.id).toLowerCase().indexOf(needle) < 0) {
                return false;
            }
            return true;
        });
    }

    function bodyHtml(group) {
        if (openGroup !== group.id) { return ""; }

        var html = '<div class="kg-body">';

        (group.topics || []).forEach(function (t) {
            html += '<div class="kg-topic">' + esc(t.name) + " · лаг " +
                lagText(t.lag) + "</div>";

            (t.parts || []).forEach(function (p) {
                var who = p.client
                    ? " · " + esc(p.client) + " (" + esc(p.host) + ")" : "";
                html += '<div class="kg-part">п. <b>' + esc(p.p) +
                    "</b> · закоммичено " +
                    (p.committed === null ? "—" : fmtN(p.committed)) +
                    " · конец " + fmtN(p.end) +
                    " · лаг " + lagText(p.lag) + who + "</div>";
            });
        });

        html += '<div class="kg-acts">' +
            '<button class="btn btn-sm btn-secondary" data-reset="' +
            esc(group.id) + '">Сбросить оффсеты</button>' +
            '<button class="btn btn-sm btn-outline-primary" data-drop="' +
            esc(group.id) + '">Удалить группу</button>';

        if (group.members) {
            html += '<span class="kg-count">Пока есть участники, действия ' +
                "недоступны</span>";
        }

        return html + "</div></div>";
    }

    function paintList() {
        var rows = visible();

        $("kgCount").textContent = groups().length
            ? "показано " + fmtN(rows.length) + " из " + fmtN(groups().length)
            : "";

        if (!rows.length) {
            $("kgList").innerHTML = '<div class="kg-empty">' +
                (groups().length
                    ? "Ничего не найдено."
                    : "Среза ещё нет — нажмите «Обновить срез».") +
                "</div>";
            return;
        }

        $("kgList").innerHTML = '<div class="kg-list">' +
            rows.map(function (g) {
                var open = openGroup === g.id;
                var busy = g.members
                    ? '<span class="kg-tag busy">' + fmtN(g.members) +
                      " участн.</span>" : "";
                var hot = g.lag ? " hot" : "";

                return '<div class="kg-row' + (open ? " open" : "") +
                    '" data-group="' + esc(g.id) + '"><span><b>' +
                    esc(g.id) + '</b><span class="kg-tag">' +
                    esc(g.state) + "</span>" + busy +
                    '</span><span class="r">' + fmtN(g.partitions) +
                    ' парт. · лаг <span class="kg-lag' + hot + '">' +
                    lagText(g.lag) + "</span></span></div>" + bodyHtml(g);
            }).join("") + "</div>";

        wireRows();
    }

    function repaint() {
        if (window.gpKeepScroll) {
            window.gpKeepScroll($("kgList"), paintList);
        } else {
            paintList();
        }
    }

    function wireRows() {
        Array.prototype.forEach.call(
            $("kgList").querySelectorAll("[data-group]"),
            function (row) {
                row.onclick = function () {
                    var id = row.getAttribute("data-group");
                    openGroup = openGroup === id ? null : id;
                    closeReset();
                    repaint();
                };
            }
        );

        Array.prototype.forEach.call(
            $("kgList").querySelectorAll("[data-reset]"),
            function (b) {
                b.onclick = function (event) {
                    event.stopPropagation();
                    openReset(b.getAttribute("data-reset"));
                };
            }
        );

        Array.prototype.forEach.call(
            $("kgList").querySelectorAll("[data-drop]"),
            function (b) {
                b.onclick = function (event) {
                    event.stopPropagation();
                    dropGroup(b.getAttribute("data-drop"));
                };
            }
        );
    }

    function openReset(id) {
        var group = byId(id);

        if (!group) { return; }

        resetFor = id;
        $("kgResetTitle").textContent = "Группа " + id;
        $("kgReset").classList.add("on");
        $("kgResetHint").textContent = "Затронет партиций: " +
            fmtN(group.partitions);
    }

    function closeReset() {
        resetFor = null;
        $("kgReset").classList.remove("on");
    }

    function runReset() {
        var id = resetFor;
        var group = byId(id);

        if (!id || !group) { return; }

        var mode = $("kgResetMode").value;
        var body = { mode: mode };

        if (mode === "timestamp") {
            if (!$("kgResetAt").value) {
                toast("Укажите дату и время", "danger");
                return;
            }
            body.timestamp = $("kgResetAt").value.replace("T", " ");
        }

        var label = $("kgResetMode")
            .options[$("kgResetMode").selectedIndex].text;
        var question = "Сбросить оффсеты группы «" + id + "» (" + label +
            ")? Затронет " + group.partitions + " партиций.";

        var doIt = function () {
            api("/api/kafka/clusters/" + clusterId() + "/groups/" +
                encodeURIComponent(id) + "/reset", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            }).then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    toast(r.data.message || "Не удалось сбросить", "danger");
                    return;
                }

                var failed = (r.data.failed || []).length;

                toast(failed
                    ? "Сброшено " + r.data.done + ", не удалось " + failed
                    : "Оффсеты сброшены: " + r.data.done + " партиций",
                    failed ? "warning" : "success");

                closeReset();
                loadAudit();
                load(true);
            });
        };

        if (window.gpConfirm) {
            window.gpConfirm(question).then(function (yes) {
                if (yes) { doIt(); }
            });
        } else if (window.confirm(question)) {
            doIt();
        }
    }

    function dropGroup(id) {
        var question = "Удалить группу «" + id + "»? Её закоммиченные " +
            "оффсеты будут потеряны.";

        var doIt = function () {
            api("/api/kafka/clusters/" + clusterId() + "/groups/" +
                encodeURIComponent(id), { method: "DELETE" })
                .then(function (r) {
                    if (r.status !== 200 || !r.data.ok) {
                        toast(r.data.message || "Не удалось удалить",
                            "danger");
                        return;
                    }

                    toast("Группа удалена", "success");
                    loadAudit();
                    load(true);
                });
        };

        if (window.gpConfirm) {
            window.gpConfirm(question).then(function (yes) {
                if (yes) { doIt(); }
            });
        } else if (window.confirm(question)) {
            doIt();
        }
    }

    function paintAudit(records) {
        if (!records.length) {
            $("kgAudit").innerHTML =
                '<div class="kg-empty">Действий пока не было.</div>';
            return;
        }

        $("kgAudit").innerHTML = records.map(function (r) {
            var bad = r.result !== "ok" ? " bad" : "";
            var extra = r.details && r.details.mode
                ? " · " + esc(r.details.mode) : "";

            return '<div class="kg-audit' + bad + '">' + esc(r.created_at) +
                " · " + esc(r.action) + " · " + esc(r.target || "") +
                extra + " · " + esc(r.result) + "</div>";
        }).join("");
    }

    function loadAudit() {
        var id = clusterId();

        if (!id) { return; }

        api("/api/kafka/clusters/" + id + "/audit").then(function (r) {
            paintAudit((r.data && r.data.records) || []);
        });
    }

    function paintAll() {
        $("kgSnap").textContent = snapshot && snapshot.taken_at
            ? "срез от " + snapshot.taken_at : "среза нет";
        repaint();
    }

    function load(force) {
        var id = clusterId();

        if (!id) { paintAll(); return; }

        var url = "/api/kafka/clusters/" + id + "/groups" +
            (force ? "/refresh" : "");

        if (force) { $("kgRefresh").disabled = true; }

        api(url, force ? { method: "POST" } : {}).then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                showError(r.data.message || "Не удалось получить данные");
                return;
            }

            showError("");
            snapshot = r.data.groups;
            openGroup = null;
            closeReset();
            paintAll();
        }).catch(function (e) {
            showError(String(e));
        }).then(function () {
            $("kgRefresh").disabled = !clusterId();
        });
    }

    function wire() {
        if (!$("kgRoot") || !$("kgCluster")) { return; }

        $("kgCluster").onchange = function () {
            snapshot = null;
            paintAll();
            load(false);
            loadAudit();
        };

        $("kgRefresh").onclick = function () { load(true); };
        $("kgFilter").oninput = repaint;
        $("kgOnlyLag").onchange = repaint;

        $("kgResetMode").onchange = function () {
            $("kgResetAt").hidden = $("kgResetMode").value !== "timestamp";
        };

        $("kgResetGo").onclick = runReset;
        $("kgResetCancel").onclick = closeReset;

        paintAll();
        load(false);
        loadAudit();
    }

    document.addEventListener("DOMContentLoaded", wire);
}());
