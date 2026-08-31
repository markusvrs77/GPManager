/* Вкладка Kafka: обзор кластера. Автообновления нет — только по кнопке. */
(function () {
    "use strict";

    var overview = null;      // последний показанный срез
    var openTopic = null;     // какой топик развёрнут

    function $(id) { return document.getElementById(id); }

    function esc(s) {
        return String(s === null || s === undefined ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }

    function fmtN(n) {
        return Number(n || 0).toLocaleString("ru-RU");
    }

    function clusterId() {
        var sel = $("kfCluster");
        return sel && sel.value ? Number(sel.value) : null;
    }

    function api(url, options) {
        return fetch(url, options || {}).then(function (r) {
            return r.json().then(function (data) {
                return { status: r.status, data: data };
            });
        });
    }

    function showError(message) {
        var box = $("kfError");
        if (!box) { return; }
        if (!message) { box.style.display = "none"; return; }
        box.textContent = message;
        box.style.display = "";
    }

    function emptyHtml(text) {
        return '<div class="kf-empty">' + text + "</div>";
    }

    function paintBrokers() {
        var rows = (overview && overview.brokers) || [];

        if (!rows.length) {
            $("kfBrokers").innerHTML =
                emptyHtml("Среза ещё нет — нажмите «Обновить срез».");
            return;
        }

        $("kfBrokers").innerHTML = '<div class="kf-list">' +
            rows.map(function (b) {
                var boss = b.id === overview.controller_id
                    ? '<span class="kf-tag">контроллер</span>' : "";
                return '<div class="kf-row"><span><b>' + esc(b.id) +
                    "</b> · " + esc(b.host) + ":" + esc(b.port) + boss +
                    '</span><span class="r">' +
                    (b.rack ? "rack " + esc(b.rack) : "") + "</span></div>";
            }).join("") + "</div>";
    }

    function visibleTopics() {
        var all = (overview && overview.topics) || [];
        var needle = ($("kfFilter").value || "").trim().toLowerCase();
        var withInternal = $("kfInternal").checked;

        return all.filter(function (t) {
            if (!withInternal && t.internal) { return false; }
            if (needle && String(t.name).toLowerCase().indexOf(needle) < 0) {
                return false;
            }
            return true;
        });
    }

    function partsHtml(topic) {
        if (openTopic !== topic.name) { return ""; }

        return '<div class="kf-parts">' + (topic.parts || []).map(
            function (p) {
                var size = Math.max(0, (p.end || 0) - (p.begin || 0));
                return '<div class="kf-part">п. <b>' + esc(p.p) +
                    "</b> · лидер " + esc(p.leader) +
                    " · реплики " + esc((p.replicas || []).join(", ")) +
                    " · ISR " + esc((p.isr || []).join(", ")) +
                    " · " + fmtN(p.begin) + "–" + fmtN(p.end) +
                    " (" + fmtN(size) + ")</div>";
            }).join("") + "</div>";
    }

    function paintTopics() {
        var rows = visibleTopics();
        var total = ((overview && overview.topics) || []).length;

        $("kfCount").textContent = total
            ? "показано " + fmtN(rows.length) + " из " + fmtN(total)
            : "";

        if (!rows.length) {
            $("kfTopics").innerHTML = emptyHtml(
                total ? "Ничего не найдено."
                      : "Среза ещё нет — нажмите «Обновить срез»."
            );
            return;
        }

        $("kfTopics").innerHTML = '<div class="kf-list">' +
            rows.map(function (t) {
                var open = openTopic === t.name;
                var tags = (t.internal
                        ? '<span class="kf-tag">системный</span>' : "") +
                    (t.under_replicated
                        ? '<span class="kf-tag crit">под-реплицирован</span>'
                        : "");
                return '<div class="kf-row topic' + (open ? " open" : "") +
                    '" data-topic="' + esc(t.name) + '"><span><b>' +
                    esc(t.name) + "</b>" + tags +
                    '</span><span class="r">' + fmtN(t.partitions) +
                    " парт. · RF " + fmtN(t.replication) + " · " +
                    fmtN(t.messages) + " сообщ.</span></div>" + partsHtml(t);
            }).join("") + "</div>";

        Array.prototype.forEach.call(
            $("kfTopics").querySelectorAll("[data-topic]"),
            function (row) {
                row.onclick = function () {
                    var name = row.getAttribute("data-topic");
                    openTopic = openTopic === name ? null : name;
                    repaintTopics();
                };
            }
        );
    }

    function repaintTopics() {
        // прокрутка не должна уезжать к началу при разворачивании строки
        if (window.gpKeepScroll) {
            window.gpKeepScroll($("kfTopics"), paintTopics);
        } else {
            paintTopics();
        }
    }

    function paintAll() {
        // saved === false — данные собрали, но в базу они не легли
        var note = overview && overview.saved === false
            ? " · срез не сохранён" : "";

        $("kfSnap").textContent = (overview && overview.taken_at
            ? "срез от " + overview.taken_at
            : "среза нет") + note;

        paintBrokers();
        repaintTopics();
    }

    function loadOverview(force) {
        var id = clusterId();

        if (!id) { paintAll(); return; }

        var url = "/api/kafka/clusters/" + id + "/overview" +
            (force ? "/refresh" : "");
        var options = force ? { method: "POST" } : {};

        if (force) { $("kfRefresh").disabled = true; }

        api(url, options).then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                // старый срез оставляем на экране — он всё ещё полезен
                showError(r.data.message || "Не удалось получить данные");
                return;
            }

            showError("");
            overview = r.data.overview;
            openTopic = null;
            paintAll();
        }).catch(function (e) {
            showError(String(e));
        }).then(function () {
            $("kfRefresh").disabled = false;
        });
    }

    function wire() {
        if (!$("kfRoot") || !$("kfCluster")) { return; }

        $("kfCluster").onchange = function () {
            overview = null;
            openTopic = null;
            paintAll();
            loadOverview(false);
        };

        $("kfRefresh").onclick = function () { loadOverview(true); };

        $("kfPing").onclick = function () {
            var id = clusterId();
            if (!id) { return; }

            api("/api/kafka/clusters/" + id + "/ping", { method: "POST" })
                .then(function (r) {
                    if (window.gpToast) {
                        window.gpToast(r.data.message,
                            r.data.ok ? "success" : "danger");
                    }
                });
        };

        $("kfFilter").oninput = repaintTopics;
        $("kfInternal").onchange = repaintTopics;

        paintAll();
        loadOverview(false);
    }

    document.addEventListener("DOMContentLoaded", wire);
}());
