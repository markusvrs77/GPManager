/* Вкладка Kafka: подключения и обзор кластера.
   Автообновления нет — кластер опрашивается только по кнопке. */
(function () {
    "use strict";

    var overview = null;      // последний показанный срез
    var openTopic = null;     // какой топик развёрнут
    var clusters = [];        // список подключений
    var editing = null;       // id редактируемого кластера (null — новый)

    function $(id) { return document.getElementById(id); }

    function esc(s) {
        return String(s === null || s === undefined ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }

    function fmtN(n) {
        return Number(n || 0).toLocaleString("ru-RU");
    }

    function toast(message, kind) {
        if (window.gpToast) { window.gpToast(message, kind); }
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

    function send(url, method, body) {
        return api(url, {
            method: method,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body || {}),
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

    /* ---------------- подключения ---------------- */

    function paintConnList() {
        if (!clusters.length) {
            $("kfConnList").innerHTML = emptyHtml(
                "Кластеров пока нет. Нажмите «+ Добавить кластер»."
            );
            return;
        }

        $("kfConnList").innerHTML = clusters.map(function (c) {
            var extra = c.security_protocol !== "PLAINTEXT"
                ? ' <span class="kf-tag">' + esc(c.security_protocol) +
                  "</span>" : "";
            return '<div class="kf-conn"><span><b>' + esc(c.name) + "</b>" +
                extra + '<div class="addr">' + esc(c.bootstrap_servers) +
                "</div></span>" +
                '<span class="acts">' +
                '<button class="btn btn-sm btn-secondary" data-edit="' +
                c.id + '">Изменить</button>' +
                '<button class="btn btn-sm btn-outline-primary" data-del="' +
                c.id + '">Удалить</button></span></div>';
        }).join("");

        wireConnButtons();
    }

    function wireConnButtons() {
        Array.prototype.forEach.call(
            $("kfConnList").querySelectorAll("[data-edit]"),
            function (b) {
                b.onclick = function () {
                    openForm(Number(b.getAttribute("data-edit")));
                };
            }
        );

        Array.prototype.forEach.call(
            $("kfConnList").querySelectorAll("[data-del]"),
            function (b) {
                b.onclick = function () {
                    removeCluster(Number(b.getAttribute("data-del")));
                };
            }
        );
    }

    function rebuildSelect() {
        var sel = $("kfCluster");
        var keep = sel.value;

        sel.innerHTML = clusters.map(function (c) {
            return '<option value="' + c.id + '">' + esc(c.name) + " — " +
                esc(c.bootstrap_servers) + "</option>";
        }).join("");

        if (keep && clusters.some(function (c) {
            return String(c.id) === String(keep);
        })) {
            sel.value = keep;
        }

        var none = clusters.length === 0;

        sel.hidden = none;
        $("kfPing").disabled = none;
        $("kfRefresh").disabled = none;

        if ($("kfNoneHint")) { $("kfNoneHint").hidden = !none; }
    }

    function loadClusters() {
        return api("/api/kafka/clusters").then(function (r) {
            clusters = (r.data && r.data.clusters) || [];
            paintConnList();
            rebuildSelect();
            return clusters;
        });
    }

    function byId(id) {
        return clusters.filter(function (c) { return c.id === id; })[0];
    }

    function openForm(id) {
        var c = id ? byId(id) : null;

        editing = c ? c.id : null;

        $("kffName").value = c ? c.name : "";
        $("kffServers").value = c ? c.bootstrap_servers : "";
        $("kffTimeout").value = c
            ? Math.round((c.request_timeout_ms || 15000) / 1000) : 15;
        $("kffProto").value = c ? c.security_protocol : "PLAINTEXT";
        $("kffMech").value = (c && c.sasl_mechanism) || "";
        $("kffUser").value = (c && c.sasl_username) || "";
        $("kffPass").value = "";
        $("kffPass").placeholder = c && c.has_password
            ? "не менять" : "••••••••";
        $("kffCa").value = (c && c.ssl_cafile) || "";
        $("kffCert").value = (c && c.ssl_certfile) || "";
        $("kffKey").value = (c && c.ssl_keyfile) || "";

        $("kffMore").open = !!(c && c.security_protocol !== "PLAINTEXT");
        $("kfForm").hidden = false;
        $("kffName").focus();
    }

    function closeForm() {
        $("kfForm").hidden = true;
        editing = null;
    }

    function formPayload() {
        var body = {
            name: $("kffName").value,
            bootstrap_servers: $("kffServers").value,
            security_protocol: $("kffProto").value,
            sasl_mechanism: $("kffMech").value,
            sasl_username: $("kffUser").value,
            ssl_cafile: $("kffCa").value,
            ssl_certfile: $("kffCert").value,
            ssl_keyfile: $("kffKey").value,
            request_timeout_ms: Math.round(
                Number($("kffTimeout").value || 15) * 1000),
        };

        // пустой пароль при правке значит «не менять», а не «стереть»
        if ($("kffPass").value || !editing) {
            body.sasl_password = $("kffPass").value;
        }

        return body;
    }

    function saveCluster(thenPing) {
        var body = formPayload();
        var url = "/api/kafka/clusters" + (editing ? "/" + editing : "");

        return send(url, editing ? "PUT" : "POST", body).then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                toast(r.data.message || "Не удалось сохранить", "danger");
                return null;
            }

            var id = editing || r.data.id;

            toast(editing ? "Подключение обновлено" : "Кластер добавлен",
                "success");
            closeForm();

            return loadClusters().then(function () {
                $("kfCluster").value = String(id);
                overview = null;
                openTopic = null;
                paintAll();
                loadOverview(false);

                if (thenPing) { pingCluster(id); }
            });
        });
    }

    function removeCluster(id) {
        var c = byId(id);
        var question = "Удалить подключение «" + (c ? c.name : id) +
            "»? Сам кластер Kafka не пострадает — уйдёт только запись " +
            "и сохранённый срез.";

        var doIt = function () {
            api("/api/kafka/clusters/" + id, { method: "DELETE" })
                .then(function () {
                    toast("Подключение удалено", "success");
                    overview = null;
                    return loadClusters();
                })
                .then(function () {
                    paintAll();
                    if (clusters.length) { loadOverview(false); }
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

    function pingCluster(id) {
        return api("/api/kafka/clusters/" + id + "/ping", { method: "POST" })
            .then(function (r) {
                toast(r.data.message, r.data.ok ? "success" : "danger");
            });
    }

    /* ---------------- обзор ---------------- */

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
            $("kfRefresh").disabled = clusters.length === 0;
        });
    }

    /* ---------------- запуск ---------------- */

    function wire() {
        if (!$("kfRoot")) { return; }

        $("kfCluster").onchange = function () {
            overview = null;
            openTopic = null;
            paintAll();
            loadOverview(false);
        };

        $("kfRefresh").onclick = function () { loadOverview(true); };

        $("kfPing").onclick = function () {
            var id = clusterId();
            if (id) { pingCluster(id); }
        };

        $("kfConnToggle").onclick = function () {
            $("kfConns").hidden = !$("kfConns").hidden;
        };

        $("kfAdd").onclick = function () { openForm(null); };
        $("kfCancel").onclick = closeForm;
        $("kfSave").onclick = function () { saveCluster(false); };
        $("kfTest").onclick = function () { saveCluster(true); };

        $("kfFilter").oninput = repaintTopics;
        $("kfInternal").onchange = repaintTopics;

        paintAll();

        loadClusters().then(function () {
            if (clusters.length) { loadOverview(false); }
        });
    }

    document.addEventListener("DOMContentLoaded", wire);
}());
