/* Вкладка Kafka: обзор кластера. Автообновления нет — только по кнопке.
   Управление подключениями живёт на отдельной вкладке. */
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

    function noSnapshotText() {
        return clusterId()
            ? "Среза ещё нет — нажмите «Обновить срез»."
            : "Сначала заведите кластер на вкладке «Подключения».";
    }

    function paintBrokers() {
        var rows = (overview && overview.brokers) || [];

        if (!rows.length) {
            $("kfBrokers").innerHTML = emptyHtml(noSnapshotText());
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
            }).join("") + "</div>" + actionsHtml(topic);
    }

    function paintTopics() {
        var rows = visibleTopics();
        var total = ((overview && overview.topics) || []).length;

        $("kfCount").textContent = total
            ? "показано " + fmtN(rows.length) + " из " + fmtN(total)
            : "";

        if (!rows.length) {
            $("kfTopics").innerHTML = emptyHtml(
                total ? "Ничего не найдено." : noSnapshotText()
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

        wireTopicActions();
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
            $("kfRefresh").disabled = !clusterId();
        });
    }

    /* ---------------- управление топиками ---------------- */

    var configs = {};   // конфиги топиков, загруженные по требованию

    function toast(message, kind) {
        if (window.gpToast) { window.gpToast(message, kind); }
    }

    function send(url, method, body) {
        return api(url, {
            method: method,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body || {}),
        });
    }

    function ask(question) {
        if (window.gpConfirm) { return window.gpConfirm(question); }
        return Promise.resolve(window.confirm(question));
    }

    function actionsHtml(topic) {
        var rows = configs[topic.name];
        var html = '<div class="kf-acts">' +
            '<button class="btn btn-sm btn-secondary" data-cfg="' +
            esc(topic.name) + '">Конфигурация</button>' +
            '<button class="btn btn-sm btn-secondary" data-parts="' +
            esc(topic.name) + '">Добавить партиции</button>' +
            '<button class="btn btn-sm btn-outline-primary" data-drop="' +
            esc(topic.name) + '">Удалить</button></div>';

        if (rows) {
            html += '<div class="kf-cfg">' + rows.map(function (c) {
                var value = c.value === null ? "" : c.value;
                return '<div class="kf-cfg-row"><span class="k">' +
                    esc(c.key) + "</span>" +
                    '<input class="form-control form-control-sm" ' +
                    'data-cfg-key="' + esc(c.key) + '" value="' +
                    esc(value) + '"' + (c.read_only ? " disabled" : "") +
                    '><span class="d">' +
                    (c.default ? "по умолчанию" : "задано") +
                    (c.sensitive ? " · скрыто" : "") + "</span></div>";
            }).join("") +
                '<div class="kf-acts"><button class="btn btn-sm btn-primary"' +
                ' data-cfg-save="' + esc(topic.name) +
                '">Сохранить</button></div></div>';
        }

        return html;
    }

    function wireTopicActions() {
        var bind = function (attr, handler) {
            Array.prototype.forEach.call(
                $("kfTopics").querySelectorAll("[" + attr + "]"),
                function (b) {
                    b.onclick = function (event) {
                        event.stopPropagation();
                        handler(b.getAttribute(attr));
                    };
                }
            );
        };

        bind("data-cfg", loadConfigs);
        bind("data-parts", growPartitions);
        bind("data-drop", dropTopic);
        bind("data-cfg-save", saveConfigs);
    }

    function loadConfigs(name) {
        api("/api/kafka/clusters/" + clusterId() + "/topics/" +
            encodeURIComponent(name) + "/configs").then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                toast(r.data.message || "Не удалось получить конфигурацию",
                    "danger");
                return;
            }

            configs[name] = r.data.configs;
            repaintTopics();
        });
    }

    function saveConfigs(name) {
        var wanted = {};

        Array.prototype.forEach.call(
            $("kfTopics").querySelectorAll("[data-cfg-key]"),
            function (input) {
                wanted[input.getAttribute("data-cfg-key")] = input.value;
            }
        );

        send("/api/kafka/clusters/" + clusterId() + "/topics/" +
            encodeURIComponent(name) + "/configs", "PUT", { configs: wanted })
            .then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    toast(r.data.message || "Не удалось сохранить", "danger");
                    return;
                }

                toast(r.data.changed
                    ? "Изменено ключей: " + r.data.changed
                    : "Изменений нет", "success");
                loadConfigs(name);
            });
    }

    function growPartitions(name) {
        var topic = ((overview && overview.topics) || []).filter(
            function (t) { return t.name === name; })[0];

        if (!topic) { return; }

        var target = window.prompt(
            "Сколько партиций должно стать у «" + name + "»?\n" +
            "Сейчас " + topic.partitions + ". Уменьшать Kafka не умеет.\n" +
            "После увеличения записи с тем же ключом пойдут в другую " +
            "партицию — порядок по ключу сломается.",
            String(topic.partitions + 1));

        if (!target) { return; }

        send("/api/kafka/clusters/" + clusterId() + "/topics/" +
            encodeURIComponent(name) + "/partitions", "POST",
            { total: Number(target) }).then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                toast(r.data.message || "Не удалось изменить", "danger");
                return;
            }

            toast("Партиций стало " + r.data.total, "success");
            loadOverview(false);
        });
    }

    function dropTopic(name) {
        ask("Удалить топик «" + name + "»? Все его данные будут потеряны.")
            .then(function (yes) {
                if (!yes) { return; }

                api("/api/kafka/clusters/" + clusterId() + "/topics/" +
                    encodeURIComponent(name), { method: "DELETE" })
                    .then(function (r) {
                        if (r.status !== 200 || !r.data.ok) {
                            toast(r.data.message || "Не удалось удалить",
                                "danger");
                            return;
                        }

                        toast("Топик удалён", "success");
                        delete configs[name];
                        openTopic = null;
                        loadOverview(false);
                    });
            });
    }

    function createTopic() {
        send("/api/kafka/clusters/" + clusterId() + "/topics", "POST", {
            name: $("kfTopicName").value,
            partitions: $("kfTopicParts").value,
            replication: $("kfTopicRf").value,
            retention_hours: $("kfTopicRet").value,
            cleanup_policy: $("kfTopicPolicy").value,
        }).then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                toast(r.data.message || "Не удалось создать", "danger");
                return;
            }

            toast("Топик «" + r.data.name + "» создан", "success");
            $("kfTopicForm").classList.remove("on");
            $("kfTopicName").value = "";
            loadOverview(false);
        });
    }

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

            if (!id) { return; }

            api("/api/kafka/clusters/" + id + "/ping", { method: "POST" })
                .then(function (r) {
                    if (window.gpToast) {
                        window.gpToast(r.data.message,
                            r.data.ok ? "success" : "danger");
                    }
                });
        };

        $("kfTopicAdd").onclick = function () {
            $("kfTopicForm").classList.toggle("on");
        };
        $("kfTopicCancel").onclick = function () {
            $("kfTopicForm").classList.remove("on");
        };
        $("kfTopicSave").onclick = createTopic;

        $("kfFilter").oninput = repaintTopics;
        $("kfInternal").onchange = repaintTopics;

        paintAll();
        loadOverview(false);
    }

    document.addEventListener("DOMContentLoaded", wire);
}());
