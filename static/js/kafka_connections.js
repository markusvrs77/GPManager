/* Вкладка «Подключения Kafka»: список кластеров и форма. */
(function () {
    "use strict";

    var clusters = [];
    var editing = null;   // id редактируемого кластера (null — новый)

    function $(id) { return document.getElementById(id); }

    function esc(s) {
        return String(s === null || s === undefined ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }

    function toast(message, kind) {
        if (window.gpToast) { window.gpToast(message, kind); }
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

    function byId(id) {
        return clusters.filter(function (c) { return c.id === id; })[0];
    }

    function paintList() {
        if (!clusters.length) {
            $("kcList").innerHTML = '<div class="kc-empty">Кластеров пока ' +
                "нет. Нажмите «+ Добавить кластер».</div>";
            return;
        }

        $("kcList").innerHTML = clusters.map(function (c) {
            var proto = c.security_protocol !== "PLAINTEXT"
                ? '<span class="kc-tag">' + esc(c.security_protocol) +
                  "</span>" : "";
            var user = c.sasl_username
                ? '<span class="kc-tag">' + esc(c.sasl_username) + "</span>"
                : "";

            return '<div class="kc-row"><span><b>' + esc(c.name) + "</b>" +
                proto + user + '<div class="addr">' +
                esc(c.bootstrap_servers) + " · таймаут " +
                Math.round((c.request_timeout_ms || 15000) / 1000) +
                ' с</div></span><span class="acts">' +
                '<button class="btn btn-sm btn-secondary" data-ping="' +
                c.id + '">Проверить</button>' +
                '<button class="btn btn-sm btn-secondary" data-edit="' +
                c.id + '">Изменить</button>' +
                '<button class="btn btn-sm btn-outline-primary" data-del="' +
                c.id + '">Удалить</button></span></div>';
        }).join("");

        wireRows();
    }

    function wireRows() {
        var bind = function (attr, handler) {
            Array.prototype.forEach.call(
                $("kcList").querySelectorAll("[" + attr + "]"),
                function (b) {
                    b.onclick = function () {
                        handler(Number(b.getAttribute(attr)), b);
                    };
                }
            );
        };

        bind("data-edit", openForm);
        bind("data-del", removeCluster);
        bind("data-ping", function (id, button) {
            var label = button.textContent;

            button.disabled = true;
            button.textContent = "проверяю…";

            pingCluster(id).then(function () {
                button.disabled = false;
                button.textContent = label;
            });
        });
    }

    function loadClusters() {
        return api("/api/kafka/clusters").then(function (r) {
            clusters = (r.data && r.data.clusters) || [];
            paintList();
            return clusters;
        });
    }

    function openForm(id) {
        var c = id ? byId(id) : null;

        editing = c ? c.id : null;

        $("kcName").value = c ? c.name : "";
        $("kcServers").value = c ? c.bootstrap_servers : "";
        $("kcTimeout").value = c
            ? Math.round((c.request_timeout_ms || 15000) / 1000) : 15;
        $("kcProto").value = c ? c.security_protocol : "PLAINTEXT";
        $("kcMech").value = (c && c.sasl_mechanism) || "";
        $("kcUser").value = (c && c.sasl_username) || "";
        $("kcPass").value = "";
        $("kcPass").placeholder = c && c.has_password
            ? "не менять" : "••••••••";
        $("kcCa").value = (c && c.ssl_cafile) || "";
        $("kcCert").value = (c && c.ssl_certfile) || "";
        $("kcKey").value = (c && c.ssl_keyfile) || "";

        $("kcMore").open = !!(c && c.security_protocol !== "PLAINTEXT");
        $("kcForm").hidden = false;
        $("kcName").focus();
    }

    function closeForm() {
        $("kcForm").hidden = true;
        editing = null;
    }

    function formPayload() {
        var body = {
            name: $("kcName").value,
            bootstrap_servers: $("kcServers").value,
            security_protocol: $("kcProto").value,
            sasl_mechanism: $("kcMech").value,
            sasl_username: $("kcUser").value,
            ssl_cafile: $("kcCa").value,
            ssl_certfile: $("kcCert").value,
            ssl_keyfile: $("kcKey").value,
            request_timeout_ms: Math.round(
                Number($("kcTimeout").value || 15) * 1000),
        };

        // пустой пароль при правке значит «не менять», а не «стереть»
        if ($("kcPass").value || !editing) {
            body.sasl_password = $("kcPass").value;
        }

        return body;
    }

    function saveCluster(thenPing) {
        var url = "/api/kafka/clusters" + (editing ? "/" + editing : "");
        var was = editing;

        return send(url, editing ? "PUT" : "POST", formPayload())
            .then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    toast(r.data.message || "Не удалось сохранить", "danger");
                    return null;
                }

                var id = was || r.data.id;

                toast(was ? "Подключение обновлено" : "Кластер добавлен",
                    "success");
                closeForm();

                return loadClusters().then(function () {
                    if (thenPing) { return pingCluster(id); }
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
                    return loadClusters();
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

    function wire() {
        if (!$("kcRoot")) { return; }

        $("kcAdd").onclick = function () { openForm(null); };
        $("kcCancel").onclick = closeForm;
        $("kcSave").onclick = function () { saveCluster(false); };
        $("kcSaveTest").onclick = function () { saveCluster(true); };

        loadClusters().then(function () {
            // пусто — сразу открываем форму, иначе первый шаг неочевиден
            if (!clusters.length) { openForm(null); }
        });
    }

    document.addEventListener("DOMContentLoaded", wire);
}());
