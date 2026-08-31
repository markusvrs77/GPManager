/* Вкладка «Доступы»: правила ACL, выдача и отзыв прав. */
(function () {
    "use strict";

    var acls = [];

    function $(id) { return document.getElementById(id); }

    function esc(s) {
        return String(s === null || s === undefined ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }

    function fmtN(n) { return Number(n || 0).toLocaleString("ru-RU"); }

    function toast(message, kind) {
        if (window.gpToast) { window.gpToast(message, kind); }
    }

    function clusterId() {
        var sel = $("kaCluster");
        return sel && sel.value ? Number(sel.value) : null;
    }

    function post(url, body) {
        return fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body || {}),
        }).then(function (r) {
            return r.json().then(function (data) {
                return { status: r.status, data: data };
            });
        });
    }

    function showError(message) {
        var box = $("kaError");
        if (!message) { box.style.display = "none"; return; }
        box.textContent = message;
        box.style.display = "";
    }

    function showDisabled(message) {
        var box = $("kaDisabled");

        if (!message) { box.style.display = "none"; return; }

        box.innerHTML = esc(message) +
            "<br><br>Чтобы правила заработали, добавьте в " +
            "<code>server.properties</code> каждого брокера строку " +
            "<code>authorizer.class.name=…</code> — " +
            "<code>org.apache.kafka.metadata.authorizer.StandardAuthorizer" +
            "</code> для KRaft или " +
            "<code>kafka.security.authorizer.AclAuthorizer</code> для " +
            "ZooKeeper — и перезапустите брокеры.";
        box.style.display = "";
    }

    function chosenOperations() {
        return Array.prototype.filter.call(
            $("kaOps").querySelectorAll("input[type=checkbox]"),
            function (box) { return box.checked; }
        ).map(function (box) { return box.value; });
    }

    function setOperations(values) {
        Array.prototype.forEach.call(
            $("kaOps").querySelectorAll("input[type=checkbox]"),
            function (box) {
                box.checked = values.indexOf(box.value) >= 0;
            }
        );
    }

    function filterBody() {
        return {
            principal: $("kaFPrincipal").value,
            resource_type: $("kaFType").value,
            resource_name: $("kaFName").value,
        };
    }

    function formBody() {
        return {
            principal: $("kaPrincipal").value,
            resource_type: $("kaType").value,
            resource_name: $("kaName").value,
            pattern_type: $("kaPattern").value,
            permission: $("kaPermission").value,
            host: $("kaHost").value,
            operations: chosenOperations(),
        };
    }

    function paint(rows) {
        $("kaCount").textContent = rows.length
            ? "правил: " + fmtN(rows.length) : "";

        if (!rows.length) {
            $("kaList").innerHTML = '<div class="ka-empty">Правил нет — ' +
                "нажмите «Показать правила».</div>";
            return;
        }

        $("kaList").innerHTML = '<div class="ka-list">' +
            rows.map(function (a) {
                var deny = a.permission === "DENY"
                    ? '<span class="ka-tag deny">DENY</span>' : "";
                var pattern = a.pattern_type === "PREFIXED"
                    ? '<span class="ka-tag">префикс</span>' : "";

                return '<div class="ka-row"><span><span class="who">' +
                    esc(a.principal) + "</span> · " + esc(a.operation) +
                    " на " + esc(a.resource_type) + " «" +
                    esc(a.resource_name) + "»" + pattern + deny +
                    '</span><span class="what">хост ' + esc(a.host) +
                    "</span></div>";
            }).join("") + "</div>";
    }

    function load() {
        var id = clusterId();

        if (!id) { return; }

        $("kaLoad").disabled = true;

        post("/api/kafka/clusters/" + id + "/acls/list", filterBody())
            .then(function (r) {
                if (r.status === 409 && r.data.disabled) {
                    showDisabled(r.data.message);
                    showError("");
                    acls = [];
                    paint(acls);
                    return;
                }

                if (r.status !== 200 || !r.data.ok) {
                    showError(r.data.message || "Не удалось получить правила");
                    return;
                }

                showDisabled("");
                showError("");
                $("kaAnon").style.display = r.data.anonymous ? "" : "none";
                acls = r.data.acls || [];
                paint(acls);
            })
            .catch(function (e) { showError(String(e)); })
            .then(function () { $("kaLoad").disabled = false; });
    }

    function grant() {
        var id = clusterId();

        if (!id) { return; }

        post("/api/kafka/clusters/" + id + "/acls", formBody())
            .then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    toast(r.data.message || "Не удалось выдать права",
                        "danger");
                    return;
                }

                toast("Создано правил: " + r.data.created, "success");
                load();
            });
    }

    function preview() {
        var id = clusterId();

        if (!id) { return; }

        // отзыв удаляет всё, что подошло под фильтр, — сначала показываем
        post("/api/kafka/clusters/" + id + "/acls/list", formBody())
            .then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    toast(r.data.message || "Не удалось проверить", "danger");
                    return;
                }

                var rows = r.data.acls || [];

                paint(rows);
                $("kaRevokeHint").textContent = rows.length
                    ? "Под отзыв попадёт правил: " + rows.length
                    : "Под фильтр ничего не попало";
            });
    }

    function revoke() {
        var id = clusterId();

        if (!id) { return; }

        post("/api/kafka/clusters/" + id + "/acls/list", formBody())
            .then(function (r) {
                var rows = (r.data || {}).acls || [];

                if (r.status !== 200 || !r.data.ok) {
                    toast(r.data.message || "Не удалось проверить", "danger");
                    return;
                }

                if (!rows.length) {
                    toast("Под фильтр ничего не попало", "warning");
                    return;
                }

                var principals = {};

                rows.forEach(function (a) { principals[a.principal] = 1; });

                var question = "Отозвать " + rows.length +
                    " правил(а)?\nПринципалы: " +
                    Object.keys(principals).join(", ") +
                    "\nДействие необратимо.";

                var doIt = function () {
                    post("/api/kafka/clusters/" + id + "/acls/delete",
                        formBody()).then(function (res) {
                        if (res.status !== 200 || !res.data.ok) {
                            toast(res.data.message || "Не удалось отозвать",
                                "danger");
                            return;
                        }

                        toast("Отозвано правил: " + res.data.removed,
                            "success");
                        $("kaRevokeHint").textContent = "";
                        load();
                    });
                };

                if (window.gpConfirm) {
                    window.gpConfirm(question).then(function (yes) {
                        if (yes) { doIt(); }
                    });
                } else if (window.confirm(question)) {
                    doIt();
                }
            });
    }

    function applyPreset(name) {
        $("kaType").value = "TOPIC";
        $("kaPermission").value = "ALLOW";

        if (name === "writer") {
            setOperations(["WRITE", "DESCRIBE"]);
            $("kaRevokeHint").textContent = "";
            return;
        }

        if (name === "full") {
            setOperations(["ALL"]);
            $("kaRevokeHint").textContent = "";
            return;
        }

        setOperations(["READ", "DESCRIBE"]);
        $("kaRevokeHint").textContent = "Шаблон «читатель»: одних прав на " +
            "топик мало — потребителю нужен ещё READ на консьюмер-группу. " +
            "Выдайте вторым правилом с типом ресурса «консьюмер-группа».";
    }

    function wire() {
        if (!$("kaRoot") || !$("kaCluster")) { return; }

        $("kaCluster").onchange = function () {
            acls = [];
            paint(acls);
            $("kaAnon").style.display = "none";
        };

        $("kaLoad").onclick = load;
        $("kaGrant").onclick = grant;
        $("kaPreview").onclick = preview;
        $("kaRevoke").onclick = revoke;

        Array.prototype.forEach.call(
            document.querySelectorAll("[data-preset]"),
            function (b) {
                b.onclick = function () {
                    applyPreset(b.getAttribute("data-preset"));
                };
            }
        );

        paint(acls);
    }

    document.addEventListener("DOMContentLoaded", wire);
}());
