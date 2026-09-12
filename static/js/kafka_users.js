/* Пользователи Kafka на вкладке «Доступы»: SCRAM-учётки, под которыми
   клиенты подключаются к брокерам.

   Живут рядом с правилами намеренно: завести учётку и выдать ей права —
   два шага одного дела, и держать их на разных страницах значит
   гарантировать, что второй забудут. */

(function () {
    "use strict";

    var raw = document.getElementById("kuData");
    var list = document.getElementById("kuList");

    if (!raw || !list) { return; }

    var DATA = JSON.parse(raw.textContent);
    var users = [];

    function esc(s) {
        return String(s === null || s === undefined ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }

    function toast(message, kind) {
        if (window.gpToast) { window.gpToast(message, kind); }
    }

    function clusterId() {
        var sel = document.getElementById("kaCluster");
        return sel && sel.value ? Number(sel.value) : null;
    }

    function call(url, options) {
        return fetch(url, options).then(function (r) {
            return r.json().catch(function () {
                return { ok: false, message: "Сервер ответил не JSON" };
            }).then(function (data) {
                return { status: r.status, data: data };
            });
        });
    }

    /* ----------------------------------------------------------- список */

    function render() {
        if (!users.length) {
            list.innerHTML = '<div class="ku-empty">' +
                'Учётных записей SCRAM на кластере нет.</div>';
            return;
        }

        list.innerHTML = users.map(function (u) {
            if (u.error) {
                return '<div class="ku-row"><b>' + esc(u.username) + '</b>' +
                    '<span class="ku-err">' + esc(u.error) + '</span></div>';
            }

            var mechs = (u.mechanisms || []).map(function (m) {
                return '<span class="ku-mech">' + esc(m.mechanism) +
                    '<i>' + esc(m.iterations) + '</i></span>';
            }).join("");

            var actions = "";

            if (DATA.canEdit) {
                actions =
                    '<button class="btn btn-sm btn-outline-primary" ' +
                        'data-pwd="' + esc(u.username) + '" type="button">' +
                        'Сменить пароль</button>' +
                    '<button class="btn btn-sm btn-outline-danger" ' +
                        'data-del="' + esc(u.username) + '" ' +
                        'data-mech="' +
                        esc((u.mechanisms[0] || {}).mechanism || "") +
                        '" type="button">Удалить</button>';
            }

            return '<div class="ku-row">' +
                '<b>' + esc(u.username) + '</b>' +
                '<code>' + esc(u.principal) + '</code>' +
                '<span class="ku-mechs">' + mechs + '</span>' +
                '<span class="ku-sp"></span>' +
                actions +
            '</div>';
        }).join("");

        wire();
    }

    function wire() {
        list.querySelectorAll("[data-pwd]").forEach(function (btn) {
            btn.addEventListener("click", function () {
                openDialog(btn.getAttribute("data-pwd"));
            });
        });

        list.querySelectorAll("[data-del]").forEach(function (btn) {
            btn.addEventListener("click", function () {
                remove(btn.getAttribute("data-del"),
                       btn.getAttribute("data-mech"));
            });
        });
    }

    function load() {
        var id = clusterId();

        if (!id) { return; }

        call("/api/kafka/clusters/" + id + "/users", { method: "GET" })
            .then(function (res) {
                if (!res.data.ok) {
                    list.innerHTML = '<div class="ku-empty">' +
                        esc(res.data.message || "Не удалось прочитать") +
                        '</div>';
                    return;
                }

                users = res.data.users || [];
                render();
            });
    }

    /* ----------------------------------------------------------- диалог */

    function openDialog(existingName) {
        var overlay = document.createElement("div");
        overlay.className = "gp-dialog-overlay";

        var panel = document.createElement("div");
        panel.className = "gp-dialog";
        panel.setAttribute("role", "dialog");
        panel.setAttribute("aria-modal", "true");

        var title = existingName
            ? "Новый пароль для «" + existingName + "»"
            : "Новый пользователь Kafka";

        panel.innerHTML =
            '<div class="gp-dialog-title">' + esc(title) + '</div>' +
            '<div class="gp-dialog-body">' +
                (existingName ? '' :
                    '<label class="form-label">Имя</label>' +
                    '<input class="form-control" type="text" id="kuName" ' +
                        'autocomplete="off" placeholder="svc_etl">') +
                '<label class="form-label ku-gap">Пароль</label>' +
                '<input class="form-control" type="text" id="kuPwd" ' +
                    'autocomplete="new-password" placeholder="не короче ' +
                    DATA.minPasswordLength + ' символов">' +
                '<label class="form-label ku-gap">Механизм</label>' +
                '<select class="form-select" id="kuMech">' +
                    DATA.mechanisms.map(function (m) {
                        return '<option value="' + esc(m[0]) + '">' +
                            esc(m[1]) + '</option>';
                    }).join("") +
                '</select>' +
                '<p class="ku-lead ku-gap">Пароль нигде не сохраняется: он ' +
                    'уходит на брокер и здесь не остаётся. Передайте его ' +
                    'владельцу сразу — посмотреть второй раз будет негде.</p>' +
            '</div>' +
            '<div class="gp-dialog-actions">' +
                '<button class="btn btn-secondary" type="button" ' +
                    'data-act="cancel">Отмена</button>' +
                '<button class="btn btn-primary" type="button" ' +
                    'data-act="ok">' +
                    (existingName ? 'Сменить' : 'Создать') + '</button>' +
            '</div>';

        overlay.appendChild(panel);
        document.body.appendChild(overlay);

        var nameField = panel.querySelector("#kuName");
        (nameField || panel.querySelector("#kuPwd")).focus();

        panel.querySelector('[data-act="cancel"]')
            .addEventListener("click", function () { overlay.remove(); });

        panel.querySelector('[data-act="ok"]')
            .addEventListener("click", function () {
                var id = clusterId();

                if (!id) {
                    toast("Сначала выберите кластер", "error");
                    return;
                }

                var payload = {
                    username: existingName || (nameField ? nameField.value : ""),
                    password: panel.querySelector("#kuPwd").value,
                    mechanism: panel.querySelector("#kuMech").value
                };

                call("/api/kafka/clusters/" + id + "/users", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload)
                }).then(function (res) {
                    if (!res.data.ok) {
                        toast(res.data.message || "Не удалось", "error");
                        return;
                    }

                    overlay.remove();
                    toast(res.data.hint || "Готово", "success");

                    // подставляем принципал в форму выдачи прав: следующий
                    // шаг почти всегда именно он
                    var principal = document.getElementById("kaPrincipal");

                    if (principal && !existingName) {
                        principal.value = res.data.user.principal;
                    }

                    load();
                });
            });
    }

    function remove(username, mechanism) {
        var id = clusterId();

        if (!id) { return; }

        var question = "Снять учётную запись «" + username + "»? " +
            "Клиенты под ней перестанут подключаться сразу, а выданные ей " +
            "правила останутся — их придётся отозвать отдельно.";

        var ask = window.gpConfirm
            ? window.gpConfirm(question, {
                title: "Удаление пользователя Kafka",
                confirmText: "Снять",
                danger: true
            })
            : Promise.resolve(true);

        ask.then(function (agreed) {
            if (!agreed) { return; }

            call("/api/kafka/clusters/" + id + "/users/" +
                 encodeURIComponent(username) +
                 "?mechanism=" + encodeURIComponent(mechanism || ""), {
                method: "DELETE"
            }).then(function (res) {
                if (!res.data.ok) {
                    toast(res.data.message || "Не удалось удалить", "error");
                    return;
                }

                toast(res.data.hint || "Учётная запись снята", "success");
                load();
            });
        });
    }

    /* ------------------------------------------------------------ старт */

    var addBtn = document.getElementById("kuAdd");

    if (addBtn) {
        addBtn.addEventListener("click", function () { openDialog(null); });
    }

    var cluster = document.getElementById("kaCluster");

    if (cluster) {
        cluster.addEventListener("change", load);
    }

    load();
})();
