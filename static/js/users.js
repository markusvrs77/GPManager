/* Страница «Пользователи»: список слева, разбор выбранного справа.

   Галочка возможности показывает то, что человеку реально доступно, а
   не то, что записано в базе: роль плюс исключения. Отличие от роли
   помечается ярлыком «искл.» — иначе непонятно, почему у двух
   операторов разные наборы. */

(function () {
    "use strict";

    var raw = document.getElementById("usData");
    if (!raw) { return; }

    var DATA = JSON.parse(raw.textContent);

    var ROLE_LABEL = {
        viewer: "наблюдатель",
        operator: "оператор",
        admin: "администратор"
    };

    var GROUP_LABEL = {
        dashboard: "Dashboard",
        health: "Здоровье БД",
        connections: "Подключения",
        objects: "Объекты",
        jobs: "Задачи",
        sync: "Синхронизация",
        maintenance: "Maintenance",
        vacuum: "Vacuum / Analyze",
        backups: "Резервные копии",
        grants: "Гранты",
        schedules: "Расписания",
        kafka: "Kafka",
        users: "Пользователи"
    };

    var users = DATA.users.slice();
    var selectedId = users.length ? users[0].id : null;

    var listEl = document.getElementById("usList");
    var detailEl = document.getElementById("usDetail");

    function esc(value) {
        var d = document.createElement("div");
        d.textContent = value == null ? "" : String(value);
        return d.innerHTML;
    }

    function initials(name) {
        return (name || "?").slice(0, 2).toUpperCase();
    }

    function byId(id) {
        for (var i = 0; i < users.length; i++) {
            if (users[i].id === id) { return users[i]; }
        }
        return null;
    }

    /* ---------------------------------------------------------- список */

    function renderList() {
        listEl.innerHTML = "";

        users.forEach(function (u) {
            var tag = "";

            if (!u.is_active) {
                tag = '<span class="us-tag off">выключен</span>';
            } else if (u.must_change_password) {
                tag = '<span class="us-tag">новый</span>';
            }

            var row = document.createElement("button");
            row.type = "button";
            row.className = "us-row" + (u.id === selectedId ? " active" : "");
            row.innerHTML =
                '<span class="us-av' + (u.is_active ? "" : " off") + '">' +
                    esc(initials(u.username)) +
                '</span>' +
                '<span class="us-nm">' +
                    '<b>' + esc(u.username) + '</b>' +
                    '<span>' + esc(ROLE_LABEL[u.role] || u.role) + '</span>' +
                '</span>' + tag;

            row.addEventListener("click", function () {
                selectedId = u.id;
                renderList();
                renderDetail();
            });

            listEl.appendChild(row);
        });
    }

    /* -------------------------------------------------------- возможности */

    function groupCapabilities() {
        var groups = [];
        var index = {};

        DATA.capabilities.forEach(function (pair) {
            var code = pair[0];
            var key = code.split(".")[0];

            if (!index[key]) {
                index[key] = {
                    key: key,
                    label: GROUP_LABEL[key] || key,
                    items: []
                };
                groups.push(index[key]);
            }

            index[key].items.push({ code: code, label: pair[1] });
        });

        return groups;
    }

    function roleHas(role, code) {
        var defaults = DATA.roleDefaults[role] || [];
        return defaults.indexOf(code) !== -1;
    }

    /* ----------------------------------------------------------- разбор */

    function renderDetail() {
        var user = byId(selectedId);

        if (!user) {
            detailEl.innerHTML =
                '<div class="us-empty">Выберите учётную запись слева</div>';
            return;
        }

        var isAdmin = user.role === "admin";
        var isMe = user.id === DATA.meId;

        var html =
            '<div class="us-head">' +
                '<span class="us-av' + (user.is_active ? "" : " off") + '">' +
                    esc(initials(user.username)) + '</span>' +
                '<div>' +
                    '<h2>' + esc(user.username) +
                        (isMe ? " — это вы" : "") + '</h2>' +
                    '<div class="sub">' +
                        (user.created_at
                            ? "заведён " + esc(user.created_at) : "") +
                    '</div>' +
                '</div>' +
                '<div class="sp">' +
                    '<button class="btn btn-secondary btn-sm" type="button" ' +
                        'id="usPwdBtn">Сбросить пароль</button>' +
                '</div>' +
            '</div>';

        html +=
            '<div class="us-grid">' +
                '<div>' +
                    '<label class="form-label">Роль</label>' +
                    '<select class="form-select" id="usRole"' +
                        (isMe ? " disabled" : "") + '>' +
                        DATA.roles.map(function (r) {
                            return '<option value="' + esc(r) + '"' +
                                (r === user.role ? " selected" : "") + '>' +
                                esc(ROLE_LABEL[r] || r) + '</option>';
                        }).join("") +
                    '</select>' +
                '</div>' +
                '<div>' +
                    '<label class="form-label">Состояние</label>' +
                    '<select class="form-select" id="usActive"' +
                        (isMe ? " disabled" : "") + '>' +
                        '<option value="1"' +
                            (user.is_active ? " selected" : "") +
                            '>работает</option>' +
                        '<option value="0"' +
                            (user.is_active ? "" : " selected") +
                            '>выключен</option>' +
                    '</select>' +
                '</div>' +
            '</div>';

        if (isMe) {
            html += '<div class="us-note">Свою роль и состояние здесь ' +
                'изменить нельзя: иначе легко выйти из системы навсегда. ' +
                'Пусть это сделает другой администратор.</div>';
        }

        if (isAdmin) {
            html += '<div class="us-note">У администратора все возможности ' +
                'и все кластеры. Снять с него отдельное право нельзя — ' +
                'последний администратор, лишённый управления людьми, ' +
                'запер бы систему без единого входа.</div>';
        }

        html += '<h6 class="us-sec">Возможности</h6><div class="us-caps">';

        groupCapabilities().forEach(function (group) {
            html += '<div class="us-cap-grp"><h6>' + esc(group.label) + '</h6>';

            group.items.forEach(function (item) {
                var override = user.overrides[item.code];
                var effective = isAdmin
                    || (override === undefined
                        ? roleHas(user.role, item.code) : override);
                var pinned = override === undefined || isAdmin;

                html +=
                    '<label class="us-cap">' +
                        '<input type="checkbox" class="form-check-input" ' +
                            'data-cap="' + esc(item.code) + '"' +
                            (effective ? " checked" : "") +
                            (isAdmin ? " disabled" : "") + '>' +
                        '<span class="lbl">' + esc(item.label) + '</span>' +
                        '<span class="ex" data-reset="' + esc(item.code) + '" ' +
                            'title="Вернуть под роль"' +
                            (pinned ? " hidden" : "") + '>искл.</span>' +
                    '</label>';
            });

            html += '</div>';
        });

        html += '</div>';

        html += '<h6 class="us-sec">Доступ к кластерам</h6>';

        if (isAdmin) {
            html += '<div class="us-note">Все кластеры.</div>';
        } else if (!DATA.connections.length) {
            html += '<div class="us-note">Подключений пока нет.</div>';
        } else {
            var granted = user.connection_ids || [];

            html += '<div class="us-conns">';
            DATA.connections.forEach(function (c) {
                html +=
                    '<label class="us-conn">' +
                        '<input type="checkbox" class="form-check-input" ' +
                            'data-conn="' + c.id + '"' +
                            (granted.indexOf(c.id) !== -1 ? " checked" : "") +
                            '>' +
                        '<span>' + esc(c.name) + '</span>' +
                        '<span class="host">' + esc(c.host || "") + '</span>' +
                    '</label>';
            });
            html += '</div>';

            if (!granted.length) {
                html += '<div class="us-note">Ни одного кластера не выдано — ' +
                    'работать этот человек пока ни с чем не сможет.</div>';
            }
        }

        html +=
            '<div class="us-foot">' +
                '<button class="btn btn-primary" type="button" id="usSave">' +
                    'Сохранить</button>' +
                '<span class="sp"></span>' +
                (isMe ? '' :
                    '<button class="btn btn-danger btn-sm" type="button" ' +
                        'id="usDelete">Удалить</button>') +
            '</div>';

        detailEl.innerHTML = html;
        wireDetail(user);
    }

    function wireDetail(user) {
        detailEl.querySelectorAll("[data-reset]").forEach(function (badge) {
            badge.addEventListener("click", function (event) {
                event.preventDefault();
                delete user.overrides[badge.getAttribute("data-reset")];
                renderDetail();
            });
        });

        // отметка «искл.» появляется сразу, как галочка разошлась с ролью
        detailEl.querySelectorAll("[data-cap]").forEach(function (box) {
            box.addEventListener("change", function () {
                var code = box.getAttribute("data-cap");
                var role = detailEl.querySelector("#usRole").value;
                var badge = detailEl.querySelector('[data-reset="' + code + '"]');

                if (box.checked === roleHas(role, code)) {
                    delete user.overrides[code];
                    if (badge) { badge.hidden = true; }
                } else {
                    user.overrides[code] = box.checked;
                    if (badge) { badge.hidden = false; }
                }
            });
        });

        var roleSelect = detailEl.querySelector("#usRole");
        if (roleSelect) {
            roleSelect.addEventListener("change", function () {
                // смена роли меняет базу, от которой считаются исключения:
                // перерисовываем, чтобы галочки показали новый набор
                user.role = roleSelect.value;
                renderDetail();
            });
        }

        var saveBtn = detailEl.querySelector("#usSave");
        if (saveBtn) {
            saveBtn.addEventListener("click", function () { save(user); });
        }

        var pwdBtn = detailEl.querySelector("#usPwdBtn");
        if (pwdBtn) {
            pwdBtn.addEventListener("click", function () { resetPassword(user); });
        }

        var delBtn = detailEl.querySelector("#usDelete");
        if (delBtn) {
            delBtn.addEventListener("click", function () { remove(user); });
        }
    }

    /* ------------------------------------------------------------ запись */

    function collect(user) {
        var overrides = {};
        var connectionIds = [];

        detailEl.querySelectorAll("[data-cap]").forEach(function (box) {
            var code = box.getAttribute("data-cap");
            if (user.overrides[code] !== undefined) {
                overrides[code] = user.overrides[code];
            }
        });

        detailEl.querySelectorAll("[data-conn]").forEach(function (box) {
            if (box.checked) {
                connectionIds.push(parseInt(box.getAttribute("data-conn"), 10));
            }
        });

        var payload = { overrides: overrides };

        if (user.role !== "admin") {
            payload.connection_ids = connectionIds;
        }

        var roleSelect = detailEl.querySelector("#usRole");
        var activeSelect = detailEl.querySelector("#usActive");

        if (roleSelect && !roleSelect.disabled) {
            payload.role = roleSelect.value;
        }
        if (activeSelect && !activeSelect.disabled) {
            payload.is_active = activeSelect.value === "1";
        }

        return payload;
    }

    function send(url, method, body) {
        return fetch(url, {
            method: method,
            headers: { "Content-Type": "application/json" },
            body: body === undefined ? undefined : JSON.stringify(body)
        }).then(function (response) {
            return response.json().catch(function () {
                return { ok: false, message: "Сервер ответил не JSON" };
            });
        });
    }

    function save(user) {
        send("/api/users/" + user.id, "PUT", collect(user)).then(function (data) {
            if (!data.ok) {
                window.gpToast(data.message || "Не удалось сохранить", "error");
                return;
            }

            replaceUser(data.user);
            window.gpToast("Сохранено", "success");
        });
    }

    function replaceUser(updated) {
        for (var i = 0; i < users.length; i++) {
            if (users[i].id === updated.id) { users[i] = updated; break; }
        }
        renderList();
        renderDetail();
    }

    function remove(user) {
        window.gpConfirm(
            "Удалить «" + user.username + "»? Учётная запись и все выданные " +
            "ей права исчезнут, открытые сессии закроются.",
            {
                title: "Удаление пользователя",
                confirmText: "Удалить",
                danger: true
            }
        ).then(function (agreed) {
            if (!agreed) { return; }

            send("/api/users/" + user.id, "DELETE").then(function (data) {
                if (!data.ok) {
                    window.gpToast(data.message || "Не удалось удалить", "error");
                    return;
                }

                users = users.filter(function (u) { return u.id !== user.id; });
                selectedId = users.length ? users[0].id : null;
                renderList();
                renderDetail();
                window.gpToast("Учётная запись удалена", "success");
            });
        });
    }

    /* ---------------------------------------------------------- пароли */

    function askPassword(title, lead) {
        return new Promise(function (resolve) {
            var overlay = document.createElement("div");
            overlay.className = "gp-dialog-overlay";

            var panel = document.createElement("div");
            panel.className = "gp-dialog";
            panel.setAttribute("role", "dialog");
            panel.setAttribute("aria-modal", "true");

            panel.innerHTML =
                '<div class="gp-dialog-title">' + esc(title) + '</div>' +
                '<div class="gp-dialog-body">' +
                    '<p class="us-dlg-lead">' + esc(lead) + '</p>' +
                    '<input class="form-control" type="text" id="usPwdField" ' +
                        'autocomplete="new-password" placeholder="не короче ' +
                        DATA.minPasswordLength + ' символов">' +
                '</div>' +
                '<div class="gp-dialog-actions">' +
                    '<button class="btn btn-secondary" type="button" ' +
                        'data-act="cancel">Отмена</button>' +
                    '<button class="btn btn-primary" type="button" ' +
                        'data-act="ok">Задать</button>' +
                '</div>';

            overlay.appendChild(panel);
            document.body.appendChild(overlay);

            var field = panel.querySelector("#usPwdField");
            field.focus();

            function close(value) {
                overlay.remove();
                resolve(value);
            }

            panel.querySelector('[data-act="cancel"]')
                .addEventListener("click", function () { close(null); });
            panel.querySelector('[data-act="ok"]')
                .addEventListener("click", function () { close(field.value); });

            field.addEventListener("keydown", function (event) {
                if (event.key === "Enter") { close(field.value); }
                if (event.key === "Escape") { close(null); }
            });
        });
    }

    function resetPassword(user) {
        askPassword(
            "Пароль для «" + user.username + "»",
            "Передайте его лично. При первом входе человек обязан будет " +
            "сменить пароль — вы его знаете, значит, своим он не является."
        ).then(function (password) {
            if (!password) { return; }

            send("/api/users/" + user.id + "/password", "POST",
                 { password: password }).then(function (data) {
                if (!data.ok) {
                    window.gpToast(data.message || "Не удалось задать", "error");
                    return;
                }

                window.gpToast("Пароль задан", "success");
                refresh();
            });
        });
    }

    /* --------------------------------------------------------- создание */

    function createUser() {
        var overlay = document.createElement("div");
        overlay.className = "gp-dialog-overlay";

        var panel = document.createElement("div");
        panel.className = "gp-dialog";
        panel.setAttribute("role", "dialog");
        panel.setAttribute("aria-modal", "true");

        panel.innerHTML =
            '<div class="gp-dialog-title">Новый пользователь</div>' +
            '<div class="gp-dialog-body">' +
                '<label class="form-label">Имя</label>' +
                '<input class="form-control" type="text" id="usNewName" ' +
                    'autocomplete="off">' +
                '<label class="form-label us-dlg-gap">Пароль</label>' +
                '<input class="form-control" type="text" id="usNewPwd" ' +
                    'autocomplete="new-password" placeholder="не короче ' +
                    DATA.minPasswordLength + ' символов">' +
                '<label class="form-label us-dlg-gap">Роль</label>' +
                '<select class="form-select" id="usNewRole">' +
                    DATA.roles.map(function (r) {
                        return '<option value="' + esc(r) + '">' +
                            esc(ROLE_LABEL[r] || r) + '</option>';
                    }).join("") +
                '</select>' +
                '<p class="us-dlg-lead us-dlg-gap">Кластеры выдаются после ' +
                    'создания — по умолчанию не выдан ни один.</p>' +
            '</div>' +
            '<div class="gp-dialog-actions">' +
                '<button class="btn btn-secondary" type="button" ' +
                    'data-act="cancel">Отмена</button>' +
                '<button class="btn btn-primary" type="button" ' +
                    'data-act="ok">Создать</button>' +
            '</div>';

        overlay.appendChild(panel);
        document.body.appendChild(overlay);
        panel.querySelector("#usNewName").focus();

        panel.querySelector('[data-act="cancel"]')
            .addEventListener("click", function () { overlay.remove(); });

        panel.querySelector('[data-act="ok"]').addEventListener("click", function () {
            var payload = {
                username: panel.querySelector("#usNewName").value,
                password: panel.querySelector("#usNewPwd").value,
                role: panel.querySelector("#usNewRole").value
            };

            send("/api/users", "POST", payload).then(function (data) {
                if (!data.ok) {
                    window.gpToast(data.message || "Не удалось создать", "error");
                    return;
                }

                overlay.remove();
                users.push(data.user);
                users.sort(function (a, b) {
                    return a.username.localeCompare(b.username);
                });
                selectedId = data.user.id;
                renderList();
                renderDetail();
                window.gpToast("Пользователь создан", "success");
            });
        });
    }

    function refresh() {
        send("/api/users", "GET").then(function (data) {
            if (!data.ok) { return; }
            users = data.users;
            renderList();
            renderDetail();
        });
    }

    document.getElementById("usAddBtn").addEventListener("click", createUser);

    renderList();
    renderDetail();
})();
