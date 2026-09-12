/* Вкладка «Сообщения»: чтение записей и ручная отправка. */
(function () {
    "use strict";

    var records = [];
    var openIndex = null;

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
        var sel = $("kmCluster");
        return sel && sel.value ? Number(sel.value) : null;
    }

    function api(url, options) {
        return fetch(url, options || {}).then(function (r) {
            return r.json().then(function (data) {
                return { status: r.status, data: data };
            });
        });
    }

    function send(url, body) {
        return api(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body || {}),
        });
    }

    function showError(message) {
        var box = $("kmError");
        if (!message) { box.style.display = "none"; return; }
        box.textContent = message;
        box.style.display = "";
    }

    function payloadPeek(payload) {
        if (!payload) { return ""; }
        if (payload.kind === "empty") { return "—"; }
        if (payload.kind === "binary") {
            return "двоичные данные, " + fmtN(payload.size) + " байт";
        }
        return (payload.text || "").replace(/\s+/g, " ").slice(0, 200);
    }

    function payloadBlock(title, payload) {
        if (!payload) { return ""; }

        if (payload.kind === "empty") {
            return '<div class="km-kv">' + title + ": —</div>";
        }

        if (payload.kind === "binary") {
            return '<div class="km-kv">' + title + ": двоичные данные, " +
                fmtN(payload.size) + " байт</div><pre>" +
                esc(payload.hex) + "</pre>";
        }

        return '<div class="km-kv">' + title + " · " + fmtN(payload.size) +
            " байт" + (payload.truncated ? " · показано начало" : "") +
            "</div><pre>" + esc(payload.text) + "</pre>";
    }

    function visible() {
        var needle = ($("kmFilter").value || "").trim().toLowerCase();

        if (!needle) { return records; }

        return records.filter(function (r) {
            var hay = (payloadPeek(r.key) + " " + payloadPeek(r.value))
                .toLowerCase();
            return hay.indexOf(needle) >= 0;
        });
    }

    function paint() {
        var rows = visible();

        $("kmCount").textContent = records.length
            ? "показано " + fmtN(rows.length) + " из " + fmtN(records.length)
            : "";

        if (!rows.length) {
            $("kmList").innerHTML = '<div class="km-empty">' +
                (records.length
                    ? "Ничего не найдено."
                    : "Записей нет — выберите топик и нажмите «Прочитать».") +
                "</div>";
            return;
        }

        $("kmList").innerHTML = '<div class="km-list">' +
            rows.map(function (r, i) {
                var open = openIndex === i;
                var body = open
                    ? '<div class="km-body">' +
                      payloadBlock("Ключ", r.key) +
                      payloadBlock("Значение", r.value) +
                      (r.headers && r.headers.length
                          ? '<div class="km-kv">Заголовки: ' +
                            esc(r.headers.map(function (h) {
                                return h[0] + "=" + h[1];
                            }).join(", ")) + "</div>"
                          : "") +
                      "</div>"
                    : "";

                return '<div class="km-row' + (open ? " open" : "") +
                    '" data-i="' + i + '"><span class="meta">п.' +
                    esc(r.partition) + " · оф. " + fmtN(r.offset) + " · " +
                    esc(r.timestamp || "—") + '</span><span class="peek">' +
                    esc(payloadPeek(r.value)) + "</span></div>" + body;
            }).join("") + "</div>";

        Array.prototype.forEach.call(
            $("kmList").querySelectorAll("[data-i]"),
            function (row) {
                row.onclick = function () {
                    var i = Number(row.getAttribute("data-i"));
                    openIndex = openIndex === i ? null : i;
                    repaint();
                };
            }
        );
    }

    function repaint() {
        if (window.gpKeepScroll) {
            window.gpKeepScroll($("kmList"), paint);
        } else {
            paint();
        }
    }

    function loadTopics() {
        var id = clusterId();

        if (!id) { return; }

        api("/api/kafka/clusters/" + id + "/overview").then(function (r) {
            var topics = ((r.data || {}).overview || {}).topics || [];

            $("kmTopic").innerHTML = topics.filter(function (t) {
                return !t.internal;
            }).map(function (t) {
                return '<option value="' + esc(t.name) + '">' +
                    esc(t.name) + "</option>";
            }).join("");

            if (!topics.length) {
                showError("В срезе обзора нет топиков — обновите срез на " +
                    "вкладке «Обзор кластера».");
            }
        });
    }

    function read() {
        var id = clusterId();

        if (!id) { return; }

        var body = {
            topic: $("kmTopic").value,
            mode: $("kmMode").value,
            limit: $("kmLimit").value,
            partition: $("kmPart").value,
        };

        if (body.mode === "offset") { body.offset = $("kmOffset").value; }
        if (body.mode === "timestamp") {
            body.timestamp = ($("kmAt").value || "").replace("T", " ");
        }

        $("kmRead").disabled = true;

        send("/api/kafka/clusters/" + id + "/messages/read", body)
            .then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    showError(r.data.message || "Не удалось прочитать");
                    return;
                }

                showError("");
                records = r.data.records || [];
                openIndex = null;
                paint();

                if (!records.length) {
                    toast("Записей нет: возможно, оффсет или дата за " +
                        "концом партиции", "warning");
                }
            })
            .catch(function (e) { showError(String(e)); })
            .then(function () { $("kmRead").disabled = false; });
    }

    function sendMessage() {
        var id = clusterId();
        var topic = $("kmTopic").value;

        if (!id || !topic) { return; }

        var value = $("kmValue").value;
        var question = "Отправить сообщение в топик «" + topic + "»?\n" +
            value.slice(0, 200);

        var doIt = function () {
            send("/api/kafka/clusters/" + id + "/messages", {
                topic: topic,
                key: $("kmKey").value,
                value: value,
                partition: $("kmSendPart").value,
            }).then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    toast(r.data.message || "Не удалось отправить", "danger");
                    return;
                }

                toast("Отправлено: партиция " + r.data.partition +
                    ", оффсет " + r.data.offset, "success");
                $("kmForm").classList.remove("on");
                $("kmValue").value = "";
                read();
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

    function wire() {
        if (!$("kmRoot") || !$("kmCluster")) { return; }

        $("kmCluster").onchange = function () {
            records = [];
            paint();
            loadTopics();
        };

        $("kmMode").onchange = function () {
            $("kmOffset").hidden = $("kmMode").value !== "offset";
            $("kmAt").hidden = $("kmMode").value !== "timestamp";
        };

        $("kmRead").onclick = read;
        $("kmFilter").oninput = repaint;

        $("kmSendToggle").onclick = function () {
            $("kmForm").classList.toggle("on");
        };
        $("kmSendCancel").onclick = function () {
            $("kmForm").classList.remove("on");
        };
        $("kmSend").onclick = sendMessage;

        paint();
        loadTopics();
    }

    document.addEventListener("DOMContentLoaded", wire);
}());
