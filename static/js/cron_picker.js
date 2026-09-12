/* Календарный выбор расписания вместо cron-строки.
   Пользователь выбирает «когда» человеческими словами и время часами,
   а cron собирается сам и остаётся видимым только как подпись.

   Использование:
     var picker = gpCronPicker(document.getElementById("box"), {
         value: "0 2 * * *",
         onChange: function (cron) { ... }
     });
     picker.value();            // текущее cron-выражение
     picker.set("0 6 * * 1");   // задать снаружи
*/
(function () {
    "use strict";

    var DOW = [
        { v: "1", t: "Пн" }, { v: "2", t: "Вт" }, { v: "3", t: "Ср" },
        { v: "4", t: "Чт" }, { v: "5", t: "Пт" }, { v: "6", t: "Сб" },
        { v: "0", t: "Вс" }
    ];

    function pad(n) { return (n < 10 ? "0" : "") + n; }

    // spec -> cron. Чистая функция.
    function cronFromSpec(spec) {
        var hh = Math.max(0, Math.min(23, parseInt(spec.hour, 10) || 0));
        var mm = Math.max(0, Math.min(59, parseInt(spec.minute, 10) || 0));

        if (spec.every === "hours") {
            var step = Math.max(1, Math.min(23, parseInt(spec.step, 10) || 1));
            return mm + " */" + step + " * * *";
        }

        if (spec.every === "week") {
            var days = (spec.days || []).slice().sort(function (a, b) {
                return Number(a) - Number(b);
            });

            return mm + " " + hh + " * * " +
                (days.length ? days.join(",") : "1");
        }

        if (spec.every === "month") {
            var dates = (spec.dates || []).slice().sort(function (a, b) {
                return Number(a) - Number(b);
            });

            return mm + " " + hh + " " +
                (dates.length ? dates.join(",") : "1") + " * *";
        }

        return mm + " " + hh + " * * *";        // каждый день
    }

    // cron -> spec: чтобы открыть уже сохранённое расписание. Чистая функция.
    function specFromCron(cron) {
        var parts = String(cron || "").trim().split(/\s+/);

        if (parts.length !== 5) { return null; }

        var mm = parts[0], hh = parts[1], dom = parts[2], dow = parts[4];
        var hourStep = /^\*\/(\d+)$/.exec(hh);

        if (hourStep && /^\d+$/.test(mm)) {
            return { every: "hours", step: Number(hourStep[1]),
                     minute: Number(mm), hour: 0 };
        }

        if (!/^\d+$/.test(mm) || !/^\d+$/.test(hh)) { return null; }

        var minute = Number(mm);
        var hour = Number(hh);

        if (dow !== "*" && /^[0-6](,[0-6])*$/.test(dow)) {
            return { every: "week", days: dow.split(","),
                     minute: minute, hour: hour };
        }

        if (dom !== "*" && /^\d+(,\d+)*$/.test(dom)) {
            return { every: "month", dates: dom.split(","),
                     minute: minute, hour: hour };
        }

        if (dom === "*" && dow === "*") {
            return { every: "day", minute: minute, hour: hour };
        }

        return null;
    }

    function humanize(spec) {
        var at = pad(spec.hour) + ":" + pad(spec.minute);

        if (spec.every === "hours") {
            return "каждые " + spec.step + " ч, на " + pad(spec.minute) +
                " минуте";
        }

        if (spec.every === "week") {
            var names = (spec.days || []).map(function (d) {
                var found = DOW.filter(function (x) {
                    return x.v === String(d);
                });

                return found.length ? found[0].t : d;
            });

            return (names.length ? names.join(", ") : "Пн") + " в " + at;
        }

        if (spec.every === "month") {
            return ((spec.dates || []).join(", ") || "1") + " числа в " + at;
        }

        return "каждый день в " + at;
    }

    function gpCronPicker(mount, opts) {
        opts = opts || {};

        var spec = specFromCron(opts.value) || { every: "day", hour: 2,
                                                 minute: 0 };

        spec.days = spec.days || ["1"];
        spec.dates = spec.dates || ["1"];
        spec.step = spec.step || 6;

        function fire() {
            if (opts.onChange) { opts.onChange(cronFromSpec(spec)); }
        }

        function render() {
            var rows = "";

            if (spec.every === "week") {
                rows += '<div class="gpcp-chips">' +
                    DOW.map(function (d) {
                        var on = spec.days.indexOf(d.v) >= 0;
                        return '<button type="button" class="gpcp-chip' +
                            (on ? " on" : "") + '" data-day="' + d.v + '">' +
                            d.t + "</button>";
                    }).join("") + "</div>";
            }

            if (spec.every === "month") {
                var days = [];

                for (var i = 1; i <= 31; i++) {
                    var day = String(i);
                    var sel = spec.dates.indexOf(day) >= 0;

                    days.push('<button type="button" class="gpcp-chip sm' +
                        (sel ? " on" : "") + '" data-date="' + day + '">' +
                        day + "</button>");
                }

                rows += '<div class="gpcp-chips">' + days.join("") + "</div>";
            }

            var timeRow = spec.every === "hours"
                ? '<label class="gpcp-f">Каждые' +
                  '<select class="gpcp-step">' +
                  [1, 2, 3, 4, 6, 8, 12].map(function (h) {
                      return '<option value="' + h + '"' +
                          (Number(spec.step) === h ? " selected" : "") + ">" +
                          h + " ч</option>";
                  }).join("") + "</select></label>" +
                  '<label class="gpcp-f">На минуте' +
                  '<input type="number" class="gpcp-min" min="0" max="59"' +
                  ' value="' + spec.minute + '"></label>'
                : '<label class="gpcp-f">Время' +
                  '<input type="time" class="gpcp-time" value="' +
                  pad(spec.hour) + ":" + pad(spec.minute) + '"></label>';

            mount.innerHTML =
                '<div class="gpcp"><div class="gpcp-row">' +
                '<label class="gpcp-f">Повторять' +
                '<select class="gpcp-every">' +
                [["day", "каждый день"], ["week", "по дням недели"],
                 ["month", "по числам месяца"], ["hours", "каждые N часов"]]
                    .map(function (o) {
                        return '<option value="' + o[0] + '"' +
                            (spec.every === o[0] ? " selected" : "") + ">" +
                            o[1] + "</option>";
                    }).join("") + "</select></label>" +
                timeRow + "</div>" +
                (rows ? '<div class="gpcp-row">' + rows + "</div>" : "") +
                '<div class="gpcp-note">' + humanize(spec) +
                " · <code>" + cronFromSpec(spec) + "</code></div></div>";

            mount.querySelector(".gpcp-every").onchange = function () {
                spec.every = this.value;
                render();
                fire();
            };

            var time = mount.querySelector(".gpcp-time");

            if (time) {
                time.onchange = function () {
                    var v = (this.value || "00:00").split(":");

                    spec.hour = Number(v[0]) || 0;
                    spec.minute = Number(v[1]) || 0;
                    render();
                    fire();
                };
            }

            var step = mount.querySelector(".gpcp-step");

            if (step) {
                step.onchange = function () {
                    spec.step = Number(this.value) || 1;
                    render();
                    fire();
                };
            }

            var min = mount.querySelector(".gpcp-min");

            if (min) {
                min.onchange = function () {
                    spec.minute = Math.max(0,
                        Math.min(59, Number(this.value) || 0));
                    render();
                    fire();
                };
            }

            mount.querySelectorAll("[data-day]").forEach(function (btn) {
                btn.onclick = function () {
                    var day = btn.getAttribute("data-day");
                    var idx = spec.days.indexOf(day);

                    if (idx >= 0) { spec.days.splice(idx, 1); }
                    else { spec.days.push(day); }

                    if (!spec.days.length) { spec.days = [day]; }

                    render();
                    fire();
                };
            });

            mount.querySelectorAll("[data-date]").forEach(function (btn) {
                btn.onclick = function () {
                    var date = btn.getAttribute("data-date");
                    var idx = spec.dates.indexOf(date);

                    if (idx >= 0) { spec.dates.splice(idx, 1); }
                    else { spec.dates.push(date); }

                    if (!spec.dates.length) { spec.dates = [date]; }

                    render();
                    fire();
                };
            });
        }

        render();

        return {
            value: function () { return cronFromSpec(spec); },
            set: function (cron) {
                var parsed = specFromCron(cron);

                if (!parsed) { return; }

                spec = parsed;
                spec.days = spec.days || ["1"];
                spec.dates = spec.dates || ["1"];
                spec.step = spec.step || 6;
                render();
            },
        };
    }

    // человеческая подпись расписания для сводок
    function human(cron) {
        var spec = specFromCron(cron);

        return spec ? humanize(spec) : String(cron || "");
    }

    window.gpCronPicker = gpCronPicker;
    window.gpCronHuman = human;
    window.gpCronFromSpec = cronFromSpec;
    window.gpSpecFromCron = specFromCron;
})();
