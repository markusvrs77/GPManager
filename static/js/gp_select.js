/* Opsentri — тематизированная шторка для нативных <select>.

   Сам select оформлен страничным CSS и выглядит правильно, но выпадающий
   список рисует ОС: до <option> не доходит ни один селектор, поэтому в
   светлой теме посреди скруглённого интерфейса появляется синяя подсветка
   и квадратные углы.

   Контрол при этом не подменяется — иначе пришлось бы переносить стили всех
   селектов приложения на кнопку-заменитель. Перехватывается только открытие:
   нативная шторка отменяется, вместо неё показывается своя. <select>
   остаётся источником правды — value, отправка формы и весь существующий
   JS читают его как раньше.

   Обработчики висят на document, поэтому селекты, которые появятся позже
   (часть разметки страницы рисуют из JS), работают без дорегистрации. */
(function () {
    "use strict";

    var menu = null;      // единственная шторка в документе
    var owner = null;     // select, которому она сейчас принадлежит
    var active = -1;      // подсвеченный пункт (клавиатура)

    function esc(s) {
        return String(s == null ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    }

    // data-gpsel="off" — для селектов с собственным выпадающим списком
    function eligible(sel) {
        return Boolean(sel) && sel.tagName === "SELECT" && !sel.disabled &&
            !sel.multiple && (!sel.size || sel.size <= 1) &&
            sel.dataset.gpsel !== "off" && sel.options.length > 0;
    }

    function ensureMenu() {
        if (menu) { return menu; }
        menu = document.createElement("div");
        menu.className = "gpsel-menu";
        menu.setAttribute("role", "listbox");
        document.body.appendChild(menu);
        return menu;
    }

    function isOpen() { return Boolean(owner); }

    function close() {
        if (!menu) { return; }
        menu.classList.remove("show");
        if (owner) { owner.setAttribute("aria-expanded", "false"); }
        owner = null;
        active = -1;
    }

    /* Шторка живёт в body с position: fixed — так её не обрежет предок с
       overflow: hidden и не перекроет чужой z-index. */
    function place() {
        if (!owner || !menu) { return; }

        var r = owner.getBoundingClientRect();
        var gap = 6;
        var vh = document.documentElement.clientHeight;
        var vw = document.documentElement.clientWidth;

        menu.style.minWidth = Math.round(r.width) + "px";
        menu.style.maxWidth = Math.max(240, Math.round(vw - 24)) + "px";
        menu.style.maxHeight = "";

        // высоту меряем уже раскрытой, иначе получим ноль
        var need = menu.offsetHeight;
        var below = vh - r.bottom - gap;
        var above = r.top - gap;
        var up = below < Math.min(need, 260) && above > below;

        menu.style.maxHeight = Math.max(120, (up ? above : below) - 8) + "px";
        var h = menu.offsetHeight;

        var left = Math.min(r.left, vw - menu.offsetWidth - 8);
        menu.style.left = Math.max(8, Math.round(left)) + "px";
        menu.style.top = Math.round(up ? r.top - gap - h : r.bottom + gap) + "px";
    }

    function optHtml(o, i) {
        var cur = o.selected;
        return '<div class="gpsel-opt' + (cur ? " cur" : "") +
            (o.disabled ? " off" : "") + '" role="option" data-i="' + i +
            '" aria-selected="' + (cur ? "true" : "false") + '">' +
            "<span>" + esc(o.textContent.trim() || o.value) + "</span>" +
            (cur ? '<span class="gpsel-ck">✓</span>' : "") + "</div>";
    }

    function render() {
        var html = "";
        var idx = 0;

        Array.prototype.forEach.call(owner.children, function (node) {
            if (node.tagName === "OPTGROUP") {
                html += '<div class="gpsel-grp">' + esc(node.label) + "</div>";
                Array.prototype.forEach.call(node.children, function (o) {
                    html += optHtml(o, idx);
                    idx += 1;
                });
                return;
            }
            if (node.tagName === "OPTION") {
                html += optHtml(node, idx);
                idx += 1;
            }
        });

        menu.innerHTML = html;

        menu.querySelectorAll(".gpsel-opt").forEach(function (el) {
            el.onmouseenter = function () {
                setActive(Number(el.getAttribute("data-i")), false);
            };
            el.onclick = function (ev) {
                ev.stopPropagation();
                choose(Number(el.getAttribute("data-i")));
            };
        });
    }

    function items() { return menu.querySelectorAll(".gpsel-opt"); }

    function setActive(i, scroll) {
        var list = items();
        if (!list.length) { return; }
        if (i < 0) { i = 0; }
        if (i > list.length - 1) { i = list.length - 1; }
        active = i;
        list.forEach(function (el, n) { el.classList.toggle("on", n === i); });
        if (scroll !== false && list[i]) {
            list[i].scrollIntoView({ block: "nearest" });
        }
    }

    function step(delta) {
        var list = items();
        var i = active < 0 ? owner.selectedIndex : active;

        for (var n = 0; n < list.length; n++) {
            i += delta;
            if (i < 0 || i > list.length - 1) { return; }
            if (!list[i].classList.contains("off")) { setActive(i); return; }
        }
    }

    function choose(i) {
        var sel = owner;
        if (!sel) { return; }

        var opts = sel.options;
        if (i < 0 || i >= opts.length || opts[i].disabled) { return; }

        var changed = sel.selectedIndex !== i;
        close();

        if (changed) {
            sel.selectedIndex = i;
            // страницы слушают и то, и другое
            sel.dispatchEvent(new Event("input", { bubbles: true }));
            sel.dispatchEvent(new Event("change", { bubbles: true }));
        }
        sel.focus();
    }

    function open(sel) {
        ensureMenu();
        owner = sel;
        sel.setAttribute("aria-expanded", "true");
        render();
        menu.classList.add("show");
        place();
        setActive(sel.selectedIndex);
    }

    function toggle(sel) {
        if (owner === sel) { close(); return; }
        close();
        open(sel);
    }

    /* ---------------- события ---------------- */

    // capture: отменяем нативную шторку раньше, чем её откроет браузер
    document.addEventListener("mousedown", function (e) {
        if (menu && menu.contains(e.target)) { return; }

        var sel = e.target && e.target.closest
            ? e.target.closest("select") : null;

        if (!eligible(sel)) { close(); return; }

        e.preventDefault();   // вместе с событием гасится фокус — возвращаем
        sel.focus();
        toggle(sel);
    }, true);

    document.addEventListener("keydown", function (e) {
        if (isOpen()) {
            if (e.key === "Escape") {
                e.preventDefault();
                var back = owner;
                close();
                back.focus();
                return;
            }
            if (e.key === "ArrowDown") { e.preventDefault(); step(1); return; }
            if (e.key === "ArrowUp") { e.preventDefault(); step(-1); return; }
            if (e.key === "Home") { e.preventDefault(); setActive(0); return; }
            if (e.key === "End") {
                e.preventDefault();
                setActive(items().length - 1);
                return;
            }
            if (e.key === "Enter" || e.key === " " || e.key === "Tab") {
                e.preventDefault();
                choose(active < 0 ? owner.selectedIndex : active);
            }
            return;
        }

        var sel = document.activeElement;
        if (!eligible(sel)) { return; }

        if (e.key === "Enter" || e.key === " " ||
            e.key === "ArrowDown" || e.key === "ArrowUp") {
            e.preventDefault();
            open(sel);
        }
    }, true);

    window.addEventListener("resize", close);
    // прокрутка любого предка — переставляем, а не закрываем
    window.addEventListener("scroll", function () {
        if (isOpen()) { place(); }
    }, true);
    window.addEventListener("blur", close);

    window.gpSelectMenu = { close: close, isOpen: isOpen };
}());
