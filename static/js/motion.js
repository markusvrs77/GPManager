/* GPManager motion helpers — Web Animations API, motion-framer principles.
   All motion is gated behind prefers-reduced-motion. */
(function () {
    "use strict";

    var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    var EASE = "cubic-bezier(.22,.61,.36,1)";

    // enter: fade + rise. Returns the Animation (or null when reduced).
    function enter(el, opts) {
        if (reduce || !el || !el.animate) { return null; }
        opts = opts || {};
        var y = opts.y == null ? 12 : opts.y;
        return el.animate(
            [
                { opacity: 0, transform: "translateY(" + y + "px)" },
                { opacity: 1, transform: "none" }
            ],
            {
                duration: opts.dur || 260,
                delay: opts.delay || 0,
                easing: EASE,
                fill: "both"
            }
        );
    }

    // stagger: enter a list of elements with an incremental delay.
    function stagger(els, opts) {
        opts = opts || {};
        var step = opts.step == null ? 45 : opts.step;
        Array.prototype.forEach.call(els || [], function (el, i) {
            enter(el, Object.assign({}, opts, { delay: (opts.delay || 0) + i * step }));
        });
    }

    window.gpMotion = { enter: enter, stagger: stagger, reduced: reduce };

    // Auto entrance for cards on every page.
    document.addEventListener("DOMContentLoaded", function () {
        if (reduce) { return; }
        stagger(document.querySelectorAll("main .card"), { step: 40 });
    });
})();
