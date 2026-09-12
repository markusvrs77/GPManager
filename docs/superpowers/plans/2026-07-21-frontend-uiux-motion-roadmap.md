# GPManager Frontend / UI-UX / Motion Program Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn GPManager's ad-hoc Bootstrap frontend into a consistent, accessible, tastefully animated design-system-driven UI without leaving the Flask + Jinja + vanilla-JS stack.

**Architecture:** A single token layer (`static/css/tokens.css`) drives light/dark themes and motion timing. Two small framework-free JS helpers — `motion.js` (Web Animations API wrapper applying motion-framer principles) and `dom.js` (safe node builders) — replace ad-hoc animation and unsafe `innerHTML`. Templates and per-page JS are reworked page-by-page on top of the token layer. All assets are self-hosted so the RHEL offline package has no external fetch.

**Tech Stack:** Flask, Jinja2, Bootstrap 5 (self-hosted), vanilla JS, Web Animations API, self-hosted Fira Sans / Fira Code + an SVG icon set (Lucide), Chart.js (self-hosted, page-scoped), pytest (Flask test client) for smoke tests.

## Global Constraints

- Do not change the metadata-store, executor, or job interfaces frozen after backend Phase 1 of `2026-07-20-rhel-production-roadmap.md`; this program is frontend-only.
- No external CDN references anywhere in shipped templates (RHEL offline requirement) — self-host Bootstrap, Chart.js, fonts, icons.
- Every animation must be gated behind `prefers-reduced-motion` and animate only `transform`/`opacity`.
- Never convey status by color alone — always pair with a label or icon (WCAG).
- Text contrast ≥ 4.5:1 in both light and dark themes.
- Semantic color tokens only in components — no raw hex in templates or JS.
- One primary CTA per screen; touch targets ≥ 44×44px.
- Brand name is a single value everywhere: **GPManager** (retire "Greenplum Reorganize Center").
- UI copy language is unified (default: English UI strings; no mixed RU/EN in the same screen).
- The XSS/`innerHTML` work here is the frontend execution of the "XSS sinks" exit criterion in `2026-07-20-auth-web-security.md` — align, do not fork.

---

## Program sequence

### Phase 0: Correctness & code-health cleanup

Execute [Frontend Phase 0 — Cleanup Plan](2026-07-21-frontend-phase0-cleanup.md).

Exit criteria:

- `base.html` loads Bootstrap exactly once; Chart.js is loaded only on Dashboard and Skew;
- orphaned `templates/index.html`, `templates/skew.html`, `templates/reorganize.html`, `static/js/skew.js.old`, empty `static/js/app.js`, and empty `models.py` are removed (the still-used `skew.js`/`reorganize.js` are kept — `maintenance.html` loads them);
- `dashboard.html` has a single `loadSessionLimitsStats()` implementation with no references to non-existent element IDs;
- `openpyxl` is pinned in `requirements.txt` and a clean install imports `app` without error;
- `static/css/style.css` has one definition per selector (no duplicated `.gpcopy-*` grids);
- a pytest smoke test asserts every GET route returns 200 and `base.html` contains exactly one Bootstrap bundle; `pytest` passes in CI.

### Phase 1: Design-system foundation

Execute Frontend Phase 1 — Design System plan (to be written from this roadmap when Phase 0 lands).

Exit criteria:

- `static/css/tokens.css` defines color (light + dark), spacing (4/8), radius, elevation, typography, and motion tokens;
- Fira Sans/Fira Code and the SVG icon set are self-hosted; no font/icon CDN remains;
- `base.html` has one brand + logo, per-page `<title>`, favicon, a persistent theme toggle, correct `active` state for every nav item, and no dead "Queries" link;
- `style.css` is rebuilt on tokens with component classes (`.gp-card`, `.gp-table`, `.gp-badge--{ok,warn,crit}`, `.gp-btn`, `.gp-skeleton`, `.gp-empty`) and contains no raw hex;
- the design system is persisted to `design-system/MASTER.md` (+ per-page overrides);
- every page passes a light/dark contrast check.

### Phase 2: Motion system

Execute Frontend Phase 2 — Motion plan.

Exit criteria:

- `static/js/motion.js` exposes `enter()`, `exit()`, `stagger()`, `press()` over the Web Animations API using the Phase 1 motion tokens, all gated by `prefers-reduced-motion`;
- nav/tab active indicator slides between items; dashboard cards and table rows reveal with 30–50ms stagger;
- Connections "Test" result, toasts, and modals have enter/exit animations (exit ≈ 60–70% of enter);
- `reorgSpin`/`reorgPulse` are replaced by tokenized skeleton/shimmer for loads > 300ms;
- with reduced-motion enabled, all animations collapse to instant and no `top/left/width/height` is animated.

### Phase 3: Page-by-page UX rework

Execute Frontend Phase 3 — Pages plan.

Exit criteria (per page: Dashboard, Skew, gpcopy, Reorganize, Maintenance, Vacuum, Connections, Objects):

- data-dense tables use tabular figures, sticky headers, status badges (label+color), loading skeletons, and designed empty-states;
- each page renders cleanly at 375 / 768 / 1024 / 1440px with no horizontal scroll and one primary CTA;
- job-run surfaces (skew, gpcopy, reorganize, vacuum) have consistent progress + stop affordances;
- Connections "Test" replaces the raw `JSON.stringify` `<pre>` dump with a structured result card.

### Phase 4: Safe rendering, forms, feedback, accessibility

Execute Frontend Phase 4 — Safety & A11y plan.

Exit criteria:

- `static/js/dom.js` safe builders replace every `innerHTML` sink that interpolates DB/server values; remaining `innerHTML` uses are provably static;
- forms have visible labels, required indicators, on-blur validation, `aria-live` errors, and focus-first-invalid on submit;
- a reusable toast (`aria-live="polite"`, auto-dismiss 3–5s) and confirm-dialog replace native `confirm()` for destructive actions;
- icon-only controls have `aria-label`; focus rings are visible; charts have data-table or `aria` summary fallbacks;
- a keyboard-only pass of Dashboard, Connections, and Skew reaches every control in logical order.

### Phase 5: Responsive polish & final QA

Execute Frontend Phase 5 — QA plan.

Exit criteria:

- systematic breakpoints, adaptive gutters, `min-h-dvh`, and readable long-text measure on wide screens;
- dark-mode contrast verified independently of light;
- zero external-host references remain in shipped templates/JS/CSS;
- Quick-Reference §1–§3 (accessibility, interaction, performance) pass recorded.

## Program-level verification

- [ ] `pip install -r requirements.txt` in a clean venv, then `pytest` passes and `python app.py` boots.
- [ ] Load every route; Dashboard charts, session-limits, skew, and gpcopy job flows work.
- [ ] Toggle light/dark on every page; contrast ≥ 4.5:1 confirmed with an automated checker (axe) in both themes.
- [ ] Enable OS reduce-motion; confirm animations become instant with no console errors.
- [ ] Keyboard-only + screen-reader pass on Dashboard, Connections, Skew.
- [ ] `grep -rn "innerHTML" static/js` — remaining hits are static only.
- [ ] `grep -rn "cdn\.\|jsdelivr\|googleapis\|unpkg" templates static` returns nothing.
- [ ] Verified at 375 / 768 / 1024 / 1440px with no horizontal scroll.
