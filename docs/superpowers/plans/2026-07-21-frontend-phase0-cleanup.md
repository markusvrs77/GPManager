# Frontend Phase 0 — Correctness & Code-Health Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the frontend correctness bugs and dead code found in the audit, and seed a smoke-test safety net, so later design/motion phases build on a clean, verifiable base.

**Architecture:** Introduce a pytest smoke test using Flask's test client that pins the invariants each cleanup task must preserve (routes return 200, Bootstrap loads exactly once). Then perform each cleanup as its own tested, committed task.

**Tech Stack:** Flask, Jinja2, pytest, Flask test client.

## Global Constraints

(Inherited from `2026-07-21-frontend-uiux-motion-roadmap.md`. Phase-0-relevant items:)

- Frontend-only; do not touch executor/job/metadata interfaces.
- Brand name is a single value everywhere: **GPManager**.
- No external CDN references added; Phase 0 does not yet self-host — it only removes duplicates/dead code.
- Every GET route must keep returning 200 after each task.

## File Structure

- `tests/__init__.py` — makes `tests` a package (empty).
- `tests/conftest.py` — pytest fixtures: a Flask test `client` with the metadata DB initialized once.
- `tests/test_smoke.py` — route-200 smoke tests + asset-invariant assertions.
- `requirements.txt` — add `pytest` and `openpyxl`.
- `templates/base.html` — remove the duplicate Bootstrap script + global Chart.js.
- `templates/dashboard.html` — add page-scoped Chart.js; delete the dead first `loadSessionLimitsStats()` block.
- `templates/maintenance.html` — add page-scoped Chart.js (it loads `skew.js`, which renders charts). NOTE: `/skew` and `/reorganize` are 302 redirects to `/maintenance`; `skew.html`/`reorganize.html` templates are orphaned (no route renders them) and are deleted in Task 4. `static/js/skew.js` and `static/js/reorganize.js` stay — `maintenance.html` loads them.
- `static/css/style.css` — collapse duplicated `.gpcopy-*` / `.job-items-scroll` selectors to one definition each.
- Delete: `templates/index.html`, `templates/skew.html`, `templates/reorganize.html`, `static/js/skew.js.old`, `static/js/app.js`, `models.py`.

---

### Task 1: Smoke-test harness (safety net)

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/test_smoke.py`
- Modify: `requirements.txt` (add `pytest`)

**Interfaces:**
- Consumes: `app.app` (the Flask instance, `app.py:62`), `db.init_db` (`db.py`).
- Produces: a `client` pytest fixture and passing baseline tests that later tasks must keep green.

- [ ] **Step 1: Add pytest to requirements**

Append to `requirements.txt`:

```
pytest==7.4.4
```

- [ ] **Step 2: Create the tests package**

Create `tests/__init__.py` (empty file).

- [ ] **Step 3: Write the fixture**

Create `tests/conftest.py`:

```python
import pytest

from app import app as flask_app
from db import init_db


@pytest.fixture(scope="session", autouse=True)
def _init_metadata_db():
    # init_db() runs only under __main__ in app.py, so tests must init explicitly.
    init_db()


@pytest.fixture
def client():
    flask_app.config.update(TESTING=True)
    with flask_app.test_client() as c:
        yield c
```

- [ ] **Step 4: Write the failing smoke test**

Create `tests/test_smoke.py`. NOTE: `/skew` and `/reorganize` are intentional 302 redirects to `/maintenance` (functionality was consolidated there), so they are tested separately as redirects, not as direct-200 pages:

```python
import pytest

GET_ROUTES = [
    "/",
    "/dashboard",
    "/connections",
    "/objects",
    "/maintenance",
    "/vacuum",
    "/gpcopy",
]

REDIRECT_ROUTES = [
    "/skew",
    "/reorganize",
]


@pytest.mark.parametrize("path", GET_ROUTES)
def test_get_route_returns_200(client, path):
    resp = client.get(path)
    assert resp.status_code == 200


@pytest.mark.parametrize("path", REDIRECT_ROUTES)
def test_legacy_route_redirects_to_maintenance(client, path):
    resp = client.get(path)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/maintenance")


def test_bootstrap_loaded_exactly_once(client):
    html = client.get("/").get_data(as_text=True)
    assert html.count("bootstrap.bundle.min.js") == 1
```

- [ ] **Step 5: Run tests — expect the bootstrap assertion to FAIL**

Run: `pytest tests/test_smoke.py -v`
Expected: route + redirect tests PASS; `test_bootstrap_loaded_exactly_once` FAILs with `assert 2 == 1` (base.html currently has two bundle scripts).

- [ ] **Step 6: Commit the harness**

```bash
git add tests/__init__.py tests/conftest.py tests/test_smoke.py requirements.txt
git commit -m "test: add Flask smoke-test harness for routes and asset invariants"
```

---

### Task 2: Fix base.html — single Bootstrap, page-scoped Chart.js

**Files:**
- Modify: `templates/base.html:92-99`

**Interfaces:**
- Consumes: the smoke test from Task 1.
- Produces: a `base.html` whose `<head>`/end-of-body loads Bootstrap once and no longer loads Chart.js globally; pages opt into Chart.js via `{% block scripts %}`.

- [ ] **Step 1: Remove the duplicate Bootstrap script and global Chart.js**

In `templates/base.html`, replace the end-of-body block (currently lines 92–99):

```html
<script
    src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js">
</script>

{% block scripts %}{% endblock %}
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</body>
```

with:

```html
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>

{% block scripts %}{% endblock %}
</body>
```

- [ ] **Step 2: Run the smoke test — expect PASS**

Run: `pytest tests/test_smoke.py -v`
Expected: all routes 200 AND `test_bootstrap_loaded_exactly_once` PASS.

- [ ] **Step 3: Commit**

```bash
git add templates/base.html
git commit -m "fix: load Bootstrap once and stop loading Chart.js globally"
```

---

### Task 3: Restore Chart.js on the pages that chart

**Files:**
- Modify: `templates/dashboard.html` (add a `{% block scripts %}` before its inline chart script)
- Modify: `templates/maintenance.html` (add Chart.js to its existing scripts block — it loads `skew.js`, which calls `new Chart` 9×)
- Modify: `tests/test_smoke.py` (add a regression assertion)

**Interfaces:**
- Consumes: the `{% block scripts %}` slot from Task 2.
- Produces: Chart.js present on `/dashboard` and `/maintenance`, absent on other pages.

- [ ] **Step 1: Write the failing assertion**

Append to `tests/test_smoke.py`:

```python
def test_chartjs_only_on_charting_pages(client):
    dash = client.get("/dashboard").get_data(as_text=True)
    maint = client.get("/maintenance").get_data(as_text=True)
    conns = client.get("/connections").get_data(as_text=True)
    assert "chart.js" in dash
    assert "chart.js" in maint
    assert "chart.js" not in conns
```

- [ ] **Step 2: Run it — expect FAIL**

Run: `pytest tests/test_smoke.py::test_chartjs_only_on_charting_pages -v`
Expected: FAIL (`"chart.js" in dash` is False — dashboard no longer pulls the global Chart.js).

- [ ] **Step 3: Add Chart.js to dashboard.html**

In `templates/dashboard.html`, before the existing inline `<script>` at line 232, add a scripts block that loads Chart.js:

```html
{% block scripts %}
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
{% endblock %}
```

(The existing inline chart `<script>` stays where it is; it runs on `DOMContentLoaded` after the block-scripts Chart.js is parsed.)

- [ ] **Step 4: Add Chart.js to maintenance.html**

`templates/maintenance.html` already defines a `{% block scripts %}` that loads `skew.js` + `reorganize.js` (lines 355–356). Add the Chart.js `<script>` line at the **top** of that existing block, before `skew.js`:

```html
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
```

- [ ] **Step 5: Run the smoke suite — expect PASS**

Run: `pytest -v`
Expected: all tests PASS, including `test_chartjs_only_on_charting_pages`.

- [ ] **Step 6: Commit**

```bash
git add templates/dashboard.html templates/maintenance.html tests/test_smoke.py
git commit -m "fix: scope Chart.js to Dashboard and Maintenance pages only"
```

---

### Task 4: Delete orphaned templates and dead JS/py files

**Files:**
- Delete: `templates/index.html` (no route renders it; `/` uses `dashboard.html`)
- Delete: `templates/skew.html` (orphaned; `/skew` redirects to `/maintenance`, which loads `skew.js` directly)
- Delete: `templates/reorganize.html` (orphaned; `/reorganize` redirects to `/maintenance`, which loads `reorganize.js` directly)
- Delete: `static/js/skew.js.old`
- Delete: `static/js/app.js`
- Delete: `models.py`

**Interfaces:**
- Consumes: nothing.
- Produces: a repo with no dead frontend entrypoints. KEEP `static/js/skew.js` and `static/js/reorganize.js` — `maintenance.html` loads both.

- [ ] **Step 1: Confirm nothing references the files**

Run: `grep -rn "index.html\|skew.html\|reorganize.html\|skew.js.old\|js/app.js\|import models\|from models" app.py templates modules job_manager.py db.py`
Expected: no matches referencing these templates as `render_template`/`include` targets (a `<script src=".../js/skew.js">` hit in `maintenance.html` is fine — that is the `.js`, not the `.html`).

- [ ] **Step 2: Delete the files**

```bash
git rm templates/index.html templates/skew.html templates/reorganize.html static/js/skew.js.old static/js/app.js models.py
```

- [ ] **Step 3: Run the smoke suite — expect PASS**

Run: `pytest -v`
Expected: all GET routes still 200 and both redirects still resolve to `/maintenance` (which does not depend on the deleted templates).

- [ ] **Step 4: Commit**

```bash
git commit -m "chore: remove orphaned index/skew/reorganize templates and dead app.js/skew.js.old/models.py"
```

---

### Task 5: Remove the dead duplicate `loadSessionLimitsStats()` in dashboard.html

**Files:**
- Modify: `templates/dashboard.html` (delete the first, broken implementation ~lines 340–435)

**Interfaces:**
- Consumes: nothing.
- Produces: one `loadSessionLimitsStats()` — the version at ~lines 497–524 that targets the real IDs (`sessionLimitsBody`, `sessionNodes`, `sessionTotal`, `sessionActive`, `sessionIdle`, `sessionIdleTxn`) via `renderSessionLimits()` and `setSessionMessage()`.

- [ ] **Step 1: Add a regression assertion for single definition**

Append to `tests/test_smoke.py`:

```python
def test_single_session_limits_function(client):
    html = client.get("/dashboard").get_data(as_text=True)
    assert html.count("async function loadSessionLimitsStats(") == 1
```

- [ ] **Step 2: Run it — expect FAIL**

Run: `pytest tests/test_smoke.py::test_single_session_limits_function -v`
Expected: FAIL (`assert 2 == 1` — two definitions exist).

- [ ] **Step 3: Delete the first (broken) implementation**

In `templates/dashboard.html`, delete the first `sessionLimitBadge()` + first `async function loadSessionLimitsStats()` block (the one that references the non-existent IDs `slNodes`, `slTotal`, `slActive`, `slIdle`, `slIdleTxn`, and `sessionLimitsTableBody`). Keep `renderSessionLimits()`, `setSessionMessage()`, the second `loadSessionLimitsStats()`, and the `DOMContentLoaded` handler.

- [ ] **Step 4: Run the smoke suite — expect PASS**

Run: `pytest -v`
Expected: all PASS, including `test_single_session_limits_function`.

- [ ] **Step 5: Commit**

```bash
git add templates/dashboard.html tests/test_smoke.py
git commit -m "fix: remove dead duplicate loadSessionLimitsStats with broken element IDs"
```

---

### Task 6: Pin openpyxl in requirements

**Files:**
- Modify: `requirements.txt`

**Interfaces:**
- Consumes: nothing.
- Produces: importable `app` in a clean environment (`app.py:21` imports `openpyxl`).

- [ ] **Step 1: Write the failing dependency test**

Append to `tests/test_smoke.py`:

```python
def test_openpyxl_is_declared():
    reqs = open("requirements.txt", encoding="utf-8").read().lower()
    assert "openpyxl" in reqs
```

- [ ] **Step 2: Run it — expect FAIL**

Run: `pytest tests/test_smoke.py::test_openpyxl_is_declared -v`
Expected: FAIL (openpyxl not declared).

- [ ] **Step 3: Add the pin**

Append to `requirements.txt`:

```
openpyxl==3.1.2
```

- [ ] **Step 4: Run it — expect PASS**

Run: `pytest tests/test_smoke.py::test_openpyxl_is_declared -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt tests/test_smoke.py
git commit -m "fix: declare openpyxl dependency used by skew xlsx export"
```

---

### Task 7: De-duplicate style.css selectors

**Files:**
- Modify: `static/css/style.css` (collapse the 3× `.gpcopy-layout`, 3× `.gpcopy-run-grid`, 2× `.gpcopy-target-grid`, 3× `.gpcopy-options-grid`, 2× `.job-items-scroll`, plus the repeated `@media (max-width:1200px)` blocks into one definition each)

**Interfaces:**
- Consumes: nothing.
- Produces: a stylesheet where each selector appears once, keeping the last-wins values that are currently in effect so rendering is unchanged.

- [ ] **Step 1: Record current computed layout values**

Note the currently-effective values (last definition wins in CSS):
- `.gpcopy-layout` → `grid-template-columns: 420px 1fr; gap: 16px; align-items: start;`
- `.gpcopy-run-grid` → `grid-template-columns: 1fr 120px 180px; gap: 12px;`
- `.gpcopy-target-grid` → `grid-template-columns: 1fr 260px; gap: 12px;`
- `.gpcopy-options-grid` → `grid-template-columns: repeat(4, minmax(130px,1fr)); gap: 10px 18px;`
- `.job-items-scroll` → `max-height: 360px; overflow-y: auto;`

- [ ] **Step 2: Collapse to single definitions**

Edit `static/css/style.css` so each of the above selectors is declared exactly once with the Step-1 values, plus the sticky-left column rules kept once:

```css
.gpcopy-layout {
    display: grid;
    grid-template-columns: 420px 1fr;
    gap: 16px;
    align-items: start;
}

.gpcopy-left,
.gpcopy-right {
    min-width: 0;
    border-radius: 12px;
}

.gpcopy-left {
    position: sticky;
    top: 80px;
}

.gpcopy-run-grid {
    display: grid;
    grid-template-columns: 1fr 120px 180px;
    gap: 12px;
    align-items: end;
}

.gpcopy-target-grid {
    display: grid;
    grid-template-columns: 1fr 260px;
    gap: 12px;
    margin-top: 10px;
}

.gpcopy-options-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(130px, 1fr));
    gap: 10px 18px;
    margin-top: 12px;
}

.job-items-scroll {
    max-height: 360px;
    overflow-y: auto;
}
```

Keep a single `@media (max-width: 1200px)` block that collapses `.gpcopy-layout`, `.gpcopy-run-grid`, `.gpcopy-target-grid` to `1fr`, sets `.gpcopy-options-grid` to `repeat(2, minmax(130px, 1fr))`, and sets `.gpcopy-left` to `position: static;`. Delete the other duplicated definitions and duplicated media blocks.

- [ ] **Step 3: Verify no selector is defined twice**

Run: `grep -c "^\.gpcopy-layout {" static/css/style.css`
Expected: `1`. Repeat for `.gpcopy-run-grid`, `.gpcopy-target-grid`, `.gpcopy-options-grid`, `.job-items-scroll` — each `1`.

- [ ] **Step 4: Manual visual check**

Run: `python app.py`, open `/gpcopy`, confirm the two-column layout (420px sidebar + content), options grid, and run grid look unchanged at desktop width and collapse to one column below 1200px.

- [ ] **Step 5: Commit**

```bash
git add static/css/style.css
git commit -m "refactor: collapse duplicated gpcopy grid selectors in style.css"
```

---

## Self-Review

- **Spec coverage:** every Phase 0 exit criterion in the roadmap maps to a task — Bootstrap-once (T2), Chart.js scoping (T3), orphan deletion (T4), single session-limits fn (T5), openpyxl (T6), CSS dedup (T7), smoke test/pytest-green (T1 + assertions across T3/T5/T6). ✓
- **Placeholder scan:** no TBD/TODO; all edits show exact before/after and exact commands. ✓
- **Type consistency:** the `client` fixture and `GET_ROUTES` names are stable across all test additions; assertion helper strings (`loadSessionLimitsStats(`, `chart.js`, `bootstrap.bundle.min.js`) match the real markup. ✓

## Verification (whole phase)

- [ ] Fresh venv: `pip install -r requirements.txt` then `pytest -v` → all green.
- [ ] `python app.py` boots; `/`, `/dashboard`, `/gpcopy`, `/skew` render; dashboard charts + session-limits work.
- [ ] `grep -rn "bootstrap.bundle.min.js" templates/base.html` → one line.
- [ ] `git log --oneline` shows one commit per task.
