# Scheduler (Cron) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the scheduler subsystem per `docs/superpowers/specs/2026-07-21-scheduler-cron-design.md` (spec is the source of truth for all field lists and semantics).

**Architecture:** Polling daemon thread (60s) + SQLite leader-lock; schedules materialize ordinary jobs via existing `create_job`/runners; pure-function date-window resolver feeds existing gpcopy-date path; pluggable notifier adapters.

**Tech Stack:** Flask, SQLite, threading, `croniter` (new dep), pytest.

## Global Constraints
- Date bounds semantics: `>= from AND < to` (spec §4).
- Day-granularity `to` endpoints resolve exclusive-next-day (so "to: yesterday" fully includes yesterday); hour-granularity endpoints are exact timestamps.
- Overlap default `skip`; retries default 0; misfire grace default 3600s (spec §5).
- Toolchain: coding = ECC skills; UI phase = ui-ux-pro-max + motion-framer.
- Each phase ends with a visible result rendered in the browser (project convention).

---

### Phase 1: Foundation
- [ ] Task 1.1 Add `croniter` to `requirements.txt`; pip install; commit.
- [ ] Task 1.2 `db.py::init_db`: add tables `schedules`, `schedule_runs`, `scheduler_lock`, `notification_channels` (+ index `idx_schedule_runs_schedule_id`), columns per spec §3. Test: `tests/test_scheduler_db.py` asserts tables/columns exist after `init_db()` on a temp DB.
- [ ] Task 1.3 `modules/date_window.py`: `resolve_date_window(spec, run_date) -> (str, str)`; presets `today|yesterday|n_days_ago|this_month_to_date|last_month|n_hours_ago` + shift grammar `run_date±Nd|Nh`; raises `ValueError` on unknown preset/bad expr. TDD in `tests/test_date_window.py` (month boundary, to-inclusivity, hour granularity, errors).

### Phase 2: Engine
- [ ] Task 2.1 `scheduler_store.py`: CRUD for schedules/runs/channels + `acquire_leader_lock(holder, now) -> bool` (atomic UPDATE per spec §5.1) + `next_run_at` compute via croniter. Tests with injectable clock.
- [ ] Task 2.2 `scheduler.py`: loop (due selection, overlap policy skip/queue/parallel, misfire grace, retries), `JOB_RUNNERS` registry {gpcopy, gpcopy_date, vacuum, reorganize, skew} wired to existing runner funcs; start hook in `app.py.__main__`. Integration tests with mock runner.

### Phase 3: Notifications
- [ ] Task 3.1 `notifiers.py`: `send(channel, event)`; adapters webhook/teams/telegram/email (whatsapp adapter stub per spec §2); mocked HTTP/SMTP tests.

### Phase 4: API
- [ ] Task 4.1 Routes per spec §8 (schedules CRUD/toggle/run-now/runs/preview; channels CRUD/test) in `app.py`; validation: `croniter.is_valid` on save. Smoke tests extended.

### Phase 5: UI
- [ ] Task 5.1 `templates/schedules.html` + `static/js/schedules.js` per spec §9 (list, form with cron presets + live next-5, date-window builder preview, channels screen). escapeHtml on all sinks. Browser demo via launch.json.

Each task: failing test → minimal impl → green → commit (Co-Authored-By Claude).
