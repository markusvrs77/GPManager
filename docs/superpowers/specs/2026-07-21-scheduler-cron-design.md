# Scheduler (Cron) Subsystem — Design Spec

**Date:** 2026-07-21
**Status:** Approved (brainstorming complete)
**Author:** GPManager team (markusvrs77 + Claude)

## 1. Purpose

GPManager jobs (gpcopy, vacuum/analyze, reorganize, skew) are currently started
manually from the web UI and run as in-process background threads, with state in
SQLite (`jobs` / `job_items`). There is **no scheduling**. This spec adds a
**cron/scheduler subsystem** that fires any job type on a schedule, plus the one
gpcopy capability that scheduling makes essential: **relative date windows**.

### Success criteria
- A user can create a schedule for any `job_type` from the UI, using presets or a
  raw cron expression, and see the next 5 fire times live.
- A scheduled gpcopy-by-date resolves its date window relative to the fire time
  (e.g. "yesterday", `run_date-7d .. run_date-1d`) at run time, not at save time.
- Schedules fire reliably under multiple gunicorn workers (exactly one fires).
- Failures are recorded, visible in the UI, and pushed to configured notification
  channels. Overlapping runs are handled per a configurable policy.

## 2. Scope

**In v1:**
- Scheduling for **any** `job_type` (gpcopy normal, gpcopy date-filter,
  vacuum/analyze, reorganize, skew).
- Relative date windows for gpcopy-by-date (presets + shift grammar).
- Multi-channel notifications: webhook, Microsoft Teams, Telegram, email.
- Leader election for multi-worker deployment.

**Out of scope (separate follow-up spec — "gpcopy capabilities v2"):**
- gpcopy row-count validation gate, incremental/watermark copy, saved gpcopy
  presets library, bandwidth/`--jobs` auto-tuning.

**Best-effort / phase 2:**
- WhatsApp channel — the channel architecture supports it as one more adapter,
  but it requires a WhatsApp Business account and Meta-approved message templates
  (or paid Twilio). Include in v1 **only if** onboarding is painless; otherwise
  ship the adapter interface and defer the working integration.

## 3. Data model (new SQLite tables)

### `schedules`
| column | type | notes |
|---|---|---|
| id | INTEGER PK | |
| name | TEXT | |
| enabled | INTEGER | 0/1 |
| job_type | TEXT | gpcopy / gpcopy_date / vacuum / reorganize / skew |
| config_json | TEXT | full job config: connections, tables, gpcopy options, and `date_window` spec (see §4) |
| cron_expr | TEXT | 5-field cron; presets compile to this |
| timezone | TEXT | IANA tz, default server local |
| overlap_policy | TEXT | `skip` (default) / `queue` / `parallel` |
| max_retries | INTEGER | default 0 |
| retry_delay_seconds | INTEGER | default 0 |
| notify_on | TEXT | `never` / `failure` / `always` |
| notify_channel_ids | TEXT | JSON array of `notification_channels.id` |
| next_run_at | TEXT | computed, ISO datetime |
| last_run_at | TEXT | |
| last_status | TEXT | done / failed / skipped / running |
| last_job_id | INTEGER | FK jobs.id |
| last_error | TEXT | |
| created_at / updated_at | TEXT | |

Config is stored inline on the schedule (no separate "job template" table — YAGNI
for v1).

### `schedule_runs` (history + overlap/retry ledger)
| column | type | notes |
|---|---|---|
| id | INTEGER PK | |
| schedule_id | INTEGER | FK |
| fired_at | TEXT | |
| run_date | TEXT | logical fire timestamp used to resolve relative date windows |
| job_id | INTEGER | FK jobs.id (nullable if skipped before job creation) |
| status | TEXT | running / done / failed / skipped |
| attempt_no | INTEGER | 0 = first try |
| error | TEXT | |

Overlap detection = "does this schedule have a `schedule_runs` row whose job is
still active?" Retry tracking = new `schedule_runs` row with incremented
`attempt_no`.

### `scheduler_lock` (leader election, single row id=1)
| column | type | notes |
|---|---|---|
| id | INTEGER PK | always 1 |
| holder | TEXT | worker uuid/pid |
| heartbeat_at | TEXT | |
| expires_at | TEXT | now + 90s on each renew |

### `notification_channels`
| column | type | notes |
|---|---|---|
| id | INTEGER PK | |
| name | TEXT | |
| type | TEXT | `webhook` / `teams` / `telegram` / `email` / `whatsapp` |
| config_json | TEXT | type-specific (URL, SMTP host/port/from/to/TLS, bot token + chat_id, etc.); optional `proxy` field per channel |
| enabled | INTEGER | 0/1 |

## 4. Relative date windows (gpcopy capability)

Stored inside `schedules.config_json` for `gpcopy_date` schedules:

```json
"date_window": {
  "column": "date_change$",
  "from": {"preset": "n_days_ago", "n": 7},
  "to":   {"preset": "yesterday"}
}
```

Each of `from` / `to` is either:
- a **preset**: `today`, `yesterday`, `n_days_ago` (with `n`),
  `this_month_to_date`, `last_month`, `n_hours_ago` (with `n`); or
- a **shift expression**: `run_date±Nd` / `run_date±Nh` (e.g. `run_date-7d`).

**Resolver:** pure function
`resolve_date_window(spec, run_date) -> (date_from, date_to)`, evaluated in the
schedule's timezone at fire time. Its concrete output feeds the **existing**
`build_gpcopy_date_include_json` path — no change to gpcopy command construction.
This function is the primary unit-test target (month boundaries, DST, inclusive
vs exclusive bounds — bounds are `>= from AND < to`, matching current code).

## 5. Scheduler engine (`scheduler.py`)

A daemon thread started at app initialization (in `create_app` / `__main__`).
Loop every 60s (interval configurable):

1. **Leader lock:** atomic
   `UPDATE scheduler_lock SET holder=me, heartbeat_at=now, expires_at=now+90s
   WHERE id=1 AND (expires_at < now OR holder=me)`.
   If 0 rows updated → not leader → sleep, retry. Only the leader proceeds.
2. **Select due:** `enabled` schedules where `next_run_at <= now`.
3. **Per schedule** (wrapped in its own try/except so one bad schedule can't kill
   the loop):
   - Overlap check → apply `overlap_policy`
     (`skip` = record skipped run; `queue` = defer, retry next loop until clear;
     `parallel` = proceed).
   - Resolve config: relative dates → concrete via `resolve_date_window`.
   - `create_job(...)` → insert `schedule_runs` row → dispatch runner.
   - Recompute `next_run_at` via `croniter`.
4. **Retries:** a `failed` `schedule_runs` row with `attempt_no < max_retries`
   is re-enqueued after `retry_delay_seconds`.
5. **Notifications:** fire per `notify_on` to each channel in `notify_channel_ids`.

**Job-type dispatch registry:** introduce `JOB_RUNNERS = {job_type: target_func}`
so both the existing API routes and the scheduler dispatch through one place.
Today dispatch is spread across `app.py`; centralizing it is a targeted
improvement to code we're already touching (do not refactor unrelated code).

**Misfire grace:** after downtime or a leader handover, a `next_run_at` in the
past but within grace (default 1h) fires once (catch-up); older than grace →
skip and just advance `next_run_at`, to avoid a stampede.

## 6. Notifications (`notifiers.py`)

Single interface `send(channel, event) -> (ok, error)`, one adapter per `type`.
The `event` payload: schedule name, job_type, status, fired_at, duration,
error (if any), job_id (link).

| Channel | Mechanism | Notes |
|---|---|---|
| webhook | generic HTTP POST (JSON) | base primitive |
| teams | Incoming Webhook URL + MessageCard payload | a webhook variant |
| telegram | Bot API `sendMessage` (bot token + chat_id) | |
| email | SMTP (host/port/from/to, TLS) | needs SMTP relay |
| whatsapp | Meta Cloud API (phone_number_id + token + template) | best-effort / phase 2 |

All adapters follow the same "HTTP POST per config" shape (email excepted), so
adding a channel = one small adapter. Each channel config may carry an optional
`proxy`. Production has outbound internet, so Telegram/Teams/WhatsApp are
reachable directly; the proxy field is forward-looking.

## 7. Dependencies

One new runtime dependency: **`croniter`** (pure-python, installs offline —
consistent with the project's deliberate CDN-free/offline posture). Used for
`next_run_at` computation and `cron_expr` validation (`croniter.is_valid` on
save; invalid expressions are rejected with an error).

## 8. API & routes (Flask)

- `GET /schedules` — page.
- `GET /api/schedules` (list), `POST /api/schedules` (create),
  `PUT /api/schedules/<id>` (update), `DELETE /api/schedules/<id>`.
- `POST /api/schedules/<id>/run-now` — manual fire (bypasses cron timing, still
  respects overlap policy).
- `POST /api/schedules/<id>/toggle` — enable/disable.
- `GET /api/schedules/<id>/runs` — run history.
- `POST /api/schedules/preview` — given `cron_expr` return next 5 fire times;
  given `date_window` + a sample `run_date` return resolved `from`/`to`
  (live UI feedback).
- Notification channels CRUD: `GET/POST /api/notification-channels`,
  `PUT/DELETE /api/notification-channels/<id>`, plus
  `POST /api/notification-channels/<id>/test` (send a test event).

## 9. UI (Schedules page)

- **List:** name, job_type, human-readable cron, next run, last run status,
  enabled toggle, actions (edit / run-now / history / delete).
- **Create/edit form:** pick `job_type` → reuse the existing per-type job-config
  UI (table picker, gpcopy options); add a schedule section (preset buttons + raw
  cron field with live "next 5 runs"); a `date_window` builder for gpcopy-date
  (presets + shift, with live resolved-dates preview); overlap policy; retries;
  notification channel selection.
- **Notification channels:** a small management screen (list + add/edit/test).
- Follows the existing glass theme / motion language. When reusing `gpcopy.js`,
  audit its remaining `innerHTML` sinks (see the XSS memory — `escapeHtml`-wrap
  or numeric-only every interpolation).

## 10. Error handling

- Per-schedule try/except inside the loop; a failing schedule is recorded
  (`last_status=failed`, `schedule_runs.error`) and does not stop other schedules.
- Leader-lock contention/failure → log and retry next loop.
- Invalid `cron_expr` rejected at save time.
- On startup, the existing `mark_interrupted_jobs_on_startup` reconciles hung
  jobs; the scheduler additionally reconciles `schedule_runs` whose job was
  interrupted (mark run `failed`/`interrupted`).

## 11. Testing

- **Unit:** `resolve_date_window` (presets + shift grammar, month/DST boundaries);
  `next_run_at` via croniter; overlap-policy decision function; leader-lock
  acquire/renew/expire with an injectable clock; each notifier adapter
  (payload shape, error propagation) with mocked HTTP/SMTP.
- **Integration:** schedule fires → job created (mock runner) → `schedule_runs`
  written; retry path; skip-on-overlap; run-now.
- **Smoke:** extend `tests/test_smoke.py` — `/schedules` page loads, schedule +
  channel CRUD roundtrip.

## 12. Implementation toolchain (per project convention)

- **Coding / implementation:** ECC skills (`ecc:python-patterns`,
  Flask patterns, `ecc:python-testing`, `ecc:security-review`).
- **UI/UX for the Schedules page:** `ui-ux-pro-max` + `motion-framer`.
- **Planning:** this spec (superpowers) → superpowers `writing-plans` next.

## 13. Open items carried to the plan

- Confirm WhatsApp go/no-go for v1 based on Business-account availability.
- Decide `schedule_runs` retention (prune policy) — likely a follow-up.
- Branding note (out of scope but tracked): navbar/dashboard still say
  "Greenplum Reorganize Center" in places, inconsistent with "GPManager".
