"""
Хранилище планировщика: schedules / schedule_runs / scheduler_lock /
notification_channels (spec §3, §5.1).
Все datetime — строки TS_FMT в локальном времени, сравнимые лексикографически.
"""

import json
from datetime import datetime, timedelta

from croniter import croniter

from db import sqlite_cursor


TS_FMT = "%Y-%m-%d %H:%M:%S"

LOCK_TTL_SECONDS = 90


def fmt(dt):
    return dt.strftime(TS_FMT)


def parse_ts(value):
    return datetime.strptime(str(value), TS_FMT)


# ------------------------------------------------------------
# Cron
# ------------------------------------------------------------

def validate_cron(cron_expr):
    try:
        return bool(croniter.is_valid(str(cron_expr)))
    except Exception:
        return False


def compute_next_run(cron_expr, base_dt):
    return fmt(croniter(str(cron_expr), base_dt).get_next(datetime))


# ------------------------------------------------------------
# Schedules CRUD
# ------------------------------------------------------------

_SCHEDULE_COLUMNS = (
    "name", "enabled", "job_type", "config_json", "cron_expr", "timezone",
    "overlap_policy", "max_retries", "retry_delay_seconds",
    "notify_on", "notify_channel_ids", "next_run_at",
)


def create_schedule(data, now=None):
    now = now or datetime.now()

    if not validate_cron(data.get("cron_expr")):
        raise ValueError("Invalid cron expression: {!r}".format(data.get("cron_expr")))

    values = {
        "name": data.get("name") or "schedule",
        "enabled": int(data.get("enabled", 1)),
        "job_type": data["job_type"],
        "config_json": data.get("config_json"),
        "cron_expr": data["cron_expr"],
        "timezone": data.get("timezone"),
        "overlap_policy": data.get("overlap_policy") or "skip",
        "max_retries": int(data.get("max_retries") or 0),
        "retry_delay_seconds": int(data.get("retry_delay_seconds") or 0),
        "notify_on": data.get("notify_on") or "failure",
        "notify_channel_ids": data.get("notify_channel_ids"),
        "next_run_at": compute_next_run(data["cron_expr"], now),
    }

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO schedules ({cols}, created_at)
            VALUES ({marks}, ?)
            """.format(
                cols=", ".join(_SCHEDULE_COLUMNS),
                marks=", ".join("?" for _ in _SCHEDULE_COLUMNS),
            ),
            tuple(values[c] for c in _SCHEDULE_COLUMNS) + (fmt(now),),
        )
        return cur.lastrowid


def update_schedule(schedule_id, data, now=None):
    now = now or datetime.now()

    if "cron_expr" in data and not validate_cron(data.get("cron_expr")):
        raise ValueError("Invalid cron expression: {!r}".format(data.get("cron_expr")))

    fields = [c for c in _SCHEDULE_COLUMNS if c in data]

    if not fields:
        return

    sets = ", ".join("{} = ?".format(c) for c in fields)
    params = [data[c] for c in fields]

    if "cron_expr" in data and "next_run_at" not in data:
        sets += ", next_run_at = ?"
        params.append(compute_next_run(data["cron_expr"], now))

    sets += ", updated_at = ?"
    params.append(fmt(now))
    params.append(schedule_id)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE schedules SET {} WHERE id = ?".format(sets),
            params,
        )


def get_schedule(schedule_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM schedules WHERE id = ?", (schedule_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def list_schedules():
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM schedules ORDER BY id")
        return [dict(row) for row in cur.fetchall()]


def delete_schedule(schedule_id):
    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM schedule_runs WHERE schedule_id = ?", (schedule_id,))
        cur.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))


def set_enabled(schedule_id, enabled):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE schedules SET enabled = ? WHERE id = ?",
            (int(bool(enabled)), schedule_id),
        )


def set_next_run(schedule_id, next_run_at):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE schedules SET next_run_at = ? WHERE id = ?",
            (next_run_at, schedule_id),
        )


def list_due_schedules(now_str):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT * FROM schedules
            WHERE enabled = 1
              AND next_run_at IS NOT NULL
              AND next_run_at <= ?
            ORDER BY id
            """,
            (now_str,),
        )
        return [dict(row) for row in cur.fetchall()]


def update_schedule_last(schedule_id, status, job_id=None, error=None, run_at=None):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE schedules
            SET last_status = ?,
                last_job_id = COALESCE(?, last_job_id),
                last_error = ?,
                last_run_at = COALESCE(?, last_run_at)
            WHERE id = ?
            """,
            (status, job_id, error, run_at, schedule_id),
        )


# ------------------------------------------------------------
# Schedule runs
# ------------------------------------------------------------

def record_run(schedule_id, fired_at, run_date, status, job_id=None,
               attempt_no=0, error=None):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO schedule_runs
                (schedule_id, fired_at, run_date, job_id, status, attempt_no, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (schedule_id, fired_at, run_date, job_id, status, attempt_no, error),
        )
        return cur.lastrowid


def update_run(run_id, status=None, error=None, job_id=None):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE schedule_runs
            SET status = COALESCE(?, status),
                error = COALESCE(?, error),
                job_id = COALESCE(?, job_id)
            WHERE id = ?
            """,
            (status, error, job_id, run_id),
        )


def get_run(run_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM schedule_runs WHERE id = ?", (run_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def get_active_run(schedule_id):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT * FROM schedule_runs
            WHERE schedule_id = ? AND status = 'running'
            ORDER BY id DESC LIMIT 1
            """,
            (schedule_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def list_runs(schedule_id, limit=50):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT * FROM schedule_runs
            WHERE schedule_id = ?
            ORDER BY id DESC LIMIT ?
            """,
            (schedule_id, int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def list_due_retry_runs(now_str):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT * FROM schedule_runs
            WHERE status = 'queued' AND fired_at <= ?
            ORDER BY id
            """,
            (now_str,),
        )
        return [dict(row) for row in cur.fetchall()]


# ------------------------------------------------------------
# Leader lock (spec §5.1) — атомарный захват/продление
# ------------------------------------------------------------

def acquire_leader_lock(holder, now, ttl_seconds=LOCK_TTL_SECONDS):
    now_str = fmt(now)
    expires_str = fmt(now + timedelta(seconds=ttl_seconds))

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE scheduler_lock
            SET holder = ?, heartbeat_at = ?, expires_at = ?
            WHERE id = 1
              AND (holder IS NULL OR expires_at IS NULL OR expires_at < ? OR holder = ?)
            """,
            (holder, now_str, expires_str, now_str, holder),
        )
        return cur.rowcount == 1


# ------------------------------------------------------------
# Notification channels CRUD
# ------------------------------------------------------------

def create_channel(data):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO notification_channels (name, type, config_json, enabled)
            VALUES (?, ?, ?, ?)
            """,
            (
                data.get("name") or "channel",
                data["type"],
                data.get("config_json"),
                int(data.get("enabled", 1)),
            ),
        )
        return cur.lastrowid


def get_channel(channel_id):
    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT * FROM notification_channels WHERE id = ?", (channel_id,)
        )
        row = cur.fetchone()
    return dict(row) if row else None


def list_channels():
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM notification_channels ORDER BY id")
        return [dict(row) for row in cur.fetchall()]


def update_channel(channel_id, data):
    fields = [c for c in ("name", "type", "config_json", "enabled") if c in data]

    if not fields:
        return

    sets = ", ".join("{} = ?".format(c) for c in fields)
    params = [data[c] for c in fields] + [channel_id]

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE notification_channels SET {} WHERE id = ?".format(sets),
            params,
        )


def delete_channel(channel_id):
    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM notification_channels WHERE id = ?", (channel_id,))
