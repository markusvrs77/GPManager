from db import init_db, sqlite_cursor


def _columns(table_name):
    with sqlite_cursor() as cur:
        cur.execute("PRAGMA table_info({})".format(table_name))
        return {row["name"] for row in cur.fetchall()}


def test_schedules_table():
    init_db()
    assert {
        "id", "name", "enabled", "job_type", "config_json", "cron_expr",
        "timezone", "overlap_policy", "max_retries", "retry_delay_seconds",
        "notify_on", "notify_channel_ids", "next_run_at", "last_run_at",
        "last_status", "last_job_id", "last_error", "created_at", "updated_at",
    } <= _columns("schedules")


def test_schedule_runs_table():
    init_db()
    assert {
        "id", "schedule_id", "fired_at", "run_date", "job_id",
        "status", "attempt_no", "error",
    } <= _columns("schedule_runs")


def test_scheduler_lock_table_seeded():
    init_db()
    assert {"id", "holder", "heartbeat_at", "expires_at"} <= _columns("scheduler_lock")

    with sqlite_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM scheduler_lock WHERE id = 1")
        assert cur.fetchone()["n"] == 1


def test_notification_channels_table():
    init_db()
    assert {"id", "name", "type", "config_json", "enabled"} <= _columns(
        "notification_channels"
    )
