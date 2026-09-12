import json
from datetime import datetime, timedelta

import pytest

import scheduler
import scheduler_store as store
from db import init_db, sqlite_cursor
from job_manager import mark_job_done


NOW = datetime(2026, 7, 21, 2, 0, 0)
TS = "%Y-%m-%d %H:%M:%S"


@pytest.fixture(autouse=True)
def _clean():
    init_db()
    scheduler.RUN_SYNC = True
    yield
    scheduler.RUN_SYNC = False
    scheduler.JOB_RUNNERS.pop("mocktype", None)
    scheduler.JOB_RUNNERS.pop("gpcopy_date_mock", None)

    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM schedule_runs")
        cur.execute("DELETE FROM schedules")
        cur.execute("DELETE FROM notification_channels")
        cur.execute(
            "UPDATE scheduler_lock SET holder=NULL, heartbeat_at=NULL, expires_at=NULL WHERE id=1"
        )


def _mk_schedule(**over):
    data = {
        "name": "test-sched",
        "job_type": "mocktype",
        "config_json": json.dumps({"connection_id": 1, "tables": []}),
        "cron_expr": "*/1 * * * *",
        "overlap_policy": "skip",
    }
    data.update(over)
    return store.create_schedule(data, now=NOW)


def _due(sid):
    store.set_next_run(sid, (NOW - timedelta(minutes=1)).strftime(TS))


def test_validate_cron():
    assert store.validate_cron("0 2 * * *")
    assert not store.validate_cron("not a cron")


def test_compute_next_run():
    assert (
        store.compute_next_run("0 2 * * *", datetime(2026, 7, 21, 2, 0, 0))
        == "2026-07-22 02:00:00"
    )


def test_leader_lock_lifecycle():
    assert store.acquire_leader_lock("w1", NOW)
    # чужой валидный лок не отдаём
    assert not store.acquire_leader_lock("w2", NOW + timedelta(seconds=10))
    # свой лок продлевается
    assert store.acquire_leader_lock("w1", NOW + timedelta(seconds=20))
    # истёкший лок захватывается другим воркером
    assert store.acquire_leader_lock("w2", NOW + timedelta(seconds=200))


def test_tick_fires_due_schedule():
    fired = []

    def runner(job_id):
        fired.append(job_id)
        mark_job_done(job_id)

    scheduler.JOB_RUNNERS["mocktype"] = runner
    sid = _mk_schedule()
    _due(sid)

    assert scheduler.tick(now=NOW) == "leader"

    assert len(fired) == 1
    assert store.list_runs(sid)[0]["status"] == "done"

    sched = store.get_schedule(sid)
    assert sched["last_status"] == "done"
    assert sched["next_run_at"] > NOW.strftime(TS)


def test_overlap_skip_records_skipped_run():
    scheduler.JOB_RUNNERS["mocktype"] = lambda job_id: None
    sid = _mk_schedule()
    _due(sid)
    store.record_run(
        sid,
        fired_at=NOW.strftime(TS),
        run_date=NOW.strftime(TS),
        status="running",
    )

    scheduler.tick(now=NOW)

    assert "skipped" in [r["status"] for r in store.list_runs(sid)]


def test_misfire_older_than_grace_advances_without_firing():
    fired = []
    scheduler.JOB_RUNNERS["mocktype"] = lambda job_id: fired.append(job_id)
    sid = _mk_schedule()
    store.set_next_run(sid, (NOW - timedelta(hours=3)).strftime(TS))

    scheduler.tick(now=NOW)

    assert fired == []
    assert store.get_schedule(sid)["next_run_at"] > NOW.strftime(TS)


def test_unregistered_job_type_marks_run_failed():
    sid = _mk_schedule(job_type="nosuch")
    _due(sid)

    scheduler.tick(now=NOW)

    assert store.list_runs(sid)[0]["status"] == "failed"


def test_date_window_materialized_into_config():
    captured = {}

    def runner(job_id):
        from job_manager import get_job

        captured.update(json.loads(get_job(job_id)["config_json"]))
        mark_job_done(job_id)

    scheduler.JOB_RUNNERS["gpcopy_date_mock"] = runner
    cfg = {
        "connection_id": 1,
        "tables": [],
        "date_window": {
            "column": "dt",
            "from": {"preset": "yesterday"},
            "to": {"preset": "yesterday"},
        },
    }
    sid = _mk_schedule(job_type="gpcopy_date_mock", config_json=json.dumps(cfg))
    _due(sid)

    scheduler.tick(now=NOW)

    assert captured["date_from"] == "2026-07-20"
    assert captured["date_to"] == "2026-07-21"
    assert captured["date_filter_column"] == "dt"


def test_retry_after_failure():
    calls = []

    def runner(job_id):
        calls.append(job_id)
        if len(calls) == 1:
            raise RuntimeError("boom")
        mark_job_done(job_id)

    scheduler.JOB_RUNNERS["mocktype"] = runner
    sid = _mk_schedule(max_retries=1, retry_delay_seconds=0)
    _due(sid)

    scheduler.tick(now=NOW)  # первый запуск падает, ретрай в очередь
    scheduler.tick(now=NOW + timedelta(seconds=1))  # ретрай успешен

    statuses = sorted(r["status"] for r in store.list_runs(sid))
    assert statuses == ["done", "failed"]
    assert len(calls) == 2


def test_channel_crud():
    cid = store.create_channel(
        {"name": "tg", "type": "telegram", "config_json": "{}"}
    )
    assert store.get_channel(cid)["type"] == "telegram"

    store.update_channel(cid, {"enabled": 0})
    assert store.get_channel(cid)["enabled"] == 0

    store.delete_channel(cid)
    assert store.get_channel(cid) is None
