# -*- coding: utf-8 -*-
"""
Расписание, вставшее на одном запуске.

Запись о запуске закрывает поток _execute, и живёт он ровно столько,
сколько идёт gpcopy. Перезапуск приложения убивает поток: задача
доживает сама, а запуск навсегда остаётся running. Дальше политика skip
не пускает ни один новый — в истории висит один и тот же запуск, кнопка
«Запустить» молча отказывает.
"""

import json
from datetime import datetime, timedelta

import pytest

import scheduler
import scheduler_store as store
from db import sqlite_cursor
from job_manager import (
    create_job, mark_job_done, mark_job_failed, mark_job_running,
)

NOW = datetime(2026, 9, 16, 12, 0, 0)
TS = "%Y-%m-%d %H:%M:%S"


@pytest.fixture(autouse=True)
def _clean():
    scheduler.RUN_SYNC = True
    yield
    scheduler.RUN_SYNC = False
    scheduler.JOB_RUNNERS.pop("stale_mock", None)

    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM schedule_runs")
        cur.execute("DELETE FROM schedules")
        cur.execute(
            "UPDATE scheduler_lock SET holder=NULL, heartbeat_at=NULL, "
            "expires_at=NULL WHERE id=1"
        )


def _schedule(**over):
    data = {
        "name": "partition_bi_dds_dm_stage",
        "job_type": "stale_mock",
        "config_json": json.dumps({"connection_id": 1, "tables": []}),
        "cron_expr": "30 19 * * 5",
        "overlap_policy": "skip",
    }
    data.update(over)
    return store.create_schedule(data, now=NOW)


def _run(schedule_id, job_id, status="running"):
    return store.record_run(
        schedule_id,
        fired_at=NOW.strftime(TS),
        run_date=NOW.strftime(TS),
        status=status,
        job_id=job_id,
    )


def _job(status):
    job_id = create_job("stale_mock", 1, {"tables": []})

    if status == "running":
        mark_job_running(job_id)
    elif status == "done":
        mark_job_done(job_id)
    elif status == "failed":
        mark_job_failed(job_id, "gpcopy упал")

    return job_id


# ------------------------------------------------------------ починка

def test_run_of_a_failed_job_is_closed():
    sid = _schedule()
    run_id = _run(sid, _job("failed"))

    assert scheduler.reconcile_runs() == 1
    assert store.get_run(run_id)["status"] == "failed"


def test_run_of_a_finished_job_is_closed_as_done():
    sid = _schedule()
    run_id = _run(sid, _job("done"))

    scheduler.reconcile_runs()

    assert store.get_run(run_id)["status"] == "done"


def test_interrupted_job_says_what_happened():
    """Задачу оборвал перезапуск — в истории это должно быть видно."""
    sid = _schedule()
    job_id = _job("running")

    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE jobs SET status = 'interrupted' WHERE id = ?",
                    (job_id,))

    run_id = _run(sid, job_id)
    scheduler.reconcile_runs()
    run = store.get_run(run_id)

    assert run["status"] == "failed"
    assert "перезапуском" in run["error"]


def test_run_without_a_job_is_closed_too():
    """Иначе его не закроет уже никто, и расписание встанет навсегда."""
    sid = _schedule()
    run_id = _run(sid, None)

    scheduler.reconcile_runs()

    assert store.get_run(run_id)["status"] == "failed"


def test_a_really_running_job_is_left_alone():
    sid = _schedule()
    run_id = _run(sid, _job("running"))

    assert scheduler.reconcile_runs() == 0
    assert store.get_run(run_id)["status"] == "running"


def test_stale_run_does_not_overwrite_a_newer_status():
    """Подвисший старый запуск не должен затирать свежий результат."""
    sid = _schedule()
    _run(sid, _job("failed"))                      # старый, подвис
    _run(sid, _job("done"), status="done")         # свежий, уже закрыт

    scheduler.reconcile_runs()

    assert store.get_schedule(sid)["last_status"] != "failed"


# ------------------------------------------------- расписание снова работает

def test_run_now_starts_after_the_stale_run_is_closed():
    """
    Ровно то, что видел пользователь: задача давно упала, а кнопка
    «Запустить» молчала, потому что запуск числился идущим.
    """
    started = []
    scheduler.JOB_RUNNERS["stale_mock"] = lambda job_id: started.append(job_id)

    sid = _schedule()
    _run(sid, _job("failed"))

    result = scheduler.run_now(sid, now=NOW)

    assert result["started"] is True
    assert len(started) == 1


def test_run_now_still_refuses_while_a_job_is_really_running():
    scheduler.JOB_RUNNERS["stale_mock"] = lambda job_id: None
    sid = _schedule()
    _run(sid, _job("running"))

    result = scheduler.run_now(sid, now=NOW)

    assert result["started"] is False
    # страница переводит именно этот код в «предыдущий запуск ещё не завершился»
    assert result["reason"] == "overlap"


def test_timer_fires_again_after_the_stale_run_is_closed():
    fired = []
    scheduler.JOB_RUNNERS["stale_mock"] = lambda job_id: fired.append(job_id)

    sid = _schedule()
    _run(sid, _job("failed"))
    store.set_next_run(sid, (NOW - timedelta(minutes=1)).strftime(TS))

    scheduler.tick(now=NOW)

    assert len(fired) == 1
