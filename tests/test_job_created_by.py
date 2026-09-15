# -*- coding: utf-8 -*-
"""
Кто запустил задачу.

Поводом стала полная заливка, которую приняли за запуск расписания
«Партиции»: в задаче не было ни слова о том, откуда она взялась.
"""

import json
from datetime import datetime, timedelta

import pytest

import scheduler
import scheduler_store as store
from app import app as flask_app
from db import sqlite_cursor
from job_manager import create_job, get_job, mark_job_done

NOW = datetime(2026, 9, 14, 12, 0, 0)


@pytest.fixture
def mock_runner():
    scheduler.RUN_SYNC = True
    scheduler.JOB_RUNNERS["createdby_mock"] = lambda job_id: mark_job_done(job_id)
    yield
    scheduler.RUN_SYNC = False
    scheduler.JOB_RUNNERS.pop("createdby_mock", None)

    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM schedule_runs")
        cur.execute("DELETE FROM schedules")
        cur.execute("UPDATE scheduler_lock SET holder=NULL, heartbeat_at=NULL, "
                    "expires_at=NULL WHERE id=1")


def _schedule(name="partition_bi_dds_dm_stage"):
    return store.create_schedule({
        "name": name,
        "job_type": "createdby_mock",
        "config_json": json.dumps({"connection_id": 1, "tables": []}),
        "cron_expr": "30 19 * * 5",
        "overlap_policy": "parallel",
    }, now=NOW)


def _last_job_id():
    with sqlite_cursor() as cur:
        cur.execute("SELECT MAX(id) AS id FROM jobs")
        return cur.fetchone()["id"]


def test_job_started_outside_a_request_has_no_author():
    job_id = create_job("gpcopy", 1, {"tables": []})

    assert get_job(job_id)["created_by"] is None


def test_explicit_author_is_kept():
    job_id = create_job("gpcopy", 1, {"tables": []}, created_by="миграция")

    assert get_job(job_id)["created_by"] == "миграция"


def test_manual_start_records_the_logged_in_user(client, admin_user):
    """Задача, запущенная со страницы, подписана тем, кто нажал кнопку."""
    from modules.web_auth import SESSION_COOKIE
    import modules.security as sec

    token = sec.create_session(admin_user["id"])

    with flask_app.test_request_context("/"):
        from flask import g
        g.user = sec.resolve_session(token)
        job_id = create_job("gpcopy", 1, {"tables": []})

    assert get_job(job_id)["created_by"] == "pytest-admin"


def test_timer_run_is_signed_by_the_schedule(mock_runner):
    sid = _schedule()
    store.set_next_run(sid, (NOW - timedelta(minutes=1)).strftime(
        "%Y-%m-%d %H:%M:%S"))

    scheduler.tick(now=NOW)

    assert get_job(_last_job_id())["created_by"] == \
        "расписание «partition_bi_dds_dm_stage»"


def test_run_now_button_names_both_the_person_and_the_schedule(
        mock_runner, admin_user):
    import modules.security as sec

    sid = _schedule()
    token = sec.create_session(admin_user["id"])

    with flask_app.test_request_context("/"):
        from flask import g
        g.user = sec.resolve_session(token)
        scheduler.run_now(sid, now=NOW)

    assert get_job(_last_job_id())["created_by"] == \
        "pytest-admin · расписание «partition_bi_dds_dm_stage»"


def test_run_feed_shows_the_author(client):
    create_job("gpcopy", 1, {"tables": []}, created_by="kafka-user")

    jobs = client.get("/api/jobs/recent?limit=5").get_json()["jobs"]

    assert jobs[0]["created_by"] == "kafka-user"
