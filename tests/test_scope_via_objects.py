# -*- coding: utf-8 -*-
"""
Доступ к кластерам через задачу, расписание и результат анализа.

У этих объектов в адресе их собственный номер, а кластеры записаны
внутри. Проверка по одному запросу их не видела: пользователь с одним
TEST мог по номеру PROD-задачи открыть её лог, остановить её и
дозапустить её упавшие таблицы, а расписание на PROD — завести и
запустить.
"""

import json

import pytest

import scheduler_store as store
from datetime import datetime
from db import sqlite_cursor
from job_manager import create_job

PROD, TEST = 1, 2


@pytest.fixture
def jobs():
    prod = create_job("gpcopy", PROD, {
        "source_connection_id": PROD, "dest_connection_id": TEST,
        "tables": []})
    test = create_job("gpcopy", TEST, {
        "source_connection_id": TEST, "dest_connection_id": TEST,
        "tables": []})
    return prod, test


@pytest.fixture
def prod_schedule():
    schedule_id = store.create_schedule({
        "name": "prod-nightly", "job_type": "gpcopy",
        "config_json": json.dumps({"source_connection_id": PROD,
                                   "dest_connection_id": TEST}),
        "cron_expr": "0 1 * * *",
    }, now=datetime(2026, 9, 15, 0, 0, 0))
    yield schedule_id

    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))


@pytest.fixture
def test_only(as_user, admin_user):
    """Оператор, которому выдан только TEST.

    admin_user нужен не сам по себе: без единого администратора
    приложение считает себя ненастроенным и отвечает 503 раньше, чем
    дело дойдёт до проверки кластеров.
    """
    return as_user("operator", connection_ids=[TEST])


# ------------------------------------------------------------ задачи

def test_foreign_job_details_are_closed(test_only, jobs):
    prod, _test = jobs

    assert test_only.get("/api/jobs/%d/status" % prod).status_code == 403
    assert test_only.get("/api/jobs/%d/log" % prod).status_code == 403
    assert test_only.get("/api/jobs/%d/items" % prod).status_code == 403


def test_own_job_details_stay_open(test_only, jobs):
    _prod, test = jobs

    assert test_only.get("/api/jobs/%d/status" % test).status_code != 403


def test_foreign_job_cannot_be_stopped(test_only, jobs):
    prod, _test = jobs

    assert test_only.post("/api/jobs/%d/stop" % prod).status_code == 403


def test_foreign_failures_cannot_be_retried(test_only, jobs):
    """Дозапуск берёт кластеры из старой задачи — это запись на PROD."""
    prod, _test = jobs

    response = test_only.post("/api/gpcopy/retry-failed",
                              json={"job_id": prod})

    assert response.status_code == 403
    assert "кластеру" in response.get_json()["message"]


def test_run_feed_hides_foreign_jobs(test_only, jobs):
    prod, test = jobs

    ids = [j["id"] for j in
           test_only.get("/api/jobs/recent?limit=100").get_json()["jobs"]]

    assert prod not in ids
    assert test in ids


def test_active_feed_hides_foreign_jobs(test_only, jobs):
    prod, test = jobs

    ids = [j["id"] for j in test_only.get("/api/jobs/active").get_json()["jobs"]]

    assert prod not in ids
    assert test in ids


def test_admin_sees_every_job(client, jobs):
    prod, test = jobs

    ids = [j["id"] for j in
           client.get("/api/jobs/recent?limit=100").get_json()["jobs"]]

    assert {prod, test} <= set(ids)


# ------------------------------------------------------------ расписания

def test_foreign_schedule_cannot_be_run(test_only, prod_schedule):
    response = test_only.post("/api/schedules/%d/run-now" % prod_schedule)

    assert response.status_code == 403
    assert "кластеру" in response.get_json()["message"]


def test_foreign_schedule_cannot_be_changed(test_only, prod_schedule):
    assert test_only.put("/api/schedules/%d" % prod_schedule,
                         json={"enabled": 0}).status_code == 403
    assert test_only.post(
        "/api/schedules/%d/toggle" % prod_schedule).status_code == 403


def test_foreign_schedule_is_not_listed(test_only, prod_schedule):
    ids = [s["id"] for s in test_only.get("/api/schedules").get_json()["schedules"]]

    assert prod_schedule not in ids


def test_schedule_on_a_foreign_cluster_cannot_be_created(test_only):
    """Кластеры расписания приходят во вложенном config."""
    response = test_only.post("/api/schedules", json={
        "name": "sneaky", "job_type": "gpcopy", "cron_expr": "0 1 * * *",
        "config": {"source_connection_id": PROD, "dest_connection_id": TEST},
    })

    assert response.status_code == 403
    assert "кластеру" in response.get_json()["message"]


# ------------------------------------------------------------ анализ перекоса

@pytest.fixture
def prod_skew_result():
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "INSERT INTO skew_results (connection_id, schema_name, table_name) "
            "VALUES (?, 'dwh_dm', 'dm_secret')", (PROD,))
        result_id = cur.lastrowid

    yield result_id

    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM skew_results WHERE id = ?", (result_id,))


def test_foreign_skew_segments_are_closed(as_user, admin_user,
                                          prod_skew_result):
    c = as_user("operator", connection_ids=[TEST])

    response = c.get("/api/skew-results/%d/segments" % prod_skew_result)

    assert response.status_code == 403
    assert "кластеру" in response.get_json()["message"]


def test_foreign_skew_results_are_not_listed(as_user, admin_user,
                                            prod_skew_result):
    c = as_user("operator", connection_ids=[TEST])

    body = c.get("/api/skew/results?limit=1000").get_data(as_text=True)

    assert "dm_secret" not in body
