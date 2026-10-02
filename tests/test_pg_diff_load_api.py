# -*- coding: utf-8 -*-
"""
Маршрут POST /api/pg/diff-load/start: валидация, ключи и ожидаемые числа
из результатов сравнения (не с клиента), права.
"""

import json
import threading

import pytest

import modules.pg_compare as cmp
import modules.pg_diff_load as pdl
import modules.pg_sync_common as common
from job_manager import (create_job, get_job, get_job_items, mark_job_done,
                         mark_job_running)
from modules import web_auth
from tests.pg_fakes import FakeConn

PG = {"host": "h", "port": 5432, "username": "u", "database_name": "cash",
      "db_type": "postgres"}


@pytest.fixture
def pg_pair(monkeypatch):
    """Подключения 31, 32 — Postgres, 33 — Greenplum; раннер не стартует."""
    kinds = {31: dict(PG, id=31, name="prod"), 32: dict(PG, id=32, name="test"),
             33: dict(PG, id=33, name="gp", db_type="greenplum")}
    started = []

    monkeypatch.setattr(common, "get_connection_by_id",
                        lambda cid: kinds.get(int(cid)))
    monkeypatch.setattr(common, "open_psycopg2_connection_by_cfg",
                        lambda cfg: FakeConn())
    monkeypatch.setattr(cmp, "expand_selection", lambda s, d, schemas, tables: [
        {"schema": "sales", "table": "orders", "in_src": True, "in_dst": True},
        {"schema": "sales", "table": "items", "in_src": True, "in_dst": False},
    ])

    class NoThread(object):
        def __init__(self, target=None, args=(), daemon=None, **_kw):
            self.target, self.args = target, args

        def start(self):
            started.append((self.target, self.args))

    monkeypatch.setattr(threading, "Thread", NoThread)
    return started


@pytest.fixture
def compared():
    """Сравнение пары 31 → 32 с таблицами в разных статусах."""
    job_id = create_job("pg_compare", 31, {
        "source_connection_id": 31, "dest_connection_id": 32,
        "tables": [], "item_action": "COMPARE"})
    rows = [("orders", "differs", ["id"], "pk", 4, 1, 0),
            ("fresh", "no_dest", [], None, None, None, None),
            ("logs", "same", [], None, 0, 0, 0),
            ("wide", "structure_diff", [], None, None, None, None)]
    for table, status, key, source, ins, upd, dele in rows:
        cmp.save_result(job_id, {"schema": "sales", "table": table,
                                 "status": status, "key_columns": key,
                                 "key_source": source, "to_insert": ins,
                                 "to_update": upd, "to_delete": dele})
    mark_job_done(job_id)
    return job_id


@pytest.mark.parametrize("action, table", [("diff", "orders"),
                                           ("create", "fresh")])
def test_diff_and_create_wait_for_the_compare_to_finish(client, pg_pair,
                                                        compared, action,
                                                        table):
    mark_job_running(compared)

    response = _start(client, compare_job_id=compared, tables=[
        {"schema": "sales", "table": table, "action": action}])

    assert response.status_code == 400
    assert "не завершено" in response.get_json()["message"]
    assert pg_pair == []


def _start(client, **body):
    payload = {"source_connection_id": 31, "dest_connection_id": 32,
               "delete_missing": False,
               "tables": [{"schema": "sales", "table": "orders",
                           "action": "diff"}]}
    payload.update(body)
    return client.post("/api/pg/diff-load/start", json=payload)


def test_start_takes_keys_and_expected_from_the_compare(client, pg_pair,
                                                        compared):
    response = _start(client, compare_job_id=compared, delete_missing=True,
                      tables=[
                          # ключ с клиента не принимается
                          {"schema": "sales", "table": "orders",
                           "action": "diff", "key_columns": ["evil"]},
                          {"schema": "sales", "table": "fresh",
                           "action": "create"},
                          {"schema": "sales", "table": "logs",
                           "action": "full"},
                      ])
    body = response.get_json()

    assert response.status_code == 200, body
    job = get_job(body["job_id"])
    config = json.loads(job["config_json"])
    assert job["job_type"] == "pg_diff_load"
    assert (config["source_connection_id"], config["dest_connection_id"],
            config["compare_job_id"], config["delete_missing"]) == (
        31, 32, compared, True)
    tables = {t["table"]: t for t in config["tables"]}
    assert tables["orders"]["action"] == "diff"
    assert tables["orders"]["key_columns"] == ["id"]
    assert tables["fresh"]["action"] == "create"
    assert tables["logs"]["action"] == "full"
    expected = {e["table"]: e for e in config["expected"]}
    assert (expected["orders"]["to_insert"], expected["orders"]["to_update"],
            expected["orders"]["to_delete"]) == (4, 1, 0)
    assert [i["action"] for i in get_job_items(body["job_id"])] == [
        "DIFF", "CREATE", "FULL"]
    assert pg_pair == [(pdl.run_pg_diff_load_job, (body["job_id"],))]


def test_full_load_without_compare_checks_the_catalog(client, pg_pair):
    response = _start(client, tables=[
        {"schema": "sales", "table": "orders", "action": "full"},
        {"schema": "sales", "table": "items", "action": "full"}])
    body = response.get_json()

    assert response.status_code == 200, body
    config = json.loads(get_job(body["job_id"])["config_json"])
    assert config["compare_job_id"] is None
    assert {t["table"]: t["in_dst"] for t in config["tables"]} == {
        "orders": True, "items": False}


@pytest.mark.parametrize("body, words", [
    ({"dest_connection_id": 31}, "совпадают"),
    ({"dest_connection_id": 33}, "не PostgreSQL"),
    ({"delete_missing": "true"}, "delete_missing"),
    ({"tables": []}, "Не выбраны"),
    ({"tables": [{"schema": "sales", "table": "orders", "action": "drop"}]},
     "Неизвестное действие"),
    ({"tables": [{"schema": "sales", "table": "orders", "action": "diff"}]},
     "Без сравнения"),
    ({"tables": [{"schema": "sales", "table": "fresh", "action": "create"}]},
     "Без сравнения"),
])
def test_start_refuses_bad_requests_in_russian(client, pg_pair, body, words):
    response = _start(client, **body)

    assert response.status_code == 400
    assert words in response.get_json()["message"]
    assert pg_pair == []


@pytest.mark.parametrize("table, action, words", [
    ("ghost", "diff", "нет в результатах"),
    ("wide", "diff", "structure_diff"),
    ("orders", "create", "differs"),
    ("fresh", "full", "no_dest"),
])
def test_action_must_fit_the_compare_status(client, pg_pair, compared, table,
                                           action, words):
    response = _start(client, compare_job_id=compared, tables=[
        {"schema": "sales", "table": table, "action": action}])

    assert response.status_code == 400
    assert words in response.get_json()["message"]
    assert pg_pair == []


def test_compare_of_another_pair_is_refused(client, pg_pair, compared):
    response = _start(client, compare_job_id=compared,
                      source_connection_id=32, dest_connection_id=31)

    assert response.status_code == 400
    assert "другой пары" in response.get_json()["message"]


def test_viewer_cannot_start_a_load(as_user, pg_pair):
    response = as_user("viewer").post("/api/pg/diff-load/start", json={
        "source_connection_id": 31, "dest_connection_id": 32,
        "tables": [{"schema": "sales", "table": "orders", "action": "full"}]})

    assert response.status_code == 403
    assert pg_pair == []


def test_policy_start_needs_sync_run():
    assert web_auth.POLICY["api_pg_diff_load_start"] == "sync.run"
