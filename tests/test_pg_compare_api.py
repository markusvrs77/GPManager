# -*- coding: utf-8 -*-
"""
Маршруты сравнения баз PostgreSQL: старт, результаты, последнее сравнение.
"""

import threading

import pytest

import modules.pg_compare as cmp
import modules.pg_sync_common as common
from job_manager import create_job, get_job, get_job_items
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


def _start(client, **body):
    payload = {"source_connection_id": 31, "dest_connection_id": 32,
               "schemas": ["sales"], "tables": []}
    payload.update(body)
    return client.post("/api/pg/compare/start", json=payload)


def test_start_creates_a_job_with_an_item_per_table(client, pg_pair):
    response = _start(client)
    body = response.get_json()

    assert response.status_code == 200, body
    job = get_job(body["job_id"])
    assert job["job_type"] == "pg_compare"
    assert '"source_connection_id": 31' in job["config_json"]
    assert '"dest_connection_id": 32' in job["config_json"]
    assert [(i["schema_name"], i["table_name"]) for i in
            get_job_items(body["job_id"])] == [("sales", "orders"),
                                               ("sales", "items")]
    assert pg_pair == [(cmp.run_pg_compare_job, (body["job_id"],))]


@pytest.mark.parametrize("body, words", [
    ({"dest_connection_id": 31}, "совпадают"),
    ({"dest_connection_id": 33}, "не PostgreSQL"),
    ({"schemas": [], "tables": []}, "Не выбраны"),
    ({"source_connection_id": None}, "источник"),
])
def test_start_refuses_bad_requests_in_russian(client, pg_pair, body, words):
    response = _start(client, **body)

    assert response.status_code == 400
    assert words in response.get_json()["message"]
    assert pg_pair == []


def test_unknown_names_from_the_catalog_are_400(client, pg_pair, monkeypatch):
    def unknown(*_a):
        raise ValueError("Таблица не найдена в источнике: sales.nope")

    monkeypatch.setattr(cmp, "expand_selection", unknown)

    response = _start(client, schemas=[],
                      tables=[{"schema": "sales", "table": "nope"}])

    assert response.status_code == 400
    assert "sales.nope" in response.get_json()["message"]


def test_results_and_latest_return_the_job_and_its_rows(client):
    job_id = create_job("pg_compare", 41, {
        "source_connection_id": 41, "dest_connection_id": 42,
        "tables": [{"schema": "s", "table": "t"}], "item_action": "COMPARE"})
    cmp.save_result(job_id, {"schema": "s", "table": "t", "status": "differs",
                             "key_columns": ["id"], "key_source": "pk",
                             "src_rows": 10, "dst_rows": 9, "to_insert": 1,
                             "to_update": 2, "to_delete": 0})

    by_id = client.get("/api/pg/compare/results?job_id=%d" % job_id).get_json()
    latest = client.get("/api/pg/compare/latest?source_connection_id=41"
                        "&dest_connection_id=42").get_json()

    for body in (by_id, latest):
        assert body["job"]["id"] == job_id
        row = body["results"][0]
        assert (row["schema"], row["table"], row["status"]) == ("s", "t",
                                                                "differs")
        assert row["key_columns"] == ["id"] and row["to_update"] == 2


def test_latest_without_compares_is_empty(client):
    body = client.get("/api/pg/compare/latest?source_connection_id=51"
                      "&dest_connection_id=52").get_json()

    assert body["job"] is None


def test_results_of_a_foreign_job_type_are_404(client):
    job_id = create_job("copy_pipe", 41, {"tables": []})

    assert client.get("/api/pg/compare/results?job_id=%d"
                      % job_id).status_code == 404


def test_policy_start_runs_reads_view():
    assert web_auth.POLICY["api_pg_compare_start"] == "sync.run"
    assert web_auth.POLICY["api_pg_compare_results"] == "sync.view"
    assert web_auth.POLICY["api_pg_compare_latest"] == "sync.view"
