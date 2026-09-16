# -*- coding: utf-8 -*-
"""
Перенос через COPY (Postgres Toolkit).

Раннер читал job["config"], которого get_job никогда не отдавал: конфиг
выходил пустым, и каждый запуск падал с 'source_connection_id'. Тестов
на раннер не было, поэтому никто этого не видел.
"""

import pytest

import modules.sync_transport as st
from db import sqlite_cursor
from job_manager import create_job, get_job, get_job_items

PG = {"host": "pg-host", "port": 5432, "username": "u",
      "database_name": "cash", "db_type": "postgres"}
GP = dict(PG, db_type="greenplum")


class FakeConn(object):
    def close(self):
        pass

    def rollback(self):
        pass


@pytest.fixture
def pipe(monkeypatch):
    """Раннер без настоящих баз: запоминает, какие таблицы он переносил."""
    copied = []

    monkeypatch.setattr(st, "get_connection_by_id", lambda cid: PG)
    monkeypatch.setattr(st, "open_psycopg2_connection_by_cfg",
                        lambda cfg: FakeConn())
    monkeypatch.setattr(st, "fetch_table_sizes", lambda conn, tables: {})
    monkeypatch.setattr(st, "ensure_dest_table", lambda *a, **k: None)

    def fake_copy(src, dst, s_schema, s_table, d_schema, d_table, **kw):
        copied.append((s_schema, s_table))

    monkeypatch.setattr(st, "copy_table_pipe", fake_copy)
    return copied


def _job(tables, **config):
    base = {"source_connection_id": 11, "dest_connection_id": 12,
            "tables": [{"schema": s, "table": t} for s, t in tables]}
    base.update(config)
    return create_job("copy_pipe", 11, base)


def test_config_is_read_from_the_stored_job(pipe):
    job_id = _job([("loyalty", "bilim_marks_forpay_archive")])

    st.run_copy_pipe_job(job_id)

    assert get_job(job_id)["status"] == "done"
    assert pipe == [("loyalty", "bilim_marks_forpay_archive")]


def test_job_without_endpoints_says_so_in_words(pipe):
    job_id = create_job("copy_pipe", 11, {"tables": [
        {"schema": "loyalty", "table": "t"}]})

    st.run_copy_pipe_job(job_id)

    job = get_job(job_id)
    assert job["status"] == "failed"
    assert "источник или назначение" in job["error_message"]


def test_config_helper_prefers_a_ready_dict():
    assert st.job_config({"config": {"a": 1}, "config_json": '{"a": 2}'}) == \
        {"a": 1}


def test_config_helper_survives_garbage():
    assert st.job_config({"config_json": "{не json"}) == {}


def test_stop_skips_the_rest_but_keeps_what_is_done(pipe, monkeypatch):
    """
    Статусы в списке сняты до цикла: готовая в этом запуске таблица там
    всё ещё queued. Прежняя проверка искала pending, которого не бывает,
    и после «Остановить» остаток навсегда висел в очереди.
    """
    calls = {"n": 0}

    def stop_after_first(job_id):
        calls["n"] += 1
        return calls["n"] > 1

    monkeypatch.setattr(st, "is_stop_requested", stop_after_first)
    job_id = _job([("s", "first"), ("s", "second"), ("s", "third")])

    st.run_copy_pipe_job(job_id)

    by_table = {i["table_name"]: i["status"] for i in get_job_items(job_id)}

    assert by_table == {"first": "done", "second": "skipped",
                        "third": "skipped"}
    assert get_job(job_id)["status"] == "cancelled"


# ------------------------------------------------------------ лента запусков

@pytest.fixture
def two_kinds_of_runs(monkeypatch):
    """Два запуска: Postgres → Postgres и Postgres → Greenplum."""
    import app as app_module

    types = {11: "postgres", 12: "postgres", 13: "greenplum"}
    monkeypatch.setattr(app_module, "_all_connections", lambda: [
        {"id": cid, "name": "c%d" % cid, "db_type": t}
        for cid, t in types.items()
    ])

    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM job_items")
        cur.execute("DELETE FROM jobs")

    pg_only = create_job("copy_pipe", 11, {
        "source_connection_id": 11, "dest_connection_id": 12, "tables": []})
    mixed = create_job("copy_pipe", 11, {
        "source_connection_id": 11, "dest_connection_id": 13, "tables": []})

    return pg_only, mixed


def test_postgres_only_run_stays_out_of_the_greenplum_feed(client,
                                                           two_kinds_of_runs):
    pg_only, mixed = two_kinds_of_runs

    ids = [j["id"] for j in client.get(
        "/api/jobs/recent?types=copy_pipe&toolkit=gp").get_json()["jobs"]]

    assert pg_only not in ids
    assert mixed in ids


def test_postgres_feed_shows_only_postgres_runs(client, two_kinds_of_runs):
    pg_only, mixed = two_kinds_of_runs

    ids = [j["id"] for j in client.get(
        "/api/jobs/recent?types=copy_pipe&toolkit=pg").get_json()["jobs"]]

    assert ids == [pg_only]


def test_feed_without_toolkit_keeps_everything(client, two_kinds_of_runs):
    pg_only, mixed = two_kinds_of_runs

    ids = [j["id"] for j in client.get(
        "/api/jobs/recent?types=copy_pipe").get_json()["jobs"]]

    assert set(ids) == {pg_only, mixed}


# ------------------------------------------------------------ путь целиком

def test_start_route_runs_a_postgres_transfer_to_the_end(client, pipe,
                                                         monkeypatch):
    """
    Тот самый запуск, что падал на сервере: Postgres → Postgres через
    /api/gpcopy/start. Маршрут кладёт кластеры в конфиг, а строки задачи
    создаёт отдельно — не так, как тесты выше, поэтому проверяется
    отдельно.
    """
    import threading

    import modules.connections as connections

    kinds = {21: dict(PG, name="cashprod"), 22: dict(PG, name="cashbdb_test")}
    monkeypatch.setattr(connections, "get_connection_by_id",
                        lambda cid: kinds.get(int(cid)))

    class InlineThread(object):
        """Раннер выполняется сразу, чтобы дождаться его в тесте."""

        def __init__(self, target=None, args=(), daemon=None, **_kw):
            self.target, self.args = target, args

        def start(self):
            self.target(*self.args)

    monkeypatch.setattr(threading, "Thread", InlineThread)

    response = client.post("/api/gpcopy/start", json={
        "source_connection_id": 21, "dest_connection_id": 22,
        "tables": [{"schema": "stage", "table": "s01_g_clihst"}],
        "truncate": True,
    })

    body = response.get_json()
    job = get_job(body["job_id"])

    assert body["transport"] == "copy_pipe"
    assert job["status"] == "done", job["error_message"]
    assert pipe == [("stage", "s01_g_clihst")]
