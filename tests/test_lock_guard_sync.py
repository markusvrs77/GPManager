# -*- coding: utf-8 -*-
"""Синхронизация по ключу: занятая таблица откладывается и догружается в конце."""

import json

import modules.gpcopy_sync as gs
import modules.lock_guard as lg
from job_manager import create_job, create_job_items, get_job, get_job_items


class FakeConn(object):
    autocommit = False

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


class BusyOnce(object):
    """Таблица dwh.a занята приоритетным ETL при первых проверках."""

    def __init__(self, busy_checks=1):
        self.left = busy_checks
        self.round_roots = set()
        self.closed = False

    def root_of_name(self, schema, table):
        return (schema, table)

    def busy(self, roots, need):
        if ("dwh", "a") in roots and self.left > 0:
            self.left -= 1
            return {("dwh", "a"): "источник: работает приоритетная сессия etl@10.1.2.3"}
        return {}

    def summary(self):
        return {"deferred": [], "paused": [], "warnings": []}

    def save(self, guard=None):
        pass

    def close(self):
        self.closed = True


def run_sync(monkeypatch, guard, attempts=2):
    order = []

    monkeypatch.setattr(gs, "open_conn", lambda cid: FakeConn())
    monkeypatch.setattr(gs, "get_conn_host", lambda cid: "h")
    monkeypatch.setattr(gs, "get_conn_port", lambda cid: 5432)
    monkeypatch.setattr(gs, "get_conn_user", lambda cid: "u")
    monkeypatch.setattr(gs, "get_conn_dbname", lambda cid: "adb")
    monkeypatch.setattr(gs, "normalize_columns", lambda *a: (["id", "v"], ["v"]))
    monkeypatch.setattr(gs, "run_sql_execute", lambda conn, sql, params=None: None)
    monkeypatch.setattr(gs, "run_sql_fetch_one",
                        lambda conn, sql, params=None: {"cnt": 0, "duplicate_keys": 0})
    monkeypatch.setattr(gs, "copy_source_to_stage_via_gpcopy",
                        lambda **kw: order.append(kw["source_table"]))
    monkeypatch.setattr(gs, "start_sync_lock_guard", lambda *a: guard)
    monkeypatch.setattr(lg, "load_settings", lambda: dict(
        lg.DEFAULTS, retry_minutes=0, max_wait_minutes=0, attempts=attempts))

    tables = [{"schema": "dwh", "table": t, "key_columns": ["id"]} for t in ("a", "b")]
    job_id = create_job("gpcopy_sync", 1, {"source_connection_id": 1,
                                           "dest_connection_id": 2,
                                           "table_configs": tables})
    create_job_items(job_id=job_id, items=[
        {"schema_name": "dwh", "table_name": t, "action": "SYNC"} for t in ("a", "b")])

    gs.run_gpcopy_sync_job(job_id)
    return job_id, order


def test_busy_table_is_skipped_then_loaded_at_the_end(monkeypatch):
    guard = BusyOnce(busy_checks=1)
    job_id, order = run_sync(monkeypatch, guard)

    assert order == ["b", "a"]                      # a не ждали — догрузили после b
    assert {i["table_name"]: i["status"] for i in get_job_items(job_id)} == \
        {"a": "done", "b": "done"}
    assert get_job(job_id)["status"] == "done"
    assert guard.closed

    state = json.loads(get_job(job_id)["config_json"])["lock_state"]
    assert state["attempt"] == 1 and state["deferred"][0]["table"] == "dwh.a"


def test_attempts_are_bounded(monkeypatch):
    job_id, order = run_sync(monkeypatch, BusyOnce(busy_checks=99), attempts=2)

    items = {i["table_name"]: i for i in get_job_items(job_id)}

    assert order == ["b"]
    assert items["a"]["status"] == "failed"
    assert "попыток: 2" in items["a"]["error_message"]
    assert "etl@10.1.2.3" in items["a"]["error_message"]
    assert get_job(job_id)["status"] == "failed"
