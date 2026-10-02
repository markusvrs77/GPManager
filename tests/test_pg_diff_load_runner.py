# -*- coding: utf-8 -*-
"""
Раннер pg_diff_load на фейковых соединениях и временной SQLite:
действия diff / full / create, фактические числа в item, ошибки и стоп.
"""

import pytest
from psycopg2.extensions import QueryCanceledError

import modules.ddl_check as ddl_check
import modules.pg_diff_load as pdl
import modules.pg_sync_common as common
from job_manager import create_job, get_job, get_job_items
from tests.pg_fakes import FakeConn


@pytest.fixture
def world(monkeypatch):
    state = {"opened": [], "diff": [], "full": [], "created": [],
             "stop": False, "outcome": {}, "create_error": {},
             "require_empty": {}, "dst_tables": set(), "seq_calls": [],
             "seq_warn": []}

    def fake_open(cid, readonly=False):
        conn = FakeConn()
        conn.readonly = readonly
        state["opened"].append(conn)
        return conn

    def run(table, default):
        outcome = state["outcome"].get(table, default)
        return outcome() if callable(outcome) else dict(outcome)

    def fake_diff(src, dst, schema, table, key_columns, delete_missing,
                  stage_name):
        state["diff"].append((table, list(key_columns), delete_missing,
                              stage_name))
        return run(table, {"insert": 3, "update": 2, "delete": 0})

    def fake_full(src, dst, schema, table, truncate, require_empty=False):
        state["full"].append((table, truncate))
        state["require_empty"][table] = require_empty
        return run(table, {"rows": 5})

    def fake_sequences(conn, schema, table):
        state["seq_calls"].append(table)
        return list(state["seq_warn"])

    def fake_create(src_id, dst_id, tables):
        state["created"].append((src_id, dst_id, tables))
        error = state["create_error"].get(tables[0]["table"])
        return [{"schema": t["schema"], "table": t["table"], "kind": "table",
                 "ok": not error, "error": error or "", "statements": 1}
                for t in tables]

    monkeypatch.setattr(pdl, "open_pg", fake_open)
    monkeypatch.setattr(pdl, "load_diff", fake_diff)
    monkeypatch.setattr(pdl, "load_full", fake_full)
    monkeypatch.setattr(pdl, "sync_sequences", fake_sequences)
    monkeypatch.setattr(pdl, "dest_table_exists",
                        lambda conn, s, t: t in state["dst_tables"])
    monkeypatch.setattr(ddl_check, "create_missing_objects", fake_create)
    monkeypatch.setattr(common, "is_stop_requested",
                        lambda job_id: state["stop"])
    return state


def _job(tables, delete_missing=False):
    rows = []
    for table, action, key, in_dst in tables:
        rows.append({"schema": "s", "table": table, "action": action,
                     "key_columns": key, "in_dst": in_dst})
    return create_job("pg_diff_load", 11, {
        "source_connection_id": 11, "dest_connection_id": 12,
        "compare_job_id": 5, "delete_missing": delete_missing,
        "tables": rows, "expected": []})


def _items(job_id):
    return {i["table_name"]: (i["status"], i["error_message"])
            for i in get_job_items(job_id)}


def test_each_action_writes_actual_numbers_into_its_item(world):
    job_id = _job([("orders", "diff", ["id"], True),
                   ("log", "full", [], True),
                   ("fresh", "create", [], False)])

    pdl.run_pg_diff_load_job(job_id)

    assert _items(job_id) == {
        "orders": ("done", "insert=3; update=2; delete=0"),
        "log": ("done", "truncate+insert=5"),
        "fresh": ("done", "create+insert=5"),
    }
    assert get_job(job_id)["status"] == "done"
    assert world["diff"] == [("orders", ["id"], False, "stg_%d_1" % job_id)]
    # созданная таблица заливается без TRUNCATE
    assert world["full"] == [("log", True), ("fresh", False)]
    assert world["created"] == [(11, 12, [{"schema": "s", "table": "fresh"}])]


def test_source_is_read_only_and_both_connections_are_closed(world):
    job_id = _job([("orders", "diff", ["id"], True)])

    pdl.run_pg_diff_load_job(job_id)

    src, dst = world["opened"]
    assert src.readonly is True and dst.readonly is False
    assert src.closed and dst.closed


def test_delete_missing_reaches_the_loader_only_as_true(world):
    job_id = _job([("orders", "diff", [], True)], delete_missing=True)

    pdl.run_pg_diff_load_job(job_id)

    assert world["diff"][0][2] is True


def test_table_error_fails_its_item_and_the_rest_go_on(world):
    def dup():
        raise pdl.DuplicateKeyError("Ключ (id) не уникален в источнике")

    world["outcome"]["bad"] = dup
    job_id = _job([("bad", "diff", ["id"], True), ("ok", "full", [], True)])

    pdl.run_pg_diff_load_job(job_id)

    items = _items(job_id)
    assert items["bad"][0] == "failed" and "не уникален" in items["bad"][1]
    assert items["ok"] == ("done", "truncate+insert=5")
    assert get_job(job_id)["status"] == "failed"


def test_create_error_fails_the_item_with_its_text(world):
    world["create_error"]["fresh"] = "permission denied for schema s"
    job_id = _job([("fresh", "create", [], False)])

    pdl.run_pg_diff_load_job(job_id)

    status, message = _items(job_id)["fresh"]
    assert status == "failed" and "permission denied" in message
    assert world["full"] == []


def test_full_load_of_a_table_missing_in_dest_is_skipped(world):
    job_id = _job([("ghost", "full", [], False), ("log", "full", [], True)])

    pdl.run_pg_diff_load_job(job_id)

    items = _items(job_id)
    assert items["ghost"][0] == "skipped" and "нет в приёмнике" in items["ghost"][1]
    assert world["full"] == [("log", True)]
    assert get_job(job_id)["status"] == "done"


def test_stop_during_a_load_cancels_it_and_keeps_committed_tables(world):
    def cancelled_mid_copy():
        world["stop"] = True
        raise QueryCanceledError("canceling statement due to user request")

    world["outcome"]["big"] = cancelled_mid_copy
    job_id = _job([("first", "full", [], True), ("big", "diff", ["id"], True),
                   ("last", "full", [], True)])

    pdl.run_pg_diff_load_job(job_id)

    items = _items(job_id)
    assert items["first"] == ("done", "truncate+insert=5")
    assert items["big"][0] == "cancelled"
    assert items["last"][0] == "skipped"
    assert get_job(job_id)["status"] == "cancelled"
    assert all(c.cancelled >= 1 for c in world["opened"])
    assert world["opened"][1].rollbacks >= 1


def test_stop_between_tables_skips_the_rest(world):
    def first_then_stop():
        world["stop"] = True
        return {"rows": 1}

    world["outcome"]["first"] = first_then_stop
    job_id = _job([("first", "full", [], True), ("second", "full", [], True)])

    pdl.run_pg_diff_load_job(job_id)

    assert _items(job_id)["first"] == ("done", "truncate+insert=1")
    assert _items(job_id)["second"][0] == "skipped"
    assert get_job(job_id)["status"] == "cancelled"


def test_actual_numbers_are_in_the_item_before_it_is_done(world, monkeypatch):
    seen = []
    real_done = pdl.mark_item_done

    def spy(item_id):
        seen.append({i["id"]: i["error_message"]
                     for i in get_job_items(job_id)}[item_id])
        real_done(item_id)

    monkeypatch.setattr(pdl, "mark_item_done", spy)
    job_id = _job([("orders", "diff", ["id"], True)])

    pdl.run_pg_diff_load_job(job_id)

    assert seen == ["insert=3; update=2; delete=0"]


def test_sequences_are_synced_only_after_inserts(world):
    world["outcome"]["quiet"] = {"insert": 0, "update": 4, "delete": 0}
    world["seq_warn"] = ["setval s.log_id_seq: permission denied"]
    job_id = _job([("quiet", "diff", ["id"], True), ("log", "full", [], True),
                   ("fresh", "create", [], False)])

    pdl.run_pg_diff_load_job(job_id)

    assert world["seq_calls"] == ["log", "fresh"]
    status, message = _items(job_id)["log"]
    # ошибка setval не отменяет закоммиченную таблицу
    assert status == "done"
    assert message == ("truncate+insert=5; предупреждение: "
                       "setval s.log_id_seq: permission denied")


def test_create_of_an_existing_table_is_not_recreated_and_must_be_empty(world):
    world["dst_tables"].add("fresh")
    job_id = _job([("fresh", "create", [], False)])

    pdl.run_pg_diff_load_job(job_id)

    assert world["created"] == []
    assert world["require_empty"]["fresh"] is True


def test_create_then_failed_load_says_the_table_exists_now(world):
    def boom():
        raise RuntimeError("value too long")

    world["outcome"]["fresh"] = boom
    job_id = _job([("fresh", "create", [], False)])

    pdl.run_pg_diff_load_job(job_id)

    status, message = _items(job_id)["fresh"]
    assert status == "failed"
    assert "создана" in message and "value too long" in message


def test_stop_after_create_says_the_table_exists_now(world):
    def cancelled():
        world["stop"] = True
        raise QueryCanceledError("canceling statement due to user request")

    world["outcome"]["fresh"] = cancelled
    job_id = _job([("fresh", "create", [], False)])

    pdl.run_pg_diff_load_job(job_id)

    status, message = _items(job_id)["fresh"]
    assert status == "cancelled" and "создана" in message


def test_runner_loads_tables_with_identity_and_generated_columns(monkeypatch):
    """Настоящие load_diff / load_full на фейках: total — вычисляемая."""
    opened = []
    flags = [("attgenerated", [("id", "", "a"), ("v", "", ""),
                               ("total", "s", "")])]

    def fake_open(cid, readonly=False):
        conn = FakeConn(responses=flags + [("INSERT INTO", [(1,)])],
                        copy_out=b"1\tx\n")
        conn.readonly = readonly
        opened.append(conn)
        return conn

    monkeypatch.setattr(pdl, "open_pg", fake_open)
    monkeypatch.setattr(pdl, "table_column_types", lambda conn, s, t: {
        "id": "integer", "v": "text", "total": "integer"})
    monkeypatch.setattr(common, "is_stop_requested", lambda job_id: False)
    job_id = _job([("orders", "diff", ["id"], True),
                   ("log", "full", [], True)])

    pdl.run_pg_diff_load_job(job_id)

    assert _items(job_id) == {
        "orders": ("done", "insert=1; update=0; delete=0"),
        "log": ("done", "truncate+insert=1"),
    }
    dst = opened[1]
    assert dst.copies == [
        'COPY "opsentri_sync_stage"."stg_%d_1" ("id", "v") FROM STDIN' % job_id,
        'COPY "s"."log" ("id", "v") FROM STDIN']
    assert any(t.startswith('INSERT INTO "s"."orders" ("id", "v") OVERRIDING '
                            'SYSTEM VALUE') for t, _ in dst.executed)
