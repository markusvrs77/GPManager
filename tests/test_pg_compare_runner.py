# -*- coding: utf-8 -*-
"""
Раннер pg_compare на фейковых соединениях и временной SQLite:
результаты по таблицам, ключи, ошибки и стоп.
"""

import pytest
from psycopg2.extensions import QueryCanceledError

import modules.pg_compare as cmp
import modules.pg_sync_common as common
import modules.table_catalog as table_catalog
from job_manager import create_job, get_job, get_job_items
from tests.pg_fakes import FakeConn

SAME = {"status": "same", "src_rows": 5, "dst_rows": 5, "to_insert": 0,
        "to_update": 0, "to_delete": 0, "message": None}


@pytest.fixture
def world(monkeypatch):
    """Две фейковые базы; compare_table запоминает, с каким ключом звали."""
    state = {"opened": [], "calls": [], "stop": False, "outcome": {},
             "columns": {}, "dst_used": [],
             # годные уникальные индексы: NOT NULL, не частичные, без выражений
             "valid_uk": {"with_uk": [["code"]]}}

    def valid_uk(params):
        return [(cols,) for cols in state["valid_uk"].get(params[1], [])]

    def fake_open(cid, readonly=False):
        conn = FakeConn(responses=[("indpred IS NULL", valid_uk)])
        conn.readonly = readonly
        state["opened"].append(conn)
        return conn

    def fake_columns(conn, schema, table):
        side = "src" if conn.readonly else "dst"
        return state["columns"].get((side, table), ["id", "name", "code"])

    def fake_compare(src, dst, schema, table, key_columns):
        state["calls"].append((table, list(key_columns)))
        state["dst_used"].append(dst)
        outcome = state["outcome"].get(table, SAME)
        if callable(outcome):
            return outcome()
        return dict(outcome)

    monkeypatch.setattr(cmp, "open_pg", fake_open)
    monkeypatch.setattr(cmp, "table_columns", fake_columns)
    monkeypatch.setattr(cmp, "compare_table", fake_compare)
    monkeypatch.setattr(common, "is_stop_requested",
                        lambda job_id: state["stop"])
    monkeypatch.setattr(
        table_catalog, "fetch_unique_indexes",
        lambda cid, tables: ({("s", "with_pk"): ["id"]},
                             {("s", "with_uk"): [["code", "name"], ["code"]]}),
    )
    return state


def _job(tables):
    config = {"source_connection_id": 11, "dest_connection_id": 12,
              "tables": [dict(schema="s", table=t, in_src=src, in_dst=dst)
                         for t, src, dst in tables]}
    # строки задачи create_job заводит сам по config["tables"]
    return create_job("pg_compare", 11, dict(config, item_action="COMPARE"))


def _by_table(job_id):
    return {r["table"]: r for r in cmp.get_results(job_id)}


def test_each_table_gets_a_result_and_a_done_item(world):
    job_id = _job([("with_pk", True, True), ("only_src", True, False),
                   ("only_dst", False, True)])

    cmp.run_pg_compare_job(job_id)

    results = _by_table(job_id)
    assert {t: r["status"] for t, r in results.items()} == {
        "with_pk": "same", "only_src": "no_dest", "only_dst": "no_source"}
    assert results["with_pk"]["key_columns"] == ["id"]
    assert results["with_pk"]["key_source"] == "pk"
    assert results["with_pk"]["src_rows"] == 5
    assert [i["status"] for i in get_job_items(job_id)] == ["done"] * 3
    assert get_job(job_id)["status"] == "done"
    # отсутствующие таблицы данных не читают
    assert world["calls"] == [("with_pk", ["id"])]


def test_source_is_opened_read_only_and_both_closed(world):
    job_id = _job([("with_pk", True, True)])

    cmp.run_pg_compare_job(job_id)

    src, dst = world["opened"]
    assert src.readonly is True and dst.readonly is False
    assert src.closed and dst.closed


def test_key_chain_unique_index_then_saved_key(world):
    table_catalog.save_sync_key(11, "s", "saved", ["name"], "manual")
    job_id = _job([("with_uk", True, True), ("saved", True, True),
                   ("bare", True, True)])

    cmp.run_pg_compare_job(job_id)

    results = _by_table(job_id)
    assert (results["with_uk"]["key_columns"],
            results["with_uk"]["key_source"]) == (["code"], "unique_index")
    assert (results["saved"]["key_columns"],
            results["saved"]["key_source"]) == (["name"], "sync_keys")
    assert (results["bare"]["key_columns"],
            results["bare"]["key_source"]) == ([], None)


def test_key_missing_in_dest_is_not_used(world):
    world["columns"][("dst", "with_pk")] = ["name", "code"]
    job_id = _job([("with_pk", True, True)])

    cmp.run_pg_compare_job(job_id)

    assert world["calls"] == [("with_pk", [])]


def test_error_in_one_table_does_not_stop_the_rest(world):
    def boom():
        raise RuntimeError("relation is locked")

    world["outcome"]["bad"] = boom
    job_id = _job([("bad", True, True), ("with_pk", True, True)])

    cmp.run_pg_compare_job(job_id)

    results = _by_table(job_id)
    assert results["bad"]["status"] == "error"
    assert "locked" in results["bad"]["message"]
    assert results["with_pk"]["status"] == "same"
    items = {i["table_name"]: i["status"] for i in get_job_items(job_id)}
    assert items == {"bad": "failed", "with_pk": "done"}
    assert get_job(job_id)["status"] == "failed"


def test_stop_between_tables_skips_the_rest(world):
    def first_then_stop():
        world["stop"] = True
        return dict(SAME)

    world["outcome"]["first"] = first_then_stop
    job_id = _job([("first", True, True), ("second", True, True),
                   ("third", True, True)])

    cmp.run_pg_compare_job(job_id)

    items = {i["table_name"]: i["status"] for i in get_job_items(job_id)}
    assert items == {"first": "done", "second": "skipped", "third": "skipped"}
    assert get_job(job_id)["status"] == "cancelled"
    assert list(_by_table(job_id)) == ["first"]


def test_stop_during_a_query_cancels_it(world):
    def cancelled_mid_query():
        world["stop"] = True
        raise QueryCanceledError("canceling statement due to user request")

    world["outcome"]["big"] = cancelled_mid_query
    job_id = _job([("big", True, True), ("next", True, True)])

    cmp.run_pg_compare_job(job_id)

    assert _by_table(job_id)["big"]["status"] == "cancelled"
    items = {i["table_name"]: i["status"] for i in get_job_items(job_id)}
    assert items == {"big": "cancelled", "next": "skipped"}
    assert get_job(job_id)["status"] == "cancelled"
    assert all(c.cancelled >= 1 for c in world["opened"])
    assert all(c.rollbacks >= 1 for c in world["opened"])


def test_latest_compare_job_is_per_connection_pair(world):
    older = _job([("with_pk", True, True)])
    other_pair = create_job("pg_compare", 11, {
        "source_connection_id": 11, "dest_connection_id": 99, "tables": []})
    newest = _job([("with_pk", True, True)])

    assert cmp.latest_compare_job(11, 12)["id"] == newest
    assert cmp.latest_compare_job(11, 99)["id"] == other_pair
    assert cmp.latest_compare_job(12, 11) is None
    assert older < newest


def test_nullable_or_partial_unique_index_is_not_a_key(world):
    world["valid_uk"] = {}
    table_catalog.save_sync_key(11, "s", "with_uk", ["name"], "manual")
    job_id = _job([("with_uk", True, True)])

    cmp.run_pg_compare_job(job_id)

    row = _by_table(job_id)["with_uk"]
    assert (row["key_columns"], row["key_source"]) == (["name"], "sync_keys")


def test_failed_rollback_reopens_the_dest_connection(world):
    def boom():
        world["opened"][1].rollback_error = RuntimeError("connection lost")
        raise RuntimeError("server closed the connection")

    world["outcome"]["bad"] = boom
    job_id = _job([("bad", True, True), ("with_pk", True, True)])

    cmp.run_pg_compare_job(job_id)

    broken = world["opened"][1]
    assert world["dst_used"][1] is not broken
    assert broken.closed
    assert _by_table(job_id)["with_pk"]["status"] == "same"
