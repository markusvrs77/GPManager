# -*- coding: utf-8 -*-
"""
Раннер pg_compare на фейковых соединениях и временной SQLite:
результаты по таблицам, ключи, ошибки и стоп.
"""

import threading

import pytest
from psycopg2.extensions import QueryCanceledError

import modules.pg_compare as cmp
import modules.pg_sync_common as common
import modules.table_catalog as table_catalog
from job_manager import create_job, get_job, get_job_items
from tests.pg_fakes import FakeConn

REAL_COMPARE = cmp.compare_table

SAME = {"status": "same", "src_rows": 5, "dst_rows": 5, "to_insert": 0,
        "to_update": 0, "to_delete": 0, "message": None}


@pytest.fixture
def world(monkeypatch):
    """Две фейковые базы; compare_table запоминает, с каким ключом звали."""
    state = {"opened": [], "calls": [], "stop": False, "outcome": {},
             "columns": {}, "dst_used": [], "key_sources": {},
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

    def fake_compare(src, dst, schema, table, key_columns, key_source=None,
                     work_mem=None):
        state["calls"].append((table, list(key_columns)))
        state["key_sources"][table] = key_source
        state["dst_used"].append(dst)
        if state.get("hook"):
            state["hook"](table, src, dst)
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


def _job(tables, parallel=1):
    # parallel=1 — последовательный порядок, на нём стоят тесты ниже
    config = {"source_connection_id": 11, "dest_connection_id": 12,
              "parallel": parallel,
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
    # происхождение ключа доходит до сравнения: от него зависят проверки дублей
    assert world["key_sources"] == {"with_uk": "unique_index",
                                    "saved": "sync_keys", "bare": None}


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


# ------------------------------------------------------------------
# Параллельное сравнение
# ------------------------------------------------------------------

def _meet(world, n):
    """Первые n сравнений ждут друг друга: проходят, только если идут
    одновременно в n потоках."""
    barrier = threading.Barrier(n, timeout=5)
    started = []

    def hook(table, src, dst):
        started.append(table)
        if len(started) <= n:
            barrier.wait()

    world["hook"] = hook


def _names(n):
    return ["t%d" % i for i in range(n)]


def test_parallel_workers_compare_each_table_once(world):
    _meet(world, 3)
    names = _names(7)
    job_id = _job([(t, True, True) for t in names], parallel=3)

    cmp.run_pg_compare_job(job_id)

    assert sorted(t for t, _ in world["calls"]) == names
    statuses = {t: r["status"] for t, r in _by_table(job_id).items()}
    assert statuses == {t: "same" for t in names}
    assert [i["status"] for i in get_job_items(job_id)] == ["done"] * 7
    assert get_job(job_id)["status"] == "done"
    # пара соединений на воркер: источник read-only, приёмник нет
    assert len(world["opened"]) == 6
    assert sorted(c.readonly for c in world["opened"]) == [False] * 3 + [True] * 3
    assert all(c.closed for c in world["opened"])


def test_parallel_error_rolls_back_only_its_worker(world):
    _meet(world, 2)

    def boom():
        raise RuntimeError("relation is locked")

    world["outcome"]["bad"] = boom
    job_id = _job([("bad", True, True)] + [(t, True, True) for t in _names(4)],
                  parallel=2)

    cmp.run_pg_compare_job(job_id)

    results = _by_table(job_id)
    assert results["bad"]["status"] == "error"
    assert [results[t]["status"] for t in _names(4)] == ["same"] * 4
    items = {i["table_name"]: i["status"] for i in get_job_items(job_id)}
    assert items["bad"] == "failed" and items["t3"] == "done"
    job = get_job(job_id)
    assert job["status"] == "failed" and "1 таблиц" in job["error_message"]
    # откат — только у пары, на которой упала таблица
    assert sorted(c.rollbacks for c in world["opened"]) == [0, 0, 1, 1]


def test_parallel_stop_cancels_every_worker(world):
    _meet(world, 3)

    def long_query(table, src, dst):
        if table == "t0":
            world["stop"] = True
        # «долгий запрос»: обрывается только conn.cancel() сторожа
        if not dst.cancel_event.wait(5):
            raise AssertionError("запрос не отменён")
        raise QueryCanceledError("canceling statement due to user request")

    meet = world["hook"]
    world["hook"] = lambda t, s, d: (meet(t, s, d), long_query(t, s, d))
    names = _names(6)
    job_id = _job([(t, True, True) for t in names], parallel=3)

    cmp.run_pg_compare_job(job_id)

    items = {i["table_name"]: i["status"] for i in get_job_items(job_id)}
    assert sorted(items.values()) == ["cancelled"] * 3 + ["skipped"] * 3
    running = sorted(t for t, s in items.items() if s == "cancelled")
    statuses = {t: r["status"] for t, r in _by_table(job_id).items()}
    assert statuses == {t: "cancelled" for t in running}
    assert get_job(job_id)["status"] == "cancelled"
    assert len(world["opened"]) == 6
    assert all(c.cancelled >= 1 and c.closed for c in world["opened"])
    # ни одна таблица не начата дважды и не получила второй результат
    started = [t for t, _ in world["calls"]]
    assert sorted(started) == running
    assert len(cmp.get_results(job_id)) == 3


def test_reopened_source_is_closed_when_dest_reopen_fails(world, monkeypatch):
    def boom():
        for conn in world["opened"]:
            conn.rollback_error = RuntimeError("connection lost")
        raise RuntimeError("server closed the connection")

    world["outcome"]["bad"] = boom
    opened = world["opened"]
    fake_open = cmp.open_pg

    def open_or_fail(cid, readonly=False):
        if len(opened) == 3:  # переоткрытие приёмника после источника
            raise RuntimeError("could not connect")
        return fake_open(cid, readonly=readonly)

    monkeypatch.setattr(cmp, "open_pg", open_or_fail)
    job_id = _job([("bad", True, True), ("next", True, True)])

    cmp.run_pg_compare_job(job_id)

    assert len(opened) == 3
    assert all(c.closed for c in opened)
    assert get_job(job_id)["status"] == "failed"


def test_work_mem_budget_is_split_between_workers(world, monkeypatch):
    # настоящее compare_table: проверяем SQL, ушедший в приёмники
    monkeypatch.setattr(cmp, "compare_table", REAL_COMPARE)
    job_id = _job([(t, True, True) for t in _names(8)], parallel=8)

    cmp.run_pg_compare_job(job_id)

    dests = [c for c in world["opened"] if not c.readonly]
    assert len(dests) == 8
    for conn in dests:
        text = conn.sql_text()
        assert "SET LOCAL work_mem = '64MB'" in text
        assert "256MB" not in text
