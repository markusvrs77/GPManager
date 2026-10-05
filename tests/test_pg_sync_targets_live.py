# -*- coding: utf-8 -*-
"""
Загрузка в другую таблицу приёмника на живом PostgreSQL — opt-in
(PGCMP_LIVE_DSN_SRC / PGCMP_LIVE_DSN_DST; без них — skip).

* сравнение с целью другого имени (и с другим порядком колонок) — и
  построчно, и по диапазонам; загрузка разницы с delete_missing=True;
  повторное сравнение = same; одноимённая таблица приёмника не тронута;
* «создать и залить» создаёт цель по структуре источника;
* перенос copy_pipe: COPY с явным списком колонок в цель другого имени,
  отсутствующая цель создаётся;
* источник не меняется.
"""

import os

import psycopg2
import pytest

import modules.pg_compare as cmp
import modules.pg_diff_load as pdl
import modules.pg_ranges as pr
import modules.sync_transport as st
from job_manager import create_job, get_job, get_job_items
from modules.pg_sync_common import normalize_session

DSN_SRC = os.environ.get("PGCMP_LIVE_DSN_SRC")
DSN_DST = os.environ.get("PGCMP_LIVE_DSN_DST")

pytestmark = pytest.mark.skipif(
    not (DSN_SRC and DSN_DST),
    reason="PGCMP_LIVE_DSN_SRC / PGCMP_LIVE_DSN_DST не заданы",
)

SCHEMA = "pgtgt_live_%d" % os.getpid()
ARCH = SCHEMA + "_arch"


def _admin(dsn):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def _run(dsn, *statements):
    conn = _admin(dsn)
    try:
        cur = conn.cursor()
        for stmt in statements:
            cur.execute(stmt.format(s=SCHEMA, a=ARCH))
    finally:
        conn.close()


def _query(dsn, text, params=None):
    conn = _admin(dsn)
    try:
        cur = conn.cursor()
        cur.execute(text.format(s=SCHEMA, a=ARCH), params)
        return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def _rows(dsn, table, columns="id, name, amount"):
    return _query(dsn, "SELECT %s FROM %s ORDER BY 1" % (columns, table))


def _open(dsn, readonly):
    conn = psycopg2.connect(dsn)
    if readonly:
        conn.set_session(readonly=True)
    normalize_session(conn, readonly=readonly)
    return conn


def _open_by_id(cid, readonly=False):
    return _open(DSN_SRC if int(cid) == 1 else DSN_DST, readonly)


FILL = ("INSERT INTO {t} (id, name, amount) SELECT i, 'n' || i, i * 1.5 "
        "FROM generate_series(1, 300) AS i")


@pytest.fixture
def live(monkeypatch):
    for dsn in (DSN_SRC, DSN_DST):
        _run(dsn, "DROP SCHEMA IF EXISTS {s} CASCADE",
             "DROP SCHEMA IF EXISTS {a} CASCADE", "CREATE SCHEMA {s}")

    _run(DSN_SRC,
         "CREATE TABLE {s}.orders (id int PRIMARY KEY, name text, "
         "amount numeric(10,2))",
         FILL.replace("{t}", "{s}.orders"),
         "CREATE TABLE {s}.ranged (id int PRIMARY KEY, name text, "
         "amount numeric(10,2))",
         FILL.replace("{t}", "{s}.ranged"),
         "CREATE TABLE {s}.fresh (id int PRIMARY KEY, name text NOT NULL, "
         "amount numeric(10,2))",
         FILL.replace("{t}", "{s}.fresh"),
         "ANALYZE {s}.orders", "ANALYZE {s}.ranged")

    # приёмник: цели другого имени, порядок колонок другой; одноимённая
    # orders — ловушка, её трогать нельзя
    _run(DSN_DST,
         "CREATE SCHEMA {a}",
         "CREATE TABLE {a}.orders_copy (amount numeric(10,2), name text, "
         "id int PRIMARY KEY)",
         FILL.replace("{t}", "{a}.orders_copy"),
         "UPDATE {a}.orders_copy SET name = 'changed' WHERE id IN (5, 6)",
         "DELETE FROM {a}.orders_copy WHERE id IN (10, 11, 12)",
         "INSERT INTO {a}.orders_copy (id, name, amount) VALUES (9001, 'x', 1)",
         "CREATE TABLE {s}.orders (id int PRIMARY KEY, name text, "
         "amount numeric(10,2))",
         "INSERT INTO {s}.orders VALUES (1, 'trap', 0)",
         "CREATE TABLE {s}.ranged_copy (name text, id int PRIMARY KEY, "
         "amount numeric(10,2))",
         FILL.replace("{t}", "{s}.ranged_copy"),
         "UPDATE {s}.ranged_copy SET amount = -1 WHERE id IN (7, 250)",
         "DELETE FROM {s}.ranged_copy WHERE id = 100",
         "ANALYZE {a}.orders_copy", "ANALYZE {s}.ranged_copy")

    monkeypatch.setattr(cmp, "open_pg", _open_by_id)
    monkeypatch.setattr(pdl, "open_pg", _open_by_id)
    monkeypatch.setattr(cmp, "resolve_key_candidates", lambda sid, tables: {
        key: [{"columns": ["id"], "source": "pk"}] for key in tables})
    yield
    for dsn in (DSN_SRC, DSN_DST):
        _run(dsn, "DROP SCHEMA IF EXISTS {s} CASCADE",
             "DROP SCHEMA IF EXISTS {a} CASCADE")


def _targets():
    return {"%s.orders" % SCHEMA: "%s.orders_copy" % ARCH,
            "%s.ranged" % SCHEMA: "ranged_copy",
            "%s.fresh" % SCHEMA: "fresh_new"}


def _compare(tables):
    src, dst = _open(DSN_SRC, True), _open(DSN_DST, False)
    try:
        expanded = cmp.expand_selection(
            src, dst, [], [{"schema": SCHEMA, "table": t} for t in tables],
            targets={k: v for k, v in _targets().items()
                     if k.split(".", 1)[1] in tables})
    finally:
        src.close()
        dst.close()

    targets = {"%s.%s" % (t["schema"], t["table"]): t["target"]
               for t in expanded if t.get("target")}
    job_id = create_job("pg_compare", 1, {
        "source_connection_id": 1, "dest_connection_id": 2, "parallel": 2,
        "item_action": "COMPARE", "tables": expanded, "targets": targets})
    cmp.run_pg_compare_job(job_id)
    assert get_job(job_id)["status"] == "done", get_job(job_id)
    return job_id, targets, {r["table"]: r for r in cmp.get_results(job_id)}


def test_compare_and_load_into_tables_with_other_names(live, monkeypatch):
    # ranged сравнивается по диапазонам, orders — построчно
    monkeypatch.setattr(pr, "should_chunk",
                        lambda conn, s, t: t == "ranged")
    monkeypatch.setattr(pr, "TOP_CHUNKS", 4)
    monkeypatch.setattr(pr, "LEAF_ROWS", 20)
    monkeypatch.setattr(pr, "SPLIT_FANOUT", 4)
    src_before = {t: _rows(DSN_SRC, "{s}.%s" % t)
                  for t in ("orders", "ranged", "fresh")}

    compared, targets, results = _compare(["orders", "ranged", "fresh"])

    orders, ranged, fresh = (results["orders"], results["ranged"],
                             results["fresh"])
    assert orders["target"] == "%s.orders_copy" % ARCH
    assert (orders["status"], orders["to_insert"], orders["to_update"],
            orders["to_delete"]) == ("differs", 3, 2, 1)
    assert ranged["target"] == "%s.ranged_copy" % SCHEMA
    assert ranged["chunked"] and ranged["status"] == "differs"
    assert (ranged["to_insert"], ranged["to_update"],
            ranged["to_delete"]) == (1, 2, 0)
    assert fresh["status"] == "no_dest"
    assert fresh["target"] == "%s.fresh_new" % SCHEMA

    job_id = create_job("pg_diff_load", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "compare_job_id": compared, "delete_missing": True,
        "targets": targets,
        "tables": [
            {"schema": SCHEMA, "table": "orders", "action": "diff",
             "key_columns": ["id"], "in_dst": True},
            {"schema": SCHEMA, "table": "ranged", "action": "diff",
             "key_columns": ["id"], "in_dst": True},
            {"schema": SCHEMA, "table": "fresh", "action": "create",
             "key_columns": [], "in_dst": False}],
        "expected": []})
    pdl.run_pg_diff_load_job(job_id)

    assert get_job(job_id)["status"] == "done", [
        (i["table_name"], i["error_message"]) for i in get_job_items(job_id)]
    items = {i["table_name"]: i["error_message"]
             for i in get_job_items(job_id)}
    assert items["orders"].startswith("insert=3; update=2; delete=1")
    assert "по диапазонам" in items["ranged"]
    assert items["fresh"].startswith("create+insert=300")

    # цели совпадают с источником; одноимённая orders приёмника не тронута
    assert _rows(DSN_DST, "{a}.orders_copy") == src_before["orders"]
    assert _rows(DSN_DST, "{s}.ranged_copy") == src_before["ranged"]
    assert _rows(DSN_DST, "{s}.fresh_new") == src_before["fresh"]
    assert _rows(DSN_DST, "{s}.orders") == [(1, "trap", 0)]
    assert _query(DSN_DST, "SELECT count(*) FROM pg_tables WHERE schemaname "
                           "= %s AND tablename = 'fresh'",
                  (SCHEMA,)) == [(0,)]
    # NOT NULL и PK цели — по источнику
    assert _query(DSN_DST, "SELECT attnotnull FROM pg_attribute WHERE "
                           "attrelid = '{s}.fresh_new'::regclass AND "
                           "attname = 'name'") == [(True,)]

    # источник не менялся
    for t, rows in src_before.items():
        assert _rows(DSN_SRC, "{s}.%s" % t) == rows

    _again, _t, again = _compare(["orders", "ranged", "fresh"])
    assert {t: r["status"] for t, r in again.items()} == {
        "orders": "same", "ranged": "same", "fresh": "same"}


def test_copy_pipe_loads_into_target_with_explicit_columns(live,
                                                           monkeypatch):
    cfgs = {1: {"db_type": "postgres", "dsn": DSN_SRC},
            2: {"db_type": "postgres", "dsn": DSN_DST}}
    monkeypatch.setattr(st, "get_connection_by_id", lambda cid: cfgs[cid])
    monkeypatch.setattr(st, "open_psycopg2_connection_by_cfg",
                        lambda cfg: psycopg2.connect(cfg["dsn"]))
    src_before = _rows(DSN_SRC, "{s}.orders")

    job_id = create_job("copy_pipe", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "truncate": True,
        "tables": [{"schema": SCHEMA, "table": "orders"},
                   {"schema": SCHEMA, "table": "fresh"}],
        "targets": {"%s.orders" % SCHEMA: "%s.orders_copy" % ARCH,
                    "%s.fresh" % SCHEMA: "%s.fresh_pipe" % SCHEMA}})

    st.run_copy_pipe_job(job_id)

    assert get_job(job_id)["status"] == "done", [
        (i["table_name"], i["error_message"]) for i in get_job_items(job_id)]
    # цель с другим порядком колонок: значения легли по именам
    assert _rows(DSN_DST, "{a}.orders_copy") == src_before
    # отсутствующая цель создана по структуре источника
    assert _rows(DSN_DST, "{s}.fresh_pipe") == _rows(DSN_SRC, "{s}.fresh")
    assert _rows(DSN_DST, "{s}.orders") == [(1, "trap", 0)]
    assert _rows(DSN_SRC, "{s}.orders") == src_before
