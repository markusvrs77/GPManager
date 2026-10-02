# -*- coding: utf-8 -*-
"""
Загрузка разницы по диапазонам на живом PostgreSQL — opt-in.

Запуск: PGCMP_LIVE_DSN_SRC и PGCMP_LIVE_DSN_DST (как в
test_pg_compare_live.py). Без них тесты пропускаются. Маленькие таблицы,
порог и размеры диапазонов понижены. Весь путь на временной SQLite:
run_pg_compare_job → run_pg_diff_load_job (diff, delete_missing=True) →
повторный run_pg_compare_job.
"""

import os

import psycopg2
import pytest

import modules.pg_compare as cmp
import modules.pg_diff_load as pdl
import modules.pg_ranges as pr
from job_manager import create_job, get_job, get_job_items
from modules.pg_sync_common import normalize_session

DSN_SRC = os.environ.get("PGCMP_LIVE_DSN_SRC")
DSN_DST = os.environ.get("PGCMP_LIVE_DSN_DST")

pytestmark = pytest.mark.skipif(
    not (DSN_SRC and DSN_DST),
    reason="PGCMP_LIVE_DSN_SRC / PGCMP_LIVE_DSN_DST не заданы",
)

SCHEMA = "pgdl_rng_live_%d" % os.getpid()
S = "generate_series(1, 500) AS i"

# таблица: (DDL источника, DDL приёмника, наполнение, правки приёмника,
#           ключ, колонка-«ловушка» для правки вне листьев)
TABLES = {
    "int_key": (
        "(id int PRIMARY KEY, v text)", None,
        "SELECT i, 'v' || i FROM " + S,
        ["UPDATE {t} SET v = 'X' WHERE id IN (7, 250)",
         "DELETE FROM {t} WHERE id = 300",
         "INSERT INTO {t} VALUES (-5, 'low'), (600, 'high')"],
        ["id"], "v",
    ),
    "text_key": (
        '(code text COLLATE "C" PRIMARY KEY, v text)',
        '(code text COLLATE "und-x-icu" PRIMARY KEY, v text)',
        "SELECT (ARRAY['A','b','C','d'])[i % 4 + 1] || md5(i::text), "
        "'v' || i FROM " + S,
        ["UPDATE {t} SET v = 'X' WHERE v IN ('v3', 'v333')",
         "DELETE FROM {t} WHERE v = 'v100'",
         "INSERT INTO {t} VALUES ('0zzz', 'n1'), ('~tail', 'n2'), "
         "('Zed', 'n3')"],
        ["code"], "v",
    ),
    "no_key": (
        "(d date, v text)", None,
        "SELECT CASE WHEN i % 50 = 0 THEN NULL "
        "ELSE date '2024-01-01' + (i % 200) END, 'v' || (i % 7) FROM " + S,
        ["UPDATE {t} SET v = 'X' WHERE d = date '2024-02-01'",
         "DELETE FROM {t} WHERE d IS NULL AND v = 'v1'",
         # лишняя копия существующей строки и строки вне [min, max]
         "INSERT INTO {t} SELECT d, v FROM {t} WHERE d = date '2024-05-01' "
         "LIMIT 1",
         "INSERT INTO {t} VALUES (NULL, 'v5'), (date '2023-01-01', 'v1'), "
         "(date '2030-01-01', 'v1')"],
        [], "v",
    ),
}


def _admin(dsn):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def _setup(dsn, side):
    conn = _admin(dsn)
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS %s CASCADE" % SCHEMA)
    cur.execute("CREATE SCHEMA %s" % SCHEMA)

    for name, (ddl_src, ddl_dst, fill, edits, _key, _trap) in TABLES.items():
        t = "%s.%s" % (SCHEMA, name)
        ddl = ddl_dst if side == "dst" and ddl_dst else ddl_src
        cur.execute("CREATE TABLE %s %s" % (t, ddl))
        cur.execute("INSERT INTO %s %s" % (t, fill))
        if side == "dst":
            for stmt in edits:
                cur.execute(stmt.format(t=t))
        cur.execute("ANALYZE %s" % t)

    conn.close()


def _open(dsn, readonly):
    conn = psycopg2.connect(dsn)
    if readonly:
        conn.set_session(readonly=True)
    normalize_session(conn, readonly=readonly)
    return conn


def _open_by_id(cid, readonly=False):
    return _open(DSN_SRC if int(cid) == 1 else DSN_DST, readonly)


@pytest.fixture
def live(monkeypatch):
    _setup(DSN_SRC, "src")
    _setup(DSN_DST, "dst")

    monkeypatch.setattr(pr, "CHUNK_MIN_ROWS", 0)
    monkeypatch.setattr(pr, "TOP_CHUNKS", 4)
    monkeypatch.setattr(pr, "LEAF_ROWS", 20)
    monkeypatch.setattr(pr, "SPLIT_FANOUT", 4)
    monkeypatch.setattr(cmp, "open_pg", _open_by_id)
    monkeypatch.setattr(pdl, "open_pg", _open_by_id)
    monkeypatch.setattr(cmp, "resolve_key_candidates", lambda sid, tables: {
        key: ([{"columns": TABLES[key[1]][4], "source": "pk"}]
              if TABLES[key[1]][4] else [])
        for key in tables})

    # сколько строк ушло в staging по таблицам
    staged = {}
    real_copy = pdl.stream_copy

    def counting_copy(src, dst, select_sql, dst_table, columns):
        rows = real_copy(src, dst, select_sql, dst_table, columns)
        staged.setdefault(dst_table.strings[-1], 0)
        staged[dst_table.strings[-1]] += rows
        return rows

    monkeypatch.setattr(pdl, "stream_copy", counting_copy)
    yield staged

    for dsn in (DSN_SRC, DSN_DST):
        conn = _admin(dsn)
        conn.cursor().execute("DROP SCHEMA IF EXISTS %s CASCADE" % SCHEMA)
        conn.close()


def _compare():
    job_id = create_job("pg_compare", 1, {
        "source_connection_id": 1, "dest_connection_id": 2, "parallel": 2,
        "item_action": "COMPARE",
        "tables": [{"schema": SCHEMA, "table": t, "in_src": True,
                    "in_dst": True} for t in TABLES]})
    cmp.run_pg_compare_job(job_id)
    assert get_job(job_id)["status"] == "done"
    return job_id, {r["table"]: r for r in cmp.get_results(job_id)}


def _load(compare_job_id, results):
    job_id = create_job("pg_diff_load", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "compare_job_id": compare_job_id, "delete_missing": True,
        "tables": [{"schema": SCHEMA, "table": t, "action": "diff",
                    "key_columns": results[t]["key_columns"], "in_dst": True}
                   for t in TABLES],
        "expected": []})
    pdl.run_pg_diff_load_job(job_id)
    assert get_job(job_id)["status"] == "done"
    return {i["table_name"]: i["error_message"] for i in get_job_items(job_id)}


def _query(dsn, text, params=None):
    conn = _admin(dsn)
    try:
        cur = conn.cursor()
        cur.execute(text, params)
        return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def _source_md5():
    return _query(DSN_SRC, "SELECT %s" % ", ".join(
        "(SELECT md5(string_agg(md5(x::text), '' ORDER BY md5(x::text))) "
        "FROM %s.%s x)" % (SCHEMA, t) for t in TABLES))


def _set_traps(compare_job_id):
    """
    После сравнения правим по строке приёмника вне листьев: загрузка
    по листьям их не видит и не трогает (загрузка всей таблицы вернула бы).
    -> {таблица: (ctid-независимый признак, исходное значение)}
    """
    conn = _admin(DSN_DST)
    traps = {}
    try:
        cur = conn.cursor()
        for name in TABLES:
            leaves = cmp.get_mismatched_ranges(compare_job_id, SCHEMA, name)
            assert leaves, name
            outside = pdl.build_ranges_where("t", leaves)
            trap = TABLES[name][5]
            cur.execute(psycopg2.sql.SQL(
                "SELECT t.ctid::text, t.{c} FROM {tbl} AS t WHERE NOT ({w}) "
                "AND t.{c} IS NOT NULL LIMIT 1").format(
                    c=psycopg2.sql.Identifier(trap),
                    tbl=psycopg2.sql.Identifier(SCHEMA, name),
                    w=outside).as_string(conn))
            ctid, value = cur.fetchone()
            cur.execute(psycopg2.sql.SQL(
                "UPDATE {tbl} SET {c} = 'TRAP' WHERE ctid = %s::tid "
                "RETURNING ctid::text").format(
                    c=psycopg2.sql.Identifier(trap),
                    tbl=psycopg2.sql.Identifier(SCHEMA, name)).as_string(conn),
                (ctid,))
            traps[name] = (cur.fetchone()[0], value)
    finally:
        conn.close()
    return traps


def test_load_by_ranges_aligns_dest_and_leaves_the_rest_alone(live):
    staged = live
    before = _source_md5()

    compared, results = _compare()
    leaves = {t: cmp.get_mismatched_ranges(compared, SCHEMA, t)
              for t in TABLES}
    for name in TABLES:
        assert results[name]["status"] == "differs", name
        assert results[name]["chunked"]["mismatched"] == len(leaves[name])

    traps = _set_traps(compared)
    messages = _load(compared, results)

    for name in TABLES:
        assert messages[name].endswith(
            "; по диапазонам: %d" % len(leaves[name])), messages[name]

    # в staging — ровно строки источника из листьев, а не вся таблица
    assert sorted(staged.values()) == sorted(
        sum(r["src_rows"] for r in leaves[t]) for t in TABLES)
    assert all(v < 500 for v in staged.values()), staged

    # строки приёмника вне листьев не тронуты
    for name, (ctid, value) in traps.items():
        trap = TABLES[name][5]
        rows = _query(DSN_DST, "SELECT %s FROM %s.%s WHERE ctid = '%s'::tid"
                      % (trap, SCHEMA, name, ctid))
        assert rows == [("TRAP",)], name
        _query(DSN_DST, "UPDATE %s.%s SET %s = %%s WHERE ctid = '%s'::tid"
               % (SCHEMA, name, trap, ctid), (value,))

    # ловушки сняты — приёмник совпадает с источником
    _again, results = _compare()
    assert {t: results[t]["status"] for t in TABLES} == {
        t: "same" for t in TABLES}
    assert _source_md5() == before
