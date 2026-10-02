# -*- coding: utf-8 -*-
"""
Сравнение по диапазонам на живом PostgreSQL — opt-in.

Запуск: PGCMP_LIVE_DSN_SRC и PGCMP_LIVE_DSN_DST (как в
test_pg_compare_live.py). Без них тесты пропускаются. Маленькие таблицы,
порог и размеры диапазонов понижены; весь раннер run_pg_compare_job идёт
на временной SQLite, а его числа сверяются с построчным compare_table
всей таблицы.
"""

import os

import psycopg2
import pytest

import modules.pg_compare as cmp
import modules.pg_ranges as pr
from job_manager import create_job, get_job
from modules.pg_sync_common import normalize_session

DSN_SRC = os.environ.get("PGCMP_LIVE_DSN_SRC")
DSN_DST = os.environ.get("PGCMP_LIVE_DSN_DST")

pytestmark = pytest.mark.skipif(
    not (DSN_SRC and DSN_DST),
    reason="PGCMP_LIVE_DSN_SRC / PGCMP_LIVE_DSN_DST не заданы",
)

SCHEMA = "pgrng_live_%d" % os.getpid()
S = 'generate_series(1, 500) AS i'

# таблица: (DDL источника, DDL приёмника, наполнение, правки приёмника, ключ)
TABLES = {
    "int_key": (
        "(id int PRIMARY KEY, v text)", None,
        "SELECT i, 'v' || i FROM " + S,
        ["UPDATE {t} SET v = 'X' WHERE id IN (7, 250)",
         "DELETE FROM {t} WHERE id = 300",
         # вне [min, max] источника
         "INSERT INTO {t} VALUES (-5, 'low'), (600, 'high')"],
        (["id"], "pk"),
    ),
    "text_key": (
        '(code text COLLATE "C" PRIMARY KEY, v int)',
        '(code text COLLATE "und-x-icu" PRIMARY KEY, v int)',
        "SELECT (ARRAY['A','b','C','d'])[i % 4 + 1] || md5(i::text), i "
        "FROM " + S,
        ["UPDATE {t} SET v = -1 WHERE v IN (3, 333)",
         "DELETE FROM {t} WHERE v = 100",
         "INSERT INTO {t} VALUES ('0zzz', 1), ('~tail', 2), ('Zed', 3)"],
        (["code"], "pk"),
    ),
    "composite": (
        "(a int, b text, v text, PRIMARY KEY (a, b))", None,
        "SELECT i / 10, 'x' || (i % 10), 'v' || i FROM " + S,
        ["UPDATE {t} SET v = 'X' WHERE a = 20 AND b = 'x3'",
         "DELETE FROM {t} WHERE a = 40",
         "INSERT INTO {t} VALUES (20, 'new', 'n')"],
        (["a", "b"], "pk"),
    ),
    "uuid_key": (
        "(id uuid PRIMARY KEY, v int)", None,
        "SELECT md5(i::text)::uuid, i FROM " + S,
        ["UPDATE {t} SET v = -1 WHERE v IN (10, 490)",
         "DELETE FROM {t} WHERE v = 250",
         "INSERT INTO {t} VALUES ('00000000-0000-0000-0000-000000000000', 0),"
         " ('ffffffff-ffff-ffff-ffff-ffffffffffff', 0)"],
        (["id"], "pk"),
    ),
    "no_key_date": (
        "(d date, v int)", None,
        "SELECT CASE WHEN i % 50 = 0 THEN NULL "
        "ELSE date '2024-01-01' + (i % 200) END, i % 7 FROM " + S,
        ["UPDATE {t} SET v = 99 WHERE d = date '2024-02-01'",
         "DELETE FROM {t} WHERE d IS NULL AND v = 1",
         "INSERT INTO {t} VALUES (NULL, 5), (date '2023-01-01', 1), "
         "(date '2030-01-01', 1), (date '2024-03-01', 3)"],
        ([], None),
    ),
    # NULL в колонке нарезки у ключа из sync_keys: одинаковые строки с
    # NULL-ключом построчно — вставка и удаление
    "null_key": (
        "(k int, v text)", None,
        "SELECT CASE WHEN i = 25 THEN NULL ELSE i END, 'v' || i FROM " + S,
        ["UPDATE {t} SET v = 'X' WHERE k = 42"],
        (["k"], "sync_keys"),
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

    for name, (ddl_src, ddl_dst, fill, edits, _key) in TABLES.items():
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


@pytest.fixture(scope="module")
def live():
    _setup(DSN_SRC, "src")
    _setup(DSN_DST, "dst")
    yield
    for dsn in (DSN_SRC, DSN_DST):
        conn = _admin(dsn)
        conn.cursor().execute("DROP SCHEMA IF EXISTS %s CASCADE" % SCHEMA)
        conn.close()


@pytest.fixture
def small_chunks(monkeypatch):
    monkeypatch.setattr(pr, "CHUNK_MIN_ROWS", 0)
    monkeypatch.setattr(pr, "TOP_CHUNKS", 4)
    monkeypatch.setattr(pr, "LEAF_ROWS", 20)
    monkeypatch.setattr(pr, "SPLIT_FANOUT", 4)
    monkeypatch.setattr(cmp, "open_pg", lambda cid, readonly=False: _open(
        DSN_SRC if int(cid) == 1 else DSN_DST, readonly))
    monkeypatch.setattr(cmp, "resolve_key_candidates", lambda sid, tables: {
        key: ([{"columns": TABLES[key[1]][4][0],
                "source": TABLES[key[1]][4][1]}]
              if TABLES[key[1]][4][0] else [])
        for key in tables})


def _numbers(row):
    return (row["status"], row["src_rows"], row["dst_rows"],
            row["to_insert"], row["to_update"], row["to_delete"])


def _full(name):
    src, dst = _open(DSN_SRC, True), _open(DSN_DST, False)
    try:
        key, source = TABLES[name][4]
        return cmp.compare_table(src, dst, SCHEMA, name, key,
                                 key_source=source)
    finally:
        src.close()
        dst.close()


@pytest.mark.parametrize("parallel", [1, 4])
def test_ranges_give_the_same_numbers_as_row_by_row(live, small_chunks,
                                                    parallel):
    job_id = create_job("pg_compare", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "parallel": parallel, "item_action": "COMPARE",
        "tables": [{"schema": SCHEMA, "table": t, "in_src": True,
                    "in_dst": True} for t in TABLES]})

    cmp.run_pg_compare_job(job_id)

    assert get_job(job_id)["status"] == "done"
    results = {r["table"]: r for r in cmp.get_results(job_id)}

    for name in TABLES:
        full = _full(name)
        assert full["status"] == "differs", name
        assert _numbers(results[name]) == _numbers(full), name
        chunked = results[name]["chunked"]
        assert chunked and chunked["checked"] > 1, name
        leaves = cmp.get_mismatched_ranges(job_id, SCHEMA, name)
        assert len(leaves) == chunked["mismatched"] >= 1, name
        if name == "text_key":
            # collation сторон разный ("C" и ICU) — режим корзин: листья
            # сравниваются одним проходом, счётчиков по листу нет
            assert {r["mode"] for r in leaves} == {"bucket"}, name
            continue
        assert {r["mode"] for r in leaves} == {"range"}, name
        assert sum(r["to_insert"] + r["to_update"] + r["to_delete"]
                   for r in leaves) == (full["to_insert"] + full["to_update"]
                                        + full["to_delete"]), name


def test_leaf_predicates_rebuilt_from_storage_select_the_same_rows(
        live, small_chunks):
    """Границы из pg_compare_ranges годятся для range_predicate."""
    job_id = create_job("pg_compare", 1, {
        "source_connection_id": 1, "dest_connection_id": 2, "parallel": 2,
        "item_action": "COMPARE",
        "tables": [{"schema": SCHEMA, "table": t, "in_src": True,
                    "in_dst": True} for t in TABLES]})
    cmp.run_pg_compare_job(job_id)
    src, dst = _open(DSN_SRC, True), _open(DSN_DST, False)

    try:
        for name in TABLES:
            for leaf in cmp.get_mismatched_ranges(job_id, SCHEMA, name):
                # общий построитель: диапазон или корзина md5-префикса
                pred = pr.leaves_predicate("t", [leaf])
                assert pr.range_checksum(src, SCHEMA, name, ["v"], pred)[0] \
                    == leaf["src_rows"], (name, leaf)
                assert pr.range_checksum(dst, SCHEMA, name, ["v"], pred)[0] \
                    == leaf["dst_rows"], (name, leaf)
    finally:
        src.close()
        dst.close()
