# -*- coding: utf-8 -*-
"""
compare_table и dest_key_is_unique на живом PostgreSQL — opt-in.

Запуск: PGCMP_LIVE_DSN_SRC и PGCMP_LIVE_DSN_DST — DSN двух баз
(например "host=localhost port=55432 user=pgcheck dbname=src_db").
Без них тесты пропускаются. Таблицы создаются в отдельной схеме
pgcmp_live_<pid> обеих баз и удаляются после модуля.
"""

import os

import psycopg2
import pytest

import modules.pg_compare as cmp
from modules.pg_sync_common import normalize_session

DSN_SRC = os.environ.get("PGCMP_LIVE_DSN_SRC")
DSN_DST = os.environ.get("PGCMP_LIVE_DSN_DST")

pytestmark = pytest.mark.skipif(
    not (DSN_SRC and DSN_DST),
    reason="PGCMP_LIVE_DSN_SRC / PGCMP_LIVE_DSN_DST не заданы",
)

SCHEMA = "pgcmp_live_%d" % os.getpid()

# таблица: (DDL, строки источника, строки приёмника)
TABLES = {
    # ключ sync_keys на nullable-колонке: NULL-ключ пары не находит
    "null_key": (
        "(k int, v text)",
        "(1,'a'),(2,'b'),(NULL,'c')",
        "(1,'a'),(2,'b'),(NULL,'c')",
    ),
    # без ключа: мультимножество строк
    "dups": (
        "(a int, b text)",
        "(1,'a'),(1,'a'),(1,'a'),(2,'b')",
        "(1,'a'),(2,'b'),(2,'b'),(3,'c')",
    ),
    "empty_dst": (
        "(id int PRIMARY KEY, v text)",
        "(1,'a'),(2,'b'),(3,'c')",
        None,
    ),
    "empty_src": (
        "(id int PRIMARY KEY, v text)",
        None,
        "(1,'a'),(2,'b')",
    ),
    "changed": (
        "(id int PRIMARY KEY, v text)",
        "(1,'a'),(2,'b'),(3,'c')",
        "(1,'a'),(2,'X'),(3,'c')",
    ),
}

# только в приёмнике: индексы для dest_key_is_unique
DEST_ONLY = [
    "CREATE TABLE {s}.k_pk (id int PRIMARY KEY, v text)",
    "CREATE TABLE {s}.k_uq_null (code text, v text)",
    "CREATE UNIQUE INDEX ON {s}.k_uq_null (code)",
    "CREATE TABLE {s}.k_partial (code text NOT NULL, v text)",
    "CREATE UNIQUE INDEX ON {s}.k_partial (code) WHERE code > ''",
    "CREATE TABLE {s}.k_include (id int NOT NULL, name text NOT NULL)",
    "CREATE UNIQUE INDEX ON {s}.k_include (id) INCLUDE (name)",
]


def _admin(dsn):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def _setup(dsn, side):
    conn = _admin(dsn)
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS %s CASCADE" % SCHEMA)
    cur.execute("CREATE SCHEMA %s" % SCHEMA)

    for name, (ddl, src_rows, dst_rows) in TABLES.items():
        rows = src_rows if side == "src" else dst_rows
        cur.execute("CREATE TABLE %s.%s %s" % (SCHEMA, name, ddl))
        if rows:
            cur.execute("INSERT INTO %s.%s VALUES %s" % (SCHEMA, name, rows))

    if side == "dst":
        for stmt in DEST_ONLY:
            cur.execute(stmt.format(s=SCHEMA))

    conn.close()


def _drop(dsn):
    conn = _admin(dsn)
    conn.cursor().execute("DROP SCHEMA IF EXISTS %s CASCADE" % SCHEMA)
    conn.close()


@pytest.fixture(scope="module")
def live():
    _setup(DSN_SRC, "src")
    _setup(DSN_DST, "dst")

    src = psycopg2.connect(DSN_SRC)
    src.set_session(readonly=True)
    normalize_session(src, readonly=True)
    dst = psycopg2.connect(DSN_DST)
    normalize_session(dst)

    yield src, dst

    src.close()
    dst.close()
    _drop(DSN_SRC)
    _drop(DSN_DST)


def _numbers(row):
    return (row["status"], row["src_rows"], row["dst_rows"],
            row["to_insert"], row["to_update"], row["to_delete"])


def test_null_in_sync_key_never_pairs(live):
    src, dst = live
    row = cmp.compare_table(src, dst, SCHEMA, "null_key", ["k"],
                            key_source="sync_keys")

    # (NULL,'c') есть с обеих сторон, но «=» по NULL пары не даёт:
    # вставка из источника и удаление в приёмнике
    assert _numbers(row) == ("differs", 3, 3, 1, 0, 1)


def test_repeated_rows_without_key_are_counted_as_multiset(live):
    src, dst = live
    row = cmp.compare_table(src, dst, SCHEMA, "dups", [])

    # (1,a): 3 против 1 -> +2; (2,b): 1 против 2 -> -1; (3,c): -1
    assert _numbers(row) == ("differs", 4, 4, 2, 0, 2)


def test_empty_table_on_one_side(live):
    src, dst = live

    to_empty = cmp.compare_table(src, dst, SCHEMA, "empty_dst", ["id"],
                                 key_source="pk")
    from_empty = cmp.compare_table(src, dst, SCHEMA, "empty_src", ["id"],
                                   key_source="pk")

    assert _numbers(to_empty) == ("differs", 3, 0, 3, 0, 0)
    assert _numbers(from_empty) == ("differs", 0, 2, 0, 0, 2)


def test_changed_row_is_an_update(live):
    src, dst = live
    row = cmp.compare_table(src, dst, SCHEMA, "changed", ["id"],
                            key_source="pk")

    assert _numbers(row) == ("differs", 3, 3, 0, 1, 0)


@pytest.mark.parametrize("table, key, expected", [
    ("k_pk", ["id"], True),
    ("k_uq_null", ["code"], False),
    ("k_partial", ["code"], False),
    ("k_include", ["id"], True),
    ("k_include", ["id", "name"], False),
])
def test_dest_key_is_unique(live, table, key, expected):
    _src, dst = live
    try:
        assert cmp.dest_key_is_unique(dst, SCHEMA, table, key) is expected
    finally:
        dst.rollback()
