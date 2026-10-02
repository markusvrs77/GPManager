# -*- coding: utf-8 -*-
"""
Поправка D01 на живом PostgreSQL — opt-in (PGCMP_LIVE_DSN_SRC /
PGCMP_LIVE_DSN_DST; без них — skip). Маленькие таблицы, порог понижен.

* режим корзин принудительно (детектор порядка — «разный»): сравнение →
  загрузка разницы с delete_missing=True → повторное сравнение = same,
  строки приёмника вне листьев не тронуты;
* граница → SQLite → предикат выбирает ровно те же строки: timestamptz
  (с поясом), timestamp, numeric (с масштабом), text (кавычки, юникод);
* ±infinity у timestamp: нарезка по конечным значениям, числа точные.
"""

import os

import psycopg2
import pytest
from psycopg2 import sql

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

SCHEMA = "pgd01_live_%d" % os.getpid()
S = "generate_series(1, 600) AS i"

TABLES = {
    # (DDL, наполнение, правки приёмника, ключ)
    "bucket_key": (
        "(code text PRIMARY KEY, v int)",
        "SELECT (ARRAY['A','b','C','d'])[i % 4 + 1] || md5(i::text), i "
        "FROM " + S,
        ["UPDATE {t} SET v = -1 WHERE v IN (3, 333)",
         "DELETE FROM {t} WHERE v IN (100, 101)",
         "INSERT INTO {t} VALUES ('0zzz', 1), ('~tail', 2), ('Zed', 3)"],
        ["code"],
    ),
    "inf_ts": (
        "(ts timestamp, v int)",
        "SELECT CASE WHEN i = 1 THEN '-infinity'::timestamp "
        "WHEN i = 2 THEN 'infinity'::timestamp "
        "WHEN i % 60 = 0 THEN NULL "
        "ELSE timestamp '2024-01-01' + i * interval '7 hours' END, i % 5 "
        "FROM " + S,
        ["UPDATE {t} SET v = 99 WHERE ts = 'infinity'",
         "UPDATE {t} SET v = 98 WHERE ts = timestamp '2024-01-01' "
         "+ 300 * interval '7 hours'",
         "INSERT INTO {t} VALUES ('-infinity', 7), ('2000-01-01', 1)"],
        [],
    ),
    "b_tstz": ("(c timestamptz, v int)",
               "SELECT timestamptz '2024-03-01 00:00:00.123456+05' "
               "+ i * interval '13 minutes 7.5 seconds', i FROM " + S,
               [], []),
    "b_ts": ("(c timestamp, v int)",
             "SELECT timestamp '2024-03-01 00:00:00.5' "
             "+ i * interval '1 hour 1 second', i FROM " + S, [], []),
    "b_num": ("(c numeric(12,3), v int)",
              "SELECT round((i * 1.37 - 200)::numeric, 3), i FROM " + S,
              [], []),
    "b_text": ("(c text, v int)",
               "SELECT (ARRAY['o''k', 'ü', 'Ж', 'a\"b', 'zz'])[i % 5 + 1] "
               "|| ' ' || i, i FROM " + S, [], []),
}
BOUNDS = ("b_tstz", "b_ts", "b_num", "b_text")


def _admin(dsn):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def _setup(dsn, side):
    conn = _admin(dsn)
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS %s CASCADE" % SCHEMA)
    cur.execute("CREATE SCHEMA %s" % SCHEMA)
    for name, (ddl, fill, edits, _key) in TABLES.items():
        t = "%s.%s" % (SCHEMA, name)
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
    monkeypatch.setattr(pr, "LEAF_BATCH", 3)
    monkeypatch.setattr(cmp, "open_pg", _open_by_id)
    monkeypatch.setattr(pdl, "open_pg", _open_by_id)
    monkeypatch.setattr(cmp, "resolve_key_candidates", lambda sid, tables: {
        key: ([{"columns": TABLES[key[1]][3], "source": "pk"}]
              if TABLES[key[1]][3] else []) for key in tables})
    yield
    for dsn in (DSN_SRC, DSN_DST):
        conn = _admin(dsn)
        conn.cursor().execute("DROP SCHEMA IF EXISTS %s CASCADE" % SCHEMA)
        conn.close()


def _compare(tables):
    job_id = create_job("pg_compare", 1, {
        "source_connection_id": 1, "dest_connection_id": 2, "parallel": 3,
        "item_action": "COMPARE",
        "tables": [{"schema": SCHEMA, "table": t, "in_src": True,
                    "in_dst": True} for t in tables]})
    cmp.run_pg_compare_job(job_id)
    assert get_job(job_id)["status"] == "done"
    return job_id, {r["table"]: r for r in cmp.get_results(job_id)}


def _full(name):
    src, dst = _open(DSN_SRC, True), _open(DSN_DST, False)
    try:
        return cmp.compare_table(src, dst, SCHEMA, name, TABLES[name][3],
                                 key_source="pk" if TABLES[name][3] else None)
    finally:
        src.close()
        dst.close()


def _numbers(row):
    return (row["status"], row["src_rows"], row["dst_rows"],
            row["to_insert"], row["to_update"], row["to_delete"])


def _query(dsn, text, params=None):
    conn = _admin(dsn)
    try:
        cur = conn.cursor()
        cur.execute(text, params)
        return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def _md5(dsn, name):
    return _query(dsn, "SELECT md5(string_agg(md5(x::text), '' ORDER BY "
                       "md5(x::text))) FROM %s.%s x" % (SCHEMA, name))


def test_forced_buckets_compare_load_and_compare_again(live, monkeypatch):
    monkeypatch.setattr(pr, "same_order", lambda a, b: False)
    name = "bucket_key"
    full = _full(name)

    compared, results = _compare([name])

    row = results[name]
    assert _numbers(row) == _numbers(full) and full["status"] == "differs"
    leaves = cmp.get_mismatched_ranges(compared, SCHEMA, name)
    assert leaves and {lf["mode"] for lf in leaves} == {"bucket"}
    assert len(leaves) == row["chunked"]["mismatched"]
    assert row["message"] is None

    # ловушка: строка приёмника вне листьев
    conn = _admin(DSN_DST)
    cur = conn.cursor()
    cur.execute(sql.SQL(
        "SELECT t.code FROM {} AS t WHERE NOT ({}) LIMIT 1").format(
            sql.Identifier(SCHEMA, name),
            pdl.build_ranges_where("t", leaves)))
    trap = cur.fetchone()[0]
    cur.execute("UPDATE %s.%s SET v = -777 WHERE code = %%s" % (SCHEMA, name),
                (trap,))
    conn.close()

    # корзины грузятся одним COPY источника, сколько бы их ни было
    assert len(leaves) > pr.LEAF_BATCH
    copies = []
    real_copy = pdl.stream_copy

    def counting_copy(src, dst, select_sql, *args):
        copies.append(select_sql)
        return real_copy(src, dst, select_sql, *args)

    monkeypatch.setattr(pdl, "stream_copy", counting_copy)

    job_id = create_job("pg_diff_load", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "compare_job_id": compared, "delete_missing": True,
        "tables": [{"schema": SCHEMA, "table": name, "action": "diff",
                    "key_columns": ["code"], "in_dst": True}],
        "expected": []})
    pdl.run_pg_diff_load_job(job_id)
    assert get_job(job_id)["status"] == "done"
    assert len(copies) == 1
    item = get_job_items(job_id)[0]
    assert item["error_message"].endswith("; по диапазонам: %d"
                                          % len(leaves))

    assert _query(DSN_DST, "SELECT v FROM %s.%s WHERE code = %%s"
                  % (SCHEMA, name), (trap,)) == [(-777,)]
    _query(DSN_DST, "UPDATE %s.%s SET v = s.v FROM (SELECT %%s::int AS v) s "
           "WHERE code = %%s" % (SCHEMA, name),
           (_query(DSN_SRC, "SELECT v FROM %s.%s WHERE code = %%s"
                   % (SCHEMA, name), (trap,))[0][0], trap))

    _again, results = _compare([name])
    assert results[name]["status"] == "same"
    assert _md5(DSN_SRC, name) == _md5(DSN_DST, name)


def test_infinity_rows_go_to_open_edges_with_exact_numbers(live):
    name = "inf_ts"
    src = _open(DSN_SRC, True)
    try:
        column = {"name": "ts", "kind": "timestamp", "collate_c": False}
        top = pr.top_ranges(src, SCHEMA, name, column)
    finally:
        src.close()
    # арифметика от ±infinity дала бы один полный диапазон
    assert len([r for r in top if not r["is_null"]]) >= 3
    assert top[0]["lo"] is None and top[-2]["hi"] is None

    _job, results = _compare([name])
    assert _numbers(results[name]) == _numbers(_full(name))
    assert results[name]["chunked"]["checked"] > 1


@pytest.mark.parametrize("name", BOUNDS)
def test_bounds_survive_sqlite_and_select_the_same_rows(live, name):
    kind = {"b_tstz": "timestamptz", "b_ts": "timestamp", "b_num": "numeric",
            "b_text": "text"}[name]
    column = {"name": "c", "kind": kind, "collate_c": False}
    job_id = create_job("pg_compare", 1, {
        "source_connection_id": 1, "dest_connection_id": 2, "tables": []})
    src, dst = _open(DSN_SRC, True), _open(DSN_DST, False)

    try:
        ranges = []
        for rng in pr.top_ranges(src, SCHEMA, name, column):
            subs = pr.split_range(src, SCHEMA, name, column, rng)
            ranges.extend(subs or [rng])
        assert len(ranges) > 4
        for rng in ranges:
            cmp.save_leaf(job_id, SCHEMA, name, column, rng,
                          {"status": "differs"})
        stored = cmp.get_mismatched_ranges(job_id, SCHEMA, name)
        assert len(stored) == len(ranges)

        total = 0
        for rng, leaf in zip(ranges, stored):
            want = pr.range_checksum(src, SCHEMA, name, ["c", "v"],
                                     pr.range_predicate("t", "c", rng))
            # загрузка строит предикат по листу из SQLite в своей сессии
            got = pr.range_checksum(dst, SCHEMA, name, ["c", "v"],
                                    pdl.build_ranges_where("t", [leaf]))
            assert got == want, (rng, leaf)
            total += want[0]
        assert total == 600
    finally:
        src.close()
        dst.close()


def _plan(dsn, query):
    conn = _admin(dsn)
    try:
        cur = conn.cursor()
        cur.execute(sql.SQL("EXPLAIN {}").format(query))
        return "\n".join(r[0] for r in cur.fetchall())
    finally:
        conn.close()


@pytest.mark.parametrize("with_null", [False, True])
def test_bucket_set_is_hashed_not_scanned_per_row(live, with_null):
    leaves = [{"column": "code", "lo": "%02x" % i, "hi": None,
               "is_null": False, "mode": "bucket"} for i in range(0, 256, 3)]
    if with_null:
        leaves.append({"column": "code", "lo": None, "hi": None,
                       "is_null": True, "mode": "bucket"})
    # загрузка (COPY в staging) и уровень дробления сравнения
    queries = [
        pdl.build_full_select_sql(SCHEMA, "bucket_key", ["code", "v"],
                                  pdl.build_ranges_where(None, leaves)),
        pr.build_bucket_sql(SCHEMA, "bucket_key", ["code", "v"], "code", 4,
                            parents=[lf["lo"] for lf in leaves
                                     if lf["lo"]]),
    ]
    for query in queries:
        plan = _plan(DSN_SRC, query)
        assert "Hash Semi Join" in plan or "hashed SubPlan" in plan, plan


def test_both_cancels_the_running_side_when_the_other_fails(live):
    import time

    class Boom(Exception):
        pass

    def fail():
        time.sleep(0.3)
        raise Boom("src failed")

    src, dst = _open(DSN_SRC, True), _open(DSN_DST, False)
    try:
        started = time.time()
        with pytest.raises(Boom):
            cmp._both(fail,
                      lambda: dst.cursor().execute("SELECT pg_sleep(30)"),
                      src, dst)
        assert time.time() - started < 10
    finally:
        src.close()
        dst.close()
