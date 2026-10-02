# -*- coding: utf-8 -*-
"""
Поправка D01 в pg_ranges: порядок сортировки сравнивается вместе с версией,
режим корзин по md5-префиксу, общий построитель предиката листьев, пачки
листьев, ±infinity у дат и времени.
"""

import datetime

from psycopg2 import sql

import modules.pg_ranges as rng
from tests.pg_fakes import FakeConn, render


def _literals(composable):
    if isinstance(composable, sql.Literal):
        return [composable.wrapped]
    if isinstance(composable, sql.Composed):
        out = []
        for part in composable.seq:
            out.extend(_literals(part))
        return out
    return []


ICU_A = ("und-x-icu", "i", "153.120")
ICU_B = ("und-x-icu", "i", "153.14")


# ------------------------------------------------------------------
# Порядок сортировки
# ------------------------------------------------------------------

def test_same_order_needs_name_provider_and_version():
    assert rng.same_order(ICU_A, ICU_A) is True
    # то же имя, другая версия библиотеки (RHEL7 → RHEL8)
    assert rng.same_order(ICU_A, ICU_B) is False
    assert rng.same_order(ICU_A, ("und-x-icu", "c", "153.120")) is False
    assert rng.same_order(ICU_A, ("en-x-icu", "i", "153.120")) is False


def test_unknown_version_means_different_order():
    unknown = ("default/en_US.UTF-8", "c", None)
    assert rng.same_order(unknown, unknown) is False
    assert rng.same_order(None, ICU_A) is False


def test_c_locale_is_byte_order_without_version():
    c = ("default/C", "c", None)
    posix = ("POSIX/POSIX", "c", None)
    assert rng.same_order(c, c) is True
    assert rng.same_order(posix, posix) is True
    assert rng.same_order(c, ICU_A) is False


def test_text_key_with_different_or_unknown_order_goes_to_buckets():
    src = {"code": ("text", ICU_A), "id": ("bigint", None)}
    dst = {"code": ("text", ICU_B), "id": ("bigint", None)}

    col = rng.choose_chunk_column(src, dst, ["code"])
    assert (col["name"], col["kind"], col["mode"]) == ("code", "text",
                                                        "bucket")
    # одинаковый порядок — диапазоны по индексу, без COLLATE "C"
    col = rng.choose_chunk_column(src, src, ["code"])
    assert (col["mode"], col["collate_c"]) == ("range", False)
    # числа от collation не зависят
    assert rng.choose_chunk_column(src, dst, ["id"])["mode"] == "range"


def test_citext_is_a_text_kind():
    assert rng.type_kind("citext") == "text"
    assert rng.type_kind("public.citext") == "text"


def test_column_query_reads_collation_versions_by_server_version():
    old = render(rng.build_columns_sql(90400))
    new = render(rng.build_columns_sql(170000))
    assert "pg_collation_actual_version" not in old
    assert "pg_database_collation_actual_version" not in old
    assert "pg_collation_actual_version" in new
    assert "pg_database_collation_actual_version" in new
    assert "collversion" in new


# ------------------------------------------------------------------
# Корзины
# ------------------------------------------------------------------

def test_bucket_sql_groups_by_md5_prefix_in_one_pass():
    text = render(rng.build_bucket_sql("s", "t", ["v", "code"], "code", 2))
    assert 'substr(md5("t"."code"::text), 1, 2)' in text
    assert "GROUP BY" in text
    assert "WHERE" not in text
    assert "TEMP" not in text.upper()


def test_next_level_reads_only_mismatched_parents():
    q = rng.build_bucket_sql("s", "t", ["v"], "code", 4,
                             parents=["0a", "ff"])
    text = render(q)
    assert 'substr(md5("t"."code"::text), 1, 4)' in text
    assert 'substr(md5("t"."code"::text), 1, 2) IN (' in text
    assert [v for v in _literals(q) if isinstance(v, str)] == ["{0a,ff}"]


def test_bucket_sql_counts_null_keys_on_request():
    text = render(rng.build_bucket_sql("s", "t", ["v"], "a", 2,
                                       null_keys=["a", "b"]))
    assert '"t"."a" IS NULL OR "t"."b" IS NULL' in text


def test_diff_buckets_splits_only_big_mismatches(monkeypatch):
    monkeypatch.setattr(rng, "LEAF_ROWS", 10)
    src = {"00": (5, 1, 1, 0), "01": (50, 2, 2, 0), "02": (3, 3, 3, 0),
           "03": (4, 4, 4, 2), None: (1, 9, 9, 0)}
    dst = {"00": (5, 1, 1, 0), "01": (51, 2, 2, 0), "02": (3, 7, 3, 0),
           "03": (4, 4, 4, 0), "04": (2, 1, 1, 0)}

    out = rng.diff_buckets(src, dst, 2)

    assert out["same"] == {"00": (5, 5)}
    assert out["deeper"] == ["01"]
    leaves = {(lf["lo"], lf["is_null"]): lf for lf in out["leaves"]}
    # 03: суммы совпали, но NULL в ключе — построчно это вставка и удаление
    assert set(leaves) == {("02", False), ("03", False), ("04", False),
                           (None, True)}
    assert all(lf["mode"] == "bucket" for lf in out["leaves"])
    assert leaves[("04", False)]["src_rows"] == 0
    assert leaves[("04", False)]["dst_rows"] == 2
    assert leaves[("02", False)]["depth"] == 2


def test_last_level_makes_leaves_even_if_big(monkeypatch):
    monkeypatch.setattr(rng, "LEAF_ROWS", 10)
    out = rng.diff_buckets({"0123abcd": (99, 1, 1, 0)}, {}, rng.BUCKET_MAX)
    assert out["deeper"] == []
    assert [lf["lo"] for lf in out["leaves"]] == ["0123abcd"]


# ------------------------------------------------------------------
# Общий предикат листьев и пачки
# ------------------------------------------------------------------

def test_leaves_predicate_mixes_ranges_and_buckets_of_any_length():
    leaves = [
        {"column": "code", "lo": "ab", "hi": None, "is_null": False,
         "depth": 2, "mode": "bucket"},
        {"column": "code", "lo": "cd", "hi": None, "is_null": False,
         "depth": 2, "mode": "bucket"},
        {"column": "code", "lo": "ef01", "hi": None, "is_null": False,
         "depth": 4, "mode": "bucket"},
        {"column": "code", "lo": None, "hi": None, "is_null": True,
         "depth": 0, "mode": "bucket"},
    ]
    q = rng.leaves_predicate("t", leaves)
    text = render(q)
    assert 'substr(md5("t"."code"::text), 1, 2) IN (' in text
    assert 'substr(md5("t"."code"::text), 1, 4) IN (' in text
    assert '"t"."code" IS NULL' in text
    assert text.count(" OR ") == 2
    assert sorted(v for v in _literals(q) if isinstance(v, str)) == \
        ["{ab,cd}", "{ef01}"]


def test_leaves_predicate_of_ranges_is_or_of_range_predicates():
    leaves = [{"column": "id", "lo": 1, "hi": 5, "is_null": False,
               "depth": 1},
              {"column": "id", "lo": 9, "hi": None, "is_null": False,
               "depth": 1, "mode": "range"}]
    text = render(rng.leaves_predicate(None, leaves))
    assert text == '("id" >= 1 AND "id" < 5) OR ("id" >= 9)'


def test_bucket_prefix_must_be_hex():
    bad = {"column": "c", "lo": "a') OR true --", "hi": None,
           "is_null": False, "depth": 2, "mode": "bucket"}
    try:
        rng.leaves_predicate("t", [bad])
    except ValueError:
        return
    raise AssertionError("префикс не hex — ошибка")


def test_leaf_batches():
    assert [len(b) for b in rng.leaf_batches(list(range(450)), 200)] == \
        [200, 200, 50]
    assert rng.LEAF_BATCH == 200
    assert rng.leaf_batches([], 200) == []


# ------------------------------------------------------------------
# ±infinity
# ------------------------------------------------------------------

def test_infinite_min_or_max_cuts_by_sample_of_finite_values():
    col = {"name": "ts", "kind": "timestamp"}
    a = datetime.datetime(2024, 1, 1)
    conn = FakeConn(responses=[
        ("pg_total_relation_size", [(10 ** 9, 10 ** 7)]),
        ("min(", [(datetime.datetime.min, datetime.datetime.max, False)]),
        ("percentile_disc", [(["2024-01-01 00:00:00",
                               "2024-02-01 00:00:00",
                               "2024-03-01 00:00:00"],)]),
    ])

    out = rng.top_ranges(conn, "s", "t", col)

    query = render(conn.executed[-1][0]) if isinstance(
        conn.executed[-1], tuple) else render(conn.executed[-1])
    assert "percentile_disc" in query
    assert 'isfinite("t"."ts")' in query
    assert "TABLESAMPLE" in query
    assert [(r["lo"], r["hi"]) for r in out if not r["is_null"]] == [
        (None, "2024-02-01 00:00:00"),
        ("2024-02-01 00:00:00", "2024-03-01 00:00:00"),
        ("2024-03-01 00:00:00", None)]
    assert a  # дата для читателя: арифметика не участвует


def test_finite_dates_still_cut_arithmetically():
    col = {"name": "d", "kind": "date"}
    conn = FakeConn(responses=[
        ("min(", [(datetime.date(2024, 1, 1), datetime.date(2024, 1, 5),
                   True)]),
    ])
    out = rng.split_range(conn, "s", "t", col, rng.make_range(depth=0))
    assert len(out) > 1
    assert not any("percentile_disc" in render(q if not isinstance(q, tuple)
                                               else q[0])
                   for q in conn.executed)
