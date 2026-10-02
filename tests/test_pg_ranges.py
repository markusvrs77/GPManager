# -*- coding: utf-8 -*-
"""
Чистые построители pg_ranges: предикаты диапазонов, SQL контрольной суммы,
арифметическое деление, выбор колонки нарезки, порог включения.
"""

import datetime
from decimal import Decimal

from psycopg2 import sql

import modules.pg_ranges as rng
from tests.pg_fakes import FakeConn, render


def R(lo=None, hi=None, is_null=False, depth=0):
    return {"lo": lo, "hi": hi, "is_null": is_null, "depth": depth}


def _literals(composable):
    """Все sql.Literal внутри Composable — значения границ."""
    if isinstance(composable, sql.Literal):
        return [composable.wrapped]
    if isinstance(composable, sql.Composed):
        out = []
        for part in composable.seq:
            out.extend(_literals(part))
        return out
    return []


# ------------------------------------------------------------------
# Предикаты
# ------------------------------------------------------------------

def test_predicate_closed_range_is_lo_inclusive_hi_exclusive():
    pred = rng.range_predicate("t", "id", R(10, 20))
    assert render(pred) == '"t"."id" >= 10 AND "t"."id" < 20'


def test_predicate_open_bounds():
    assert render(rng.range_predicate("t", "id", R(None, 20))) == \
        '"t"."id" < 20'
    assert render(rng.range_predicate("t", "id", R(10, None))) == \
        '"t"."id" >= 10'
    # ни одной границы — все значения, кроме NULL (у NULL свой диапазон)
    assert render(rng.range_predicate("t", "id", R())) == \
        '"t"."id" IS NOT NULL'


def test_predicate_null_range():
    pred = rng.range_predicate("t", "id", R(is_null=True))
    assert render(pred) == '"t"."id" IS NULL'


def test_predicate_collate_c_on_text():
    pred = rng.range_predicate("t", "name", R("a", "m"), collate_c=True)
    assert render(pred) == \
        '"t"."name" COLLATE "C" >= \'a\' AND "t"."name" COLLATE "C" < \'m\''


def test_predicate_values_are_literals_not_sql_text():
    evil = "x'; DROP TABLE t; --"
    pred = rng.range_predicate("t", "name", R(evil, None))
    assert _literals(pred) == [evil]


# ------------------------------------------------------------------
# Контрольная сумма
# ------------------------------------------------------------------

def test_checksum_sql_hashes_sorted_columns_under_predicate():
    pred = rng.range_predicate("t", "id", R(1, None))
    text = render(rng.build_checksum_sql("s", "tb", ["b", "a"], pred))

    assert text.startswith("SELECT count(*), ")
    assert "substr(s.h, 1, 16))::bit(64)::bigint" in text
    assert "substr(s.h, 17, 16))::bit(64)::bigint" in text
    assert 'md5(ROW("t"."a", "t"."b")::text) AS h' in text
    assert text.endswith('FROM "s"."tb" AS "t" WHERE "t"."id" >= 1) AS s')


def test_checksum_without_predicate_reads_whole_table():
    text = render(rng.build_checksum_sql("s", "tb", ["a"], None))
    assert "WHERE" not in text


def test_range_checksum_returns_three_ints():
    conn = FakeConn(responses=[("substr", [(3, Decimal("10"), Decimal("-4"))])])
    assert rng.range_checksum(conn, "s", "tb", ["a"], None) == (3, 10, -4)


# ------------------------------------------------------------------
# Арифметическое деление
# ------------------------------------------------------------------

def test_int_cuts_are_even_and_inside_min_max():
    assert rng.arithmetic_cuts("int", 0, 100, 4) == [25, 50, 75]
    assert rng.arithmetic_cuts("int", 5, 7, 16) == [6, 7]
    assert rng.arithmetic_cuts("int", 5, 5, 16) == []


def test_numeric_cuts():
    assert rng.arithmetic_cuts("numeric", Decimal("0"), Decimal("1"), 4) == \
        [Decimal("0.25"), Decimal("0.5"), Decimal("0.75")]


def test_date_cuts():
    d = datetime.date
    assert rng.arithmetic_cuts("date", d(2024, 1, 1), d(2024, 1, 5), 4) == \
        [d(2024, 1, 2), d(2024, 1, 3), d(2024, 1, 4)]
    assert rng.arithmetic_cuts("date", d(2024, 1, 1), d(2024, 1, 2), 16) == \
        [d(2024, 1, 2)]


def test_timestamp_cuts_naive_and_aware():
    for tz in (None, datetime.timezone.utc):
        t = lambda h: datetime.datetime(2024, 1, 1, h, tzinfo=tz)  # noqa: E731
        assert rng.arithmetic_cuts("timestamptz" if tz else "timestamp",
                                   t(0), t(4), 4) == [t(1), t(2), t(3)]


def test_ranges_from_cuts_keep_outer_bounds_and_cover_everything():
    out = rng.ranges_from_cuts(None, None, [10, 20], depth=1)
    assert out == [R(None, 10, depth=1), R(10, 20, depth=1),
                   R(20, None, depth=1)]


# ------------------------------------------------------------------
# Выбор колонки нарезки
# ------------------------------------------------------------------

COLS = {"id": ("integer", None), "code": ("text", ("en_US", "c", "2.28")),
        "created": ("date", None), "amount": ("numeric(12,2)", None),
        "flag": ("boolean", None)}


def test_key_column_is_the_first_key_column():
    assert rng.choose_chunk_column(COLS, COLS, ["id", "code"]) == \
        {"name": "id", "kind": "int", "collate_c": False, "mode": "range"}


def test_text_key_with_different_collation_goes_to_buckets():
    # COLLATE "C" отключал индекс; теперь разный порядок — режим корзин
    dst = dict(COLS, code=("text", ("und-x-icu", "i", "153.120")))
    assert rng.choose_chunk_column(COLS, dst, ["code"]) ==         {"name": "code", "kind": "text", "collate_c": False, "mode": "bucket"}


def test_unsupported_key_type_gives_no_column():
    assert rng.choose_chunk_column(COLS, COLS, ["flag"]) is None


def test_without_key_date_wins_then_most_distinct():
    assert rng.choose_chunk_column(COLS, COLS, [])["name"] == "created"

    ints = {"a": ("integer", ""), "b": ("bigint", "")}
    assert rng.choose_chunk_column(ints, ints, [],
                                   {"a": 10, "b": 5000})["name"] == "b"


def test_without_key_type_must_match_on_both_sides():
    src = {"d": ("date", ""), "f": ("boolean", "")}
    dst = {"d": ("timestamp without time zone", ""), "f": ("boolean", "")}
    assert rng.choose_chunk_column(src, dst, []) is None


# ------------------------------------------------------------------
# Порог
# ------------------------------------------------------------------

def test_should_chunk_by_size_or_rows(monkeypatch):
    monkeypatch.setattr(rng, "CHUNK_MIN_BYTES", 1000)
    monkeypatch.setattr(rng, "CHUNK_MIN_ROWS", 100)

    def conn(size, rows):
        return FakeConn(responses=[("pg_total_relation_size",
                                    [(size, rows)])])

    assert rng.should_chunk(conn(1000, 1), "s", "t") is True
    assert rng.should_chunk(conn(10, 100), "s", "t") is True
    assert rng.should_chunk(conn(10, 99), "s", "t") is False
    # таблицы не нашлось — как сейчас, без нарезки
    assert rng.should_chunk(FakeConn(), "s", "t") is False
