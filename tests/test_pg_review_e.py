# -*- coding: utf-8 -*-
"""
Ревью E: один снимок источника при загрузке, корзины одним проходом,
множество префиксов параметром-массивом, отмена второй стороны в _both.
"""

import threading

import pytest

import modules.pg_diff_load as pdl
import modules.pg_ranges as pr
from tests.pg_fakes import FakeConn, render
from tests.test_pg_diff_load_ranges import (KEYED, _pair, _texts,  # noqa
                                            columns)


SNAPSHOT = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"


def _range(lo, hi):
    return {"column": "id", "lo": lo, "hi": hi, "is_null": False,
            "depth": 1, "collate_c": False, "mode": "range"}


def _bucket(prefix):
    return {"column": "id", "lo": prefix, "hi": None, "is_null": False,
            "depth": len(prefix), "collate_c": False, "mode": "bucket"}


NULL_BUCKET = {"column": "id", "lo": None, "hi": None, "is_null": True,
               "depth": 0, "collate_c": False, "mode": "bucket"}


class TimelineConn(FakeConn):
    """Источник, который пишет запросы, COPY и откаты в одну ленту."""

    def __init__(self, *args, **kwargs):
        FakeConn.__init__(self, *args, **kwargs)
        self.timeline = []

    def respond(self, text, params):
        self.timeline.append(("sql", text))
        return FakeConn.respond(self, text, params)

    def rollback(self):
        self.timeline.append(("rollback", None))
        FakeConn.rollback(self)

    def commit(self):
        self.timeline.append(("commit", None))
        FakeConn.commit(self)


def _timeline_pair(monkeypatch):
    src = TimelineConn(copy_out=b"101\ta\n")
    _, dst = _pair(KEYED)
    real = src.cursor

    def cursor():
        cur = real()
        copy = cur.copy_expert

        def copy_expert(query, stream):
            src.timeline.append(("copy", render(query)))
            return copy(query, stream)

        cur.copy_expert = copy_expert
        return cur

    src.cursor = cursor
    return src, dst


# ------------------------------------------------------------------
# 1. Один снимок источника на все COPY таблицы
# ------------------------------------------------------------------

def test_all_source_copies_run_in_one_repeatable_read_snapshot(
        columns, monkeypatch):  # noqa: F811
    monkeypatch.setattr(pr, "LEAF_BATCH", 2)
    src, dst = _timeline_pair(monkeypatch)
    leaves = [_range(i * 10, i * 10 + 5) for i in range(5)]

    pdl.load_diff(src, dst, "s", "t", ["id"], True, "stg_7_1",
                  ranges=leaves)

    events = src.timeline
    copies = [i for i, (kind, _) in enumerate(events) if kind == "copy"]
    assert len(copies) == 3
    snap = [i for i, e in enumerate(events) if e == ("sql", SNAPSHOT)]
    assert len(snap) == 1
    # снимок открыт новой транзакцией до первого COPY
    assert events[snap[0] - 1][0] == "rollback"
    assert snap[0] < copies[0]
    # между снимком и последним COPY транзакция не закрывается
    between = events[snap[0]:copies[-1]]
    assert not [e for e in between if e[0] in ("rollback", "commit")]
    # источник только читается
    assert all(not text.startswith(("INSERT", "UPDATE", "DELETE"))
               for kind, text in events if kind == "sql")


# ------------------------------------------------------------------
# 2. Корзины — один COPY и один DELETE, диапазоны — пачками
# ------------------------------------------------------------------

def test_bucket_leaves_load_in_one_pass_ranges_by_batches(
        columns, monkeypatch):  # noqa: F811
    monkeypatch.setattr(pr, "LEAF_BATCH", 2)
    src, dst = _timeline_pair(monkeypatch)
    buckets = [_bucket("0a"), _bucket("1b"), _bucket("2c"), _bucket("ff12"),
               _bucket("ee34"), NULL_BUCKET]
    ranges = [_range(i * 10, i * 10 + 5) for i in range(3)]

    pdl.load_diff(src, dst, "s", "t", ["id"], True, "stg_7_1",
                  ranges=buckets[:3] + ranges + buckets[3:])

    md5 = [c for c in src.copies if "md5" in c]
    assert len(md5) == 1
    for prefix in ("0a", "1b", "2c", "ff12", "ee34"):
        assert prefix in md5[0]
    assert '"id" IS NULL' in md5[0]
    assert '"id" >=' not in md5[0]
    # 3 диапазона по 2 — ещё два COPY, без корзин
    assert len(src.copies) == 3

    deletes = [t for t in _texts(dst) if t.startswith("DELETE FROM")]
    assert len([d for d in deletes if "md5" in d]) == 1
    assert len(deletes) == 3
    texts = _texts(dst)
    first = texts.index(deletes[0])
    last = next(i for i, t in enumerate(texts) if t.startswith("INSERT INTO"))
    assert "COMMIT" not in texts[first:last]


def test_only_buckets_give_one_copy_and_one_delete_whatever_the_count(
        columns, monkeypatch):  # noqa: F811
    monkeypatch.setattr(pr, "LEAF_BATCH", 2)
    src, dst = _pair(KEYED)
    leaves = [_bucket("%02x" % i) for i in range(50)]

    pdl.load_diff(src, dst, "s", "t", ["id"], True, "stg_7_1",
                  ranges=leaves)

    assert len(src.copies) == 1
    assert len([t for t in _texts(dst) if t.startswith("DELETE FROM")]) == 1


# ------------------------------------------------------------------
# 3. Множество префиксов — один массив в хеш-полусоединении
# ------------------------------------------------------------------

def _literal_count(composable):
    from psycopg2 import sql
    if isinstance(composable, sql.Literal):
        return 1
    if isinstance(composable, sql.Composed):
        return sum(_literal_count(p) for p in composable.seq)
    return 0


def test_bucket_set_is_one_array_in_a_semi_join():
    text = render(pr.bucket_predicate("t", "code", ["0a", "ff"], 2))
    assert text == ('substr(md5("t"."code"::text), 1, 2) IN '
                    "(SELECT unnest('{0a,ff}'::text[]))")


def test_big_bucket_set_is_still_one_value():
    prefixes = ["%04x" % i for i in range(20000)]
    q = pr.bucket_predicate("t", "code", prefixes, 4)
    # длина префикса и один массив — без 20 тыс. литералов в тексте
    assert _literal_count(q) == 2


def test_compare_levels_and_leaves_use_the_same_array_predicate():
    level = render(pr.build_bucket_sql("s", "t", ["v"], "code", 4,
                                       parents=["0a", "ff"]))
    assert "IN (SELECT unnest('{0a,ff}'::text[]))" in level
    leaves = render(pr.leaves_predicate("t", [
        {"column": "code", "lo": "ab", "hi": None, "is_null": False,
         "mode": "bucket"},
        {"column": "code", "lo": None, "hi": None, "is_null": True,
         "mode": "bucket"}]))
    assert leaves == ('(substr(md5("t"."code"::text), 1, 2) IN '
                      "(SELECT unnest('{ab}'::text[]))) OR "
                      '("t"."code" IS NULL)')


# ------------------------------------------------------------------
# 4. _both: сбой одной стороны отменяет запрос другой
# ------------------------------------------------------------------

class Boom(Exception):
    pass


class Cancelled(Exception):
    pass


def _until_cancelled(conn):
    def run():
        if not conn.cancel_event.wait(5):
            return "прошёл до конца"
        raise Cancelled("canceling statement due to user request")
    return run


def _fail():
    raise Boom("source failed")


@pytest.mark.parametrize("failing", ["src", "dst"])
def test_both_cancels_the_other_side_and_raises_the_first_error(failing):
    import time

    from modules.pg_compare import _both

    src, dst = FakeConn(), FakeConn()
    on_src = _fail if failing == "src" else _until_cancelled(src)
    on_dst = _fail if failing == "dst" else _until_cancelled(dst)

    started = time.time()
    with pytest.raises(Boom):
        _both(on_src, on_dst, src, dst)

    assert time.time() - started < 2
    other = dst if failing == "src" else src
    assert other.cancelled >= 1


def test_both_returns_both_values_without_cancel():
    from modules.pg_compare import _both

    src, dst = FakeConn(), FakeConn()
    assert _both(lambda: 1, lambda: 2, src, dst) == (1, 2)
    assert src.cancelled == dst.cancelled == 0
