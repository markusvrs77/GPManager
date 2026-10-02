# -*- coding: utf-8 -*-
"""
Загрузка разницы по листьям обоих видов (поправка D01): общий построитель
предиката, COPY в staging и DELETE пачками по LEAF_BATCH листьев, все
пачки применения — одна транзакция приёмника.
"""

import modules.pg_diff_load as pdl
import modules.pg_ranges as pr
from tests.test_pg_diff_load_ranges import (KEYED, _pair, _texts,  # noqa
                                            columns)


def _leaf(lo, hi):
    return {"column": "id", "lo": lo, "hi": hi, "is_null": False,
            "depth": 1, "collate_c": False, "mode": "range"}


RANGE_LEAVES = [_leaf(i * 10, i * 10 + 5) for i in range(5)]

BUCKET_LEAVES = [
    {"column": "id", "lo": "0a", "hi": None, "is_null": False, "depth": 2,
     "collate_c": False, "mode": "bucket"},
    {"column": "id", "lo": "ff12", "hi": None, "is_null": False,
     "depth": 4, "collate_c": False, "mode": "bucket"},
]


def test_staging_and_delete_go_by_leaf_batches_in_one_transaction(
        columns, monkeypatch):  # noqa: F811
    monkeypatch.setattr(pr, "LEAF_BATCH", 2)
    src, dst = _pair(KEYED)

    result = pdl.load_diff(src, dst, "s", "t", ["id"], True, "stg_7_1",
                           ranges=RANGE_LEAVES)

    # 5 листьев по 2 — три COPY и три DELETE
    assert len(src.copies) == 3
    assert '"id" >= 0 AND "id" < 5' in src.copies[0]
    assert '"id" >= 40 AND "id" < 45' in src.copies[2]
    assert '"id" >= 40' not in src.copies[0]
    texts = _texts(dst)
    deletes = [t for t in texts if t.startswith("DELETE FROM")]
    assert len(deletes) == 3
    assert '"t"."id" >= 20' in deletes[1] and '"t"."id" >= 0 ' \
        not in deletes[1]
    assert result["delete"] == 3
    # UPDATE и INSERT — по staging один раз
    assert len([t for t in texts if t.startswith("UPDATE")]) == 1
    assert len([t for t in texts if t.startswith("INSERT INTO")]) == 1
    first = texts.index(deletes[0])
    last = next(i for i, t in enumerate(texts) if t.startswith("INSERT INTO"))
    assert "COMMIT" not in texts[first:last]


def test_bucket_leaves_use_md5_prefix_predicate(columns):  # noqa: F811
    src, dst = _pair(KEYED)

    pdl.load_diff(src, dst, "s", "t", ["id"], True, "stg_7_1",
                  ranges=BUCKET_LEAVES)

    assert src.copies == [
        'COPY (SELECT "id", "name" FROM "s"."t" WHERE '
        '(substr(md5("id"::text), 1, 2) IN '
        '(SELECT unnest(\'{0a}\'::text[]))) OR '
        '(substr(md5("id"::text), 1, 4) IN '
        '(SELECT unnest(\'{ff12}\'::text[])))) TO STDOUT']
    delete = [t for t in _texts(dst) if t.startswith("DELETE FROM")][0]
    assert ('(substr(md5("t"."id"::text), 1, 2) IN '
            '(SELECT unnest(\'{0a}\'::text[])))') in delete


def test_keyless_apply_goes_by_batches_in_one_transaction(
        columns, monkeypatch):  # noqa: F811
    monkeypatch.setattr(pr, "LEAF_BATCH", 3)
    src, dst = _pair([("DELETE FROM", [(1,)]), ("INSERT INTO", [(1,)] * 2)])

    result = pdl.load_diff(src, dst, "s", "t", [], True, "stg_7_1",
                           ranges=RANGE_LEAVES)

    applied = [t for t in _texts(dst) if t.startswith("WITH")]
    assert len(applied) == 4
    assert result == {"insert": 4, "update": 0, "delete": 2}
    texts = _texts(dst)
    first = texts.index(applied[0])
    last = texts.index(applied[-1])
    assert "COMMIT" not in texts[first:last]


def test_ranges_where_is_the_shared_builder():
    leaves = RANGE_LEAVES[:1] + BUCKET_LEAVES
    assert pdl.build_ranges_where("t", leaves).as_string is not None
    from tests.pg_fakes import render
    assert render(pdl.build_ranges_where("t", leaves)) == \
        render(pr.leaves_predicate("t", leaves))
