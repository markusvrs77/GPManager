# -*- coding: utf-8 -*-
"""
Раннер pg_compare по поправке D01 на фейках: режим корзин (md5-префикс,
один проход на уровень и сторону, одно построчное сравнение на все листья),
число воркеров по числу единиц, аварийный путь, очистка листьев прошлой
попытки, итог без дубля в message.
"""

import hashlib

import pytest

import modules.pg_compare as cmp
import modules.pg_ranges as pr
import modules.table_catalog as table_catalog
from db import sqlite_cursor
from job_manager import get_job, get_job_items
from tests.test_pg_compare_chunks import chunks  # noqa: F401
from tests.test_pg_compare_runner import _by_table, _job, world  # noqa: F401

SRC = {"k%d" % i: "a" for i in range(1000)}
DST = dict(SRC)
DST.update({"k10": "X", "k11": "X", "x1": "a", "x2": "a"})
del DST[("k600")]
DIFF_KEYS = {"k10", "k11", "k600", "x1", "x2"}


def _md5(key):
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def _in_leaf(leaf, key):
    return leaf["mode"] == "bucket" and not leaf["is_null"] \
        and _md5(key).startswith(leaf["lo"])


@pytest.fixture
def buckets(chunks, monkeypatch):  # noqa: F811
    state = {"passes": [], "compares": []}
    monkeypatch.setattr(pr, "LEAF_ROWS", 3)
    monkeypatch.setattr(pr, "pick_chunk_column",
                        lambda sc, dc, s, t, key: {
                            "name": "code", "kind": "text",
                            "collate_c": False, "mode": "bucket"})
    monkeypatch.setattr(cmp, "table_column_types",
                        lambda conn, s, t: {"code": "text", "v": "text"})
    monkeypatch.setattr(cmp, "table_columns", lambda conn, s, t: ["code", "v"])
    monkeypatch.setattr(
        table_catalog, "fetch_unique_indexes",
        lambda cid, tables: ({("s", "big"): ["code"]}, {}))
    monkeypatch.setattr(pr, "leaves_predicate",
                        lambda alias, leaves, column=None: ("LEAVES",
                                                            list(leaves)))

    def checksums(conn, schema, table, columns, column, length, parents=None,
                  null_keys=None):
        data = SRC if conn.readonly else DST
        state["passes"].append(("src" if conn.readonly else "dst", length,
                                sorted(parents or [])))
        out = {}
        for key, value in data.items():
            h = _md5(key)
            if parents and h[:len(parents[0])] not in parents:
                continue
            c = out.setdefault(h[:length], [0, 0, 0, 0])
            c[0] += 1
            c[1] = (c[1] + hash((key, value))) % 997
        return {b: tuple(v) for b, v in out.items()}

    def compare(src, dst, schema, table, key_columns, key_source=None,
                work_mem=None, where=None):
        if where is None:
            return dict(status="same", src_rows=5, dst_rows=5, to_insert=0,
                        to_update=0, to_delete=0, message=None)
        leaves = where[1]
        state["compares"].append(leaves)
        inside = [k for k in set(SRC) | set(DST)
                  if any(_in_leaf(lf, k) for lf in leaves)]
        s = {k: SRC[k] for k in inside if k in SRC}
        d = {k: DST[k] for k in inside if k in DST}
        ins = len(set(s) - set(d))
        upd = len([k for k in set(s) & set(d) if s[k] != d[k]])
        dele = len(set(d) - set(s))
        return dict(status="differs" if ins or upd or dele else "same",
                    src_rows=len(s), dst_rows=len(d), to_insert=ins,
                    to_update=upd, to_delete=dele, message=None)

    monkeypatch.setattr(pr, "bucket_checksums", checksums)
    monkeypatch.setattr(cmp, "compare_table", compare)
    return state


def _numbers(row):
    return (row["status"], row["src_rows"], row["dst_rows"],
            row["to_insert"], row["to_update"], row["to_delete"])


def test_buckets_give_exact_totals_with_one_pass_per_level(buckets):
    job_id = _job([("big", True, True)], parallel=2)

    cmp.run_pg_compare_job(job_id)

    row = _by_table(job_id)["big"]
    # как построчно всей таблицы: вставить k600, изменить k10 и k11,
    # удалить x1 и x2
    assert _numbers(row) == ("differs", 1000, 1001, 1, 2, 2)
    assert get_job(job_id)["status"] == "done"

    # на каждом уровне — по одному проходу на сторону
    levels = sorted({p[1] for p in buckets["passes"]})
    assert levels[0] == pr.BUCKET_START and len(levels) >= 2
    for length in levels:
        assert sorted(p[0] for p in buckets["passes"]
                      if p[1] == length) == ["dst", "src"]
    # следующий уровень читает только несовпавшие корзины прошлого
    deeper = [p for p in buckets["passes"] if p[1] > pr.BUCKET_START]
    assert all(0 < len(p[2]) <= len(DIFF_KEYS) for p in deeper)

    # построчно — одно сравнение на все листья сразу
    assert len(buckets["compares"]) == 1

    leaves = cmp.get_mismatched_ranges(job_id, "s", "big")
    assert leaves and all(lf["mode"] == "bucket" for lf in leaves)
    assert {lf["column"] for lf in leaves} == {"code"}
    assert all(lf["depth"] == len(lf["lo"]) for lf in leaves)
    for key in DIFF_KEYS:
        assert any(_in_leaf(lf, key) for lf in leaves), key
    assert row["chunked"]["mismatched"] == len(leaves)
    assert row["chunked"]["checked"] == row["chunked"]["total"] >= 256
    # итог только в chunked — message не дублирует
    assert row["message"] is None


def test_range_leaves_are_stored_with_range_mode(chunks):  # noqa: F811
    job_id = _job([("big", True, True)])
    cmp.run_pg_compare_job(job_id)
    leaves = cmp.get_mismatched_ranges(job_id, "s", "big")
    assert leaves and {lf["mode"] for lf in leaves} == {"range"}
    assert _by_table(job_id)["big"]["message"] is None


def test_workers_open_only_as_many_pairs_as_units(world):  # noqa: F811
    job_id = _job([("with_pk", True, True)], parallel=4)
    cmp.run_pg_compare_job(job_id)
    assert get_job(job_id)["status"] == "done"
    # одна таблица — одна пара, а не четыре
    assert len(world["opened"]) == 2


def test_ranges_grow_workers_up_to_parallel(chunks):  # noqa: F811
    job_id = _job([("big", True, True)], parallel=3)
    cmp.run_pg_compare_job(job_id)
    opened = len(chunks["world"]["opened"])
    assert 2 < opened <= 6 and opened % 2 == 0


def test_fatal_error_closes_chunked_tables_like_stop(chunks,  # noqa: F811
                                                     monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("worker died")

    monkeypatch.setattr(cmp, "_compare_range", boom)
    job_id = _job([("big", True, True)])

    cmp.run_pg_compare_job(job_id)

    assert get_job(job_id)["status"] == "failed"
    row = _by_table(job_id)["big"]
    assert row["status"] == "error"
    assert row["chunked"] is not None
    assert [i["status"] for i in get_job_items(job_id)] == ["failed"]
    assert cmp.chunk_progress(job_id) == {}


def test_leaves_of_previous_attempt_are_removed(chunks):  # noqa: F811
    job_id = _job([("big", True, True)])
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "INSERT INTO pg_compare_ranges (job_id, schema_name, table_name, "
            "column_name, lo_json, hi_json, depth, status) "
            "VALUES (?, 's', 'big', 'id', '1', '2', 0, 'differs')",
            (job_id,))

    cmp.run_pg_compare_job(job_id)

    leaves = cmp.get_mismatched_ranges(job_id, "s", "big")
    assert len(leaves) == _by_table(job_id)["big"]["chunked"]["mismatched"]
    assert (1, 2) not in [(lf["lo"], lf["hi"]) for lf in leaves]
