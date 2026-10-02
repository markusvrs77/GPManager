# -*- coding: utf-8 -*-
"""
Раннер pg_compare в режиме диапазонов на фейках: нарезка, дробление только
несовпавших диапазонов, итог по таблице, листья в pg_compare_ranges, стоп
и ошибка в диапазоне.

Данные — словари {id: значение} источника и приёмника; предикат диапазона
заменён маркером ("PRED", range), контрольная сумма и построчное сравнение
считаются по словарям.
"""

import pytest

import modules.pg_compare as cmp
import modules.pg_ranges as pr
import modules.table_catalog as table_catalog
from job_manager import get_job, get_job_items
from tests.test_pg_compare_runner import _by_table, _job, world  # noqa: F401

SRC = {i: "a" for i in range(1000)}
DST = dict(SRC)
DST.update({10: "X", 11: "X", -7: "a", 5000: "a"})
del DST[600]
DIFF_IDS = {-7, 10, 11, 600, 5000}


def _inside(rng, i):
    if rng["is_null"]:
        return False
    return ((rng["lo"] is None or i >= rng["lo"])
            and (rng["hi"] is None or i < rng["hi"]))


@pytest.fixture
def chunks(world, monkeypatch):  # noqa: F811
    state = {"checksums": 0, "splits": [], "leaves": [], "hook": None}

    monkeypatch.setattr(pr, "LEAF_ROWS", 50)
    monkeypatch.setattr(pr, "SPLIT_FANOUT", 4)
    monkeypatch.setattr(pr, "should_chunk",
                        lambda conn, s, t: t == "big")
    monkeypatch.setattr(pr, "pick_chunk_column",
                        lambda sc, dc, s, t, key: {"name": "id", "kind": "int",
                                                   "collate_c": False})
    monkeypatch.setattr(pr, "top_ranges", lambda conn, s, t, col: (
        pr.ranges_from_cuts(None, None, [250, 500, 750], 0)
        + [pr.make_range(is_null=True)]))
    monkeypatch.setattr(pr, "range_predicate",
                        lambda alias, col, rng, collate_c=False: ("PRED", rng))

    def data(conn):
        return SRC if conn.readonly else DST

    def checksum(conn, schema, table, columns, pred):
        state["checksums"] += 1
        if state["hook"]:
            state["hook"](pred[1])
        rows = [(i, v) for i, v in data(conn).items() if _inside(pred[1], i)]
        return (len(rows), sum(hash(r) for r in rows) % 997, 0)

    def split(conn, schema, table, col, rng):
        state["splits"].append(rng)
        ids = [i for i in SRC if _inside(rng, i)]
        cuts = pr.arithmetic_cuts("int", min(ids), max(ids), pr.SPLIT_FANOUT) \
            if ids else []
        return pr.ranges_from_cuts(rng["lo"], rng["hi"], cuts,
                                   rng["depth"] + 1) if cuts else []

    def compare(src, dst, schema, table, key_columns, key_source=None,
                work_mem=None, where=None):
        if where is None:
            return dict(status="same", src_rows=5, dst_rows=5, to_insert=0,
                        to_update=0, to_delete=0, message=None)
        rng = where[1]
        s = {i: v for i, v in SRC.items() if _inside(rng, i)}
        d = {i: v for i, v in DST.items() if _inside(rng, i)}
        state["leaves"].append(rng)
        ins = len(set(s) - set(d))
        upd = len([i for i in set(s) & set(d) if s[i] != d[i]])
        dele = len(set(d) - set(s))
        return dict(status="differs" if ins or upd or dele else "same",
                    src_rows=len(s), dst_rows=len(d), to_insert=ins,
                    to_update=upd, to_delete=dele, message=None)

    monkeypatch.setattr(pr, "range_checksum", checksum)
    monkeypatch.setattr(pr, "split_range", split)
    monkeypatch.setattr(cmp, "compare_table", compare)
    monkeypatch.setattr(cmp, "table_column_types",
                        lambda conn, s, t: {"id": "integer"})
    monkeypatch.setattr(
        table_catalog, "fetch_unique_indexes",
        lambda cid, tables: ({("s", "big"): ["id"]}, {}))
    state["world"] = world
    return state


def _numbers(row):
    return (row["status"], row["src_rows"], row["dst_rows"],
            row["to_insert"], row["to_update"], row["to_delete"])


@pytest.mark.parametrize("parallel", [1, 3])
def test_big_table_is_compared_by_ranges_with_exact_totals(chunks, parallel):
    job_id = _job([("big", True, True), ("small", True, True)],
                  parallel=parallel)

    cmp.run_pg_compare_job(job_id)

    results = _by_table(job_id)
    # как построчно всей таблицы: 1000 / 1001, вставить 600, изменить 10
    # и 11, удалить -7 и 5000
    assert _numbers(results["big"]) == ("differs", 1000, 1001, 1, 2, 2)
    assert results["small"]["status"] == "same"
    assert get_job(job_id)["status"] == "done"
    assert {i["status"] for i in get_job_items(job_id)} == {"done"}

    # дробятся только несовпавшие диапазоны, листья — не больше LEAF_ROWS
    for rng in chunks["splits"] + chunks["leaves"]:
        assert any(_inside(rng, i) for i in DIFF_IDS)
    for rng in chunks["leaves"]:
        assert len([i for i in DST if _inside(rng, i)]) <= 50

    leaves = cmp.get_mismatched_ranges(job_id, "s", "big")
    assert len(leaves) == 3
    assert {r["column"] for r in leaves} == {"id"}
    assert sum(r["to_insert"] + r["to_update"] + r["to_delete"]
               for r in leaves) == 5
    assert all(r["status"] == "differs" for r in leaves)
    covered = sorted(i for i in DIFF_IDS
                     for r in leaves if _inside(r, i))
    assert covered == sorted(DIFF_IDS)

    chunked = results["big"]["chunked"]
    assert chunked["mismatched"] == 3
    assert chunked["checked"] == chunked["total"] == chunks["checksums"] // 2
    assert "по диапазонам" in results["big"]["message"]
    assert results["small"]["chunked"] is None
    assert cmp.get_mismatched_ranges(job_id, "s", "small") == []


def test_stop_in_the_middle_of_ranges(chunks):
    def stop_after_some(rng):
        if chunks["checksums"] >= 8:
            chunks["world"]["stop"] = True

    chunks["hook"] = stop_after_some
    job_id = _job([("big", True, True)])

    cmp.run_pg_compare_job(job_id)

    assert get_job(job_id)["status"] == "cancelled"
    items = {i["table_name"]: i["status"] for i in get_job_items(job_id)}
    assert items == {"big": "cancelled"}
    assert _by_table(job_id)["big"]["status"] == "cancelled"
    # оставшиеся диапазоны не считаются (полный прогон — десятки сумм)
    assert chunks["checksums"] <= 10
    assert chunks["leaves"] == []


def test_error_in_one_range_fails_only_its_table(chunks):
    def boom(rng):
        if _inside(rng, 600) and rng["depth"] == 1:
            raise RuntimeError("could not read block")

    chunks["hook"] = boom
    job_id = _job([("big", True, True), ("small", True, True)])

    cmp.run_pg_compare_job(job_id)

    results = _by_table(job_id)
    assert results["big"]["status"] == "error"
    assert "could not read block" in results["big"]["message"]
    assert results["small"]["status"] == "same"
    items = {i["table_name"]: i["status"] for i in get_job_items(job_id)}
    assert items == {"big": "failed", "small": "done"}
    assert get_job(job_id)["status"] == "failed"
