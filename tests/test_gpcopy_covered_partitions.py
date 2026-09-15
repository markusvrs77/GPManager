# -*- coding: utf-8 -*-
"""
Партиция, выбранная вместе со своим корнем.

Маска dwh_bi.* подхватывала и dwh_bi.osv, и её партиции osv_1_prt_*.
gpcopy получал одну партицию дважды и копировал её параллельно — два
потока могли оба очистить её и оба залить, строки задваивались.
"""

import pytest

import modules.gpcopy as gp
import modules.gpcopy_partition as gpp
import modules.table_catalog as catalog
from job_manager import create_job, get_job_items

OSV = ("dwh_bi", "osv")
P1 = ("dwh_bi", "osv_1_prt_p20260826")
P2 = ("dwh_bi", "osv_1_prt_p20260827")
SUB = ("dwh_bi", "osv_1_prt_p20260826_2_prt_x")   # субпартиция
INV = ("dwh_bi", "inv_1")

PAIRS = {P1: OSV, P2: OSV, SUB: P1}

CONN = {"host": "gp-host", "port": 5432, "username": "gpadmin",
        "database_name": "adb"}


class Stop(Exception):
    """Останавливает раннер до запуска настоящего gpcopy."""


# ------------------------------------------------------------ чистые

def test_leaf_is_dropped_when_its_root_is_selected():
    kept, covered = catalog.drop_covered_partitions([OSV, P1, INV], PAIRS)

    assert kept == [OSV, INV]
    assert covered == {P1: OSV}


def test_subpartition_is_dropped_by_any_selected_ancestor():
    kept, covered = catalog.drop_covered_partitions([OSV, SUB], PAIRS)

    assert kept == [OSV]
    assert covered[SUB] == OSV


def test_leaf_without_its_root_stays():
    """Одну партицию можно перелить отдельно — если корень не выбран."""
    kept, covered = catalog.drop_covered_partitions([P1, INV], PAIRS)

    assert kept == [P1, INV]
    assert covered == {}


def test_exact_repeat_is_kept_once():
    kept, _covered = catalog.drop_covered_partitions([INV, OSV, INV], PAIRS)

    assert kept == [INV, OSV]


def test_cycle_in_catalog_does_not_hang():
    looped = {("s", "a"): ("s", "b"), ("s", "b"): ("s", "a")}

    kept, _covered = catalog.drop_covered_partitions([("s", "a")], looped)

    assert kept == [("s", "a")]


def test_copy_items_are_deduplicated_in_order():
    items = [
        {"schema_name": "dwh_bi", "table_name": "p1"},
        {"schema_name": "dwh_bi", "table_name": "p2"},
        {"schema_name": "dwh_bi", "table_name": "p1"},
    ]

    assert [i["table_name"] for i in gpp._dedupe_copy_items(items)] == \
        ["p1", "p2"]


# ------------------------------------------------------------ строки задачи

def _job(tables):
    return create_job("gpcopy_partition_diff", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": s, "table": t} for s, t in tables],
        "recompute": True,
    })


def test_covered_item_is_skipped_with_the_root_named(monkeypatch):
    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: PAIRS)
    job_id = _job([OSV, P1, INV])

    kept, covered = gp.skip_covered_items(get_job_items(job_id), 1)

    assert [i["table_name"] for i in kept] == ["osv", "inv_1"]

    leaf = [i for i in get_job_items(job_id) if i["table_name"] == P1[1]][0]
    assert leaf["status"] == "skipped"
    assert "dwh_bi.osv" in leaf["error_message"]


def test_repeat_item_is_skipped_as_a_repeat(monkeypatch):
    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: PAIRS)
    job_id = _job([INV, INV])

    kept, _covered = gp.skip_covered_items(get_job_items(job_id), 1)

    assert len(kept) == 1
    statuses = sorted(i["status"] for i in get_job_items(job_id))
    assert statuses.count("skipped") == 1


def test_unreadable_hierarchy_stops_the_run(monkeypatch):
    """Без иерархии нельзя поручиться, что партиция не уйдёт дважды."""
    def boom(cid):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(catalog, "fetch_partition_pairs", boom)
    job_id = _job([OSV, P1])

    with pytest.raises(Exception) as error:
        gp.skip_covered_items(get_job_items(job_id), 1)

    assert "иерархию партиций" in str(error.value)


def test_skipped_items_do_not_attract_log_progress(monkeypatch):
    """
    find_owner_item ищет сначала точное имя: оставшаяся строка партиции
    забирала бы прогресс у корня, через который партиция льётся.
    """
    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: PAIRS)
    job_id = _job([OSV, P1])
    gp.skip_covered_items(get_job_items(job_id), 1)

    keys = gp.owner_item_keys(get_job_items(job_id))
    owner = gp.find_owner_item(P1[0], P1[1], keys)
    osv_id = [i["id"] for i in get_job_items(job_id)
              if i["table_name"] == "osv"][0]

    assert owner == osv_id


# ------------------------------------------------------------ раннер партиций

@pytest.fixture
def partition_run(monkeypatch):
    """Прогоняет раннер партиций до gpcopy и возвращает, что ушло бы в него."""
    seen = {}

    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: PAIRS)
    monkeypatch.setattr(gpp, "get_connection_by_id", lambda cid: CONN)

    def fake_diff(src, dst, roots, exact=False):
        seen["roots"] = list(roots)
        diff = {
            OSV: [
                {"partition": P1[1], "action": "copy_changed"},
                {"partition": P2[1], "action": "skip"},
            ],
            INV: [{"partition": INV[1], "action": "copy_missing"}],
        }
        leaves = {OSV: {P1[1]: P1, P2[1]: P2}, INV: {INV[1]: INV}}
        return ({r: diff[r] for r in roots if r in diff},
                {r: leaves[r] for r in roots if r in leaves})

    def fake_include(items, dbname=None):
        seen["include"] = [(i["schema_name"], i["table_name"]) for i in items]
        return "unused-include-file"

    def fake_command(**kwargs):
        seen["command"] = kwargs
        raise Stop()

    monkeypatch.setattr(gpp, "diff_partitions_stats", fake_diff)
    monkeypatch.setattr(gpp, "make_include_table_file", fake_include)
    monkeypatch.setattr(gpp, "build_gpcopy_command", fake_command)

    def run(tables):
        job_id = _job(tables)
        gpp.run_gpcopy_partition_diff_job(job_id)
        seen["job_id"] = job_id
        return seen

    return run


def test_partition_runner_diffs_each_root_once(partition_run):
    seen = partition_run([OSV, P1, P2, INV, INV])

    assert seen["roots"] == [OSV, INV]


def test_partition_runner_sends_each_partition_once(partition_run):
    seen = partition_run([OSV, P1, P2, INV])

    assert seen["include"] == [P1, INV]


def test_partition_runner_asks_gpcopy_to_analyze(partition_run):
    """Без ANALYZE сравнение по статистике снова видит расхождение."""
    seen = partition_run([OSV, INV])

    assert seen["command"]["analyze"] is True
    assert seen["command"]["truncate"] is True


# ------------------------------------------------------------ полный раннер

def test_full_runner_leaves_covered_partitions_out(monkeypatch):
    seen = {}

    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: PAIRS)
    monkeypatch.setattr(gp, "get_connection_by_id", lambda cid: CONN)

    def fake_include(items, dbname=None):
        seen["include"] = [(i["schema_name"], i["table_name"]) for i in items]
        raise Stop()

    monkeypatch.setattr(gp, "make_include_table_file", fake_include)

    job_id = create_job("gpcopy", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": s, "table": t} for s, t in [OSV, P1, P2, INV]],
        "truncate": True, "gpcopy_path": "/usr/local/bin/gpcopy",
    })

    gp.run_gpcopy_job(job_id)

    assert seen["include"] == [OSV, INV]
