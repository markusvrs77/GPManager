# -*- coding: utf-8 -*-
"""
«Только общие колонки»: таблицы, у которых колонки источника и приёмника
различаются, идут через промежуточные таблицы, а в цель переливаются
только общие колонки.
"""

import json

import pytest

import modules.gpcopy as gp
import modules.gpcopy_partition as gpp
import modules.table_catalog as catalog
from job_manager import create_job, get_job, get_job_items
from modules import gpcopy_columns as gc
from modules import gpcopy_stage as st
from tests.pg_fakes import FakeConn

CONN = {"host": "gp-host", "port": 5432, "username": "gpadmin",
        "database_name": "adb"}

OSV = ("s", "osv")
P1 = ("s", "osv_1_prt_1")
P2 = ("s", "osv_1_prt_2")
INV = ("s", "inv")
SAME = ("s", "same")
BAD = ("s", "bad")


class Stop(Exception):
    """Останавливает раннер до запуска настоящего gpcopy."""


def _col(name, not_null=False, has_default=False):
    return {"name": name, "not_null": not_null, "has_default": has_default}


def _diff(src, dest=None, common=("id",), src_only=(), dst_only=(),
          blocking=()):
    return {"source": src, "dest": dest or src, "common": list(common),
            "src_only": list(src_only), "dst_only": list(dst_only),
            "blocking": list(blocking)}


# ------------------------------------------------------------ сравнение

def test_column_diff_keeps_source_order_and_finds_blocking():
    diff = gc.column_diff(
        ["id", "old", "d"],
        [_col("d"), _col("id"), _col("docp_correctdt"),
         _col("must", not_null=True), _col("dflt", True, True)])

    assert diff["common"] == ["id", "d"]
    assert diff["src_only"] == ["old"]
    assert diff["dst_only"] == ["docp_correctdt", "must", "dflt"]
    # NOT NULL с DEFAULT — не помеха, без DEFAULT — помеха
    assert diff["blocking"] == ["must"]


def test_differing_tables_one_query_per_side_and_skips_missing():
    def columns(rows):
        def respond(params):
            wanted = set(params[0])
            return [r for r in rows if (r[0], r[1]) in wanted]
        return respond

    src = FakeConn([("pg_attribute", columns([
        ("s", "a", "id", True, False), ("s", "a", "x", False, False),
        ("s", "b", "id", True, False),
        ("s", "gone_in_dst", "id", True, False),
    ]))])
    dst = FakeConn([("pg_attribute", columns([
        ("s", "a", "id", True, False), ("s", "a", "y", False, False),
        ("t", "b2", "id", True, False),
    ]))])

    out = gc.differing_tables(src, dst, [
        ("s", "a", "s", "a"), ("s", "b", "t", "b2"),
        ("s", "gone_in_dst", "s", "gone_in_dst")])

    assert [(d["source"], d["dest"]) for d in out] == [(("s", "a"), ("s", "a"))]
    assert out[0]["src_only"] == ["x"] and out[0]["dst_only"] == ["y"]
    assert len(src.executed) == 1 and len(dst.executed) == 1
    assert "attisdropped" in src.executed[0][0]


def test_refuse_reason():
    assert gc.refuse_reason(_diff(INV)) is None
    assert "NOT NULL" in gc.refuse_reason(_diff(INV, blocking=["must"]))
    assert "нет общих колонок" in gc.refuse_reason(_diff(INV, common=()))


# ------------------------------------------------------------ перелив

class _MergeConn(FakeConn):
    """Колонки промежуточной: id, old, d; цели: d, id, extra."""

    def __init__(self, target_cols=(("d",), ("id",), ("extra",))):
        def cols(params):
            if params[0] == "stg":
                return [("id",), ("old",), ("d",)]
            return list(target_cols)

        super().__init__([("pg_attribute", cols)])


def _merge(**extra):
    return dict({"item": ["s", "a"], "target": ["s", "a"], "truncate": True,
                 "stages": [["stg", "j1_00001"]]}, **extra)


def _dml(conn):
    return [t for t, _ in conn.executed if "pg_attribute" not in t]


def test_common_columns_merge_inserts_only_shared_columns():
    conn = _MergeConn()

    assert st.apply_merges(conn, [_merge(common_columns=True)]) == {}
    assert _dml(conn) == [
        'TRUNCATE TABLE "s"."a"',
        'INSERT INTO "s"."a" ("id", "d") SELECT "id", "d" '
        'FROM "stg"."j1_00001"',
    ]
    assert conn.commits == 1


def test_merge_without_flag_is_unchanged():
    conn = _MergeConn()

    st.apply_merges(conn, [_merge()])

    assert _dml(conn)[1] == ('INSERT INTO "s"."a" ("id", "old", "d") '
                             'SELECT "id", "old", "d" FROM "stg"."j1_00001"')
    # колонки цели без флага не читаются
    assert len([t for t, _ in conn.executed if "pg_attribute" in t]) == 1


def test_no_shared_columns_rolls_back_target():
    conn = _MergeConn(target_cols=[("other",)])

    errors = st.apply_merges(conn, [_merge(common_columns=True)])

    assert "нет общих колонок" in errors[("s", "a")]
    assert "цель не изменена" in errors[("s", "a")]
    assert conn.rollbacks == 1 and conn.commits == 0


def test_plan_stages_carries_the_flag():
    roots = {"adb.s.a": {"item": ["s", "a"], "target": ["s", "a"],
                         "truncate": False, "common_columns": True},
             "adb.s.b": {"item": ["s", "b"], "target": ["s", "b"],
                         "truncate": False}}
    entries = [{"source": "adb.s.a", "dest": "adb.s.a"},
               {"source": "adb.s.b", "dest": "adb.s.b"}]

    _out, merges = st.plan_stages(entries, roots, 5,
                                  lambda s, t: "adb.{}.{}".format(s, t))

    assert merges[0]["common_columns"] is True
    assert "common_columns" not in merges[1]


# ------------------------------------------------------------ полный режим

def test_full_mode_routing_decision():
    diffs = [_diff(OSV), _diff(INV), _diff(P1), _diff(BAD, blocking=["m"])]
    leaves = {OSV: [P1, P2], INV: [INV], P1: [P1]}

    part, plain, errors = gp.route_common_columns(
        diffs, lambda s, t: leaves[(s, t)])

    assert part == {OSV: [P1, P2]}
    # партиция строкой задачи (дозагрузка, продолжение) — как обычная
    assert plain == [INV, P1]
    assert list(errors) == [BAD] and "NOT NULL" in errors[BAD]


@pytest.mark.parametrize("config,mode,drop,skip,dry,expected", [
    ({"common_columns": True}, "", False, False, False, True),
    ({"common_columns": True}, "full", False, False, False, True),
    ({}, "", False, False, False, False),
    ({"common_columns": True}, "date_filter", False, False, False, False),
    ({"common_columns": True}, "", True, False, False, False),
    ({"common_columns": True}, "", False, True, False, False),
    ({"common_columns": True}, "", False, False, True, False),
    ({"common_columns": True, "extra_args": "--skip-existing"},
     "", False, False, False, False),
    ({"common_columns": True, "stage_resume": [{}]},
     "", False, False, False, False),
])
def test_common_columns_enabled(config, mode, drop, skip, dry, expected):
    assert gp.common_columns_enabled(config, mode, drop, skip, dry) is expected


@pytest.fixture
def full_run(monkeypatch):
    """Полный раннер до gpcopy: что ушло бы в include JSON и в перелив."""
    seen = {"dropped": 0}

    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: {})
    monkeypatch.setattr(gp, "get_connection_by_id", lambda cid: CONN)
    monkeypatch.setattr(gp, "open_psycopg2_connection_by_cfg",
                        lambda cfg: FakeConn())
    monkeypatch.setattr(gpp, "list_leaf_partitions",
                        lambda conn, s, t: [P1, P2] if (s, t) == OSV
                        else [(s, t)])
    monkeypatch.setattr(gp, "common_columns_diff", lambda src, dst, pairs: [
        _diff(p[:2], p[2:]) for p in pairs if p[:2] in (OSV, INV)
    ] + [_diff(BAD, blocking=["must"]) for p in pairs if p[:2] == BAD])
    monkeypatch.setattr(gp, "create_job_stages",
                        lambda job_id, merges, s, d: seen.update(created=merges))

    def fake_command(**kwargs):
        seen["command"] = kwargs
        with open(kwargs["include_json_file"], encoding="utf-8") as f:
            seen["entries"] = json.load(f)
        raise Stop()

    def fake_drop(config):
        seen["dropped"] += 1

    monkeypatch.setattr(gp, "build_gpcopy_command", fake_command)
    monkeypatch.setattr(gp, "drop_stages_quietly", fake_drop)

    def run(tables, **config):
        job_id = create_job("gpcopy", 1, dict({
            "source_connection_id": 1, "dest_connection_id": 2,
            "tables": [{"schema": s, "table": t} for s, t in tables],
            "truncate": True, "gpcopy_path": "/usr/local/bin/gpcopy",
            "common_columns": True,
        }, **config))
        gp.run_gpcopy_job(job_id)
        seen["job_id"] = job_id
        return seen

    return run


def test_full_runner_stages_only_differing_tables(full_run):
    seen = full_run([OSV, INV, SAME])

    dests = {e["source"]: e["dest"] for e in seen["entries"]}
    assert dests["adb.s.same"] == "adb.s.same"
    assert dests["adb.s.inv"].startswith("adb.opsentri_gpcopy_stage.")
    assert dests["adb.s.osv_1_prt_1"].startswith("adb.opsentri_gpcopy_stage.")
    assert dests["adb.s.osv_1_prt_2"].startswith("adb.opsentri_gpcopy_stage.")

    merges = {tuple(m["target"]): m for m in seen["created"]}
    assert set(merges) == {OSV, INV}
    assert len(merges[OSV]["stages"]) == 2
    assert all(m["common_columns"] and m["truncate"] for m in merges.values())

    saved = json.loads(get_job(seen["job_id"])["config_json"])
    assert len(saved["stage_merges"]) == 2
    # gpcopy не дошёл до запуска — промежуточные удаляются
    assert seen["dropped"] >= 1


def test_full_runner_append_merges_without_truncate(full_run):
    seen = full_run([INV], truncate=False, append=True)

    assert seen["created"][0]["truncate"] is False


def test_full_runner_fails_not_null_target_column_up_front(monkeypatch,
                                                           full_run):
    sent = {}

    def fake_include(items, dbname=None):
        sent["include"] = [(i["schema_name"], i["table_name"]) for i in items]
        raise Stop()

    monkeypatch.setattr(gp, "make_include_table_file", fake_include)
    seen = full_run([BAD, SAME])

    items = {i["table_name"]: i for i in get_job_items(seen["job_id"])}
    assert items["bad"]["status"] == "failed"
    assert "NOT NULL" in items["bad"]["error_message"]
    # в gpcopy ушла только вторая таблица — без промежуточных
    assert sent["include"] == [SAME]
    assert "created" not in seen


def test_full_runner_without_differences_keeps_include_file(monkeypatch,
                                                           full_run):
    seen = {}

    def fake_include(items, dbname=None):
        seen["include"] = [(i["schema_name"], i["table_name"]) for i in items]
        raise Stop()

    monkeypatch.setattr(gp, "make_include_table_file", fake_include)
    full_run([SAME])

    assert seen["include"] == [SAME]


# ------------------------------------------------------------ режим партиций

def test_partition_plan_stages_only_leaves_of_differing_roots():
    copy_items = [{"schema_name": s, "table_name": t}
                  for s, t in (P1, P2, INV)]
    leaf_root = {P1: OSV, P2: OSV, INV: INV}

    entries, merges = gpp.plan_common_columns_stages(
        copy_items, leaf_root, {OSV: _diff(OSV)}, 9, "src", "dst")

    assert [e["source"] for e in entries] == ["src.s.osv_1_prt_1",
                                              "src.s.osv_1_prt_2", "src.s.inv"]
    assert entries[2]["dest"] == "dst.s.inv"
    assert entries[0]["dest"] == "dst.opsentri_gpcopy_stage.j9_00001"
    # одна промежуточная на партицию, перелив в одноимённую партицию
    assert [(m["item"], m["target"], m["truncate"], m["common_columns"],
             len(m["stages"])) for m in merges] == [
        (["s", "osv"], ["s", "osv_1_prt_1"], True, True, 1),
        (["s", "osv"], ["s", "osv_1_prt_2"], True, True, 1),
    ]


def test_partition_plan_without_differences_needs_no_stages():
    entries, merges = gpp.plan_common_columns_stages(
        [{"schema_name": "s", "table_name": "inv"}], {}, {}, 9, "a", "b")

    assert merges == [] and entries[0]["dest"] == "b.s.inv"


def test_partition_refused_root_drops_its_leaves():
    copy_items = [{"schema_name": s, "table_name": t} for s, t in (P1, INV)]

    kept, errors = gpp.split_refused_leaves(
        copy_items, {P1: OSV, INV: INV},
        {OSV: _diff(OSV, blocking=["must"]), INV: _diff(INV)})

    assert [c["table_name"] for c in kept] == ["inv"]
    assert list(errors) == [OSV]


@pytest.fixture
def partition_run(monkeypatch):
    seen = {"dropped": 0}

    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: {})
    monkeypatch.setattr(gpp, "get_connection_by_id", lambda cid: CONN)

    def fake_diff(src, dst, roots, exact=False):
        diff = {OSV: [{"partition": P1[1], "action": "copy_changed"},
                      {"partition": P2[1], "action": "copy_missing"}],
                INV: [{"partition": INV[1], "action": "copy_changed"}]}
        leaves = {OSV: {P1[1]: P1, P2[1]: P2}, INV: {INV[1]: INV}}
        return ({r: diff[r] for r in roots}, {r: leaves[r] for r in roots})

    monkeypatch.setattr(gpp, "diff_partitions_stats", fake_diff)
    monkeypatch.setattr(gpp, "partition_column_diffs",
                        lambda s, d, roots: ({OSV: _diff(OSV)},
                                             {P1: OSV, P2: OSV, INV: INV}))
    monkeypatch.setattr(gpp, "create_job_stages",
                        lambda job_id, merges, s, d: seen.update(created=merges))

    def fake_include(items, dbname=None):
        seen["include_file"] = True
        return "unused"

    def fake_command(**kwargs):
        seen["command"] = kwargs
        if kwargs.get("include_json_file"):
            with open(kwargs["include_json_file"], encoding="utf-8") as f:
                seen["entries"] = json.load(f)
        raise Stop()

    def fake_drop(config):
        seen["dropped"] += 1

    monkeypatch.setattr(gpp, "make_include_table_file", fake_include)
    monkeypatch.setattr(gpp, "build_gpcopy_command", fake_command)
    monkeypatch.setattr(gpp, "drop_stages_quietly", fake_drop)

    def run(**config):
        job_id = create_job("gpcopy_partition_diff", 1, dict({
            "source_connection_id": 1, "dest_connection_id": 2,
            "tables": [{"schema": s, "table": t} for s, t in (OSV, INV)],
            "recompute": True,
        }, **config))
        gpp.run_gpcopy_partition_diff_job(job_id)
        seen["job_id"] = job_id
        return seen

    return run


def test_partition_runner_routes_differing_root_through_stages(partition_run):
    seen = partition_run(common_columns=True)

    assert "include_file" not in seen
    assert seen["command"]["include_tables_file"] is None
    dests = {e["source"]: e["dest"] for e in seen["entries"]}
    assert dests["adb.s.inv"] == "adb.s.inv"
    assert dests["adb.s.osv_1_prt_1"].startswith("adb.opsentri_gpcopy_stage.")
    assert [m["target"] for m in seen["created"]] == [list(P1), list(P2)]

    saved = json.loads(get_job(seen["job_id"])["config_json"])
    assert len(saved["stage_merges"]) == 2
    assert saved["planned_leaves"]    # план «Продолжить» не потерян
    assert seen["dropped"] == 1       # авария до gpcopy — промежуточные вон


def test_partition_runner_without_flag_is_unchanged(partition_run):
    seen = partition_run()

    assert seen["include_file"] is True
    assert seen["command"]["include_json_file"] is None
    assert "created" not in seen


def test_continue_and_retry_inherit_the_flag():
    cfg = {"source_connection_id": 1, "dest_connection_id": 2,
           "truncate": True, "common_columns": True}
    items = [{"schema_name": "s", "table_name": "a", "status": "failed"}]

    cont, _ = gp.build_continue_config(cfg, items)
    pcont, _ = gp.build_partition_continue_config(cfg, items)
    retry = gp.build_retry_config(cfg, [("s", "a")])

    assert cont["common_columns"] and pcont["common_columns"]
    assert retry["common_columns"]


def test_stage_resume_lists_each_item_once():
    kept = [{"item": ["s", "osv"], "target": ["s", "osv_1_prt_1"],
             "stages": [], "entries": []},
            {"item": ["s", "osv"], "target": ["s", "osv_1_prt_2"],
             "stages": [], "entries": []}]

    resume = gp.build_stage_resume_config(
        {"source_connection_id": 1, "dest_connection_id": 2}, kept)

    assert resume["tables"] == [{"schema": "s", "table": "osv"}]
