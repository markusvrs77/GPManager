# -*- coding: utf-8 -*-
"""Умная синхронизация: волны, отпечатки, пачки, срок окна, раннер, расписание."""

import json
from datetime import datetime

import pytest

import modules.smart_sync as ss
import scheduler_store
from job_manager import create_job, get_job, get_job_items

SETTINGS = ss.normalize_settings({
    "waves": [
        {"name": "Stage", "include": "dwh_stage.*", "start": "03:00", "deadline": "05:30"},
        {"name": "Основная", "include": ["*.*"], "exclude": "dwh_tmp.*",
         "start": "00:30", "deadline": "05:30"},
    ],
    "exclude": "*.*_bkp_*",
    "big_gb": 1, "batch_gb": 2, "batch_tables": 2,
})


def rel(oid, schema, table, parent=None, fn=None):
    return {"oid": oid, "schema": schema, "table": table, "kind": "r",
            "filenode": fn or oid * 10, "parent": parent}


def catalog(rels, dml=None, last_op=None):
    return {"relations": {r["oid"]: r for r in rels}, "last_op": last_op or {},
            "dml": {r["oid"]: 0 for r in rels} if dml is None else dml, "stats": "v"}


SRC = catalog([
    rel(1, "dwh_stage", "clients"),
    rel(2, "dwh_dds", "fact"), rel(3, "dwh_dds", "fact_1_prt_d1", 2),
    rel(4, "dwh_dds", "fact_1_prt_d2", 2),
    rel(5, "dwh_dds", "dim"),
    rel(6, "dwh_tmp", "junk"), rel(7, "dwh_dds", "dim_bkp_1"),
])

DST = catalog([
    rel(11, "dwh_dds", "fact"), rel(12, "dwh_dds", "fact_1_prt_d1", 11),
    rel(13, "dwh_dds", "fact_1_prt_d0", 11),
    rel(14, "dwh_dds", "dim"), rel(15, "dwh_stage", "clients"),
])


# ------------------------------------------------------------ настройки

def test_settings_validation():
    with pytest.raises(ValueError):
        ss.normalize_settings({"waves": []})
    with pytest.raises(ValueError):
        ss.normalize_settings({"waves": [{"name": "x", "include": "dwh_stage"}]})   # без схемы
    with pytest.raises(ValueError):
        ss.normalize_settings({"waves": [{"name": "x", "include": "a.*; drop"}]})
    with pytest.raises(ValueError):
        ss.normalize_settings({"waves": [{"include": "a.*", "start": "25:00"}]})
    with pytest.raises(ValueError):
        ss.normalize_settings({"waves": [{"include": "a.*"}], "gpcopy_path": "gpcopy; rm"})


def test_wave_of_first_match_and_excludes():
    assert ss.wave_of(("dwh_stage", "clients"), SETTINGS) == 0
    assert ss.wave_of(("dwh_dds", "fact"), SETTINGS) == 1
    assert ss.wave_of(("dwh_tmp", "junk"), SETTINGS) is None        # исключено в волне
    assert ss.wave_of(("dwh_dds", "dim_bkp_1"), SETTINGS) is None   # исключено везде
    assert ss.wave_of(("DWH_STAGE", "X"), SETTINGS) == 0            # регистр не важен


def test_build_tree_leaves():
    tree = ss.build_tree(SRC["relations"])

    assert set(tree[("dwh_dds", "fact")]["leaves"]) == {("dwh_dds", "fact_1_prt_d1"),
                                                         ("dwh_dds", "fact_1_prt_d2")}
    assert set(tree[("dwh_dds", "dim")]["leaves"]) == {("dwh_dds", "dim")}


# ------------------------------------------------------------ что изменилось

def test_first_run_everything_changed_new_and_missing_parts():
    plan = ss.plan_wave(SRC, DST, SETTINGS, 1, state={})
    items = {it["root"]: it for it in plan["items"]}

    assert plan["roots"] == 2                           # fact, dim (junk и bkp исключены)
    fact = items[("dwh_dds", "fact")]
    assert fact["missing"] == [("dwh_dds", "fact_1_prt_d2")]      # новой партиции в тесте нет
    assert fact["extra"] == [("dwh_dds", "fact_1_prt_d0")]        # в тесте лишняя
    assert set(fact["leaves"]) == {("dwh_dds", "fact_1_prt_d1"), ("dwh_dds", "fact_1_prt_d2")}


def test_unchanged_after_sync_and_change_detected():
    first = ss.plan_wave(SRC, DST, SETTINGS, 1, state={})
    state = {leaf: {"fingerprint": fp, "synced_at": "2026-11-10 01:00:00"}
             for it in first["items"] for leaf, fp in it["fingerprints"].items()}

    # после синхронизации новая партиция в тесте уже есть
    synced = catalog(list(DST["relations"].values()) + [rel(16, "dwh_dds", "fact_1_prt_d2", 11)])
    again = ss.plan_wave(SRC, synced, SETTINGS, 1, state=state)
    fact = [it for it in again["items"] if it["root"] == ("dwh_dds", "fact")][0]
    assert fact["leaves"] == [] and fact["extra"]          # только отчёт о лишней партиции
    assert again["unchanged"] == 3

    changed = catalog(list(SRC["relations"].values()), dml={3: 5})   # DML в d1
    plan = ss.plan_wave(changed, synced, SETTINGS, 1, state=state)
    fact = [it for it in plan["items"] if it["root"] == ("dwh_dds", "fact")][0]
    assert fact["leaves"] == [("dwh_dds", "fact_1_prt_d1")]

    truncated = catalog([dict(r, filenode=999) if r["oid"] == 5 else r
                         for r in SRC["relations"].values()])
    plan = ss.plan_wave(truncated, synced, SETTINGS, 1, state=state)
    assert ("dwh_dds", "dim") in {it["root"] for it in plan["items"]}

    full = ss.plan_wave(SRC, synced, SETTINGS, 1, state=state, full=True)
    assert sum(len(it["leaves"]) for it in full["items"]) == 3


def test_new_table_copied_whole():
    plan = ss.plan_wave(SRC, catalog([]), SETTINGS, 0, state={})
    assert plan["items"][0]["action"] == "new"


def test_batches_big_alone_and_caps():
    gb = 1024 ** 3
    items = [{"root": ("s", "a"), "action": "changed",
              "leaves": [("s", "a1"), ("s", "a2"), ("s", "a3")]},
             {"root": ("s", "b"), "action": "new", "leaves": [("s", "b")], "bytes": 10}]
    sizes = {("s", "a1"): 3 * gb, ("s", "a2"): 1, ("s", "a3"): 1}

    batches = ss.make_batches(items, sizes, SETTINGS)

    assert [b["leaves"] for b in batches] == [[("s", "a1")], [("s", "a2"), ("s", "a3")],
                                              [("s", "b")]]


def test_deadline_crosses_midnight():
    wave = {"deadline": "05:30"}

    assert ss.deadline_of(wave, datetime(2026, 11, 11, 0, 30)) == datetime(2026, 11, 11, 5, 30)
    assert ss.deadline_of(wave, datetime(2026, 11, 11, 22, 0)) == datetime(2026, 11, 12, 5, 30)
    assert ss.deadline_of({"deadline": ""}, datetime.now()) is None


# ------------------------------------------------------------ раннер

def _plan():
    return ss.save_plan({"name": "Тест", "source_connection_id": 1,
                         "dest_connection_id": 2,
                         "settings": {"waves": [{"name": "W", "include": "*.*",
                                                 "deadline": "05:30"}],
                                      "batch_tables": 1}})


def fake_analysis():
    items = [
        {"root": ("s", t), "action": "changed", "leaves": [("s", t)], "missing": [],
         "extra": [], "fingerprints": {("s", t): "f" + t}, "synced_at": "", "bytes": 1}
        for t in ("a", "b", "c")
    ]
    wave = {"index": 0, "name": "W", "full": False, "roots": 3, "units": 3, "unchanged": 0,
            "items": items, "bytes": 3}
    return {"mode": "m", "last_op": True, "waves": [wave], "tables_total": 3, "outside": 0,
            "sizes": {}, "src_tree": {it["root"]: {"oid": 1, "leaves": {it["root"]: 1}}
                                      for it in items}}


def test_runner_copies_batches_saves_state_and_stops_at_deadline(monkeypatch):
    plan_id = _plan()
    monkeypatch.setattr(ss, "analyze", lambda plan, wave, full=None: fake_analysis())
    ran = []
    times = iter([datetime(2026, 11, 11, 0, 30), datetime(2026, 11, 11, 1, 0),
                  datetime(2026, 11, 11, 2, 0), datetime(2026, 11, 11, 6, 0)])

    def runner(child_id):
        ran.append(child_id)
        from job_manager import mark_item_done, mark_item_failed
        for item in get_job_items(child_id):
            if item["table_name"] == "b":
                mark_item_failed(item["id"], "boom")
            else:
                mark_item_done(item["id"])

    job_id = create_job("smart_sync", 1, {"plan_id": plan_id, "wave": 0})
    ss.run_smart_sync_job(job_id, runner=runner, clock=lambda: next(times))

    items = {i["table_name"]: i for i in get_job_items(job_id)}
    assert len(ran) == 2                                  # c не начали: срок 05:30 прошёл
    assert items["a"]["status"] == "done"
    assert items["b"]["status"] == "failed" and "boom" in items["b"]["error_message"]
    assert items["c"]["status"] == "skipped" and "05:30" in items["c"]["error_message"]

    state = ss.load_state(plan_id)
    assert set(state) == {("s", "a")}                     # отпечаток — только у доехавшего
    job = get_job(job_id)
    assert job["status"] == "failed"
    cfg = json.loads(job["config_json"])["smart"]
    assert cfg["late"] is True and cfg["left"] == 1 and cfg["failed"] == 1

    child = json.loads(get_job(ran[0])["config_json"])
    assert child["truncate"] is True and child["smart_parent"] == job_id


def test_runner_nothing_changed_is_done(monkeypatch):
    plan_id = _plan()
    empty = fake_analysis()
    empty["waves"][0]["items"] = []
    monkeypatch.setattr(ss, "analyze", lambda plan, wave, full=None: empty)

    job_id = create_job("smart_sync", 1, {"plan_id": plan_id, "wave": 0})
    ss.run_smart_sync_job(job_id, runner=lambda j: None)

    assert get_job(job_id)["status"] == "done"


# ------------------------------------------------------------ маршруты

def test_plan_routes_and_schedules(client):
    body = {"name": "Прод → тест", "source_connection_id": 1, "dest_connection_id": 2,
            "settings": {"waves": [
                {"name": "Основная", "include": "*.*", "exclude": "dwh_stage.*",
                 "start": "00:30", "deadline": "05:30"},
                {"name": "Stage", "include": "dwh_stage.*"}]}}

    r = client.post("/api/smart-sync/plans", json=body).get_json()
    assert r["ok"] is True, r
    plan_id = r["plan"]["id"]

    r = client.post("/api/smart-sync/plans/{}/schedule".format(plan_id), json={}).get_json()
    assert [(s["wave"], s["cron"]) for s in r["schedules"]] == [(0, "30 0 * * *")]

    body["settings"]["waves"][1]["start"] = "03:00"
    r = client.put("/api/smart-sync/plans/{}".format(plan_id), json=body).get_json()
    assert set(r["plan"]["schedules"]) == {"0", "1"}           # время волн подхвачено
    assert r["plan"]["schedules"]["1"]["cron"] == "0 3 * * *"

    bad = client.post("/api/smart-sync/plans", json=dict(body, settings={"waves": []}))
    assert bad.status_code == 400

    assert client.delete("/api/smart-sync/plans/{}".format(plan_id)).get_json()["ok"]
    assert not [s for s in scheduler_store.list_schedules()
                if s["job_type"] == "smart_sync"
                and json.loads(s["config_json"])["plan_id"] == plan_id]


def test_page_renders(client):
    assert client.get("/smart-sync").status_code == 200
