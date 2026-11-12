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


def test_runner_marks_tables_as_each_batch_finishes_and_progress_shows_it(monkeypatch, client):
    plan_id = _plan()
    monkeypatch.setattr(ss, "analyze", lambda plan, wave, full=None: fake_analysis())
    seen = []
    job_id = create_job("smart_sync", 1, {"plan_id": plan_id, "wave": 0})

    def runner(child_id):
        from job_manager import mark_item_done, mark_item_running
        for item in get_job_items(child_id):
            mark_item_running(item["id"])
        seen.append(ss.run_progress(plan_id)["0"])          # что видно странице во время пачки
        for item in get_job_items(child_id):
            mark_item_done(item["id"])

    ss.run_smart_sync_job(job_id, runner=runner, clock=lambda: datetime(2026, 11, 11, 1, 0))

    # идёт пачка 2 — таблица a из пачки 1 уже скопирована, а не «в работе» до конца волны
    during_second = seen[1]
    assert during_second["status"] == "running"
    assert during_second["tables"]["s.a"]["s"] == "done"
    assert during_second["tables"]["s.b"]["s"] == "running"
    assert during_second["tables"]["s.c"]["s"] == "queued"
    assert during_second["batch"] == {"n": 2, "of": 3, "running": 1, "limit": 1}
    assert during_second["counts"]["done"] == 1 and during_second["total"] == 3
    assert during_second["bytes_total"] == 3 and during_second["bytes_done"] == 1

    r = client.get("/api/smart-sync/plans/{}/progress".format(plan_id)).get_json()
    final = r["waves"]["0"]
    assert r["ok"] and final["status"] == "done" and final["counts"] == {"done": 3}
    assert final["batch"] is None and final["tables"]["s.c"]["t"]

    recent = client.get("/api/jobs/recent?types=smart_sync").get_json()["jobs"]
    mine = [j for j in recent if j["id"] == job_id][0]
    assert "batch" not in (mine["smart"] or {})


def test_resume_after_restart_waits_for_child_and_continues(monkeypatch):
    from job_manager import (mark_item_done, mark_job_running, update_job_config)

    plan_id = _plan()
    job_id = create_job("smart_sync", 1, {"plan_id": plan_id, "wave": 0})
    # до рестарта: волна шла, пачка с таблицей a — в живой gpcopy
    child = create_job("gpcopy", 1, {"tables": [{"schema": "s", "table": "a"}],
                                     "smart_parent": job_id})
    mark_job_running(child)
    cfg = json.loads(get_job(job_id)["config_json"])
    cfg["smart"] = {"deadline": "2099-01-01 05:30", "children": [child],
                    "batch": {"n": 1, "of": 3, "job": child, "units": {"s.a": "s.a"},
                              "save": {"s.a": [["s", "a", "fa"]]}}}
    update_job_config(job_id, cfg)
    from job_manager import create_job_items
    create_job_items(job_id, [{"schema_name": "s", "table_name": t, "action": "SMART SYNC"}
                              for t in ("a", "b", "c")])
    mark_job_running(job_id)

    def gpcopy_finishes(_):
        for item in get_job_items(child):
            mark_item_done(item["id"])
        from job_manager import mark_job_done
        mark_job_done(child)

    monkeypatch.setattr(ss, "_sleep", gpcopy_finishes)

    def fresh(plan, wave, full=None):
        assert ("s", "a") in ss.load_state(plan_id)       # доехавшее зачтено ДО анализа
        r = fake_analysis()                                # a уже совпадает — в анализе её нет
        r["waves"][0]["items"] = [it for it in r["waves"][0]["items"] if it["root"] != ("s", "a")]
        return r

    monkeypatch.setattr(ss, "analyze", fresh)
    ran = []

    def runner(child_id):
        ran.append(child_id)
        for item in get_job_items(child_id):
            mark_item_done(item["id"])

    ss.run_smart_sync_job(job_id, runner=runner, clock=lambda: datetime(2026, 11, 11, 18, 0))

    items = {i["table_name"]: i for i in get_job_items(job_id)}
    assert len(items) == 3                                 # строки не задвоились
    assert {i["status"] for i in items.values()} == {"done"}
    assert len(ran) == 2                                   # копировали только b и c
    smart = json.loads(get_job(job_id)["config_json"])["smart"]
    assert smart["resumed"] == 1 and smart["children"][0] == child
    assert smart["deadline"] == "2099-01-01 05:30"          # срок прежний
    assert get_job(job_id)["status"] == "done"


def test_parallel_batches_run_at_the_same_time(monkeypatch):
    import threading

    plan_id = ss.save_plan({"name": "П", "source_connection_id": 1, "dest_connection_id": 2,
                            "settings": {"waves": [{"name": "W", "include": "*.*"}],
                                         "batch_tables": 1, "parallel": 2}})
    assert ss.get_plan(plan_id)["settings"]["parallel"] == 2
    monkeypatch.setattr(ss, "analyze", lambda plan, wave, full=None: fake_analysis())
    both = threading.Barrier(2, timeout=5)        # двое должны встретиться одновременно
    met = []

    def runner(child_id):
        from job_manager import mark_item_done
        if len(met) < 2:
            met.append(child_id)
            both.wait()
        for item in get_job_items(child_id):
            mark_item_done(item["id"])

    job_id = create_job("smart_sync", 1, {"plan_id": plan_id, "wave": 0})
    ss.run_smart_sync_job(job_id, runner=runner)

    assert len(met) == 2
    assert get_job(job_id)["status"] == "done"
    assert {i["status"] for i in get_job_items(job_id)} == {"done"}
    assert set(ss.load_state(plan_id)) == {("s", "a"), ("s", "b"), ("s", "c")}
    assert "active" not in json.loads(get_job(job_id)["config_json"])["smart"]


def test_parallel_can_be_raised_while_wave_runs(monkeypatch):
    import threading

    plan_id = _plan()                                   # gpcopy одновременно = 1
    monkeypatch.setattr(ss, "PARALLEL_REREAD_SECONDS", 0)
    monkeypatch.setattr(ss, "analyze", lambda plan, wave, full=None: fake_analysis())
    both = threading.Barrier(2, timeout=5)
    met = []

    def runner(child_id):
        from job_manager import mark_item_done
        if not met:
            # во время первой пачки подняли до 2 и сохранили план
            p = ss.get_plan(plan_id)
            ss.save_plan({"name": p["name"], "source_connection_id": 1, "dest_connection_id": 2,
                          "settings": dict(p["settings"], parallel=2)}, plan_id=plan_id)
            met.append(child_id)
        else:
            met.append(child_id)
            both.wait()                                 # 2-я и 3-я пачки — одновременно
        for item in get_job_items(child_id):
            mark_item_done(item["id"])

    job_id = create_job("smart_sync", 1, {"plan_id": plan_id, "wave": 0})
    ss.run_smart_sync_job(job_id, runner=runner)

    assert len(met) == 3
    assert get_job(job_id)["status"] == "done"
    assert json.loads(get_job(job_id)["config_json"])["smart"]["parallel"] == 2


def test_continue_interrupted_run_from_where_it_stopped(monkeypatch, client):
    from job_manager import mark_interrupted_jobs_on_startup, mark_item_done

    plan_id = _plan()
    copied = []

    def runner(child_id):
        for item in get_job_items(child_id):
            copied.append(item["table_name"])
            mark_item_done(item["id"])

    # первый запуск: успел только a, потом рестарт Opsentri
    monkeypatch.setattr(ss, "analyze", lambda plan, wave, full=None: fake_analysis())
    times = iter([datetime(2026, 11, 11, 0, 30), datetime(2026, 11, 11, 1, 0),
                  datetime(2026, 11, 11, 6, 0)])
    job_id = create_job("smart_sync", 1, {"plan_id": plan_id, "wave": 0})
    ss.run_smart_sync_job(job_id, runner=runner, clock=lambda: next(times))
    from job_manager import update_job_status
    update_job_status(job_id, "interrupted")
    assert copied == ["a"]

    r = client.post("/api/smart-sync/jobs/{}/continue".format(job_id + 999))
    assert r.status_code == 404

    def fresh(plan, wave, full=None):        # a уже совпадает с продом
        res = fake_analysis()
        res["waves"][0]["items"] = [it for it in res["waves"][0]["items"]
                                    if it["root"] != ("s", "a")]
        return res

    monkeypatch.setattr(ss, "analyze", fresh)
    ss.continue_job(job_id)
    assert get_job(job_id)["status"] == "queued"
    ss.run_smart_sync_job(job_id, runner=runner, clock=lambda: datetime(2026, 11, 11, 12, 0))

    assert copied == ["a", "b", "c"]                      # a не повторили
    items = get_job_items(job_id)
    assert len(items) == 3 and {i["status"] for i in items} == {"done"}
    job = get_job(job_id)
    assert job["status"] == "done" and not job["error_message"]

    r = client.post("/api/smart-sync/jobs/{}/continue".format(job_id))
    assert r.status_code == 409                            # готовый не продолжают


def test_same_wave_is_not_started_twice(client):
    from job_manager import mark_job_running

    plan_id = _plan()
    first = create_job("smart_sync", 1, {"plan_id": plan_id, "wave": 0})
    mark_job_running(first)

    r = client.post("/api/smart-sync/plans/{}/run".format(plan_id), json={"wave": 0})
    assert r.status_code == 409 and "#{}".format(first) in r.get_json()["message"]

    second = create_job("smart_sync", 1, {"plan_id": plan_id, "wave": 0})
    ss.run_smart_sync_job(second, runner=lambda j: None)
    assert get_job(second)["status"] == "failed"


def test_interrupted_run_shows_what_children_copied():
    from job_manager import (create_job_items, mark_item_done, mark_job_running,
                             mark_interrupted_jobs_on_startup, update_job_config)

    plan_id = _plan()
    job_id = create_job("smart_sync", 1, {"plan_id": plan_id, "wave": 0})
    child = create_job("gpcopy", 1, {"tables": [{"schema": "s", "table": "a"}]})
    for item in get_job_items(child):
        mark_item_done(item["id"])
    cfg = json.loads(get_job(job_id)["config_json"])
    cfg["smart"] = {"children": [child]}
    update_job_config(job_id, cfg)
    create_job_items(job_id, [{"schema_name": "s", "table_name": t, "action": "SMART SYNC"}
                              for t in ("a", "b")])
    mark_job_running(job_id)
    mark_interrupted_jobs_on_startup()

    p = ss.run_progress(plan_id)["0"]
    assert p["status"] == "interrupted"
    assert p["tables"]["s.a"]["s"] == "done" and p["tables"]["s.b"]["s"] == "interrupted"
    assert p["counts"]["done"] == 1 and p["pct"] == 50.0


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

    # «по пятницам»: дни запуска — в cron, не «полная сверка»
    r = client.post("/api/smart-sync/plans/{}/schedule".format(plan_id),
                    json={"days": "5"}).get_json()
    assert sorted(s["cron"] for s in r["schedules"]) == ["0 3 * * 5", "30 0 * * 5"]
    assert client.post("/api/smart-sync/plans/{}/schedule".format(plan_id),
                       json={"days": "1,3,5"}).get_json()["schedules"][0]["cron"].endswith(" 1,3,5")

    bad = client.post("/api/smart-sync/plans", json=dict(body, settings={"waves": []}))
    assert bad.status_code == 400

    assert client.delete("/api/smart-sync/plans/{}".format(plan_id)).get_json()["ok"]
    assert not [s for s in scheduler_store.list_schedules()
                if s["job_type"] == "smart_sync"
                and json.loads(s["config_json"])["plan_id"] == plan_id]


def test_page_renders(client):
    assert client.get("/smart-sync").status_code == 200


# ------------------------------------------------------------ размеры без ожидания ETL

class LockTimeout(Exception):
    pgcode = "55P03"


class FakeSizeConn(object):
    """gp_dist_random по пачке; таблицу 7 держит ETL — lock_timeout."""

    def __init__(self, busy=7):
        self.busy = busy
        self.sql = []
        self.chunks = []

    def cursor(self):
        conn = self

        class Cur(object):
            def execute(self, sql, params=None):
                conn.sql.append(sql)
                self.rows = []
                if "gp_dist_random" not in sql:
                    return
                oids = [int(x) for x in __import__("re").findall(r"\((\d+)::oid\)", sql)]
                conn.chunks.append(len(oids))
                if conn.busy in oids:
                    raise LockTimeout("canceling statement due to lock timeout")
                self.rows = [(o, o * 10) for o in oids]

            def fetchall(self):
                return self.rows

        return Cur()


def test_read_sizes_never_waits_for_etl():
    conn = FakeSizeConn(busy=7)
    locked = []

    sizes = ss.read_sizes(conn, range(1, 251), locked)

    assert conn.chunks[:3] == [100, 100, 100] or max(conn.chunks) == ss.SIZE_CHUNK
    assert locked == [7] and 7 not in sizes               # неизвестно, а не ноль
    assert sizes[8] == 80 and len(sizes) == 249
    assert any("lock_timeout = %s" in q for q in conn.sql)
    assert conn.sql[-2] == "SET lock_timeout = 0"         # сессия возвращена как была


# ------------------------------------------------------------ проверка переживает перезагрузку

def test_analysis_is_stored_and_survives_reload(client, monkeypatch):
    plan_id = client.post("/api/smart-sync/plans", json={
        "name": "П", "source_connection_id": 1, "dest_connection_id": 2,
        "settings": {"waves": [{"name": "W", "include": "*.*"}]}}).get_json()["plan"]["id"]

    started = {}
    monkeypatch.setattr(ss, "_spawn", lambda fn, *a: started.setdefault("call", (fn, a)))
    monkeypatch.setattr(ss, "analyze", lambda plan, wave=None, full=None: fake_analysis())

    r = client.post("/api/smart-sync/plans/{}/analyze".format(plan_id), json={}).get_json()
    assert r["analysis"]["status"] == "running"

    # второй клик, пока идёт, — та же проверка, новая не запускается
    calls = len(started)
    client.post("/api/smart-sync/plans/{}/analyze".format(plan_id), json={})
    assert len(started) == calls

    fn, args = started["call"]
    fn(*args)                                             # фоновая проверка закончилась

    # «перезагрузка страницы» — результат на месте
    a = client.get("/api/smart-sync/plans/{}/analysis".format(plan_id)).get_json()["analysis"]
    assert a["status"] == "done" and a["finished_at"]
    assert a["result"]["waves"][0]["changed_tables"] == 3


def test_stored_analysis_keeps_the_whole_list_not_first_300():
    many = fake_analysis()
    item = many["waves"][0]["items"][0]
    many["waves"][0]["items"] = [dict(item, root=("s", "t{}".format(i))) for i in range(1200)]

    assert len(ss.public_analysis(many)["waves"][0]["items"]) == 300        # по умолчанию урезано
    full = ss.public_analysis(many, limit=None)["waves"][0]
    assert len(full["items"]) == 1200 and full["changed_tables"] == 1200


def test_analysis_interrupted_by_restart_is_not_running_forever():
    plan_id = _plan()
    ss._save_analysis(plan_id, status="running", started_at="2026-11-11 01:00:00")

    a = ss.get_analysis(plan_id)

    assert a["status"] == "failed" and "рестартом" in a["error"]
