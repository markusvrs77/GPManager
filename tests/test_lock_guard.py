# -*- coding: utf-8 -*-
"""Сторож блокировок синхронизации: правила, решения, догрузка отложенного."""

import json
import threading

import pytest

import modules.gpcopy as gp
import modules.lock_guard as lg
from job_manager import create_job, create_job_items, get_job, get_job_items

OUR_IP = "10.0.0.5"


# ------------------------------------------------------------ настройки

def test_parse_rules_forms():
    rules = lg.parse_rules("etl_loader@10.20.30.40\n@10.20.31.0/24, informatica;10.1.1.1")

    assert rules == [
        {"user": "etl_loader", "ip": "10.20.30.40"},
        {"user": "", "ip": "10.20.31.0/24"},
        {"user": "informatica", "ip": ""},
        {"user": "", "ip": "10.1.1.1"},
    ]
    assert [lg.rule_label(r) for r in rules[:3]] == [
        "etl_loader@10.20.30.40", "@10.20.31.0/24", "informatica"]


@pytest.mark.parametrize("bad", ["etl@10.1.2.300", "etl;drop@10.1.1.1", "@"])
def test_parse_rules_rejects_garbage(bad):
    with pytest.raises(ValueError):
        lg.parse_rule(bad)


def test_match_priority_user_ip_and_subnet():
    rules = lg.parse_rules("ETL@10.1.2.3\n@10.9.0.0/16")

    assert lg.match_priority(rules, "etl", "10.1.2.3")          # регистр учётки не важен
    assert not lg.match_priority(rules, "etl", "10.1.2.4")      # нужны оба условия
    assert lg.match_priority(rules, "anyone", "10.9.200.1")     # подсеть
    assert not lg.match_priority(rules, "anyone", None)


def test_normalize_settings_limits():
    assert lg.normalize_settings({})["wait_seconds"] == 30

    with pytest.raises(ValueError):
        lg.normalize_settings({"wait_seconds": 1})
    with pytest.raises(ValueError):
        lg.normalize_settings({"attempts": "много"})


def test_settings_route_roundtrip(client):
    try:
        r = client.post("/api/sync/lock-settings", json={
            "enabled": True, "wait_seconds": 45, "attempts": 2, "retry_minutes": 1,
            "max_wait_minutes": 10, "priority": "etl@10.1.2.3\n@10.9.0.0/16"}).get_json()

        assert r["ok"] is True
        assert r["priority_text"] == "etl@10.1.2.3\n@10.9.0.0/16"

        got = client.get("/api/sync/lock-settings").get_json()["settings"]
        assert got["wait_seconds"] == 45 and len(got["priority"]) == 2

        bad = client.post("/api/sync/lock-settings", json={"priority": "etl@10.1.2.999"})
        assert bad.status_code == 400
    finally:
        lg.save_settings({})


# ------------------------------------------------------------ решения сторожа

ROOT = ("dwh", "sales")
P1 = ("dwh", "sales_1_prt_p1")
P2 = ("dwh", "sales_1_prt_p2")


def fake_side(label="источник", rows=None):
    side = lg.Side(label, connect=None, pairs=[ROOT])
    side.family = {100: (ROOT, ROOT), 101: (ROOT, P1), 102: (ROOT, P2)}
    side.client_ip = OUR_IP
    side.baseline = {1}          # сессия, открытая до старта, — не наша
    side.cancelled = []
    side.fake_rows = rows or []
    side.rows = lambda: list(side.fake_rows)
    side.cancel = lambda pid: side.cancelled.append(pid) or True
    return side


def row(pid, oid, mode, granted, user="opsentri", ip=OUR_IP, query=""):
    return {"pid": pid, "oid": oid, "mode": mode, "granted": granted,
            "user": user, "ip": ip, "app": "", "query": query}


def guard_for(side, **settings):
    return lg.LockGuard([side], dict(lg.DEFAULTS, **settings))


def test_our_wait_is_cancelled_only_after_threshold():
    side = fake_side()
    guard = guard_for(side, wait_seconds=30)
    rows = [row(50, 101, "AccessShareLock", False),
            row(7, 101, "AccessExclusiveLock", True, user="dba", ip="10.7.7.7",
                query="TRUNCATE dwh.sales_1_prt_p1")]

    assert guard.evaluate(side, rows, now=0) == []
    actions = guard.evaluate(side, rows, now=31)

    assert [(r["pid"], r["oid"]) for r, _why in actions] == [(50, 101)]
    assert "dba@10.7.7.7" in actions[0][1] and "TRUNCATE" in actions[0][1]


def test_priority_session_on_table_defers_us_at_once():
    side = fake_side()
    guard = guard_for(side, priority=lg.parse_rules("etl@10.1.2.3"))
    rows = [row(50, 101, "AccessShareLock", False),
            row(8, 101, "AccessExclusiveLock", True, user="etl", ip="10.1.2.3")]

    actions = guard.evaluate(side, rows, now=0)

    assert [r["pid"] for r, _ in actions] == [50]
    assert "приоритетная" in actions[0][1]


def test_etl_waiting_on_us_finishes_started_partition_and_pauses_the_rest():
    side = fake_side()
    guard = guard_for(side, priority=lg.parse_rules("etl@10.1.2.3"))
    started = [row(50, 101, "AccessShareLock", True),           # P1 уже копируется
               row(50, 100, "AccessShareLock", True)]
    etl = row(8, 100, "AccessExclusiveLock", False, user="etl", ip="10.1.2.3")
    guard.last_rows[side.label] = started

    assert guard.evaluate(side, started + [etl], now=0) == []   # начатое доделывается
    assert ROOT in guard.paused

    # gpcopy берётся за следующую партицию — она сразу откладывается
    nxt = row(51, 102, "AccessShareLock", True)
    actions = guard.evaluate(side, started + [etl, nxt], now=5)

    assert [(r["pid"], r["oid"]) for r, _ in actions] == [(51, 102)]
    assert "пауза ради ETL" in actions[0][1]


def test_foreign_and_priority_sessions_are_never_ours():
    side = fake_side()
    side.rules = lg.parse_rules("etl@" + OUR_IP)
    guard = lg.LockGuard([side], dict(lg.DEFAULTS, wait_seconds=5,
                                      priority=side.rules))
    rows = [row(1, 101, "AccessShareLock", False),                     # до старта
            row(60, 101, "AccessShareLock", False, ip="10.8.8.8"),     # чужой хост
            row(61, 101, "AccessShareLock", False, user="etl"),        # приоритетная с нашего IP
            row(7, 101, "AccessExclusiveLock", True, user="dba", ip="10.7.7.7")]

    assert guard.evaluate(side, rows, now=0) == []
    assert guard.evaluate(side, rows, now=100) == []


def test_tick_cancels_and_records_deferred():
    side = fake_side(rows=[row(50, 102, "AccessShareLock", False),
                           row(8, 102, "AccessExclusiveLock", True, user="etl",
                               ip="10.1.2.3")])
    changes = []
    guard = lg.LockGuard([side], dict(lg.DEFAULTS, priority=lg.parse_rules("etl")),
                         on_change=changes.append)

    guard.tick()

    assert side.cancelled == [50]
    assert guard.round_roots == {ROOT}
    assert guard.summary()["deferred"][0]["table"] == "dwh.sales"
    assert guard.root_of_name(*P2) == ROOT
    assert changes == [guard]


def test_busy_reports_priority_and_conflicting_locks():
    side = fake_side(rows=[row(8, 101, "RowExclusiveLock", True, user="etl", ip="10.1.2.3")])
    guard = guard_for(side, priority=lg.parse_rules("etl"))

    assert "приоритетная" in guard.busy([ROOT], {"источник": "AccessShareLock"})[ROOT]

    side.fake_rows = [row(7, 101, "AccessExclusiveLock", True, user="dba", ip="10.7.7.7"),
                      row(50, 101, "AccessExclusiveLock", True)]      # наша — не помеха
    assert "dba@10.7.7.7" in guard.busy([ROOT], {"источник": "AccessShareLock"})[ROOT]

    side.fake_rows = [row(7, 101, "AccessShareLock", True, user="dba", ip="10.7.7.7")]
    assert guard.busy([ROOT], {"источник": "AccessShareLock"}) == {}


def test_wait_until_free_gives_up_after_deadline():
    clock = {"t": 0}

    class Busy(object):
        def busy(self, roots, need):
            return {ROOT: "занято"}

    def sleep(s):
        clock["t"] += s

    left = lg.wait_until_free(Busy(), [ROOT], {}, 60, lambda: False, poll=20,
                              sleep=sleep, clock=lambda: clock["t"])

    assert left == {ROOT: "занято"} and clock["t"] >= 60
    assert lg.wait_until_free(Busy(), [ROOT], {}, 60, lambda: True, poll=20,
                              sleep=sleep, clock=lambda: 0) is None


# ------------------------------------------------------------ GPCOPY: догрузка

ROUND1 = """\
[Worker 0] Finished copying table "adb"."dwh"."sales_1_prt_p1"
[Worker 1] Finished task with error:
: ERROR: canceling statement due to user request (SQLSTATE 57014)
[Worker 1] Failed to copy table "adb"."dwh"."sales_1_prt_p2" => "adb"."dwh"."sales_1_prt_p2"
Failed to copy table "adb"."dwh"."sales" => "adb"."dwh"."sales"
Total elapsed time: 1m. Copied 2 DBs, successfully copied 1 tables, skipped 0 tables, failed 2 tables
"""

ROUND2_OK = """\
[Worker 0] Finished copying table "adb"."dwh"."sales_1_prt_p2"
Total elapsed time: 1m. Copied 1 DBs, successfully copied 1 tables, skipped 0 tables, failed 0 tables
"""


def joined(*rounds):
    out = [rounds[0]]

    for i, text in enumerate(rounds[1:], 1):
        out.append("{} {}/3: 1 объектов\n{}".format(gp.LOCK_ROUND_MARK, i, text))

    return "\n".join(out)


def test_skip_reason_only_full_replace():
    assert gp.lock_guard_skip_reason("", {}, False, None, True, False) is None
    assert gp.lock_guard_skip_reason("full", {}, False, None, False, True) is None
    assert "date_filter" in gp.lock_guard_skip_reason("date_filter", {}, False, None, True, False)
    assert gp.lock_guard_skip_reason("", {}, False, None, False, False)          # append
    assert gp.lock_guard_skip_reason("", {"s.a": "t.a"}, False, None, True, False)


def test_retry_leaves_only_cancelled_partitions_not_whole_root():
    root_of = {P1: ROOT, P2: ROOT, ROOT: ROOT}.get

    leaves = gp.lock_retry_leaves(ROUND1, {ROOT}, lambda s, t: root_of((s, t)),
                                  gp.parse_finished_tables(ROUND1))

    assert leaves == [P2]                                  # не вся dwh.sales
    assert gp.lock_retry_leaves(ROUND1, set(), lambda s, t: None, set()) == []


def test_reconcile_recovered_rounds_is_success():
    text, complete = gp.reconcile_round_logs(joined(ROUND1, ROUND2_OK))

    assert complete is True
    assert gp.parse_failed_leaf_tables(text) == []
    summary = gp.parse_gpcopy_summary(text)
    assert summary["failed"] == 0 and summary["copied"] == 2
    assert gp.is_gpcopy_success(None, summary)


def test_reconcile_keeps_still_failed_and_broken_round():
    still = ROUND1.replace("sales_1_prt_p1", "sales_1_prt_p0")
    text, complete = gp.reconcile_round_logs(joined(ROUND1, still))
    assert complete is True
    assert gp.parse_gpcopy_summary(text)["failed"] >= 1

    # второй прогон оборвался без сводки — успех не выдумываем
    text, complete = gp.reconcile_round_logs(joined(ROUND1, "[CRITICAL] gpcopy died\n"))
    assert complete is False
    assert gp.parse_gpcopy_summary(text)["failed"] > 0


def test_finalize_after_recovery_marks_done():
    job_id = create_job("gpcopy", 1, {"source_connection_id": 1, "dest_connection_id": 2})
    create_job_items(job_id=job_id, items=[{"schema_name": "dwh", "table_name": "sales",
                                            "action": "GPCOPY"}])
    items = get_job_items(job_id)

    gp.finalize_gpcopy_job(job_id, items, 1, joined(ROUND1, ROUND2_OK), "", "gpcopy",
                           10, json.loads(get_job(job_id)["config_json"]))

    assert get_job(job_id)["status"] == "done"
    assert [i["status"] for i in get_job_items(job_id)] == ["done"]


def test_finalize_without_recovery_stays_failed():
    job_id = create_job("gpcopy", 1, {"source_connection_id": 1, "dest_connection_id": 2})
    create_job_items(job_id=job_id, items=[{"schema_name": "dwh", "table_name": "sales",
                                            "action": "GPCOPY"}])

    gp.finalize_gpcopy_job(job_id, get_job_items(job_id), 1,
                           joined(ROUND1, ROUND1.replace("p1", "p0")), "", "gpcopy", 10,
                           json.loads(get_job(job_id)["config_json"]))

    assert get_job(job_id)["status"] == "failed"
    assert [i["status"] for i in get_job_items(job_id)] == ["failed"]


class FakeGuard(object):
    def __init__(self):
        self.round_roots = {ROOT}
        self.lock = threading.RLock()
        self.deferred = {}

    def root_of_name(self, schema, table):
        return {P1: ROOT, P2: ROOT, ROOT: ROOT}.get((schema, table))

    def busy(self, roots, need):
        return {}

    def summary(self):
        return {"deferred": [], "paused": [], "warnings": []}

    def save(self, guard=None):
        pass


def test_lock_rounds_retry_only_deferred_partitions(monkeypatch):
    job_id = create_job("gpcopy", 1, {"source_connection_id": 1})
    config = {"lock_state": {}}
    seen = {}

    def fake_command(**kw):
        with open(kw["include_tables_file"], encoding="utf-8") as f:
            seen["include"] = f.read().split()
        seen["kw"] = kw
        return ["gpcopy"]

    def fake_launch(job_id, cmd, log_path, item_keys, guard=None, header=None):
        seen["header"] = header
        guard.round_roots = set()
        return 0, ROUND2_OK

    monkeypatch.setattr(gp, "build_gpcopy_command", fake_command)
    monkeypatch.setattr(gp, "_launch_gpcopy", fake_launch)
    monkeypatch.setattr(gp, "pick_rpc_port", lambda: None)

    settings = dict(lg.DEFAULTS, retry_minutes=0, max_wait_minutes=0, attempts=3)
    rc, text = gp._run_lock_rounds(job_id, config, FakeGuard(), settings,
                                   {"gpcopy_path": "/x", "drop": True}, "adb", "log",
                                   [], (1, ROUND1))

    assert seen["include"] == ["adb.dwh.sales_1_prt_p2"]
    assert seen["kw"]["truncate"] is True and seen["kw"]["drop"] is False
    assert gp.LOCK_ROUND_MARK in seen["header"]
    assert config["lock_state"]["phase"] == "done" and config["lock_state"]["left"] == 0
    assert gp.is_gpcopy_success(None, gp.parse_gpcopy_summary(
        gp.reconcile_round_logs(text)[0]))


def test_lock_rounds_stop_after_attempts(monkeypatch):
    job_id = create_job("gpcopy", 1, {"source_connection_id": 1})
    config = {"lock_state": {}}
    calls = []

    def fake_launch(job_id, cmd, log_path, item_keys, guard=None, header=None):
        calls.append(header)
        guard.round_roots = {ROOT}
        return 1, ROUND1.replace("sales_1_prt_p1", "sales_1_prt_p9")

    monkeypatch.setattr(gp, "build_gpcopy_command", lambda **kw: ["gpcopy"])
    monkeypatch.setattr(gp, "_launch_gpcopy", fake_launch)
    monkeypatch.setattr(gp, "pick_rpc_port", lambda: None)

    settings = dict(lg.DEFAULTS, retry_minutes=0, max_wait_minutes=0, attempts=2)
    gp._run_lock_rounds(job_id, config, FakeGuard(), settings, {}, "adb", "log",
                        [], (1, ROUND1))

    assert len(calls) == 2
    assert config["lock_state"]["left"] == 1
    assert config["lock_state"]["left_leaves"] == [list(P2)]


def test_runs_feed_exposes_lock_state(client):
    job_id = create_job("gpcopy", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "lock_state": {"enabled": True, "phase": "waiting", "attempt": 1, "attempts": 3,
                       "deferred": [{"table": "dwh.sales", "parts": 3, "reason": "x"}],
                       "leaf_roots": [[list(P2), list(ROOT)]]}})

    jobs = client.get("/api/jobs/recent?types=gpcopy&limit=100").get_json()["jobs"]
    job = next(j for j in jobs if j["id"] == job_id)

    assert job["lock"]["deferred"][0]["parts"] == 3
    assert "leaf_roots" not in job["lock"]
