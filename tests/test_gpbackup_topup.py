# -*- coding: utf-8 -*-
"""Добор пропущенных таблиц — часть основной копии; восстановление сам догружает."""

import json

import pytest

import app as app_module
import modules.gpbackup as gb
from db import sqlite_cursor
from job_manager import create_job, create_job_items, get_job, get_job_items

CID = 931


def _log(ts, partial=None):
    head = ("20261106:02:00:01 gpbackup:gpadmin:mdw:1-[INFO]:-"
            "Backup Timestamp = {}\n".format(ts))
    if not partial:
        return head + "20261106:02:20:01 gpbackup:gpadmin:mdw:1-[INFO]:-Backup completed successfully\n"
    return head + "".join(
        "[WARNING]:-Worker 1: skipping table {} due to error: boom\n".format(t) for t in partial
    ) + ("[WARNING]:-Backup completed with status Partial: {} table(s) were skipped "
         "and are NOT in this backup: {}".format(len(partial), ", ".join(partial)))


def _finish(job_type, cfg, log, rc):
    job_id = create_job(job_type, cfg["connection_id"], cfg)
    create_job_items(job_id, [{"schema_name": "adb", "table_name": job_type,
                               "action": job_type.upper()}])
    gb.finalize_backup_job(job_id, job_type, cfg, get_job_items(job_id), log, rc, job_type)
    return job_id


def _row(job_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM backups WHERE job_id = ?", (job_id,))
        return dict(cur.fetchone())


@pytest.fixture
def spawned(monkeypatch):
    calls = []
    monkeypatch.setattr(gb, "_spawn", lambda fn, *a: calls.append(a))
    monkeypatch.setattr(app_module, "run_gpbackup_job", lambda job_id: None)
    monkeypatch.setattr(app_module, "run_gprestore_job", lambda job_id: None)
    return calls


# ------------------------------------------------------------ дерево копий

def _rows(*rows):
    out = []
    for i, r in enumerate(rows, 1):
        out.append(dict({"id": i, "job_id": None, "status": "done", "backup_dir": "",
                         "duration_sec": None, "parent_id": None, "_skipped_from": None,
                         "skipped_tables": None}, **r))
    return out


def test_table_root_of_leaf_partition():
    assert gb.table_root("dwh_dds.acc_transaction_crd_1_prt_646") == "dwh_dds.acc_transaction_crd"
    assert gb.table_root("dwh_dds.acc_sc_quote") == "dwh_dds.acc_sc_quote"


def test_topup_nested_and_completes_copy():
    rows = _rows(
        {"backup_timestamp": "20261107011000", "parent_id": 2},
        {"backup_timestamp": "20261106205400", "status": "partial",
         "skipped_tables": json.dumps(["s.t_1_prt_1", "s.u"])},
    )

    top = gb.link_topups(rows)

    assert [r["id"] for r in top] == [2]
    assert top[0]["completed"] is True and top[0]["missing"] == []
    assert top[0]["topups"][0]["covered"] == ["s.t_1_prt_1", "s.u"]


def test_legacy_topup_found_by_job_config_and_chain_collapses():
    rows = _rows(
        {"backup_timestamp": "20261108000000", "status": "done", "_skipped_from": "20261107000000"},
        {"backup_timestamp": "20261107000000", "status": "partial", "_skipped_from": "20261106000000",
         "skipped_tables": json.dumps(["s.u"])},
        {"backup_timestamp": "20261106000000", "status": "partial",
         "skipped_tables": json.dumps(["s.a", "s.u"])},
    )

    top = gb.link_topups(rows)

    assert [r["id"] for r in top] == [3]
    t1, t2 = top[0]["topups"]
    assert t1["covered"] == ["s.a"] and t2["covered"] == ["s.u"]
    assert top[0]["completed"] is True


def test_partial_topup_leaves_missing():
    rows = _rows(
        {"backup_timestamp": "20261107000000", "status": "partial", "parent_id": 2,
         "skipped_tables": json.dumps(["s.u"])},
        {"backup_timestamp": "20261106000000", "status": "partial",
         "skipped_tables": json.dumps(["s.a", "s.u"])},
    )

    top = gb.link_topups(rows)[0]

    assert top["missing"] == ["s.u"] and top["completed"] is False


# ------------------------------------------------------------ шаги восстановления

FAMILY = {"backup_timestamp": "20261106205400", "topups": [
    {"backup_timestamp": "20261107011000", "status": "done", "backup_dir": "/bk",
     "covered": ["dwh.t_1_prt_1", "dwh.t_1_prt_2", "ods.u"]},
    {"backup_timestamp": "20261107020000", "status": "failed", "covered": None},
]}


def test_followups_full_restores_whole_topup_data_only():
    steps = gb.restore_followups(FAMILY, {"connection_id": 1, "redirect_db": "adb_test"})

    assert len(steps) == 1
    cmd = gb.build_gprestore_command(steps[0])
    assert "--data-only" in cmd and "--include-table" not in cmd
    assert cmd[cmd.index("--timestamp") + 1] == "20261107011000"
    assert cmd[cmd.index("--redirect-db") + 1] == "adb_test"
    assert "--create-db" not in cmd


def test_followups_tables_by_partition_root():
    cfg = gb.build_restore_config({"connection_id": 1, "backup_timestamp": "20261106205400",
                                   "restore_scope": "tables", "include_tables": "dwh.t\nods.other",
                                   "on_conflict": "truncate"})
    steps = gb.restore_followups(FAMILY, cfg)

    assert steps[0]["include_tables"] == "dwh.t_1_prt_1\ndwh.t_1_prt_2"
    cmd = gb.build_gprestore_command(steps[0])
    assert "--truncate-table" in cmd and "--on-error-continue" not in cmd


def test_followups_schemas_and_untouched_scope():
    cfg = gb.build_restore_config({"connection_id": 1, "backup_timestamp": "20261106205400",
                                   "restore_scope": "schemas", "include_schemas": "ods\nraw"})
    assert gb.restore_followups(FAMILY, cfg)[0]["include_schemas"] == "ods"

    cfg = gb.build_restore_config({"connection_id": 1, "backup_timestamp": "20261106205400",
                                   "restore_scope": "tables", "include_tables": "raw.x"})
    assert gb.restore_followups(FAMILY, cfg) == []


# ------------------------------------------------------------ сценарий целиком

def test_topup_flow_through_api(client, spawned):
    base = {"connection_id": CID, "dbname": "adb", "backup_type": "full"}
    parent_job = _finish("gpbackup", base, _log("20261106205400", ["s.t_1_prt_1", "s.u"]), 3)
    parent = _row(parent_job)
    assert parent["status"] == "partial"

    # «Добрать» -> задача-добор со ссылкой на основную копию
    r = client.post("/api/backup/backup-skipped", json={"backup_id": parent["id"]}).get_json()
    assert r["ok"] is True, r
    topup_cfg = json.loads(get_job(r["job_id"])["config_json"])
    assert topup_cfg["parent_backup_id"] == parent["id"]
    assert topup_cfg["include_tables"] == "s.t_1_prt_1\ns.u"

    gb.finalize_backup_job(r["job_id"], "gpbackup", topup_cfg, get_job_items(r["job_id"]),
                           _log("20261107011000"), 0, "gpbackup")
    assert _row(r["job_id"])["parent_id"] == parent["id"]

    # в списке — одна копия, добор внутри, всё добрано
    d = client.get("/api/backup/overview?connection_id={}".format(CID)).get_json()
    assert [b["backup_timestamp"] for b in d["backups"]] == ["20261106205400"]
    assert d["stats"]["total"] == 1
    assert d["backups"][0]["completed"] is True
    assert d["history"][0]["topup_of"] == "20261106205400"

    again = client.post("/api/backup/backup-skipped", json={"backup_id": parent["id"]})
    assert again.status_code == 400

    # восстановление основной копии сам находит добор
    r = client.post("/api/backup/restore", json={
        "connection_id": CID, "backup_id": parent["id"],
        "backup_timestamp": "20261106205400", "restore_scope": "full"}).get_json()
    assert r["ok"] is True and r["followups"] == 1
    restore_cfg = json.loads(get_job(r["job_id"])["config_json"])

    restore_log = "Restore completed successfully\n"
    gb.finalize_backup_job(r["job_id"], "gprestore", restore_cfg, get_job_items(r["job_id"]),
                           restore_log, 0, "gprestore")

    assert len(spawned) == 1
    step = get_job(spawned[0][0])
    step_cfg = json.loads(step["config_json"])
    assert step["job_type"] == "gprestore"
    assert step_cfg["backup_timestamp"] == "20261107011000"
    assert step_cfg["data_only"] is True and step_cfg["followups"] == []
    assert "добор к 20261106205400" in get_job_items(step["id"])[0]["table_name"]


def test_failed_main_restore_does_not_load_topup(client, spawned):
    cfg = {"connection_id": CID, "backup_timestamp": "20261106205400",
           "followups": [{"connection_id": CID, "backup_timestamp": "20261107011000",
                          "data_only": True}]}
    job_id = _finish("gprestore", cfg, "[CRITICAL]:-boom", 1)

    assert spawned == []
    assert "Добор не применён" in get_job(job_id)["error_message"]


# ------------------------------------------------------------ сбойные записи

def test_forget_failed_records_but_not_live_copies(client):
    cid = 932
    failed = [gb.insert_backup(cid, None, None, "adb", "full", "", "failed") for _ in range(3)]
    live = gb.insert_backup(cid, None, "20261106205400", "adb", "full", "", "done")

    # живую копию так не убрать — только «Удалить с диска»
    r = client.post("/api/backup/forget", json={"backup_id": live})
    assert r.status_code == 400

    r = client.post("/api/backup/forget", json={"backup_id": failed[0]}).get_json()
    assert r == {"ok": True, "removed": 1}

    r = client.post("/api/backup/forget", json={"connection_id": cid, "all_failed": True}).get_json()
    assert r == {"ok": True, "removed": 2}

    d = client.get("/api/backup/overview?connection_id={}".format(cid)).get_json()
    assert [b["id"] for b in d["backups"]] == [live]


# ------------------------------------------------------------ выбор схем и таблиц

def test_backup_catalog_hides_leaf_partitions():
    tables = [("dwh", "f_sales"), ("dwh", "f_sales_1_prt_1"), ("dwh", "f_sales_1_prt_1_2_prt_a"),
              ("dwh", "d_client"), ("ods", "Mixed Case")]
    pairs = {("dwh", "f_sales_1_prt_1"): ("dwh", "f_sales"),
             ("dwh", "f_sales_1_prt_1_2_prt_a"): ("dwh", "f_sales_1_prt_1")}

    c = gb.backup_catalog(tables, pairs)

    assert c["schemas"] == [{"name": "dwh", "tables": 2, "ok": True},
                            {"name": "ods", "tables": 1, "ok": True}]
    assert [(t["name"], t["parts"], t["ok"]) for t in c["tables"]] == [
        ("dwh.f_sales", 2, True), ("dwh.d_client", 0, True), ("ods.Mixed Case", 0, False)]


def test_backup_catalog_api(client, monkeypatch):
    import modules.table_catalog as tc

    monkeypatch.setattr(tc, "get_catalog", lambda cid, force=False: ([("dwh", "t")], 1.0))
    monkeypatch.setattr(tc, "fetch_partition_pairs", lambda cid: {})

    d = client.get("/api/backup/catalog?connection_id=1").get_json()
    assert d["ok"] is True and d["tables"][0]["name"] == "dwh.t"
