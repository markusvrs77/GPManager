# -*- coding: utf-8 -*-
"""/api/backup/overview — данные страницы «Резервные копии» одним запросом."""

import json

import modules.gpbackup as gb
import scheduler_store
from db import sqlite_cursor
from job_manager import create_job


def _backup(cid, ts, status="done", started="2026-11-06 02:00:00",
            finished="2026-11-06 04:17:36", config=None):
    cfg = dict({"connection_id": cid, "dbname": "adb"}, **(config or {}))
    job_id = create_job("gpbackup", cid, cfg)
    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE jobs SET status='done', started_at=?, finished_at=? WHERE id=?",
                    (started, finished, job_id))
    gb.insert_backup(cid, job_id, ts, "adb", "full", "", status)
    return job_id


def test_duration_and_scope():
    assert gb.duration_seconds("2026-11-06 02:00:00", "2026-11-06 04:17:36") == 8256
    assert gb.duration_seconds("2026-11-06 02:00:00", None) is None
    assert gb.backup_scope({"include_schemas": "a\nb\n", "include_tables": ""}) == \
        {"schemas": ["a", "b"], "tables": [], "from_file": False}


def test_overview_lists_one_cluster_with_duration(client):
    _backup(901, "20261106020000", config={"include_schemas": "dwh_dm"})
    _backup(902, "20261106030000")
    scheduler_store.create_schedule({
        "name": "nightly", "job_type": "gpbackup", "cron_expr": "0 2 * * *",
        "config_json": json.dumps({"connection_id": 901, "jobs": 4}), "enabled": 1})

    d = client.get("/api/backup/overview?connection_id=901").get_json()

    assert d["ok"] is True
    assert [b["backup_timestamp"] for b in d["backups"]] == ["20261106020000"]
    assert d["backups"][0]["duration_sec"] == 8256
    assert d["backups"][0]["scope"]["schemas"] == ["dwh_dm"]
    assert d["stats"]["total"] == 1
    assert [s["name"] for s in d["schedules"]] == ["nightly"]
    assert d["schedules"][0]["jobs"] == 4
    assert d["history"][0]["duration_sec"] == 8256
    assert d["disk"] is None          # каталог по умолчанию — отсюда не виден


def test_overview_disk_of_visible_dir(client, tmp_path):
    job_id = create_job("gpbackup", 903, {"connection_id": 903})
    gb.insert_backup(903, job_id, "20261106020000", "adb", "full", str(tmp_path), "done")

    d = client.get("/api/backup/overview?connection_id=903").get_json()

    assert d["disk"]["path"] == str(tmp_path)
    assert d["disk"]["total"] >= d["disk"]["used"] > 0


def test_overview_requires_connection(client):
    assert client.get("/api/backup/overview").status_code == 400
