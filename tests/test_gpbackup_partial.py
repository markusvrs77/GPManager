# -*- coding: utf-8 -*-
"""
Сборка gpbackup-resilient 1.30.9: частичный бэкап.

С --on-error-continue gpbackup пропускает таблицы, на которых упал COPY,
и завершается кодом 3 со статусом Partial. Раньше такая копия считалась
упавшей и не давала себя восстановить. Строки лога — дословно из
исходников сборки (backup/resilience.go, backup/data.go).
"""

import json

import pytest

import app as app_module
import modules.gpbackup as gb
from db import sqlite_cursor
from job_manager import create_job, create_job_items, get_job, get_job_items


SKIPPED_FILE = ("/data/primary/gpseg-1/backups/20261106/20261106020001/"
                "gpbackup_20261106020001_skipped_tables")

PARTIAL_LOG = (
    "20261106:02:00:01 gpbackup:gpadmin:mdw:1-[INFO]:-"
    "Backup Timestamp = 20261106020001\n"
    "20261106:02:10:00 gpbackup:gpadmin:mdw:1-[WARNING]:-Worker 2: skipping "
    "table s.bad1 due to error: ERROR: could not read block 7 (SQLSTATE XX001)\n"
    "20261106:02:11:00 gpbackup:gpadmin:mdw:1-[WARNING]:-Worker 0: skipping "
    "table \"Sales\".t due to error: boom\n"
    "20261106:02:20:00 gpbackup:gpadmin:mdw:1-[INFO]:-List of skipped tables "
    "written to " + SKIPPED_FILE + "\n"
    "20261106:02:20:01 gpbackup:gpadmin:mdw:1-[WARNING]:-Backup completed "
    "with status Partial: 2 table(s) were skipped and are NOT in this backup: "
    "s.bad1, \"Sales\".t. Full list: " + SKIPPED_FILE + ". To back up only "
    "these tables: gpbackup --dbname adb --include-table-file "
    + SKIPPED_FILE + "\n"
)

OK_LOG = ("20261106:02:00:01 gpbackup:gpadmin:mdw:1-[INFO]:-"
          "Backup Timestamp = 20261106030000\n"
          "20261106:02:20:01 gpbackup:gpadmin:mdw:1-[INFO]:-"
          "Backup completed successfully\n")


# ------------------------------------------------------------ команда

def test_flag_only_when_asked():
    base = {"dbname": "adb"}

    assert "--on-error-continue" not in gb.build_gpbackup_command(base)
    assert "--on-error-continue" in gb.build_gpbackup_command(
        dict(base, on_error_continue=True))


def test_flag_not_doubled_by_extra_args():
    cmd = gb.build_gpbackup_command({"dbname": "adb", "on_error_continue": True,
                                     "extra_args": "--on-error-continue"})

    assert cmd.count("--on-error-continue") == 1


def test_include_table_file_only_skipped_tables_file():
    cmd = gb.build_gpbackup_command({"dbname": "adb",
                                     "include_table_file": SKIPPED_FILE})
    assert cmd[cmd.index("--include-table-file") + 1] == SKIPPED_FILE

    for bad in ("/etc/passwd", "relative/gpbackup_20261106020001_skipped_tables",
                "/data/../etc/gpbackup_20261106020001_skipped_tables",
                "/data/x y/gpbackup_20261106020001_skipped_tables"):
        with pytest.raises(ValueError):
            gb.build_gpbackup_command({"dbname": "adb",
                                       "include_table_file": bad})


# ------------------------------------------------------------ разбор лога

def test_outcome_partial():
    assert gb.backup_outcome(PARTIAL_LOG) == "partial"
    assert gb.backup_outcome(OK_LOG) == "done"
    assert gb.backup_outcome("[CRITICAL]:-boom") == "failed"


def test_parse_partial_full_list_and_file():
    p = gb.parse_partial_backup(PARTIAL_LOG)

    assert p == {"count": 2, "tables": ["s.bad1", '"Sales".t'],
                 "file": SKIPPED_FILE}


def test_parse_partial_summary_only():
    """Построчных «skipping table» нет — берём имена из итоговой строки."""
    log = ("[WARNING]:-Backup completed with status Partial: 12 table(s) were "
           "skipped and are NOT in this backup: s.a, s.b ... and 10 more")

    p = gb.parse_partial_backup(log)

    assert p["count"] == 12
    assert p["tables"] == ["s.a", "s.b"]
    assert p["file"] is None


def test_parse_partial_none_for_success():
    assert gb.parse_partial_backup(OK_LOG) is None


# ------------------------------------------------------------ финализация

def _backup_job(config=None):
    cfg = dict({"connection_id": 1, "dbname": "adb", "backup_type": "full",
                "on_error_continue": True}, **(config or {}))
    job_id = create_job("gpbackup", 1, cfg)
    create_job_items(job_id, [{"schema_name": "adb", "table_name": "gpbackup",
                               "action": "GPBACKUP"}])
    return job_id, cfg


def _record(job_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM backups WHERE job_id = ?", (job_id,))
        return dict(cur.fetchone())


def test_rc3_registers_restorable_partial_copy():
    job_id, cfg = _backup_job()

    gb.finalize_backup_job(job_id, "gpbackup", cfg, get_job_items(job_id),
                           PARTIAL_LOG, 3, "gpbackup ...")

    rec = _record(job_id)
    assert rec["status"] == "partial"
    assert rec["backup_timestamp"] == "20261106020001"
    assert json.loads(rec["skipped_tables"]) == ["s.bad1", '"Sales".t']
    assert rec["skipped_file"] == SKIPPED_FILE

    item = get_job_items(job_id)[0]
    assert item["status"] == "failed"              # не done: копия неполная
    assert "Частичный бэкап: 2" in item["error_message"]
    assert "s.bad1" in item["error_message"]
    assert get_job(job_id)["status"] == "failed"


def test_partial_after_restart_without_rc():
    job_id, cfg = _backup_job()

    gb.finalize_backup_job(job_id, "gpbackup", cfg, get_job_items(job_id),
                           PARTIAL_LOG, None, "gpbackup (переподхвачен)")

    assert _record(job_id)["status"] == "partial"


def test_rc2_is_failure_even_if_finalized_partial():
    """Фатальная ошибка или сигнал — код 2, итоговой строки нет."""
    job_id, cfg = _backup_job()
    log = PARTIAL_LOG.rsplit("20261106:02:20:01", 1)[0] + "[CRITICAL]:-boom\n"

    gb.finalize_backup_job(job_id, "gpbackup", cfg, get_job_items(job_id),
                           log, 2, "gpbackup ...")

    assert _record(job_id)["status"] == "failed"


def test_success_unchanged():
    job_id, cfg = _backup_job()

    gb.finalize_backup_job(job_id, "gpbackup", cfg, get_job_items(job_id),
                           OK_LOG, 0, "gpbackup ...")

    rec = _record(job_id)
    assert rec["status"] == "done" and rec["skipped_tables"] is None
    assert get_job_items(job_id)[0]["status"] == "done"


# ------------------------------------------------------------ добор пропущенных

def _partial_row(**kw):
    row = {"id": 1, "connection_id": 1, "dbname": "adb", "status": "partial",
           "backup_timestamp": "20261106020001", "backup_dir": "/data/b",
           "skipped_tables": json.dumps(["s.bad1", '"Sales".t']),
           "skipped_file": SKIPPED_FILE}
    row.update(kw)
    return row


def test_skipped_config_prefers_file():
    cfg = gb.skipped_backup_config(_partial_row())

    assert cfg["include_table_file"] == SKIPPED_FILE
    assert cfg["backup_dir"] == "/data/b" and cfg["on_error_continue"] is True
    assert "include_tables" not in cfg


def test_skipped_config_falls_back_to_names():
    cfg = gb.skipped_backup_config(_partial_row(
        skipped_file=None, skipped_tables=json.dumps(["s.bad1", "s.bad2"])))

    assert cfg["include_tables"] == "s.bad1\ns.bad2"


def test_skipped_config_refuses_unusable_names():
    """Имя в кавычках без файла не передать через --include-table."""
    with pytest.raises(ValueError):
        gb.skipped_backup_config(_partial_row(skipped_file=None))

    with pytest.raises(ValueError):
        gb.skipped_backup_config(_partial_row(status="done"))


def test_backup_skipped_route(client, monkeypatch):
    started = []

    class _NowThread:
        def __init__(self, target, args, daemon):
            self.target, self.args = target, args

        def start(self):
            self.target(*self.args)

    monkeypatch.setattr(app_module, "run_gpbackup_job",
                        lambda job_id: started.append(job_id))
    monkeypatch.setattr(app_module.threading, "Thread", _NowThread)

    job_id, cfg = _backup_job()
    gb.finalize_backup_job(job_id, "gpbackup", cfg, get_job_items(job_id),
                           PARTIAL_LOG, 3, "gpbackup ...")

    r = client.post("/api/backup/backup-skipped",
                    json={"backup_id": _record(job_id)["id"]})

    body = r.get_json()
    assert body["ok"] is True, body
    new_cfg = json.loads(get_job(body["job_id"])["config_json"])
    assert new_cfg["include_table_file"] == SKIPPED_FILE
    assert new_cfg["skipped_from"] == "20261106020001"
    assert started == [body["job_id"]]


def test_backup_skipped_route_rejects_full_copy(client):
    job_id, cfg = _backup_job()
    gb.finalize_backup_job(job_id, "gpbackup", cfg, get_job_items(job_id),
                           OK_LOG, 0, "gpbackup ...")

    r = client.post("/api/backup/backup-skipped",
                    json={"backup_id": _record(job_id)["id"]})

    assert r.status_code == 400


def test_start_route_passes_flag(client, monkeypatch):
    monkeypatch.setattr(app_module.threading, "Thread",
                        lambda target, args, daemon: type(
                            "T", (), {"start": lambda self: None})())
    monkeypatch.setattr("modules.connections.get_connection_by_id",
                        lambda cid: {"database_name": "adb"})

    r = client.post("/api/backup/start",
                    json={"connection_id": 1, "on_error_continue": True})

    cfg = json.loads(get_job(r.get_json()["job_id"])["config_json"])
    assert cfg["on_error_continue"] is True
