# -*- coding: utf-8 -*-
"""
Копии gpbackup по файлам координатора (замена gpbackup_manager),
состав копии и восстановление схем / таблиц.
"""

import sqlite3

import pytest

import modules.gpbackup as gb
import modules.gpbackup_files as gf
from job_manager import create_job, mark_job_failed
from modules.connections import create_connection

TS = "20261106184200"

REPORT = """Greenplum Database Backup Report

timestamp key:         20261106184200
gpdb version:          7.1.0
gpbackup version:      1.30.9

database name:         adb
command line:          gpbackup --dbname adb --backup-dir /data/data1
compression:           gzip
object filtering:      None

duration:              0:00:15

backup status:         Success

database size:         316 GB
segment count:         4

count of database objects in backup:
aggregates                   0
functions                    7
schemas                      2
sequences                    1
tables                       3
views                        2
"""

TOC = """globalentries:
- schema: ""
  name: adb
  objecttype: DATABASE
  referenceobject: ""
  startbyte: 0
  endbyte: 10
  tier:
  - 0
  - 0
predataentries:
- schema: public
  name: amort_diagnostics
  objecttype: TABLE
  referenceobject: ""
  startbyte: 10
  endbyte: 20
  tier:
  - 0
  - 0
- schema: dwh_dm
  name: '"Sales"'
  objecttype: TABLE
  referenceobject: ""
  startbyte: 20
  endbyte: 30
- schema: dwh_dm
  name: v_sales
  objecttype: VIEW
  referenceobject: ""
- schema: dwh_dm
  name: f_part
  objecttype: TABLE
  referenceobject: ""
- schema: dwh_dm
  name: f_part_1_prt_p1
  objecttype: TABLE
  referenceobject: ""
dataentries:
- schema: public
  name: amort_diagnostics
  oid: 16385
  attributestring: (a,b)
  rowscopied: 100
  partitionroot: ""
  isreplicated: false
- schema: dwh_dm
  name: f_part_1_prt_p1
  oid: 16390
  attributestring: (a)
  rowscopied: 40
  partitionroot: f_part
- schema: dwh_dm
  name: f_part_1_prt_p2
  oid: 16391
  attributestring: (a)
  rowscopied: 2
  partitionroot: f_part
incrementalmetadata:
  ao: {}
"""

CONFIG = """backupdir: /data/data1
backupversion: 1.30.9
compressed: true
databasename: adb
dataonly: false
includeschemas: []
incremental: false
metadataonly: false
timestamp: "20261106184200"
status: Success
"""


@pytest.fixture(autouse=True)
def _no_coordinator_env(monkeypatch):
    monkeypatch.delenv("COORDINATOR_DATA_DIRECTORY", raising=False)
    monkeypatch.delenv("MASTER_DATA_DIRECTORY", raising=False)
    gf._coordinator_dirs.clear()


def _backup_files(base, ts=TS, report=REPORT, toc=TOC, config=CONFIG,
                  prefix="seg"):
    """<base>/<prefix>-1/backups/<day>/<ts>/ с файлами копии (GP7: seg-1)."""
    d = base / (prefix + "-1") / "backups" / ts[:8] / ts
    d.mkdir(parents=True)
    (d / "gpbackup_{}_report".format(ts)).write_text(report, encoding="utf-8")
    (d / "gpbackup_{}_toc.yaml".format(ts)).write_text(toc, encoding="utf-8")
    (d / "gpbackup_{}_config.yaml".format(ts)).write_text(config, encoding="utf-8")
    return d


# ------------------------------------------------------------ разбор

def test_parse_report_counts_and_fields():
    r = gf.parse_backup_report(REPORT)

    assert r["objects"]["tables"] == 3
    assert r["objects"]["schemas"] == 2
    assert r["fields"]["database size"] == "316 GB"
    assert r["fields"]["backup status"] == "Success"


def test_parse_toc_tables_partitions_and_rows():
    toc = gf.parse_toc(TOC)
    names = [(t["schema"], t["name"]) for t in toc["tables"]]

    # вьюха и листовая партиция — не таблицы для восстановления,
    # "Sales" — без кавычек quote_ident
    assert names == [("public", "amort_diagnostics"), ("dwh_dm", "Sales"),
                     ("dwh_dm", "f_part")]
    assert toc["has_rows"] is True
    rows = {t["name"]: t["rows"] for t in toc["tables"]}
    assert rows["amort_diagnostics"] == 100
    assert rows["f_part"] == 42          # сумма партиций
    assert rows["Sales"] is None


def test_parse_config_type_and_status():
    cfg = gf.parse_backup_config(CONFIG)

    assert cfg["databasename"] == "adb"
    assert cfg["includeschemas"] == []
    assert gf.config_backup_type(cfg) == "full"
    assert gf.config_backup_type({"incremental": "true"}) == "incremental"
    assert gf.config_status("Success") == "done"
    assert gf.config_status("Failure") == "failed"
    assert gf.config_status("In Progress") is None


# ------------------------------------------------------------ команда gprestore

BASE = {"connection_id": 1, "backup_timestamp": TS}


def test_restore_config_full_keeps_old_behaviour():
    cfg = gb.build_restore_config(dict(BASE, include_tables="a.b"))
    cmd = gb.build_gprestore_command(cfg)

    assert "--on-error-continue" in cmd       # как раньше
    assert "--include-table" in cmd           # без restore_scope — как прислали

    cfg = gb.build_restore_config(dict(BASE, restore_scope="full", include_tables="a.b"))
    assert "--include-table" not in gb.build_gprestore_command(cfg)


def test_restore_schemas_stops_on_conflict():
    cfg = gb.build_restore_config(dict(BASE, restore_scope="schemas",
                                       include_schemas="dwh_dm\npublic"))
    cmd = gb.build_gprestore_command(cfg)

    assert cmd.count("--include-schema") == 2
    # без --on-error-continue: существующая таблица = ошибка, а не дубли
    assert "--on-error-continue" not in cmd
    assert "схемы: dwh_dm, public" in gb.restore_item_label(cfg)


def test_restore_tables_truncate():
    cfg = gb.build_restore_config(dict(BASE, restore_scope="tables",
                                       include_tables="public.t1",
                                       on_conflict="truncate"))
    cmd = gb.build_gprestore_command(cfg)

    assert "--truncate-table" in cmd and "--data-only" in cmd
    assert cmd[cmd.index("--include-table") + 1] == "public.t1"


def test_restore_redirect_schema():
    cfg = gb.build_restore_config(dict(BASE, restore_scope="tables",
                                       include_tables="public.t1",
                                       on_conflict="redirect",
                                       redirect_schema="restore_tmp"))
    cmd = gb.build_gprestore_command(cfg)

    assert cmd[cmd.index("--redirect-schema") + 1] == "restore_tmp"


@pytest.mark.parametrize("data", [
    {"restore_scope": "schemas"},                                   # пусто
    {"restore_scope": "tables", "include_tables": "nodot"},
    {"restore_scope": "schemas", "include_schemas": "a",
     "on_conflict": "truncate"},                                    # только таблицы
    {"restore_scope": "tables", "include_tables": "a.b",
     "on_conflict": "truncate", "create_db": True},
    {"restore_scope": "tables", "include_tables": "a.b",
     "on_conflict": "redirect", "redirect_schema": "bad name"},
    {"restore_scope": "everything"},
])
def test_restore_config_rejects(data):
    with pytest.raises(ValueError):
        gb.build_gprestore_command(gb.build_restore_config(dict(BASE, **data)))


def test_restore_api_validates(client):
    r = client.post("/api/backup/restore", json=dict(
        BASE, restore_scope="tables", include_tables=""))

    assert r.status_code == 400
    assert "таблиц" in r.get_json()["message"]


# ------------------------------------------------------------ состав копии

def test_details_from_files(client, tmp_path):
    _backup_files(tmp_path)
    job_id = create_job("gpbackup", 961, {"connection_id": 961})
    bid = gb.insert_backup(961, job_id, TS, "adb", "full", str(tmp_path), "done")

    d = client.get("/api/backup/details?backup_id={}&with_tables=1".format(bid)).get_json()

    assert d["ok"] is True
    assert d["summary"]["tables"] == 3
    assert d["summary"]["schemas"] == 2
    assert d["summary"]["rows"] == 142
    assert d["summary"]["db_size"] == "316 GB"
    assert d["tables_source"] == "toc"
    fq = {t["fq"]: t for t in d["tables"]}
    assert fq["dwh_dm.Sales"]["selectable"] is True
    assert {s["name"]: s["tables"] for s in d["schemas"]} == {"dwh_dm": 2, "public": 1}

    # сводка закэширована и видна в списке страницы
    o = client.get("/api/backup/overview?connection_id=961").get_json()
    assert o["backups"][0]["details"]["tables"] == 3


def test_details_without_files_is_empty_not_error(client, tmp_path, monkeypatch):
    monkeypatch.setattr(gb, "run_manager_command",
                        lambda *a, **k: (1, "[ERROR]:-^D"))
    job_id = create_job("gpbackup", 962, {"connection_id": 962})
    bid = gb.insert_backup(962, job_id, TS, "adb", "full", str(tmp_path / "nope"), "done")

    d = client.get("/api/backup/details?backup_id={}".format(bid)).get_json()

    assert d["ok"] is True
    assert d["summary"] is None


def test_report_read_from_file(tmp_path):
    _backup_files(tmp_path)

    text = gb.get_backup_report(963, TS, backup_dir=str(tmp_path))

    assert "count of database objects" in text


# ------------------------------------------------------------ «С диска»

def test_sync_disk_from_files_and_history(tmp_path, monkeypatch):
    cid = create_connection({"name": "gp-files", "host": "mdw", "port": 5432,
                             "database_name": "adb", "username": "gpadmin"})
    datadir = tmp_path / "coord"
    datadir.mkdir()
    monkeypatch.setattr(gf, "coordinator_data_dir", lambda *_a: str(datadir))
    # координатор этого кластера — здесь же (иначе файлы читались бы по SSH)
    from modules.gp_remote import save_host_settings
    save_host_settings(cid, {"mode": "local"})

    # копия в своём каталоге (--backup-dir)
    _backup_files(tmp_path / "bk", prefix="gpseg")

    # копия только в истории gpbackup + удалённая + другой БД
    hist = sqlite3.connect(str(datadir / "gpbackup_history.db"))
    hist.execute("CREATE TABLE backups (timestamp TEXT, database_name TEXT, "
                 "status TEXT, backup_dir TEXT, incremental INT, "
                 "metadata_only INT, data_only INT, date_deleted TEXT)")
    hist.executemany("INSERT INTO backups VALUES (?,?,?,?,?,?,?,?)", [
        ("20261101010101", "adb", "Success", "", 1, 0, 0, ""),
        ("20261102010101", "adb", "Success", "", 0, 0, 0, "20261103"),
        ("20261104010101", "other", "Success", "", 0, 0, 0, ""),
    ])
    hist.commit()
    hist.close()

    def no_manager(*a, **k):
        raise AssertionError("gpbackup_manager не должен вызываться")

    monkeypatch.setattr(gb, "run_manager_command", no_manager)

    res = gb.sync_disk_backups(cid, backup_dirs=[str(tmp_path / "bk")])

    assert res["source"] == "files"
    assert res["added"] == 2
    rows = {b["backup_timestamp"]: b for b in gb.list_connection_backups(cid)}
    assert rows[TS]["backup_dir"] == "/data/data1"
    assert rows["20261101010101"]["backup_type"] == "incremental"
    assert "20261102010101" not in rows       # удалена
    assert "20261104010101" not in rows       # другая БД

    assert gb.sync_disk_backups(cid, backup_dirs=[str(tmp_path / "bk")])["added"] == 0


# ------------------------------------------------------------ причина сбоя

def test_error_reason_strips_log_prefix():
    text = ("Команда: gprestore --timestamp 1\n\n"
            "20261106:18:47:05 gprestore:gpadmin:mdw:862955-[CRITICAL]:-"
            "Relation public.amort_diagnostics already exists\n")

    assert gb.error_reason(text) == "Relation public.amort_diagnostics already exists"
    assert gb.error_reason("Подключение не найдено") == "Подключение не найдено"


def test_history_has_reason(client):
    job_id = create_job("gprestore", 964, {"connection_id": 964})
    mark_job_failed(job_id, "Команда: x\n\n20261106:18:47:05 gprestore:gpadmin:mdw:1-"
                            "[CRITICAL]:-Backup directory /data/x missing")

    d = client.get("/api/backup/overview?connection_id=964").get_json()

    h = d["history"][0]
    assert h["status"] == "failed"
    assert h["reason"] == "Backup directory /data/x missing"
    assert "Команда: x" in h["error"]


# ------------------------------------------------------------ упавшие операторы

VERBOSE_LOG = (
    "20261106:18:47:39 gprestore:gpadmin:mdw:871516-[INFO]:-Restoring pre-data metadata\n"
    "20261106:18:47:39 gprestore:gpadmin:mdw:871516-[DEBUG]:-Error encountered when "
    "executing statement: CREATE TABLE public.accounts_status_history (\n"
    "\tid bigint\n) DISTRIBUTED BY (id); Error was: ERROR: relation "
    "\"accounts_status_history\" already exists (SQLSTATE 42P07)\n"
    "20261106:18:47:39 gprestore:gpadmin:mdw:871516-[ERROR]:-Encountered 1 errors "
    "during metadata restore; see log file /home/gpadmin/gpAdminLogs/"
    "gprestore_20261106.log for a list of failed statements.\n"
)


def test_gprestore_runs_verbose_once():
    cmd = gb.build_gprestore_command(dict(BASE))
    assert "--verbose" in cmd

    cmd = gb.build_gprestore_command(dict(BASE, extra_args="--verbose"))
    assert cmd.count("--verbose") == 1


def test_failed_statements_from_verbose_log():
    stmts = gb.failed_statements(VERBOSE_LOG)

    assert len(stmts) == 1
    assert "already exists" in stmts[0]["error"]
    assert stmts[0]["statement"].startswith("CREATE TABLE public.accounts_status_history")
    assert "уже есть" in gb.error_hint(stmts[0]["error"])


def test_failed_statements_saved_in_job_error(client):
    job_id = create_job("gprestore", 965, {"connection_id": 965})
    gb.finalize_backup_job(job_id, "gprestore", {"connection_id": 965}, [],
                           VERBOSE_LOG, 1, "gprestore --timestamp " + TS)

    # лог задачи потерян — причина берётся из сохранённого блока
    d = __import__("job_manager").get_job(job_id)
    assert gb.failed_statements(d["error_message"])[0]["error"].startswith("ERROR: relation")


def test_failed_statements_from_saved_block_only():
    text = ("Команда: x\n\nНе выполнено (1):\n- ERROR: role \"etl\" does not exist\n"
            "  ALTER TABLE public.t OWNER TO etl;\n\nХвост лога:\n...")

    stmts = gb.failed_statements(text)

    assert stmts == [{"error": "ERROR: role \"etl\" does not exist",
                      "statement": "ALTER TABLE public.t OWNER TO etl;"}]
    assert "роли" in gb.error_hint(stmts[0]["error"])
