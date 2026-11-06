# -*- coding: utf-8 -*-
"""Удаление копии gpbackup с диска без gpbackup_manager; остановленные копии."""

import shutil
import subprocess

import pytest

import modules.gpbackup as gb
import modules.gpbackup_delete as gd
from db import sqlite_cursor
from job_manager import create_job

TS = "20261107020200"

SEGMENTS = [
    (-1, "p", "mdw", "/data/data1/master/seg-1"),
    (-1, "m", "smdw", "/data/data1/master/seg-1"),
    (0, "p", "sdw1", "/data/primary/seg0"),
    (0, "m", "sdw2", "/data/mirror/seg0"),
    (1, "p", "sdw2", "/data/primary/seg1"),
]


# ------------------------------------------------------------ пути

def test_prefix_from_coordinator_dir():
    assert gd.segment_prefix("/data/data1/master/seg-1") == "seg"
    assert gd.segment_prefix("/data/master/gpseg-1/") == "gpseg"


def test_locations_with_backup_dir_one_per_content():
    locs = gd.backup_locations(SEGMENTS, "/dwh-backup/backups_new/", TS)

    assert locs == [
        ("mdw", "/dwh-backup/backups_new/seg-1/backups/20261107/" + TS),
        ("sdw1", "/dwh-backup/backups_new/seg0/backups/20261107/" + TS),
        ("sdw2", "/dwh-backup/backups_new/seg1/backups/20261107/" + TS),
    ]


def test_locations_in_data_dirs_include_mirrors():
    paths = [p for _h, p in gd.backup_locations(SEGMENTS, "", TS)]

    assert "/data/primary/seg0/backups/20261107/" + TS in paths
    assert "/data/mirror/seg0/backups/20261107/" + TS in paths
    assert len(paths) == 5


@pytest.mark.parametrize("segments,backup_dir,ts", [
    (SEGMENTS, "/b", "2026110702020"),                               # не метка
    (SEGMENTS, "/b; rm -rf /", TS),                                   # мусор в каталоге
    ([(-1, "p", "mdw;reboot", "/data/seg-1")], "", TS),               # мусор в хосте
    ([(-1, "p", "mdw", "/data/../etc/seg-1")], "", TS),               # выход из каталога
])
def test_locations_refuse_unsafe(segments, backup_dir, ts):
    with pytest.raises(ValueError):
        gd.backup_locations(segments, backup_dir, ts)


def test_parse_result():
    locs = [("h1", "/a/backups/20261107/" + TS), ("h2", "/b/backups/20261107/" + TS),
            ("h3", "/c/backups/20261107/" + TS)]
    out = "h1\t{}\tDELETED\nh2\t{}\tABSENT\n".format(locs[0][1], locs[1][1])

    r = gd.parse_result(out, locs)

    assert r["deleted"] == 1 and r["absent"] == 1
    assert r["failed"] == [("h3", locs[2][1], "нет ответа")]


@pytest.mark.skipif(not shutil.which("bash"), reason="нужен bash")
def test_delete_script_really_deletes_only_the_timestamp_dir(tmp_path):
    root = subprocess.run(["bash", "-c", 'cd "$1" && pwd', "_", str(tmp_path)],
                          stdout=subprocess.PIPE, check=True).stdout.decode().strip()
    keep = root + "/seg0/backups/20261107/20261106205400"
    gone = root + "/seg0/backups/20261107/" + TS
    subprocess.run(["bash", "-c", 'mkdir -p "$1" "$2" && echo x > "$2/f.gz"', "_", keep, gone],
                   check=True)

    locs = [("sdw1", gone), ("sdw2", root + "/seg1/backups/20261107/" + TS)]
    # ssh -> выполнить последнюю команду здесь
    fake = 'ssh() { shift $(( $# - 1 )); bash -c "$1"; }\n'
    out = subprocess.run(["bash", "-c", fake + gd.delete_script(locs)],
                         stdout=subprocess.PIPE, check=True).stdout.decode()

    r = gd.parse_result(out, locs)
    assert (r["deleted"], r["absent"], r["failed"]) == (1, 1, [])
    assert subprocess.run(["bash", "-c", '[ -d "$1" ] && [ ! -d "$2" ]', "_", keep, gone]).returncode == 0


# ------------------------------------------------------------ фоновое удаление

@pytest.fixture
def sync_delete(monkeypatch):
    monkeypatch.setattr(gd, "_spawn", lambda fn, *a: fn(*a))
    monkeypatch.setattr(gd, "fetch_segments", lambda cid: SEGMENTS)
    return monkeypatch


def _status(backup_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT status, delete_error FROM backups WHERE id = ?", (backup_id,))
        return tuple(cur.fetchone())


def test_delete_route_deletes_copy_and_marks_history(client, sync_delete):
    calls = []

    def fake_exec(cid, script, stdin_text=None, timeout=None):
        calls.append(script)
        if script.startswith("python3"):
            return 0, "HISTORY\t1\n"
        locs = gd.backup_locations(SEGMENTS, "/dwh", TS)
        return 0, "".join("{}\t{}\tDELETED\n".format(h, p) for h, p in locs)

    sync_delete.setattr(gd, "_exec", fake_exec)
    bid = gb.insert_backup(941, None, TS, "adb", "full", "/dwh", "cancelled")

    r = client.post("/api/backup/delete", json={"backup_id": bid}).get_json()

    assert r["ok"] is True, r
    assert _status(bid) == ("deleted", None)
    assert "gpbackup_history.db" in calls[-1] and TS in calls[-1]


def test_delete_failure_keeps_status_and_reason(client, sync_delete):
    sync_delete.setattr(gd, "_exec", lambda *a, **k: (255, "ssh: connect to host sdw1: refused"))
    bid = gb.insert_backup(942, None, TS, "adb", "full", "/dwh", "done")

    client.post("/api/backup/delete", json={"backup_id": bid})

    status, error = _status(bid)
    assert status == "done" and error.startswith("Удалить с диска не удалось")


def test_delete_refuses_record_without_files(client, sync_delete):
    bid = gb.insert_backup(943, None, None, "adb", "full", "", "failed")

    r = client.post("/api/backup/delete", json={"backup_id": bid})

    assert r.status_code == 400


def test_stale_deleting_after_restart():
    bid = gb.insert_backup(944, None, TS, "adb", "full", "", "deleting")

    assert gd.reset_stale_deleting() >= 1
    assert _status(bid)[0] == "failed"


# ------------------------------------------------------------ остановленные копии

def test_stopped_backup_registered_from_log(tmp_path):
    log = tmp_path / "job.log"
    log.write_text("20261107:02:02:00 gpbackup:gpadmin:mdw:1-[INFO]:-Backup Timestamp = "
                   + TS + "\n", encoding="utf-8")
    cfg = {"connection_id": 945, "dbname": "adb", "backup_dir": "/dwh"}
    job_id = create_job("gpbackup", 945, cfg)
    no_ts = create_job("gpbackup", 945, cfg)
    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE jobs SET status = 'cancelled', log_file = ? WHERE id = ?", (str(log), job_id))
        cur.execute("UPDATE jobs SET status = 'cancelled' WHERE id = ?", (no_ts,))

    assert gb.register_stopped_backups(945) == 1
    assert gb.register_stopped_backups(945) == 0

    with sqlite_cursor() as cur:
        cur.execute("SELECT status, backup_timestamp, backup_dir FROM backups WHERE job_id = ?", (job_id,))
        assert tuple(cur.fetchone()) == ("cancelled", TS, "/dwh")


def test_overview_counts_only_usable_copies_and_bulk_forget_keeps_leftovers(client, monkeypatch):
    monkeypatch.setattr(gb, "files_on_disk", lambda row: True)
    gb.insert_backup(946, None, "20261106205400", "adb", "full", "", "done")
    stopped = gb.insert_backup(946, None, TS, "adb", "full", "", "cancelled")
    gb.insert_backup(946, None, None, "adb", "full", "", "failed")

    d = client.get("/api/backup/overview?connection_id=946").get_json()
    assert d["stats"]["total"] == 1
    assert next(b for b in d["backups"] if b["id"] == stopped)["on_disk"] is True

    r = client.post("/api/backup/forget", json={"connection_id": 946, "all_failed": True}).get_json()
    assert r["removed"] == 1
    ids = [b["id"] for b in client.get("/api/backup/overview?connection_id=946").get_json()["backups"]]
    assert stopped in ids
