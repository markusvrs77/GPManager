# -*- coding: utf-8 -*-
"""gpbackup на координаторе кластера по SSH и ошибки из gpAdminLogs."""

import pytest

import modules.gp_remote as gr
import modules.gpbackup as gb
from modules.connections import create_connection

TARGET = {"host": "172.17.138.23", "user": "gpadmin", "port": 22,
          "gphome": "/usr/local/gpdb/greenplum-db"}

ADMIN_REF = (
    "20261106:20:47:04 gpbackup:gpadmin:ffin-admaster-test:1066037-[CRITICAL]:-"
    "Unable to create backup directories on 33 segments. See "
    "/home/gpadmin/gpAdminLogs/gpbackup_20261106.log for a complete list of errors.\n"
)

ADMIN_LOG = (
    "20261106:20:47:04 gpbackup:gpadmin:ffin-admaster-test:1066037-[ERROR]:-"
    "Unable to create directory /dwh-backup/backups_new/seg0 on host sdw1: "
    "mkdir: cannot create directory: Permission denied\n"
    "20261106:20:47:04 gpbackup:gpadmin:ffin-admaster-test:1066037-[ERROR]:-"
    "Unable to create directory /dwh-backup/backups_new/seg1 on host sdw2: "
    "ssh: connect to host sdw2 port 22: Connection refused\n"
    "20261106:20:47:04 gpbackup:gpadmin:other:999-[ERROR]:-чужой запуск\n"
) + ADMIN_REF


class FakeRemoteFS(object):
    remote = True

    def __init__(self, files):
        self.files = files

    def read(self, path, tail=None):
        return self.files.get(path)


def _cluster(name, host):
    return create_connection({"name": name, "host": host, "port": 5432,
                              "database_name": "adb", "username": "gpadmin",
                              "password": "synthetic-pass"})


# ------------------------------------------------------------ команда

def test_wrap_command_keeps_password_off_argv():
    env = {"PGHOST": "172.17.138.23", "PGPORT": "5432", "PGUSER": "gpadmin",
           "PGDATABASE": "adb", "PGPASSWORD": "synthetic-pass"}
    argv, stdin = gr.wrap_command(
        TARGET, ["/usr/local/gpdb/greenplum-db/bin/gpbackup", "--dbname", "adb",
                 "--include-schema", "dwh dds"], env)

    script = argv[-1]
    assert argv[0] == "ssh" and "gpadmin@172.17.138.23" in argv
    assert "BatchMode=yes" in argv
    assert "synthetic-pass" not in " ".join(argv)
    assert stdin == "synthetic-pass\n"
    assert "read -r PGPASSWORD" in script
    assert gr.REMOTE_PID_MARK in script and "exec " in script
    assert "greenplum_path.sh" in script
    assert "'dwh dds'" in script            # аргументы экранированы
    assert "PGHOST=172.17.138.23" in script


def test_remote_pid_from_log():
    assert gr.remote_pid("x\nOPSENTRI_REMOTE_PID=4242\nlog") == 4242
    assert gr.remote_pid("") is None


# ------------------------------------------------------------ где запускать

def test_resolve_target_modes(client):
    cid = _cluster("gp-prod-remote", "172.17.138.23")

    t = gr.resolve_target(cid)          # авто: чужой хост -> SSH
    assert t["host"] == "172.17.138.23" and t["user"] == "gpadmin"

    gr.save_host_settings(cid, {"mode": "local"})
    assert gr.resolve_target(cid) is None

    gr.save_host_settings(cid, {"mode": "ssh", "ssh_host": "mdw-prod",
                                "ssh_user": "gpadmin", "gphome": "/opt/gp7"})
    t = gr.resolve_target(cid)
    assert (t["host"], t["gphome"]) == ("mdw-prod", "/opt/gp7")

    local = _cluster("gp-here", "127.0.0.1")
    assert gr.resolve_target(local) is None


@pytest.mark.parametrize("data", [
    {"mode": "everywhere"},
    {"ssh_host": "host; rm -rf /"},
    {"ssh_user": "root$(id)"},
    {"gphome": "relative/path"},
    {"gphome": "/opt/../etc"},
    {"ssh_port": 0},
])
def test_host_settings_validation(data):
    with pytest.raises(ValueError):
        gr.save_host_settings(1, data)


def test_remote_glob_rejects_injection():
    fs = gr.RemoteFS(TARGET)

    with pytest.raises(ValueError):
        fs.glob("/data", "*; rm -rf /")


def test_host_api(client):
    cid = _cluster("gp-api-remote", "172.17.138.24")

    d = client.get("/api/backup/host?connection_id={}".format(cid)).get_json()
    assert d["effective"] == "ssh" and "gpadmin@172.17.138.24" in d["run_where"]

    d = client.post("/api/backup/host", json={"connection_id": cid,
                                              "mode": "local"}).get_json()
    assert d["effective"] == "local"

    c = client.post("/api/backup/host/check", json={"connection_id": cid}).get_json()
    assert c["local"] is True


# ------------------------------------------------------------ gpAdminLogs

def test_admin_log_errors_per_segment_from_coordinator():
    fs = FakeRemoteFS({"/home/gpadmin/gpAdminLogs/gpbackup_20261106.log": ADMIN_LOG})

    errors = gb.admin_log_errors(ADMIN_REF, fs=fs)

    assert len(errors) == 2                       # только этот pid, без итоговой
    assert "sdw1" in errors[0] and "Permission denied" in errors[0]
    assert "Connection refused" in errors[1]
    assert "координаторе" in gb.error_hint(ADMIN_REF)


def test_admin_errors_saved_block_roundtrip():
    text = "Команда: x\n\nИз лога gpAdminLogs (1):\n- seg0 on sdw1: denied\n\nХвост лога:\n"

    assert gb.admin_log_errors(text) == ["seg0 on sdw1: denied"]


def test_ssh_failure_reason_and_hint():
    text = ("Команда: gpbackup --dbname adb  [по SSH на gpadmin@172.17.138.23]\n\n"
            "(строк с ошибками не найдено)\n\nХвост лога:\n"
            "gpadmin@172.17.138.23: Permission denied (publickey,gssapi-keyex,password).\n")

    reason = gb.error_reason(text)

    assert reason.startswith("gpadmin@172.17.138.23: Permission denied (publickey")
    assert "ssh-copy-id" in gb.error_hint(reason)


def test_backup_dir_is_remembered(client):
    cid = _cluster("gp-api-dir", "172.17.138.25")

    d = client.get("/api/backup/dir?connection_id={}".format(cid)).get_json()
    assert d == {"ok": True, "backup_dir": "", "saved": False}

    d = client.post("/api/backup/dir", json={"connection_id": cid,
                                             "backup_dir": " /dwh-backup/new/ "}).get_json()
    assert d["backup_dir"] == "/dwh-backup/new"

    d = client.get("/api/backup/dir?connection_id={}".format(cid)).get_json()
    assert d == {"ok": True, "backup_dir": "/dwh-backup/new", "saved": True}

    # SSH-настройки кластера каталог не затирают, и наоборот
    client.post("/api/backup/host", json={"connection_id": cid, "mode": "local"})
    assert gr.get_backup_dir(cid) == "/dwh-backup/new"
    assert gr.get_host_settings(cid)["mode"] == "local"

    bad = client.post("/api/backup/dir", json={"connection_id": cid,
                                               "backup_dir": "/x; rm -rf /"})
    assert bad.status_code == 400

    # пусто — тоже осознанный выбор (каталоги сегментов)
    client.post("/api/backup/dir", json={"connection_id": cid, "backup_dir": ""})
    d = client.get("/api/backup/dir?connection_id={}".format(cid)).get_json()
    assert d == {"ok": True, "backup_dir": "", "saved": True}
