# -*- coding: utf-8 -*-
"""Postgres Toolkit: чистые функции pg_dump/pg_restore - без БД и бинарей."""

import pytest

from modules.pg_backup import (
    build_pg_dump_command,
    build_pg_restore_command,
    dump_file_path,
    make_timestamp,
    pg_outcome,
)


def test_build_pg_dump_command():
    cmd, out_file = build_pg_dump_command({
        "dbname": "app_db",
        "backup_dir": "/data/backups",
        "backup_timestamp": "20260727120000",
        "include_schemas": "public\nbilling",
        "compression_level": 6,
    })

    assert cmd[0].endswith("pg_dump")
    assert "--format=custom" in cmd
    assert out_file.endswith("pgdump_app_db_20260727120000.dump")
    assert cmd[cmd.index("--file") + 1] == out_file
    assert cmd.count("--schema") == 2
    assert cmd[cmd.index("--compress") + 1] == "6"
    assert cmd[-1] == "app_db"           # база - последним аргументом

    with pytest.raises(ValueError):
        build_pg_dump_command({"dbname": "app_db",
                               "backup_timestamp": "20260727120000"})  # нет dir

    with pytest.raises(ValueError):
        build_pg_dump_command({"dbname": "app_db", "backup_dir": "/d",
                               "backup_timestamp": "не-метка"})


def test_build_pg_restore_command():
    cmd = build_pg_restore_command({
        "dump_file": "/data/backups/pgdump_app_db_20260727120000.dump",
        "target_db": "app_db_restored",
        "clean": True,
        "jobs": 4,
    })

    assert cmd[0].endswith("pg_restore")
    assert cmd[cmd.index("--dbname") + 1] == "app_db_restored"
    assert "--clean" in cmd and "--if-exists" in cmd
    assert cmd[cmd.index("--jobs") + 1] == "4"
    assert cmd[-1].endswith(".dump")

    # create-db: подключение к служебной базе + --create
    cmd = build_pg_restore_command({
        "dump_file": "/d/f.dump", "target_db": "newdb", "create_db": True,
    })
    assert "--create" in cmd
    assert cmd[cmd.index("--dbname") + 1] == "postgres"

    with pytest.raises(ValueError):
        build_pg_restore_command({"dump_file": "/d/f.dump",
                                  "target_db": "bad name"})


def test_pg_outcome_and_password_not_in_argv():
    assert pg_outcome(0, "любой лог") == "done"
    assert pg_outcome(1, "") == "failed"
    # переподхват без кода возврата: судим по ошибкам в логе
    assert pg_outcome(None, "pg_dump: dumping contents") == "done"
    assert pg_outcome(None, "pg_dump: error: connection failed") == "failed"

    cmd, _ = build_pg_dump_command({
        "dbname": "db1", "backup_dir": "/d",
        "backup_timestamp": make_timestamp(),
    })
    assert "secret" not in " ".join(cmd)

    assert dump_file_path("/d", "x", "20260101000000").replace("\\", "/") == \
        "/d/pgdump_x_20260101000000.dump"


def test_pg_error_hint_known_causes():
    from modules.pg_backup import pg_error_hint

    assert "Версия pg_dump" in pg_error_hint(
        "pg_dump: error: aborting because of server version mismatch")
    assert "пароль" in pg_error_hint(
        'FATAL:  password authentication failed for user "u"')
    assert "файл дампа" in pg_error_hint(
        'pg_dump: error: could not open output file "/x/a.dump": '
        "No such file or directory")
    # «нет таблиц» точнее общего «does not exist»
    assert "schema.table" in pg_error_hint(
        "pg_dump: error: no matching tables were found")
    assert pg_error_hint("что-то непонятное") == ""


def test_launch_error_message():
    from modules.pg_backup import launch_error_message

    exc = FileNotFoundError(2, "No such file", "/usr/bin/pg_dump")
    text = launch_error_message(exc, "/usr/bin/pg_dump")
    assert "/usr/bin/pg_dump" in text and "Установи" in text

    other = FileNotFoundError(2, "No such file", "/logs/job_1.log")
    assert "/logs/job_1.log" in launch_error_message(other, "/usr/bin/pg_dump")

    assert launch_error_message(ValueError("x"), "p") == "x"


def test_run_pg_dump_missing_binary_records_clear_error(monkeypatch):
    import modules.pg_backup as pb

    failed = {}

    monkeypatch.setattr(pb, "get_job", lambda jid: {"config_json": (
        '{"connection_id": 1, "dbname": "app", "backup_dir": "/d", '
        '"backup_timestamp": "20260727120000", '
        '"pg_dump_path": "/nope/pg_dump", "run_on": "local"}')})
    monkeypatch.setattr(pb, "get_job_items", lambda jid: [{"id": 7}])
    for name in ("mark_job_running", "mark_item_running",
                 "refresh_job_progress"):
        monkeypatch.setattr(pb, name, lambda *a, **k: None)
    monkeypatch.setattr(pb, "get_connection_by_id",
                        lambda cid: {"host": "h", "username": "u"})
    monkeypatch.setattr(pb, "mark_item_failed",
                        lambda iid, msg: failed.setdefault("item", msg))
    monkeypatch.setattr(pb, "mark_job_failed",
                        lambda jid, msg: failed.setdefault("job", msg))

    def boom(job_id, cmd, env, finalize, **kwargs):
        raise FileNotFoundError(2, "No such file", cmd[0])

    monkeypatch.setattr(pb, "_run_external_tool", boom)

    pb.run_pg_dump_job(1)

    assert "Не найден исполняемый файл: /nope/pg_dump" in failed["job"]
    assert "Команда: /nope/pg_dump" in failed["job"]
    assert "/nope/pg_dump" in failed["item"]
    assert "PGPASSWORD" not in failed["job"]


def test_pg_ssh_target_defaults_and_local(monkeypatch):
    import modules.gp_remote as gr
    from modules.pg_backup import pg_ssh_target

    monkeypatch.setattr(gr, "is_local_host", lambda h: h == "127.0.0.1")

    t = pg_ssh_target({}, {"host": "10.0.0.5"})
    assert t == {"host": "10.0.0.5", "user": "postgres", "port": 22}

    assert pg_ssh_target({}, {"host": "127.0.0.1"}) is None      # тот же хост
    assert pg_ssh_target({"run_on": "local"}, {"host": "10.0.0.5"}) is None

    t = pg_ssh_target({"ssh_user": "dba", "ssh_port": "2222"},
                      {"host": "db1"})
    assert t == {"host": "db1", "user": "dba", "port": 2222}

    with pytest.raises(ValueError):
        pg_ssh_target({"ssh_user": "bad user"}, {"host": "db1"})
    with pytest.raises(ValueError):
        pg_ssh_target({}, {"host": "db1; rm -rf /"})


def test_wrap_pg_ssh_password_in_stdin_and_server_binary():
    from modules.pg_backup import build_pg_dump_command, wrap_pg_ssh

    cmd, _ = build_pg_dump_command({
        "dbname": "cashbdb", "backup_dir": "/dwh-backup/backup/cashbdb",
        "backup_timestamp": "20261119050943"})
    env = {"PGHOST": "db1", "PGPORT": "5432", "PGUSER": "u",
           "PGPASSWORD": "s3cr3t"}
    target = {"host": "db1", "user": "postgres", "port": 22}

    argv, stdin = wrap_pg_ssh(target, "pg_dump", cmd, env, "")
    script = argv[-1]

    assert argv[0] == "ssh" and "postgres@db1" in argv
    assert "s3cr3t" not in " ".join(argv)            # пароль только в stdin
    assert stdin == "s3cr3t\n"
    assert "/proc/$P/exe" in script                   # бинарь как у сервера
    assert "/usr/bin/pg_dump" not in script           # не путь Opsentri
    assert "OPSENTRI_REMOTE_PID=$$" in script
    assert "/dwh-backup/backup/cashbdb/pgdump_cashbdb_20261119050943.dump" \
        in script

    argv2, _ = wrap_pg_ssh(target, "pg_dump", cmd, env, "/usr/pgsql-16/bin/pg_dump")
    assert "BIN=/usr/pgsql-16/bin/pg_dump;" in argv2[-1]
    assert "/proc/$P/exe" not in argv2[-1]


def test_pg_dump_path_validated():
    from modules.pg_backup import build_pg_dump_command

    with pytest.raises(ValueError):
        build_pg_dump_command({
            "dbname": "a", "backup_dir": "/d",
            "backup_timestamp": "20260727120000",
            "pg_dump_path": "/usr/bin/pg_dump; id"})
