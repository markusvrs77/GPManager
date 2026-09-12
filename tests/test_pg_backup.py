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
