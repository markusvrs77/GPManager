# -*- coding: utf-8 -*-
"""Чистые функции gpbackup/gprestore — без БД и бинарей."""

import pytest

from modules.gpbackup import (
    backup_outcome,
    build_gpbackup_command,
    build_gprestore_command,
    build_manager_command,
    build_pg_env,
    parse_backup_timestamp,
    parse_manager_backups,
)


def test_build_gpbackup_command_full():
    cmd = build_gpbackup_command({
        "dbname": "adb",
        "backup_type": "full",
        "backup_dir": "/data/backups",
        "include_schemas": "dwh_dds\ndwh_dm\n",
        "jobs": 4,
        "compression_level": 3,
    })

    assert cmd[0].endswith("gpbackup")
    assert ["--dbname", "adb"] == cmd[1:3]
    assert "--backup-dir" in cmd and "/data/backups" in cmd
    assert cmd.count("--include-schema") == 2
    assert "--leaf-partition-data" in cmd
    assert cmd[cmd.index("--jobs") + 1] == "4"
    assert "--compression-level" in cmd


def test_build_gpbackup_command_variants():
    meta = build_gpbackup_command({"dbname": "adb", "backup_type": "metadata_only"})
    assert "--metadata-only" in meta
    assert "--leaf-partition-data" not in meta

    inc = build_gpbackup_command({"dbname": "adb", "backup_type": "incremental"})
    assert "--incremental" in inc and "--leaf-partition-data" in inc

    with pytest.raises(ValueError):
        build_gpbackup_command({"backup_type": "full"})          # нет dbname

    with pytest.raises(ValueError):
        build_gpbackup_command({"dbname": "adb", "backup_type": "wat"})

    with pytest.raises(ValueError):
        build_gpbackup_command({
            "dbname": "adb",
            "include_tables": "no_schema_table",                 # не schema.table
        })

    with pytest.raises(ValueError):
        build_gpbackup_command({
            "dbname": "adb",
            "include_schemas": "bad name; drop",                 # инъекция
        })


def test_build_gprestore_command():
    cmd = build_gprestore_command({
        "backup_timestamp": "20260726093045",
        "backup_dir": "/data/backups",
        "redirect_db": "adb_restored",
        "create_db": True,
        "jobs": 2,
    })

    assert cmd[0].endswith("gprestore")
    assert ["--timestamp", "20260726093045"] == cmd[1:3]
    assert "--redirect-db" in cmd and "adb_restored" in cmd
    assert "--create-db" in cmd
    assert "--on-error-continue" in cmd    # дефолт

    with pytest.raises(ValueError):
        build_gprestore_command({"backup_timestamp": "не-метка"})

    with pytest.raises(ValueError):
        build_gprestore_command({
            "backup_timestamp": "20260726093045",
            "redirect_db": "bad name",
        })


def test_parse_backup_timestamp_and_outcome():
    log = (
        "20260726:09:30:45 gpbackup:gpadmin:host:-[INFO]:-Backup Timestamp = 20260726093045\n"
        "20260726:09:35:02 gpbackup:gpadmin:host:-[INFO]:-Backup completed successfully\n"
    )

    assert parse_backup_timestamp(log) == "20260726093045"
    assert backup_outcome(log) == "done"

    assert parse_backup_timestamp("no timestamp here") is None
    assert backup_outcome("[CRITICAL]:-out of space") == "failed"


def test_parse_manager_backups():
    out = (
        " timestamp        database   type            object filtering   plugin   duration   date\n"
        " 20260726093045   adb        full                                        00:12:03   Sat Jul 26 2026 09:30:45\n"
        " 20260725010000   adb        metadata-only   include-schema              00:00:11   Fri Jul 25 2026 01:00:00\n"
        " 20260724010000   app_db     incremental                                 00:03:40   Thu Jul 24 2026 01:00:00\n"
        "какой-то мусор без метки\n"
    )

    rows = parse_manager_backups(out)

    assert len(rows) == 3
    assert rows[0] == {
        "timestamp": "20260726093045", "dbname": "adb", "backup_type": "full",
    }
    # дефисы менеджера приводим к нашим типам с подчёркиванием
    assert rows[1]["backup_type"] == "metadata_only"
    assert rows[2]["dbname"] == "app_db"

    assert parse_manager_backups("") == []


def test_build_manager_command():
    cmd = build_manager_command("list-backups")
    assert cmd[0].endswith("gpbackup_manager")
    assert cmd[1:] == ["list-backups"]

    cmd = build_manager_command(
        "delete-backup", timestamp="20260726093045", manager_path="/opt/gpbm",
    )
    assert cmd == ["/opt/gpbm", "delete-backup", "20260726093045"]

    with pytest.raises(ValueError):
        build_manager_command("delete-backup", timestamp="oops")

    with pytest.raises(ValueError):
        build_manager_command("delete-backup")                    # нет метки

    with pytest.raises(ValueError):
        build_manager_command("format-disk")                      # не из списка


def test_build_pg_env_keeps_password_out_of_argv():
    env = build_pg_env({
        "host": "10.10.0.1", "port": 5432, "username": "gpadmin",
        "database_name": "adb", "password": "secret",
    })

    assert env["PGHOST"] == "10.10.0.1"
    assert env["PGPORT"] == "5432"
    assert env["PGUSER"] == "gpadmin"
    assert env["PGDATABASE"] == "adb"
    assert env["PGPASSWORD"] == "secret"

    # команда не должна содержать пароль
    cmd = build_gpbackup_command({"dbname": "adb"})
    assert "secret" not in " ".join(cmd)
