# -*- coding: utf-8 -*-
"""Чистые функции gpbackup/gprestore — без БД и бинарей."""

import pytest

from modules.gpbackup import (
    backup_outcome,
    build_gpbackup_command,
    build_gprestore_command,
    build_pg_env,
    parse_backup_timestamp,
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
