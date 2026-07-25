# -*- coding: utf-8 -*-
"""
Резервное копирование Greenplum: gpbackup / gprestore.

Оба инструмента — внешние бинари (как gpcopy), поэтому получают тот же
паттерн: лог в постоянный файл, отвязанный процесс, стоп-кнопка и
планировщик. Подключение к кластеру передаётся через PG*-переменные
окружения — пароль не светится в командной строке.
"""

import json
import os
import re
import subprocess

from db import sqlite_cursor

from job_manager import (
    get_job,
    get_job_items,
    is_stop_requested,
    mark_item_done,
    mark_item_failed,
    mark_item_running,
    mark_job_cancelled,
    mark_job_done,
    mark_job_failed,
    mark_job_running,
    refresh_job_progress,
    set_job_runtime,
)

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id

try:
    from modules.gpcopy import _job_log_path, _watch_gpcopy_log
except ImportError:
    from gpcopy import _job_log_path, _watch_gpcopy_log


DEFAULT_GPBACKUP_PATH = "/usr/local/gpdb/greenplum-db/bin/gpbackup"
DEFAULT_GPRESTORE_PATH = "/usr/local/gpdb/greenplum-db/bin/gprestore"

BACKUP_TYPES = ("full", "metadata_only", "data_only", "incremental")

_TIMESTAMP_RE = re.compile(r"Backup Timestamp\s*=\s*(\d{14})")

_IDENT_RE = re.compile(r"^[A-Za-z0-9_$.]+$")


def _clean_names(raw):
    """Список schema или schema.table из textarea (по одной в строке)."""
    out = []

    for line in (raw or "").splitlines() if isinstance(raw, str) else (raw or []):
        name = str(line).strip()

        if not name:
            continue

        if not _IDENT_RE.match(name):
            raise ValueError("Некорректное имя объекта: {}".format(name))

        if name not in out:
            out.append(name)

    return out


def build_gpbackup_command(config):
    """Команда gpbackup из конфига задачи. Чистая функция."""
    dbname = (config.get("dbname") or "").strip()

    if not dbname:
        raise ValueError("dbname обязателен")

    backup_type = config.get("backup_type") or "full"

    if backup_type not in BACKUP_TYPES:
        raise ValueError("Неизвестный тип бэкапа: {}".format(backup_type))

    cmd = [
        config.get("gpbackup_path") or DEFAULT_GPBACKUP_PATH,
        "--dbname", dbname,
    ]

    backup_dir = (config.get("backup_dir") or "").strip()

    if backup_dir:
        cmd += ["--backup-dir", backup_dir]

    if backup_type == "metadata_only":
        cmd.append("--metadata-only")
    elif backup_type == "data_only":
        cmd.append("--data-only")
    elif backup_type == "incremental":
        # инкремент требует leaf-partition-data и существующего полного бэкапа
        cmd.append("--incremental")

    # для table-level restore и инкрементов данные пишутся по партициям
    if backup_type != "metadata_only" and config.get("leaf_partition_data", True):
        cmd.append("--leaf-partition-data")

    for schema in _clean_names(config.get("include_schemas")):
        cmd += ["--include-schema", schema]

    for table in _clean_names(config.get("include_tables")):
        if "." not in table:
            raise ValueError("Таблица должна быть schema.table: {}".format(table))
        cmd += ["--include-table", table]

    jobs = int(config.get("jobs") or 1)

    if jobs > 1:
        cmd += ["--jobs", str(jobs)]

    compression = config.get("compression_level")

    if compression:
        cmd += ["--compression-level", str(int(compression))]

    extra = (config.get("extra_args") or "").strip()

    if extra:
        cmd += extra.split()

    return cmd


def build_gprestore_command(config):
    """Команда gprestore из конфига задачи. Чистая функция."""
    timestamp = str(config.get("backup_timestamp") or "").strip()

    if not re.match(r"^\d{14}$", timestamp):
        raise ValueError("backup_timestamp должен быть меткой вида YYYYMMDDHHMMSS")

    cmd = [
        config.get("gprestore_path") or DEFAULT_GPRESTORE_PATH,
        "--timestamp", timestamp,
    ]

    backup_dir = (config.get("backup_dir") or "").strip()

    if backup_dir:
        cmd += ["--backup-dir", backup_dir]

    redirect_db = (config.get("redirect_db") or "").strip()

    if redirect_db:
        if not _IDENT_RE.match(redirect_db):
            raise ValueError("Некорректное имя БД: {}".format(redirect_db))
        cmd += ["--redirect-db", redirect_db]

    if config.get("create_db"):
        cmd.append("--create-db")

    if config.get("data_only"):
        cmd.append("--data-only")

    if config.get("metadata_only"):
        cmd.append("--metadata-only")

    for table in _clean_names(config.get("include_tables")):
        if "." not in table:
            raise ValueError("Таблица должна быть schema.table: {}".format(table))
        cmd += ["--include-table", table]

    jobs = int(config.get("jobs") or 1)

    if jobs > 1:
        cmd += ["--jobs", str(jobs)]

    if config.get("on_error_continue", True):
        cmd.append("--on-error-continue")

    extra = (config.get("extra_args") or "").strip()

    if extra:
        cmd += extra.split()

    return cmd


def build_pg_env(conn_cfg):
    """PG*-переменные для gpbackup/gprestore (пароль не в командной строке)."""
    env = os.environ.copy()

    env["PGHOST"] = str(conn_cfg.get("host") or "")
    env["PGPORT"] = str(conn_cfg.get("port") or 5432)
    env["PGUSER"] = str(conn_cfg.get("username") or "")
    env["PGDATABASE"] = str(
        conn_cfg.get("database_name") or conn_cfg.get("database") or ""
    )

    password = conn_cfg.get("password")

    if password:
        env["PGPASSWORD"] = str(password)

    return env


def parse_backup_timestamp(text):
    """'Backup Timestamp = 20260726093045' -> '20260726093045' (или None)."""
    match = _TIMESTAMP_RE.search(text or "")
    return match.group(1) if match else None


def backup_outcome(text):
    """'done' | 'failed' по финальным строкам лога gpbackup/gprestore."""
    tail = (text or "")[-4000:]

    if "completed successfully" in tail:
        return "done"

    return "failed"


def extract_backup_errors(text, limit=10):
    """Строки CRITICAL/ERROR из лога — причина падения."""
    found = []

    for line in (text or "").splitlines():
        stripped = line.strip()

        if not stripped:
            continue

        upper = stripped.upper()

        if "[CRITICAL]" in upper or "[ERROR]" in upper or upper.startswith("ERROR"):
            if stripped not in found:
                found.append(stripped)

    return found[-limit:]


# ------------------------------------------------------------------
# реестр копий
# ------------------------------------------------------------------

def insert_backup(connection_id, job_id, backup_timestamp, dbname,
                  backup_type, backup_dir, status):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO backups (
                connection_id, job_id, backup_timestamp,
                dbname, backup_type, backup_dir, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                connection_id, job_id, backup_timestamp,
                dbname, backup_type, backup_dir, status,
            ),
        )
        return cur.lastrowid


def list_backups(limit=100):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT b.*, c.name AS connection_name
            FROM backups b
            LEFT JOIN connections c ON c.id = b.connection_id
            ORDER BY b.id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        return [dict(row) for row in cur.fetchall()]


# ------------------------------------------------------------------
# раннеры
# ------------------------------------------------------------------

def _run_external_tool(job_id, cmd, env, finalize):
    """
    Общий каркас: detached-процесс, лог в постоянный файл, стоп-кнопка,
    переживание рестарта (pid + log_file в задаче). finalize(log_text, rc).
    """
    log_path = _job_log_path(job_id)
    log_handle = open(log_path, "w", encoding="utf-8", errors="replace")

    popen_kwargs = {
        "stdout": log_handle,
        "stderr": subprocess.STDOUT,
        "universal_newlines": True,
        "env": env,
    }

    if os.name == "posix":
        popen_kwargs["start_new_session"] = True
    else:
        popen_kwargs["creationflags"] = 0x00000200

    process = subprocess.Popen(cmd, **popen_kwargs)

    try:
        log_handle.close()
    except Exception:
        pass

    set_job_runtime(job_id, pid=process.pid, log_file=log_path)

    log_text = _watch_gpcopy_log(job_id, log_path, [], process=process)

    if is_stop_requested(job_id):
        mark_job_cancelled(job_id)
        refresh_job_progress(job_id)
        return

    finalize(log_text, process.returncode)


def run_gpbackup_job(job_id):
    """Раннер job_type='gpbackup'."""
    job = get_job(job_id)

    if not job:
        return

    config = json.loads(job.get("config_json") or "{}")
    mark_job_running(job_id)

    items = get_job_items(job_id)

    for item in items:
        mark_item_running(item["id"])

    refresh_job_progress(job_id)

    try:
        conn_cfg = get_connection_by_id(int(config["connection_id"]))

        if not conn_cfg:
            raise Exception("Подключение не найдено")

        if not config.get("dbname"):
            config["dbname"] = (
                conn_cfg.get("database_name") or conn_cfg.get("database")
            )

        cmd = build_gpbackup_command(config)
        env = build_pg_env(conn_cfg)

        def finalize(log_text, rc):
            timestamp = parse_backup_timestamp(log_text)
            ok = rc == 0 and backup_outcome(log_text) == "done"

            insert_backup(
                connection_id=int(config["connection_id"]),
                job_id=job_id,
                backup_timestamp=timestamp,
                dbname=config.get("dbname"),
                backup_type=config.get("backup_type") or "full",
                backup_dir=config.get("backup_dir") or "",
                status="done" if ok else "failed",
            )

            if ok:
                for item in items:
                    mark_item_done(item["id"])

                refresh_job_progress(job_id)
                mark_job_done(job_id)
                return

            errors = extract_backup_errors(log_text)
            report = "Команда: {}\n\n{}\n\nХвост лога:\n{}".format(
                " ".join(cmd),
                "\n".join(errors) or "(строк с ошибками не найдено)",
                (log_text or "")[-5000:],
            )

            for item in items:
                mark_item_failed(
                    item["id"],
                    ("\n".join(errors) or "gpbackup rc={}".format(rc))[:2000],
                )

            refresh_job_progress(job_id)
            mark_job_failed(job_id, report[:12000])

        _run_external_tool(job_id, cmd, env, finalize)

    except Exception as e:
        for item in items:
            try:
                mark_item_failed(item["id"], str(e)[:500])
            except Exception:
                pass

        refresh_job_progress(job_id)
        mark_job_failed(job_id, str(e)[:4000])


def run_gprestore_job(job_id):
    """Раннер job_type='gprestore'."""
    job = get_job(job_id)

    if not job:
        return

    config = json.loads(job.get("config_json") or "{}")
    mark_job_running(job_id)

    items = get_job_items(job_id)

    for item in items:
        mark_item_running(item["id"])

    refresh_job_progress(job_id)

    try:
        conn_cfg = get_connection_by_id(int(config["connection_id"]))

        if not conn_cfg:
            raise Exception("Подключение не найдено")

        cmd = build_gprestore_command(config)
        env = build_pg_env(conn_cfg)

        def finalize(log_text, rc):
            ok = rc == 0 and backup_outcome(log_text) == "done"

            if ok:
                for item in items:
                    mark_item_done(item["id"])

                refresh_job_progress(job_id)
                mark_job_done(job_id)
                return

            errors = extract_backup_errors(log_text)
            report = "Команда: {}\n\n{}\n\nХвост лога:\n{}".format(
                " ".join(cmd),
                "\n".join(errors) or "(строк с ошибками не найдено)",
                (log_text or "")[-5000:],
            )

            for item in items:
                mark_item_failed(
                    item["id"],
                    ("\n".join(errors) or "gprestore rc={}".format(rc))[:2000],
                )

            refresh_job_progress(job_id)
            mark_job_failed(job_id, report[:12000])

        _run_external_tool(job_id, cmd, env, finalize)

    except Exception as e:
        for item in items:
            try:
                mark_item_failed(item["id"], str(e)[:500])
            except Exception:
                pass

        refresh_job_progress(job_id)
        mark_job_failed(job_id, str(e)[:4000])
