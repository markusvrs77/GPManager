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
DEFAULT_GPBACKUP_MANAGER_PATH = "/usr/local/gpdb/greenplum-db/bin/gpbackup_manager"

BACKUP_TYPES = ("full", "metadata_only", "data_only", "incremental")

MANAGER_ACTIONS = ("list-backups", "display-report", "delete-backup")

_TIMESTAMP_RE = re.compile(r"Backup Timestamp\s*=\s*(\d{14})")

# строка list-backups: метка, база, тип (остальные колонки не нужны)
_MANAGER_ROW_RE = re.compile(r"^\s*(\d{14})\s+(\S+)\s+(\S+)")

_IDENT_RE = re.compile(r"^[A-Za-z0-9_$.]+$")

# --- частичный бэкап (сборка gpbackup-resilient 1.30.9+) ---
#
# С --on-error-continue таблица, на которой упал COPY, пропускается, а
# бэкап завершается со статусом Partial и кодом возврата 3. Копия
# восстановима: пропущенные таблицы восстанавливаются пустыми (только DDL).
PARTIAL_EXIT_CODE = 3

# финальная строка лога (FormatPartialBackupSummary)
_PARTIAL_SUMMARY_RE = re.compile(
    r"Backup completed with status Partial: (\d+) table\(s\) were skipped"
    r" and are NOT in this backup: (.*?)"
    r"(?:\. Full list: (\S+)\. To back up only these tables:.*)?$",
    re.MULTILINE,
)

# по строке на каждую пропущенную таблицу — полный список, в отличие от
# итоговой строки, где не больше 10 имён
_SKIPPED_TABLE_RE = re.compile(r"skipping table (.+?) due to error: ")

_SKIPPED_FILE_LOG_RE = re.compile(r"List of skipped tables written to (\S+)")

# файл, который пишет gpbackup рядом с отчётом
_SKIPPED_FILE_RE = re.compile(r"^/[A-Za-z0-9_./-]*/gpbackup_\d{14}_skipped_tables$")

# сколько имён хранить в реестре
MAX_SKIPPED_STORED = 1000


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

    # список таблиц из файла пропущенных (кнопка «Добрать пропущенные»)
    table_file = str(config.get("include_table_file") or "").strip()

    if table_file:
        if ".." in table_file or not _SKIPPED_FILE_RE.match(table_file):
            raise ValueError(
                "include_table_file должен быть файлом "
                "gpbackup_<метка>_skipped_tables: {}".format(table_file)
            )
        cmd += ["--include-table-file", table_file]

    extra = (config.get("extra_args") or "").strip()

    # пропускать таблицы, на которых упал COPY, вместо остановки бэкапа
    if config.get("on_error_continue") and "--on-error-continue" not in extra.split():
        cmd.append("--on-error-continue")

    if extra:
        cmd += extra.split()

    return cmd


def parse_partial_backup(text):
    """
    Итог частичного бэкапа из лога gpbackup -> {count, tables, file}
    или None, если бэкап не частичный.

    tables — полный список из построчных «skipping table …»; если их в
    логе нет (обрезан), берём до 10 имён из итоговой строки.
    """
    text = text or ""
    summary = None

    for summary in _PARTIAL_SUMMARY_RE.finditer(text):
        pass

    if summary is None:
        return None

    tables = []

    for match in _SKIPPED_TABLE_RE.finditer(text):
        name = match.group(1).strip()

        if name and name not in tables:
            tables.append(name)

    if not tables:
        listed = re.sub(r" \.\.\. and \d+ more$", "", summary.group(2).strip())
        tables = [n.strip() for n in listed.split(", ") if n.strip()]

    skipped_file = summary.group(3)

    if not skipped_file:
        found = _SKIPPED_FILE_LOG_RE.search(text)
        skipped_file = found.group(1) if found else None

    return {
        "count": int(summary.group(1)),
        "tables": tables[:MAX_SKIPPED_STORED],
        "file": skipped_file,
    }


def skipped_backup_config(backup_row):
    """
    Конфиг задачи «забэкапить только пропущенные таблицы» по записи
    реестра частичной копии. Файл со списком предпочтительнее: в нём все
    таблицы, в том числе с кавычками в имени. ValueError — добирать нечего
    или не из чего.
    """
    if (backup_row or {}).get("status") != "partial":
        raise ValueError("Копия не частичная — добирать нечего")

    config = {
        "connection_id": int(backup_row.get("connection_id") or 0),
        "dbname": backup_row.get("dbname") or "",
        "backup_type": "full",
        "backup_dir": backup_row.get("backup_dir") or "",
        "on_error_continue": True,
        "skipped_from": backup_row.get("backup_timestamp"),
    }

    skipped_file = (backup_row.get("skipped_file") or "").strip()

    if skipped_file and _SKIPPED_FILE_RE.match(skipped_file) \
            and ".." not in skipped_file:
        config["include_table_file"] = skipped_file
        return config

    try:
        tables = json.loads(backup_row.get("skipped_tables") or "[]")
    except ValueError:
        tables = []

    if not tables or not all(_IDENT_RE.match(t) and "." in t for t in tables):
        raise ValueError(
            "Нет файла со списком пропущенных таблиц, а имена в реестре "
            "неполные — запусти бэкап этих таблиц вручную из формы"
        )

    config["include_tables"] = "\n".join(tables)
    return config


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
    """
    'done' | 'partial' | 'failed' по финальным строкам лога
    gpbackup/gprestore. 'partial' — gpbackup с --on-error-continue
    пропустил часть таблиц, копия восстановима.
    """
    tail = (text or "")[-20000:]

    if "completed successfully" in tail[-4000:]:
        return "done"

    if "Backup completed with status Partial" in tail:
        return "partial"

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
# gpbackup_manager: реестр на диске
# ------------------------------------------------------------------

def build_manager_command(action, timestamp=None, manager_path=None):
    """Команда gpbackup_manager. Чистая функция."""
    if action not in MANAGER_ACTIONS:
        raise ValueError("Неизвестная команда gpbackup_manager: {}".format(action))

    cmd = [manager_path or DEFAULT_GPBACKUP_MANAGER_PATH, action]

    if action in ("display-report", "delete-backup"):
        ts = str(timestamp or "").strip()

        if not re.match(r"^\d{14}$", ts):
            raise ValueError("Нужна метка бэкапа вида YYYYMMDDHHMMSS")

        cmd.append(ts)

    return cmd


def parse_manager_backups(text):
    """
    Вывод `gpbackup_manager list-backups` -> [{timestamp, dbname, backup_type}].
    Типы менеджера с дефисами (metadata-only) приводим к нашим
    с подчёркиванием (metadata_only).
    """
    rows = []

    for line in (text or "").splitlines():
        match = _MANAGER_ROW_RE.match(line)

        if not match:
            continue

        rows.append({
            "timestamp": match.group(1),
            "dbname": match.group(2),
            "backup_type": match.group(3).replace("-", "_"),
        })

    return rows


def run_manager_command(conn_cfg, action, timestamp=None,
                        manager_path=None, timeout=600):
    """Синхронный запуск gpbackup_manager. -> (rc, вывод)."""
    cmd = build_manager_command(action, timestamp, manager_path)
    env = build_pg_env(conn_cfg)

    proc = subprocess.run(
        cmd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        timeout=timeout,
    )

    return proc.returncode, proc.stdout or ""


def sync_disk_backups(connection_id, manager_path=None):
    """
    Синхронизация реестра с диском: list-backups с координатора и
    добавление копий, которых нет в нашем реестре (в т.ч. сделанных
    мимо GPManager). -> {found, added}.
    """
    conn_cfg = get_connection_by_id(int(connection_id))

    if not conn_cfg:
        raise ValueError("Подключение не найдено")

    rc, output = run_manager_command(conn_cfg, "list-backups",
                                     manager_path=manager_path, timeout=120)

    if rc != 0:
        errors = extract_backup_errors(output)
        raise RuntimeError(
            "\n".join(errors) or "gpbackup_manager rc={}".format(rc)
        )

    disk = parse_manager_backups(output)

    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT backup_timestamp FROM backups WHERE connection_id = ?",
            (int(connection_id),),
        )
        known = {row["backup_timestamp"] for row in cur.fetchall()}

    added = 0

    for entry in disk:
        if entry["timestamp"] in known:
            continue

        backup_type = entry["backup_type"]

        if backup_type not in BACKUP_TYPES:
            backup_type = "full"

        insert_backup(
            connection_id=int(connection_id),
            job_id=None,
            backup_timestamp=entry["timestamp"],
            dbname=entry["dbname"],
            backup_type=backup_type,
            backup_dir="",
            status="done",
        )
        added += 1

    return {"found": len(disk), "added": added}


def get_backup_report(connection_id, timestamp, manager_path=None):
    """Отчёт по копии: `gpbackup_manager display-report <ts>` -> текст."""
    conn_cfg = get_connection_by_id(int(connection_id))

    if not conn_cfg:
        raise ValueError("Подключение не найдено")

    rc, output = run_manager_command(conn_cfg, "display-report", timestamp,
                                     manager_path=manager_path, timeout=120)

    if rc != 0:
        errors = extract_backup_errors(output)
        raise RuntimeError(
            "\n".join(errors) or "gpbackup_manager rc={}".format(rc)
        )

    return output


def delete_disk_backup(connection_id, timestamp, manager_path=None):
    """
    Удаление копии с диска: `gpbackup_manager delete-backup <ts>`.
    Успех -> реестровые записи с этой меткой помечаются deleted.
    """
    conn_cfg = get_connection_by_id(int(connection_id))

    if not conn_cfg:
        raise ValueError("Подключение не найдено")

    rc, output = run_manager_command(conn_cfg, "delete-backup", timestamp,
                                     manager_path=manager_path, timeout=1800)

    if rc != 0:
        errors = extract_backup_errors(output)
        raise RuntimeError(
            "\n".join(errors) or "gpbackup_manager rc={}".format(rc)
        )

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE backups SET status = 'deleted'
            WHERE connection_id = ? AND backup_timestamp = ?
            """,
            (int(connection_id), str(timestamp)),
        )

    return output


# ------------------------------------------------------------------
# реестр копий
# ------------------------------------------------------------------

def insert_backup(connection_id, job_id, backup_timestamp, dbname,
                  backup_type, backup_dir, status, tool="gpbackup",
                  skipped_tables=None, skipped_file=None):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO backups (
                connection_id, job_id, backup_timestamp,
                dbname, backup_type, backup_dir, status, tool,
                skipped_tables, skipped_file
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                connection_id, job_id, backup_timestamp,
                dbname, backup_type, backup_dir, status, tool,
                json.dumps(skipped_tables, ensure_ascii=False)
                if skipped_tables is not None else None,
                skipped_file,
            ),
        )
        return cur.lastrowid


def get_backup_record(backup_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM backups WHERE id = ?", (int(backup_id),))
        row = cur.fetchone()
        return dict(row) if row else None


def _parse_job_time(value):
    from datetime import datetime

    try:
        return datetime.strptime(str(value or "")[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def duration_seconds(started_at, finished_at):
    """Длительность задачи в секундах или None (не началась / идёт)."""
    start = _parse_job_time(started_at)
    end = _parse_job_time(finished_at)

    if start and end and end >= start:
        return int((end - start).total_seconds())

    return None


def _lines(raw):
    if isinstance(raw, (list, tuple)):
        items = raw
    else:
        items = str(raw or "").splitlines()

    return [str(x).strip() for x in items if str(x).strip()]


def backup_scope(config):
    """Что попало в бэкап по конфигу задачи: схемы, таблицы, файл."""
    config = config or {}

    return {
        "schemas": _lines(config.get("include_schemas")),
        "tables": _lines(config.get("include_tables")),
        "from_file": bool(config.get("include_table_file")),
    }


def list_connection_backups(connection_id, limit=200):
    """
    Копии gpbackup одного кластера для страницы бэкапов: запись реестра
    плюс длительность и состав из задачи, которая её сделала. Копии,
    найденные на диске мимо GPManager, задачи не имеют — там None.
    """
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT b.*, j.started_at AS job_started_at,
                   j.finished_at AS job_finished_at,
                   j.config_json AS job_config
            FROM backups b
            LEFT JOIN jobs j ON j.id = b.job_id
            WHERE b.connection_id = ?
              AND COALESCE(b.tool, 'gpbackup') = 'gpbackup'
            ORDER BY b.id DESC
            LIMIT ?
            """,
            (int(connection_id), int(limit)),
        )
        rows = [dict(row) for row in cur.fetchall()]

    for row in rows:
        try:
            config = json.loads(row.pop("job_config", None) or "null")
        except ValueError:
            config = None

        row["duration_sec"] = duration_seconds(
            row.pop("job_started_at", None), row.pop("job_finished_at", None))
        row["scope"] = backup_scope(config) if config is not None else None

    return rows


def backup_dir_usage(path):
    """
    Заполненность файловой системы каталога бэкапа на этом хосте
    (координаторе) -> {path, total, used, free} или None, если каталог
    не задан (каталоги сегментов) или отсюда не виден.
    """
    import shutil

    path = str(path or "").strip()

    if not path or not os.path.isdir(path):
        return None

    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return None

    return {"path": path, "total": usage.total, "used": usage.used,
            "free": usage.free}


def list_backups(limit=100, tool=None):
    sql = """
        SELECT b.*, COALESCE(b.tool, 'gpbackup') AS tool,
               c.name AS connection_name
        FROM backups b
        LEFT JOIN connections c ON c.id = b.connection_id
    """
    params = []

    if tool:
        sql += " WHERE COALESCE(b.tool, 'gpbackup') = ?"
        params.append(tool)

    sql += " ORDER BY b.id DESC LIMIT ?"
    params.append(int(limit))

    with sqlite_cursor() as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


# ------------------------------------------------------------------
# финализация и переподхват после рестарта
# ------------------------------------------------------------------

def _backup_recorded_for_job(job_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT 1 FROM backups WHERE job_id = ? LIMIT 1", (job_id,))
        return cur.fetchone() is not None


def finalize_backup_job(job_id, job_type, config, items, log_text, rc,
                        command_text):
    """
    Единая финализация gpbackup/gprestore: и из раннера, и при
    переподхвате после рестарта (rc=None — код возврата неизвестен,
    судим по логу).
    """
    outcome = backup_outcome(log_text)
    ok = rc in (0, None) and outcome == "done"

    # Partial: код 3 (или неизвестный код после рестарта) + итоговая строка.
    # Код 2 при статусе Partial — фатальная ошибка или сигнал: это сбой.
    partial = None

    if job_type == "gpbackup" and not ok and rc in (PARTIAL_EXIT_CODE, None):
        partial = parse_partial_backup(log_text)

        if partial is None and rc == PARTIAL_EXIT_CODE:
            partial = {"count": 0, "tables": [], "file": None}

    if job_type == "gpbackup" and not _backup_recorded_for_job(job_id):
        insert_backup(
            connection_id=int(config.get("connection_id") or 0),
            job_id=job_id,
            backup_timestamp=parse_backup_timestamp(log_text),
            dbname=config.get("dbname"),
            backup_type=config.get("backup_type") or "full",
            backup_dir=config.get("backup_dir") or "",
            status="done" if ok else ("partial" if partial else "failed"),
            skipped_tables=partial["tables"] if partial else None,
            skipped_file=partial["file"] if partial else None,
        )

    if ok:
        for item in items:
            mark_item_done(item["id"])

        refresh_job_progress(job_id)
        mark_job_done(job_id)
        return

    if partial:
        # копия есть и восстановима, но не целиком — строку done не ставим
        names = partial["tables"]
        shown = ", ".join(names[:20]) + (
            " … и ещё {}".format(len(names) - 20) if len(names) > 20 else ""
        )
        message = (
            "Частичный бэкап: {} табл. не попали в копию{}. Копия "
            "восстановима, эти таблицы восстановятся пустыми. Добрать их — "
            "кнопка «Добрать пропущенные» на странице бэкапов."
        ).format(partial["count"] or len(names),
                 ": " + shown if shown else "")

        for item in items:
            mark_item_failed(item["id"], message[:2000])

        refresh_job_progress(job_id)
        mark_job_failed(job_id, "{}\n\nКоманда: {}\n\nХвост лога:\n{}".format(
            message, command_text, (log_text or "")[-5000:])[:12000])
        return

    errors = extract_backup_errors(log_text)
    report = "Команда: {}\n\n{}\n\nХвост лога:\n{}".format(
        command_text,
        "\n".join(errors) or "(строк с ошибками не найдено)",
        (log_text or "")[-5000:],
    )

    for item in items:
        mark_item_failed(
            item["id"],
            ("\n".join(errors) or "{} rc={}".format(job_type, rc))[:2000],
        )

    refresh_job_progress(job_id)
    mark_job_failed(job_id, report[:12000])


def _resume_backup_watch(job_id, job_type, log_path, pid, items, config):
    log_text = _watch_gpcopy_log(job_id, log_path, [], pid=pid)

    if is_stop_requested(job_id):
        mark_job_cancelled(job_id)
        refresh_job_progress(job_id)
        return

    finalize_backup_job(
        job_id, job_type, config, items, log_text, None,
        "{} (переподхвачен после рестарта GPManager)".format(job_type),
    )


def resume_unfinished_backup_jobs():
    """
    Переподхват gpbackup/gprestore после рестарта приложения — тот же
    паттерн, что resume_unfinished_gpcopy_jobs: бинарь живёт отдельно,
    его лог на диске. Возвращает job_id, которые не надо помечать
    interrupted.
    """
    import threading

    from job_manager import list_unfinished_jobs

    try:
        from modules.gpcopy import pid_alive
    except ImportError:
        from gpcopy import pid_alive

    handled = []

    try:
        unfinished = list_unfinished_jobs()
    except Exception:
        return handled

    for job in unfinished:
        job_type = job.get("job_type")

        if job_type not in ("gpbackup", "gprestore"):
            continue

        log_path = job.get("log_file")
        pid = job.get("pid")
        job_id = int(job["id"])

        if not log_path or not os.path.exists(log_path):
            continue

        try:
            items = get_job_items(job_id)
            config = json.loads(job.get("config_json") or "{}")
        except Exception:
            continue

        handled.append(job_id)

        if pid and pid_alive(pid):
            threading.Thread(
                target=_resume_backup_watch,
                args=(job_id, job_type, log_path, pid, items, config),
                daemon=True,
            ).start()
        else:
            try:
                with open(log_path, "r", encoding="utf-8",
                          errors="replace") as f:
                    log_text = f.read()
            except Exception:
                log_text = ""

            finalize_backup_job(
                job_id, job_type, config, items, log_text, None,
                "{} (завершился, пока GPManager был перезапущен)".format(
                    job_type
                ),
            )

    return handled


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
            finalize_backup_job(
                job_id, "gpbackup", config, items, log_text, rc, " ".join(cmd),
            )

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
            finalize_backup_job(
                job_id, "gprestore", config, items, log_text, rc, " ".join(cmd),
            )

        _run_external_tool(job_id, cmd, env, finalize)

    except Exception as e:
        for item in items:
            try:
                mark_item_failed(item["id"], str(e)[:500])
            except Exception:
                pass

        refresh_job_progress(job_id)
        mark_job_failed(job_id, str(e)[:4000])
