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
import threading

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

# --compression-type (gpbackup 1.23+); gzip — по умолчанию, флаг не нужен
COMPRESSION_TYPES = ("gzip", "zstd")

# автодобор пропущенных после частичного бэкапа
AUTO_TOPUP_DELAY = 120
AUTO_TOPUP_ATTEMPTS = 2

# настройки основной задачи, которые добор берёт себе
_TOPUP_INHERIT = ("jobs", "compression_level", "compression_type",
                  "gpbackup_path", "auto_topup")

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


def connection_dbname(config):
    """БД задачи: из конфига, иначе — база подключения кластера."""
    dbname = (config.get("dbname") or "").strip()

    if dbname or not config.get("connection_id"):
        return dbname

    try:
        conn_cfg = get_connection_by_id(int(config["connection_id"])) or {}
    except Exception:
        return ""

    return (conn_cfg.get("database_name") or conn_cfg.get("database") or "").strip()


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

    compression_type = str(config.get("compression_type") or "").strip().lower()

    if compression_type:
        if compression_type not in COMPRESSION_TYPES:
            raise ValueError("Тип сжатия: gzip или zstd")
        if compression_type != "gzip":
            cmd += ["--compression-type", compression_type]

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
        # основная копия: добор хранится внутри неё, а не отдельной копией
        "parent_backup_id": backup_row.get("parent_id") or backup_row.get("id"),
    }

    # потоки, сжатие, автодобор — как у задачи, сделавшей копию
    try:
        source = json.loads(backup_row.get("job_config") or "{}") or {}
    except ValueError:
        source = {}

    for key in _TOPUP_INHERIT:
        if source.get(key) not in (None, ""):
            config[key] = source[key]

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

    schemas = _clean_names(config.get("include_schemas"))

    for schema in schemas:
        if "." in schema:
            raise ValueError("Схема без точки: {}".format(schema))
        cmd += ["--include-schema", schema]

    tables = _clean_names(config.get("include_tables"))

    for table in tables:
        if "." not in table:
            raise ValueError("Таблица должна быть schema.table: {}".format(table))
        cmd += ["--include-table", table]

    # заменить данные выбранных таблиц: TRUNCATE + загрузка без DDL
    if config.get("truncate_table"):
        if not tables:
            raise ValueError("TRUNCATE возможен только для списка таблиц")
        if not config.get("data_only"):
            raise ValueError("TRUNCATE требует режима «только данные»")
        cmd.append("--truncate-table")

    redirect_schema = (config.get("redirect_schema") or "").strip()

    if redirect_schema:
        if not _IDENT_RE.match(redirect_schema) or "." in redirect_schema:
            raise ValueError("Некорректное имя схемы: {}".format(redirect_schema))
        if not (schemas or tables):
            raise ValueError("Восстановление в другую схему — только для "
                             "выбранных схем или таблиц")
        cmd += ["--redirect-schema", redirect_schema]

    jobs = int(config.get("jobs") or 1)

    if jobs > 1:
        cmd += ["--jobs", str(jobs)]

    if config.get("on_error_continue", True):
        cmd.append("--on-error-continue")

    extra = (config.get("extra_args") or "").strip()

    # без --verbose gprestore пишет лишь «Encountered N errors…», а какой
    # оператор упал и почему — только в verbose-строках
    if config.get("verbose", True) and "--verbose" not in extra.split():
        cmd.append("--verbose")

    if extra:
        cmd += extra.split()

    return cmd


RESTORE_SCOPES = ("full", "schemas", "tables")
RESTORE_CONFLICTS = ("error", "truncate", "redirect")


def build_restore_config(data):
    """
    Запрос «Восстановить» -> конфиг задачи gprestore. Чистая функция.

    restore_scope: full — вся БД (как раньше), schemas — выбранные схемы,
    tables — выбранные таблицы. Для схем и таблиц on_conflict решает, что
    делать, если объект уже есть в целевой БД:
      error    — остановиться на первой ошибке (без --on-error-continue,
                 иначе gprestore пропустит CREATE и допишет данные: дубли);
      truncate — заменить данные: --data-only --truncate-table (таблицы);
      redirect — восстановить в другую существующую схему.
    Без restore_scope — прежнее поведение (совместимость API).
    """
    data = data or {}

    config = {
        "connection_id": int(data.get("connection_id")),
        "backup_timestamp": str(data.get("backup_timestamp") or ""),
        "backup_dir": (data.get("backup_dir") or "").strip(),
        "redirect_db": (data.get("redirect_db") or "").strip(),
        "create_db": bool(data.get("create_db")),
        "data_only": bool(data.get("data_only")),
        "include_tables": data.get("include_tables") or "",
        "jobs": data.get("jobs") or 1,
        "gprestore_path": (data.get("gprestore_path") or "").strip(),
    }

    scope = data.get("restore_scope")

    if not scope:
        return config

    if scope not in RESTORE_SCOPES:
        raise ValueError("Неизвестная область восстановления: {}".format(scope))

    config["restore_scope"] = scope

    if scope == "full":
        config["include_tables"] = ""
        return config

    if scope == "schemas":
        config["include_tables"] = ""
        config["include_schemas"] = data.get("include_schemas") or ""

        if not _clean_names(config["include_schemas"]):
            raise ValueError("Выберите хотя бы одну схему")
    elif not _clean_names(config["include_tables"]):
        raise ValueError("Выберите хотя бы одну таблицу")

    conflict = data.get("on_conflict") or "error"

    if conflict not in RESTORE_CONFLICTS:
        raise ValueError("Неизвестный режим: {}".format(conflict))

    config["on_conflict"] = conflict
    config["on_error_continue"] = False

    if conflict == "truncate":
        if scope != "tables":
            raise ValueError("Замена данных (TRUNCATE) — только для таблиц: "
                             "выберите таблицы схемы списком")
        if config["create_db"]:
            raise ValueError("TRUNCATE заменяет данные в существующих "
                             "таблицах — «создать БД» с ним не сочетается")
        config["data_only"] = True
        config["truncate_table"] = True
    elif conflict == "redirect":
        config["redirect_schema"] = (data.get("redirect_schema") or "").strip()

        if not config["redirect_schema"]:
            raise ValueError("Укажите схему, в которую восстановить")

    return config


def restore_item_label(config):
    """Подпись строки задачи gprestore: что восстанавливается."""
    label = "gprestore {}".format(config.get("backup_timestamp"))
    scope = config.get("restore_scope")

    if scope == "schemas":
        names = _clean_names(config.get("include_schemas"))
        label += " · схемы: " + ", ".join(names[:5]) + (
            " +{}".format(len(names) - 5) if len(names) > 5 else "")
    elif scope == "tables":
        names = _clean_names(config.get("include_tables"))
        label += " · таблиц: {}".format(len(names))

    if config.get("truncate_table"):
        label += " (TRUNCATE)"

    if config.get("redirect_schema"):
        label += " → схема {}".format(config["redirect_schema"])

    if config.get("topup_of"):
        label += " · добор к {}".format(config["topup_of"])

    return label[:250]


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


_LOG_PREFIX_RE = re.compile(
    r"^\d{8}:\d{2}:\d{2}:\d{2}\s+\S+:\S+:\S+:\S*-\[(?:CRITICAL|ERROR|WARNING)\]:-")


# Ссылка на лог gpAdminLogs в строке ERROR/CRITICAL: gprestore пишет
# «… see log file <путь> for a list of failed statements», gpbackup —
# «… See <путь> for a complete list of errors». Подробности (упавшие
# операторы, ошибки по сегментам) есть только в этом логе.
_ADMIN_LOG_REF_RE = re.compile(
    r":(\d+)-\[(?:ERROR|CRITICAL)\]:-.*?"
    r"(/[A-Za-z0-9_./-]*/gpAdminLogs/gp(?:restore|backup)_\d{8}\.log)")
_LOG_ENTRY_RE = re.compile(
    r"^\d{8}:\d{2}:\d{2}:\d{2}\s+gp\w+:[^:\s]*:[^:\s]*:(\d+)-\[(\w+)\]:-(.*)$")
_STMT_ERROR_RE = re.compile(
    r"Error encountered when executing statement:\s*(.*?)\s*Error was:\s*(.*)$",
    re.S)

# сколько хвоста дневного лога читать
LOG_TAIL_BYTES = 32 * 1024 * 1024

_admin_cache = {}


def _log_entries(raw, pid=None):
    """Записи лога gp* (многострочные) -> [(уровень, текст)]."""
    entries = []
    current = None

    for line in (raw or "").splitlines():
        m = _LOG_ENTRY_RE.match(line)

        if m:
            if pid is None or m.group(1) == pid:
                current = [m.group(2).upper(), [m.group(3)]]
                entries.append(current)
            else:
                current = None
        elif current is not None:
            current[1].append(line)

    return [(level, "\n".join(lines)) for level, lines in entries]


def _statements_from_log(raw, pid=None):
    """Записи «Error encountered when executing statement»."""
    found = []

    for _level, body in _log_entries(raw, pid):
        m = _STMT_ERROR_RE.search(body)

        if not m:
            continue

        item = {"statement": m.group(1).strip()[:2000],
                "error": m.group(2).strip()[:1000]}

        if item not in found:
            found.append(item)

    return found


def _statements_from_block(text):
    """Блок «Не выполнено (N):», который finalize сохраняет в ошибке задачи."""
    found = []
    inside = False

    for line in (text or "").splitlines():
        if re.match(r"^Не выполнено \(\d+\):$", line):
            inside = True
            continue

        if not inside:
            continue

        if line.startswith("- "):
            found.append({"error": line[2:].strip(), "statement": ""})
        elif line.startswith("  ") and found:
            found[-1]["statement"] = line.strip()
        else:
            break

    return found


def admin_log_ref(text):
    """(pid, путь) последней ссылки на лог gpAdminLogs или None."""
    match = None

    for match in _ADMIN_LOG_REF_RE.finditer(text or ""):
        pass

    return (match.group(1), match.group(2)) if match else None


def _read_admin_log(text, fs=None):
    """(pid, хвост лога gpAdminLogs) — здесь или на координаторе."""
    ref = admin_log_ref(text)

    if not ref:
        return None, None

    pid, path = ref

    if fs is not None and getattr(fs, "remote", False):
        return pid, fs.read(path, tail=LOG_TAIL_BYTES)

    try:
        st = os.stat(path)
    except OSError:
        return pid, None

    key = (path, st.st_mtime, st.st_size)

    if key not in _admin_cache:
        try:
            with open(path, "rb") as f:
                if st.st_size > LOG_TAIL_BYTES:
                    f.seek(st.st_size - LOG_TAIL_BYTES)
                raw = f.read().decode("utf-8", errors="replace")
        except OSError:
            return pid, None

        if len(_admin_cache) > 20:
            _admin_cache.clear()

        _admin_cache[key] = raw

    return pid, _admin_cache[key]


def failed_statements(text, limit=50, fs=None):
    """
    Какие операторы не выполнились -> [{statement, error}].

    Сначала сам текст (лог задачи: gprestore запускается с --verbose) и
    сохранённый блок «Не выполнено». Иначе — лог gpAdminLogs по ссылке
    из строки ошибки (verbose-строки там есть, если запуск был с --verbose).
    """
    own = _statements_from_log(text) or _statements_from_block(text)

    if own:
        return own[:limit]

    pid, raw = _read_admin_log(text, fs)

    return _statements_from_log(raw, pid)[:limit] if raw else []


def _admin_errors_from_block(text):
    """Блок «Из лога gpAdminLogs (N):», сохранённый в ошибке задачи."""
    found = []
    inside = False

    for line in (text or "").splitlines():
        if re.match(r"^Из лога gpAdminLogs \(\d+\):$", line):
            inside = True
            continue

        if not inside:
            continue

        if line.startswith("- "):
            found.append(line[2:].strip())
        else:
            break

    return found


def admin_log_errors(text, limit=20, fs=None):
    """
    Строки ERROR/CRITICAL этого запуска из лога gpAdminLogs — например,
    ошибки по каждому сегменту за «Unable to create backup directories on
    N segments». Итоговая строка со ссылкой на лог не повторяется.
    """
    saved = _admin_errors_from_block(text)

    if saved:
        return saved[:limit]

    pid, raw = _read_admin_log(text, fs)

    if not raw:
        return []

    found = []

    for level, body in _log_entries(raw, pid):
        if level not in ("ERROR", "CRITICAL"):
            continue

        line = " ".join(body.split())

        if "gpAdminLogs/" in line or _STMT_ERROR_RE.search(body):
            continue

        if line and line not in found:
            found.append(line[:500])

    return found[:limit]


def error_hint(text):
    """Что делать с типичной ошибкой gprestore — одной фразой."""
    t = (text or "").lower()

    # отказы самого ssh при запуске на координаторе
    if "permission denied (publickey" in t or "permission denied, please try again" in t:
        return ("SSH не пустил на координатор: нет ключа. На сервере Opsentri от "
                "пользователя сервиса: ssh-keygen и ssh-copy-id gpadmin@<координатор>, "
                "затем «Проверить связь» в блоке «Где запускать gpbackup».")
    if "host key verification failed" in t or "remote host identification has changed" in t:
        return ("Ключ хоста координатора изменился или неизвестен — проверьте "
                "~/.ssh/known_hosts пользователя сервиса Opsentri.")
    if "could not resolve hostname" in t or "name or service not known" in t:
        return "Имя координатора не резолвится — укажите IP в «Где запускать gpbackup»."
    if ("connection refused" in t or "connection timed out" in t) and "ssh" in t:
        return "Координатор не отвечает по SSH (порт, файрвол)."
    if "greenplum_path.sh" in t or ("gpbackup" in t and "no such file" in t):
        return "На координаторе не найден gpbackup — проверьте GPHOME в «Где запускать gpbackup»."

    if "unable to create backup directories" in t or \
            "could not create directory" in t:
        return ("gpbackup не смог создать каталог копии на сегментах. Он должен "
                "запускаться на координаторе этого кластера (блок «Где "
                "запускать gpbackup»), каталог — существовать и быть доступным "
                "на запись gpadmin на всех хостах сегментов.")
    if "already exists" in t:
        return ("Объект уже есть в целевой БД. Всю копию восстанавливайте в "
                "новую базу (Redirect-db + «создать БД»), таблицы — в режиме "
                "«Заменить данные» или «В другую схему». С --on-error-continue "
                "данные в существующую таблицу дописываются — проверьте дубли.")
    if "role" in t and "does not exist" in t:
        return "В целевом кластере нет роли из копии — создайте её и повторите."
    if "permission denied" in t or "must be owner" in t:
        return "Не хватает прав у пользователя подключения."
    if "no space left" in t:
        return "Закончилось место на диске."
    if "does not exist" in t and "database" in t:
        return "Целевой базы нет — отметьте «создать БД»."
    return None


def error_reason(text):
    """
    Короткая причина сбоя для истории операций: первая строка
    CRITICAL/ERROR лога без префикса времени и хоста, иначе первая
    содержательная строка текста ошибки задачи.
    """
    errors = extract_backup_errors(text, limit=50)

    for line in errors:
        if "[CRITICAL]" in line.upper():
            return _LOG_PREFIX_RE.sub("", line).strip()[:500]

    if errors:
        return _LOG_PREFIX_RE.sub("", errors[0]).strip()[:500]

    # строк ERROR нет — например, отказ самого ssh («Permission denied
    # (publickey)»): причина — последняя содержательная строка хвоста лога
    text = text or ""
    marker = text.rfind("Хвост лога:")
    tail = text[marker + len("Хвост лога:"):] if marker >= 0 else text
    skip = ("Команда:", "(строк с ошибками не найдено)", "Хвост лога:")

    for line in reversed(tail.splitlines()):
        line = line.strip()

        if line and not line.startswith(skip) and \
                not line.startswith("OPSENTRI_REMOTE_PID="):
            return _LOG_PREFIX_RE.sub("", line).strip()[:500]

    for line in text.splitlines():
        line = line.strip()

        if line and not line.startswith(skip):
            return line[:500]

    return None


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
    stdin_data = None

    # gpbackup_manager — тоже на координаторе кластера
    try:
        from modules.gp_remote import resolve_target, wrap_command

        target = resolve_target(conn_cfg.get("id"))
    except Exception:
        target = None

    if target:
        cmd, stdin_data = wrap_command(target, cmd, env)
        env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}

    proc = subprocess.run(
        cmd,
        env=env,
        input=stdin_data,
        stdin=None if stdin_data is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        timeout=timeout,
    )

    return proc.returncode, proc.stdout or ""


def _manager_disk_backups(conn_cfg, manager_path=None):
    """Запасной путь: `gpbackup_manager list-backups`."""
    rc, output = run_manager_command(conn_cfg, "list-backups",
                                     manager_path=manager_path, timeout=120)

    if rc != 0:
        errors = extract_backup_errors(output)
        raise RuntimeError(
            "\n".join(errors) or "gpbackup_manager rc={}".format(rc)
        )

    return [dict(e, status="done", backup_dir="", skipped_file=None)
            for e in parse_manager_backups(output)]


def sync_disk_backups(connection_id, manager_path=None, backup_dirs=()):
    """
    Синхронизация реестра с диском: копии, которых нет в нашем реестре
    (в т.ч. сделанные мимо GPManager), добавляются. -> {found, added, source}.

    Основной источник — файлы gpbackup на координаторе (история и
    каталоги копий, modules.gpbackup_files); gpbackup_manager — только
    если файлов отсюда не видно.
    """
    from modules.gpbackup_files import list_disk_backups

    conn_cfg = get_connection_by_id(int(connection_id))

    if not conn_cfg:
        raise ValueError("Подключение не найдено")

    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT backup_timestamp, backup_dir FROM backups "
            "WHERE connection_id = ?",
            (int(connection_id),),
        )
        rows = cur.fetchall()

    known = {row["backup_timestamp"] for row in rows}
    dirs = [d for d in backup_dirs if d] + \
        [row["backup_dir"] for row in rows if row["backup_dir"]]

    listing = list_disk_backups(int(connection_id), dirs)
    disk = listing["backups"]
    source = "files"

    if not listing["roots"] and not listing["history"]:
        try:
            disk = _manager_disk_backups(conn_cfg, manager_path)
            source = "gpbackup_manager"
        except Exception as e:
            raise RuntimeError(
                "Каталоги копий не видны с этого хоста ({}), а "
                "gpbackup_manager не сработал: {}".format(
                    listing["datadir"] or "каталог координатора неизвестен",
                    str(e)[:500])
            )

    dbname = conn_cfg.get("database_name") or conn_cfg.get("database")
    added = 0

    for entry in disk:
        if entry["timestamp"] in known:
            continue

        # копии других баз этого кластера — не про это подключение
        if dbname and entry.get("dbname") and entry["dbname"] != dbname:
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
            backup_dir=entry.get("backup_dir") or "",
            status=entry.get("status") or "done",
            skipped_file=entry.get("skipped_file"),
        )
        known.add(entry["timestamp"])
        added += 1

    return {"found": len(disk), "added": added, "source": source}


def get_backup_report(connection_id, timestamp, manager_path=None,
                      backup_dir=""):
    """
    Отчёт по копии -> текст. Сначала файл gpbackup_<ts>_report на
    координаторе, `gpbackup_manager display-report` — запасной путь.
    """
    from modules.gpbackup_files import read_report_text

    text = read_report_text({
        "connection_id": int(connection_id),
        "backup_timestamp": str(timestamp),
        "backup_dir": backup_dir or _registry_backup_dir(connection_id, timestamp),
    })

    if text:
        return text

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
                  skipped_tables=None, skipped_file=None, parent_id=None):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO backups (
                connection_id, job_id, backup_timestamp,
                dbname, backup_type, backup_dir, status, tool,
                skipped_tables, skipped_file, parent_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                connection_id, job_id, backup_timestamp,
                dbname, backup_type, backup_dir, status, tool,
                json.dumps(skipped_tables, ensure_ascii=False)
                if skipped_tables is not None else None,
                skipped_file,
                int(parent_id) if parent_id else None,
            ),
        )
        return cur.lastrowid


def get_backup_record(backup_id):
    """Запись реестра плюс config_json задачи, сделавшей копию (job_config)."""
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT b.*, j.config_json AS job_config
            FROM backups b
            LEFT JOIN jobs j ON j.id = b.job_id
            WHERE b.id = ?
            """,
            (int(backup_id),),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def _registry_backup_dir(connection_id, timestamp):
    """Каталог копии по реестру (пусто — каталоги данных по умолчанию)."""
    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT backup_dir FROM backups WHERE connection_id = ? "
            "AND backup_timestamp = ? AND COALESCE(backup_dir, '') <> '' LIMIT 1",
            (int(connection_id), str(timestamp)),
        )
        row = cur.fetchone()
        return row["backup_dir"] if row else ""


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


def backup_catalog(tables, child_parent):
    """
    Список для выбора в форме бэкапа: схемы с числом таблиц и таблицы
    без leaf-секций (секционированная — одной строкой, parts — сколько
    секций). ok=False — имя, которое фильтр gpbackup здесь не примет.
    """
    parts = {}

    for child in child_parent:
        top, seen = child_parent[child], set()

        while top in child_parent and top not in seen:
            seen.add(top)
            top = child_parent[top]

        parts[top] = parts.get(top, 0) + 1

    items, counts = [], {}

    for schema, table in tables:
        if (schema, table) in child_parent:
            continue

        name = "{}.{}".format(schema, table)
        counts[schema] = counts.get(schema, 0) + 1
        items.append({"name": name, "parts": parts.get((schema, table), 0),
                      "ok": bool(_IDENT_RE.match(name)) and name.count(".") == 1})

    return {
        "schemas": [{"name": s, "tables": counts[s],
                     "ok": bool(_IDENT_RE.match(s)) and "." not in s}
                    for s in sorted(counts)],
        "tables": items,
    }


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

        # добор, сделанный до появления parent_id, узнаётся по задаче
        row["_skipped_from"] = (config or {}).get("skipped_from")

        # сводка состава (таблиц/схем), если её уже читали с координатора
        try:
            row["details"] = json.loads(row.pop("details_json", None) or "null")
        except ValueError:
            row["details"] = None

        row["duration_sec"] = duration_seconds(
            row.pop("job_started_at", None), row.pop("job_finished_at", None))
        row["scope"] = backup_scope(config) if config is not None else None

    return link_topups(rows)


# ------------------------------------------------------------------
# добор пропущенных таблиц: дочерние копии основной
# ------------------------------------------------------------------

# leaf-партиция Greenplum: <корень>_1_prt_<имя>
_LEAF_RE = re.compile(r"^(.+?)_\d+_prt_")


def table_root(name):
    """schema.t_1_prt_467 -> schema.t (корень секционированной таблицы)."""
    schema, dot, table = str(name).partition(".")
    match = _LEAF_RE.match(table)

    return "{}.{}".format(schema, match.group(1)) if dot and match else str(name)


def _skipped_list(row):
    try:
        names = json.loads(row.get("skipped_tables") or "[]")
    except ValueError:
        names = []

    return [n for n in names if isinstance(n, str) and n]


def link_topups(rows):
    """
    Доборы (бэкап пропущенных таблиц частичной копии) — внутрь основной.

    Возвращает только основные копии. У каждой — topups (доборы по
    времени: метка, статус, covered — какие таблицы добор закрыл, None
    если имена неизвестны); у частичной ещё missing — чего нет ни в одном
    доборе (None — неизвестно) и completed — всё добрано.
    """
    by_id = {r["id"]: r for r in rows}
    by_ts = {}

    for r in sorted(rows, key=lambda r: r["id"]):
        if r.get("backup_timestamp") and r.get("status") != "deleted":
            by_ts.setdefault(str(r["backup_timestamp"]), r)

    def parent_of(r):
        if r.get("parent_id"):
            return by_id.get(r["parent_id"])
        if r.get("_skipped_from"):
            p = by_ts.get(str(r["_skipped_from"]))
            return p if p is not r else None
        return None

    top, children = [], {}

    for r in rows:
        parent, seen = parent_of(r), {r["id"]}

        # добор добора сводится к основной копии
        while parent is not None and parent["id"] not in seen:
            seen.add(parent["id"])
            up = parent_of(parent)
            if up is None or up["id"] in seen:
                break
            parent = up

        if parent is not None and parent["id"] != r["id"]:
            children.setdefault(parent["id"], []).append(r)
        else:
            top.append(r)

    for r in rows:
        r.pop("_skipped_from", None)

    for r in top:
        kids = sorted(children.get(r["id"], []),
                      key=lambda k: (str(k.get("backup_timestamp") or ""), k["id"]))
        partial = r.get("status") == "partial"
        missing = _skipped_list(r) if partial else []
        known = bool(missing) or not partial
        topups = []

        for k in kids:
            own = _skipped_list(k)
            usable = k.get("status") in ("done", "partial") and k.get("backup_timestamp")
            covered = None

            if usable:
                if known:
                    covered = [t for t in missing if t not in set(own)]
                if k["status"] == "done":
                    missing, known = [], True
                else:
                    missing, known = own, bool(own)

            topups.append({
                "id": k["id"],
                "job_id": k.get("job_id"),
                "backup_timestamp": k.get("backup_timestamp"),
                "backup_dir": k.get("backup_dir") or "",
                "status": k.get("status"),
                "duration_sec": k.get("duration_sec"),
                "skipped": len(own),
                "covered": covered,
            })

        r["topups"] = topups

        if partial:
            r["missing"] = missing if known else None
            r["completed"] = bool(topups) and known and not missing

    return top


# записи реестра, у которых на диске ничего нет: убрать можно без gpbackup_manager
FORGETTABLE = ("failed", "deleted")


def failed_backup_ids(connection_id):
    """Сбойные/удалённые записи gpbackup кластера."""
    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT id FROM backups WHERE connection_id = ? AND status IN ({}) "
            "AND COALESCE(tool, 'gpbackup') = 'gpbackup'".format(
                ",".join("?" for _ in FORGETTABLE)),
            [int(connection_id)] + list(FORGETTABLE))
        return [r["id"] for r in cur.fetchall()]


def forget_backups(ids):
    """
    Убрать записи реестра (не файлы!): только сбойные/удалённые или без
    метки копии. Возвращает число удалённых строк. ValueError — среди них
    живая копия: её удаляют с диска.
    """
    ids = [int(i) for i in ids]

    if not ids:
        return 0

    marks = ",".join("?" for _ in ids)

    with sqlite_cursor(commit=True) as cur:
        cur.execute("SELECT id, status, backup_timestamp FROM backups "
                    "WHERE id IN ({})".format(marks), ids)
        rows = [dict(r) for r in cur.fetchall()]

        alive = [r for r in rows if r["status"] not in FORGETTABLE
                 and (r.get("backup_timestamp") or "").strip()]

        if alive:
            raise ValueError("Копия {} есть на диске — удалите её через «Удалить с диска»"
                             .format(alive[0]["backup_timestamp"]))

        cur.execute("DELETE FROM backups WHERE id IN ({})".format(marks), ids)
        return cur.rowcount


def backup_with_topups(backup_id):
    """Основная копия реестра вместе с доборами (None — нет или это добор)."""
    record = get_backup_record(int(backup_id))

    if not record:
        return None

    for row in list_connection_backups(int(record.get("connection_id") or 0), limit=1000):
        if row["id"] == int(backup_id):
            return row

    return None


def restore_followups(record, config):
    """
    Шаги gprestore после основного: данные из доборов копии, только по
    таблицам из области восстановления. Таблицы к этому моменту уже
    созданы основным шагом — в частичной копии их DDL есть, поэтому
    добор грузится --data-only. Пустой список — догружать нечего.
    """
    if not record or not record.get("topups"):
        return []

    scope = config.get("restore_scope") or (
        "tables" if _clean_names(config.get("include_tables")) else "full")
    steps = []

    for t in record["topups"]:
        if t.get("status") not in ("done", "partial") or not t.get("backup_timestamp"):
            continue

        step = {
            "connection_id": int(config["connection_id"]),
            "backup_timestamp": str(t["backup_timestamp"]),
            "backup_dir": t.get("backup_dir") or "",
            "redirect_db": config.get("redirect_db") or "",
            "data_only": True,
            "jobs": config.get("jobs") or 1,
            "gprestore_path": config.get("gprestore_path") or "",
            "on_error_continue": config.get("on_error_continue", True),
            "topup_of": str(record.get("backup_timestamp") or ""),
        }
        covered = t.get("covered")

        if scope != "full":
            # без списка таблиц добора фильтр по схемам/таблицам не построить
            if not covered:
                continue

            if scope == "schemas":
                chosen = set(_clean_names(config.get("include_schemas")))
                hit = sorted({n.split(".", 1)[0] for n in covered} & chosen)
                if not hit:
                    continue
                step["restore_scope"] = "schemas"
                step["include_schemas"] = "\n".join(hit)
            else:
                chosen = set(_clean_names(config.get("include_tables")))
                hit = [n for n in covered if n in chosen or table_root(n) in chosen]
                if not hit:
                    continue
                step["restore_scope"] = "tables"
                step["include_tables"] = "\n".join(hit)
                if config.get("truncate_table"):
                    step["truncate_table"] = True

            if config.get("redirect_schema"):
                step["redirect_schema"] = config["redirect_schema"]

        build_gprestore_command(step)
        steps.append(step)

    return steps


def _active_topup_job(parent_id):
    """Идущая задача-добор этой копии (двойной клик, автодобор + ручной)."""
    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT id, config_json FROM jobs WHERE job_type = 'gpbackup' "
            "AND status IN ('queued', 'pending', 'running', 'stopping')"
        )
        rows = [dict(r) for r in cur.fetchall()]

    for row in rows:
        try:
            cfg = json.loads(row.get("config_json") or "{}")
        except ValueError:
            continue
        if str(cfg.get("parent_backup_id") or "") == str(parent_id):
            return row["id"]

    return None


def prepare_topup_job(backup_id, attempt=0):
    """
    Задача-добор пропущенных таблиц копии (создана, не запущена) -> job_id.
    Добирает от основной копии или от последнего частичного добора.
    ValueError — добирать нечего, уже идёт или нельзя.
    """
    from job_manager import create_job, create_job_items

    record = get_backup_record(int(backup_id))

    if not record:
        raise LookupError("Копия не найдена")

    family = backup_with_topups(int(backup_id))

    if family and family.get("completed"):
        raise ValueError("Пропущенные таблицы уже добраны")

    parent_id = family["id"] if family else record["id"]
    running = _active_topup_job(parent_id)

    if running:
        raise ValueError("Добор этой копии уже идёт — задача #{}".format(running))

    # после частичного добора добираем то, что не попало уже в него
    usable = [t for t in (family or {}).get("topups") or []
              if t["status"] in ("done", "partial")]

    if usable:
        record = get_backup_record(usable[-1]["id"]) or record

    config = skipped_backup_config(record)
    config["parent_backup_id"] = parent_id
    config["topup_attempt"] = int(attempt or 0)
    # в реестре копии БД может не быть (копия с диска, подхват после
    # рестарта) — тогда база подключения кластера
    config["dbname"] = connection_dbname(config)
    build_gpbackup_command(config)

    job_id = create_job(job_type="gpbackup",
                        connection_id=config["connection_id"], config=config)
    create_job_items(job_id=job_id, items=[{
        "schema_name": config["dbname"],
        "table_name": "gpbackup (пропущенные из {}){}".format(
            record.get("backup_timestamp"),
            " · автодобор, попытка {}".format(attempt) if attempt else ""),
        "action": "GPBACKUP",
    }])

    return job_id


def schedule_auto_topup(config, backup_id):
    """
    Частичный бэкап с автодобором -> через AUTO_TOPUP_DELAY сек добор
    пропущенных, не больше AUTO_TOPUP_ATTEMPTS попыток подряд. Номер
    попытки или None, если автодобор не положен.
    """
    config = config or {}

    if not backup_id or not config.get("auto_topup") or not config.get("on_error_continue"):
        return None

    attempt = int(config.get("topup_attempt") or 0) + 1

    if attempt > AUTO_TOPUP_ATTEMPTS:
        return None

    _spawn(_auto_topup, backup_id, attempt)
    return attempt


def _auto_topup(backup_id, attempt):
    import time

    time.sleep(AUTO_TOPUP_DELAY)

    try:
        job_id = prepare_topup_job(backup_id, attempt=attempt)
    except Exception:
        # уже добрано, добор идёт вручную или копия пропала — молча
        return

    run_gpbackup_job(job_id)


def _spawn(target, *args):
    threading.Thread(target=target, args=args, daemon=True).start()


def start_next_restore(config):
    """Основной gprestore прошёл — запустить следующий шаг (добор)."""
    steps = list((config or {}).get("followups") or [])

    if not steps:
        return None

    from job_manager import create_job, create_job_items

    step = dict(steps[0])
    step["followups"] = steps[1:]

    job_id = create_job(job_type="gprestore",
                        connection_id=int(step["connection_id"]), config=step)
    create_job_items(job_id=job_id, items=[{
        "schema_name": step.get("redirect_db") or "",
        "table_name": restore_item_label(step),
        "action": "GPRESTORE",
    }])

    _spawn(run_gprestore_job, job_id)

    return job_id


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
    Финализация задачи и письмо о её статусе (если кластер подписан;
    отправка в фоне — сбой почты задачу не трогает).
    """
    _finalize_backup_job(job_id, job_type, config, items, log_text, rc,
                         command_text)

    try:
        from modules.backup_notify import notify_finished

        notify_finished(job_id, (config or {}).get("connection_id"))
    except Exception:
        pass


# ------------------------------------------------------------------
# ход выполнения: этап и счётчики из лога gpbackup / gprestore
# ------------------------------------------------------------------

# (фраза лога, этап по-русски, % общего хода)
_PHASES = {
    "gpbackup": [
        ("Gathering table state information", "Сбор списка таблиц", 2),
        ("Acquiring ACCESS SHARE locks", "Блокировки таблиц", 4),
        ("Gathering additional table metadata", "Чтение метаданных", 10),
        ("Writing global database metadata", "Запись глобальных метаданных", 12),
        ("Writing pre-data metadata", "Запись DDL (pre-data)", 14),
        ("Writing post-data metadata", "Запись DDL (post-data)", 18),
        ("Writing data to file", "Запись данных", 20),
        ("Data backup complete", "Данные записаны", 97),
        ("Beginning cleanup", "Завершение", 99),
    ],
    "gprestore": [
        ("Restoring pre-data metadata", "Создание объектов (pre-data)", 2),
        ("Pre-data metadata restore complete", "Объекты созданы", 15),
        ("Restoring data", "Загрузка данных", 15),
        ("Data restore complete", "Данные загружены", 90),
        ("Restoring post-data metadata", "Индексы и ограничения (post-data)", 90),
        ("Post-data metadata restore complete", "Post-data готово", 99),
        ("Beginning cleanup", "Завершение", 99),
    ],
}

# (метка счётчика, этап по-русски, % от, % до)
_COUNTERS = {
    "gpbackup": [
        ("Locks acquired", "Блокировки таблиц", 4, 10),
        ("Tables backed up", "Запись данных", 20, 97),
    ],
    "gprestore": [
        ("Pre-data objects restored", "Создание объектов (pre-data)", 2, 15),
        ("Tables restored", "Загрузка данных", 15, 90),
        ("Post-data objects restored", "Индексы и ограничения (post-data)", 90, 99),
    ],
}

_TABLE_OF_RE = re.compile(r"\(table (\d+) of (\d+)\)")


def parse_progress(text, job_type):
    """
    Хвост лога -> {phase, done, total, percent} по последнему событию:
    фразе этапа или счётчику «Tables backed up: N / M» (прогресс-бар
    пишет их через \\r — тоже учитываем).
    """
    text = (text or "").replace("\r", "\n")
    best = (-1, None)

    for phrase, label, pct in _PHASES.get(job_type, []):
        pos = text.rfind(phrase)

        if pos > best[0]:
            best = (pos, {"phase": label, "done": None, "total": None,
                          "percent": pct})

    def counter(pos, label, lo, hi, done, total):
        pct = lo + (hi - lo) * (min(done, total) / total if total else 0)
        return (pos, {"phase": label, "done": done, "total": total,
                      "percent": round(pct, 1)})

    counters = _COUNTERS.get(job_type, [])

    for marker, label, lo, hi in counters:
        last = None

        for last in re.finditer(re.escape(marker) + r":\s+(\d+)\s*/\s*(\d+)", text):
            pass

        if last and last.start() > best[0]:
            best = counter(last.start(), label, lo, hi,
                           int(last.group(1)), int(last.group(2)))

    # --verbose: «(table N of M)» на каждой таблице — точнее прогресс-бара
    data = next((c for c in counters if c[0] in ("Tables backed up",
                                                  "Tables restored")), None)
    last = None

    for last in _TABLE_OF_RE.finditer(text):
        pass

    if data and last and last.start() > best[0]:
        best = counter(last.start(), data[1], data[2], data[3],
                       int(last.group(1)), int(last.group(2)))

    return best[1] or {"phase": "Запуск", "done": None, "total": None,
                       "percent": 0}


def _log_tail(path, size=256 * 1024):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            f.seek(max(0, end - size))
            return f.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


def list_running_backup_jobs(connection_id):
    """Идущие gpbackup / gprestore кластера с этапом и процентом."""
    from datetime import datetime

    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT id, job_type, status, started_at, log_file, config_json
            FROM jobs
            WHERE job_type IN ('gpbackup', 'gprestore')
              AND status IN ('queued', 'running', 'stopping')
              AND connection_id = ?
            ORDER BY id
            """,
            (int(connection_id),),
        )
        rows = [dict(r) for r in cur.fetchall()]

    out = []

    for r in rows:
        progress = parse_progress(_log_tail(r["log_file"]) if r.get("log_file")
                                  else "", r["job_type"])
        start = _parse_job_time(r.get("started_at"))
        elapsed = int((datetime.now() - start).total_seconds()) if start else None

        try:
            cfg = json.loads(r.get("config_json") or "{}")
        except ValueError:
            cfg = {}

        eta = None
        pct = progress["percent"]

        if elapsed and pct >= 5 and pct < 100 and progress.get("total"):
            eta = int(elapsed * (100 - pct) / pct)

        out.append(dict(progress, **{
            "job_id": r["id"],
            "job_type": r["job_type"],
            "status": r["status"],
            "started_at": r.get("started_at"),
            "elapsed_sec": elapsed,
            "eta_sec": eta,
            "backup_type": cfg.get("backup_type"),
            "backup_timestamp": cfg.get("backup_timestamp"),
            # добор пропущенных / его догрузка при восстановлении
            "topup_of": cfg.get("skipped_from") or cfg.get("topup_of"),
            "scope": backup_scope(cfg) if r["job_type"] == "gpbackup" else None,
        }))

    return out


def _finalize_backup_job(job_id, job_type, config, items, log_text, rc,
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

    backup_id = None

    if job_type == "gpbackup" and not _backup_recorded_for_job(job_id):
        backup_id = insert_backup(
            connection_id=int(config.get("connection_id") or 0),
            job_id=job_id,
            backup_timestamp=parse_backup_timestamp(log_text),
            # после рестарта в конфиге задачи dbname нет — берём из подключения
            dbname=connection_dbname(config) or None,
            backup_type=config.get("backup_type") or "full",
            backup_dir=config.get("backup_dir") or "",
            status="done" if ok else ("partial" if partial else "failed"),
            skipped_tables=partial["tables"] if partial else None,
            skipped_file=partial["file"] if partial else None,
            parent_id=config.get("parent_backup_id"),
        )

    if ok:
        for item in items:
            mark_item_done(item["id"])

        refresh_job_progress(job_id)
        mark_job_done(job_id)

        if job_type == "gprestore":
            start_next_restore(config)
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
        schedule_auto_topup(config, backup_id)
        return

    errors = extract_backup_errors(log_text)

    # подробности — из лога gpAdminLogs на координаторе (здесь или по SSH);
    # лог ротируется, поэтому найденное сохраняется в задаче
    fs = None

    if admin_log_ref(log_text):
        try:
            from modules.gp_remote import fs_for

            fs = fs_for((config or {}).get("connection_id"))
        except Exception:
            fs = None

    try:
        stmts = failed_statements(log_text, limit=20, fs=fs)
    except Exception:
        stmts = []

    try:
        admin_errors = [] if stmts else admin_log_errors(log_text, limit=20, fs=fs)
    except Exception:
        admin_errors = []

    failed_block = ""

    if stmts:
        failed_block = "\n\nНе выполнено ({}):\n{}".format(len(stmts), "\n".join(
            "- {}\n  {}".format(s["error"], s["statement"].splitlines()[0][:300])
            for s in stmts))
        errors = [s["error"] for s in stmts] + errors
    elif admin_errors:
        failed_block = "\n\nИз лога gpAdminLogs ({}):\n{}".format(
            len(admin_errors), "\n".join("- " + e for e in admin_errors))
        errors = errors + admin_errors

    followups = (config or {}).get("followups") or []

    if job_type == "gprestore" and followups:
        failed_block += (
            "\n\nДобор не применён: данные из {} (пропущенные таблицы) не "
            "загружены — сначала исправьте ошибку и восстановите заново."
        ).format(", ".join(s["backup_timestamp"] for s in followups))

    report = "Команда: {}\n\n{}{}\n\nХвост лога:\n{}".format(
        command_text,
        "\n".join(extract_backup_errors(log_text)) or "(строк с ошибками не найдено)",
        failed_block,
        (log_text or "")[-5000:],
    )

    for item in items:
        mark_item_failed(
            item["id"],
            ("\n".join(errors) or "{} rc={}".format(job_type, rc))[:2000],
        )

    refresh_job_progress(job_id)
    mark_job_failed(job_id, report[:12000])


def _stop_remote_if_needed(config, log_text):
    """Стоп задачи, запущенной по SSH: гасим и сам gpbackup на координаторе."""
    try:
        from modules.gp_remote import resolve_target, stop_remote

        target = resolve_target((config or {}).get("connection_id"))

        if target:
            stop_remote(target, log_text)
    except Exception:
        pass


def _resume_backup_watch(job_id, job_type, log_path, pid, items, config):
    log_text = _watch_gpcopy_log(job_id, log_path, [], pid=pid)

    if is_stop_requested(job_id):
        _stop_remote_if_needed(config, log_text)
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

def _run_external_tool(job_id, cmd, env, finalize, stdin_data=None, target=None):
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
        "stdin": subprocess.PIPE if stdin_data is not None else subprocess.DEVNULL,
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

    # по SSH пароль БД идёт первой строкой stdin, не в командной строке
    if stdin_data is not None:
        try:
            process.stdin.write(stdin_data)
            process.stdin.close()
        except (OSError, ValueError):
            pass

    log_text = _watch_gpcopy_log(job_id, log_path, [], process=process)

    if is_stop_requested(job_id):
        if target:
            from modules.gp_remote import stop_remote

            stop_remote(target, log_text)
        mark_job_cancelled(job_id)
        refresh_job_progress(job_id)
        return

    finalize(log_text, process.returncode)


def _prepare_command(config, cmd, env):
    """
    Здесь или на координаторе кластера по SSH -> (cmd, env, stdin, target,
    подпись для отчёта). gpbackup работает только с координатора своего
    кластера: иначе «Unable to create backup directories on N segments».
    """
    from modules.gp_remote import describe, resolve_target, wrap_command

    target = resolve_target(config.get("connection_id"))
    shown = " ".join(cmd)

    if not target:
        return cmd, env, None, None, shown

    ssh_cmd, stdin_data = wrap_command(target, cmd, env)
    clean_env = {k: v for k, v in os.environ.items()
                 if not k.startswith("PG")}

    return ssh_cmd, clean_env, stdin_data, target, "{}  [{}]".format(
        shown, describe(target))


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

        cmd, env, stdin_data, target, shown = _prepare_command(
            config, build_gpbackup_command(config), build_pg_env(conn_cfg))

        def finalize(log_text, rc):
            finalize_backup_job(
                job_id, "gpbackup", config, items, log_text, rc, shown,
            )

        _run_external_tool(job_id, cmd, env, finalize, stdin_data, target)

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

        cmd, env, stdin_data, target, shown = _prepare_command(
            config, build_gprestore_command(config), build_pg_env(conn_cfg))

        def finalize(log_text, rc):
            finalize_backup_job(
                job_id, "gprestore", config, items, log_text, rc, shown,
            )

        _run_external_tool(job_id, cmd, env, finalize, stdin_data, target)

    except Exception as e:
        for item in items:
            try:
                mark_item_failed(item["id"], str(e)[:500])
            except Exception:
                pass

        refresh_job_progress(job_id)
        mark_job_failed(job_id, str(e)[:4000])
