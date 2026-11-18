# -*- coding: utf-8 -*-
"""
Postgres Toolkit: резервное копирование pg_dump / pg_restore.

Тот же каркас, что у gpbackup: отдельный процесс, лог в постоянный
файл, pid в задаче (переживает рестарт Opsentri), пароль только через
PGPASSWORD. Формат дампа - custom (-Fc): один файл, сжатие, выборочное
восстановление.
"""

import datetime
import json
import os
import re

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
)

try:
    from modules.connections import get_connection_by_id
except ImportError:
    from connections import get_connection_by_id

try:
    from modules.gpbackup import (
        _IDENT_RE,
        _clean_names,
        _run_external_tool,
        build_pg_env,
        extract_backup_errors,
        insert_backup,
    )
except ImportError:
    from gpbackup import (
        _IDENT_RE,
        _clean_names,
        _run_external_tool,
        build_pg_env,
        extract_backup_errors,
        insert_backup,
    )


DEFAULT_PG_DUMP_PATH = "/usr/bin/pg_dump"
DEFAULT_PG_RESTORE_PATH = "/usr/bin/pg_restore"

_TS_RE = re.compile(r"^\d{14}$")


def make_timestamp(now=None):
    return (now or datetime.datetime.now()).strftime("%Y%m%d%H%M%S")


def dump_file_path(backup_dir, dbname, timestamp):
    """Детерминированное имя файла дампа."""
    return os.path.join(
        backup_dir, "pgdump_{}_{}.dump".format(dbname, timestamp)
    )


def build_pg_dump_command(config):
    """Команда pg_dump (custom format). Чистая функция. -> (cmd, файл)."""
    dbname = (config.get("dbname") or "").strip()

    if not dbname or not _IDENT_RE.match(dbname):
        raise ValueError("dbname обязателен")

    backup_dir = (config.get("backup_dir") or "").strip()

    if not backup_dir:
        raise ValueError("Каталог бэкапа обязателен (pg_dump пишет файл)")

    timestamp = str(config.get("backup_timestamp") or "").strip()

    if not _TS_RE.match(timestamp):
        raise ValueError("backup_timestamp должен быть меткой YYYYMMDDHHMMSS")

    out_file = dump_file_path(backup_dir, dbname, timestamp)

    cmd = [
        config.get("pg_dump_path") or DEFAULT_PG_DUMP_PATH,
        "--format=custom",
        "--file", out_file,
        "--verbose",
    ]

    if config.get("data_only"):
        cmd.append("--data-only")
    elif config.get("schema_only"):
        cmd.append("--schema-only")

    for schema in _clean_names(config.get("include_schemas")):
        cmd += ["--schema", schema]

    for table in _clean_names(config.get("include_tables")):
        if "." not in table:
            raise ValueError("Таблица должна быть schema.table: {}".format(table))
        cmd += ["--table", table]

    compression = config.get("compression_level")

    if compression is not None and str(compression) != "":
        cmd += ["--compress", str(int(compression))]

    # имя БД - последним аргументом (host/user/пароль - через PG*-env)
    cmd.append(dbname)

    return cmd, out_file


def build_pg_restore_command(config):
    """Команда pg_restore. Чистая функция."""
    dump_file = (config.get("dump_file") or "").strip()

    if not dump_file:
        raise ValueError("Файл дампа обязателен")

    target_db = (config.get("target_db") or "").strip()

    if not target_db or not _IDENT_RE.match(target_db):
        raise ValueError("Целевая БД обязательна")

    cmd = [
        config.get("pg_restore_path") or DEFAULT_PG_RESTORE_PATH,
        "--verbose",
        "--no-owner",
    ]

    if config.get("create_db"):
        # -C создаёт БД из дампа; подключаться нужно к служебной базе
        cmd += ["--create", "--dbname",
                (config.get("maintenance_db") or "postgres").strip()]
    else:
        cmd += ["--dbname", target_db]

    if config.get("clean"):
        cmd += ["--clean", "--if-exists"]

    if config.get("data_only"):
        cmd.append("--data-only")

    jobs = int(config.get("jobs") or 1)

    if jobs > 1:
        cmd += ["--jobs", str(jobs)]

    for table in _clean_names(config.get("include_tables")):
        # pg_restore -t принимает имя таблицы без схемы
        cmd += ["--table", table.split(".")[-1]]

    cmd.append(dump_file)

    return cmd


def pg_outcome(rc, log_text):
    """pg_dump/pg_restore не печатают 'success' - судим по rc и ошибкам."""
    if rc == 0:
        return "done"

    if rc is None:
        # переподхват после рестарта: код возврата неизвестен -
        # считаем успехом отсутствие строк с ошибками
        has_errors = any(
            "error:" in line.lower() or "fatal" in line.lower()
            for line in (log_text or "").splitlines()
        )
        return "failed" if has_errors else "done"

    return "failed"


def _extract_pg_errors(log_text, limit=10):
    found = []

    for line in (log_text or "").splitlines():
        low = line.lower()

        if "error:" in low or "fatal" in low:
            stripped = line.strip()
            if stripped and stripped not in found:
                found.append(stripped)

    return found[-limit:] or extract_backup_errors(log_text, limit)


# частые причины падения pg_dump/pg_restore -> что делать (чистые правила)
_PG_HINTS = (
    ("server version mismatch",
     "Версия pg_dump старше версии сервера. Поставь клиент той же или более "
     "новой версии (postgresql<N>-client) или укажи путь к нему."),
    ("password authentication failed",
     "Неверный логин или пароль в подключении — проверь его на вкладке "
     "«Подключения»."),
    ("no pg_hba.conf entry",
     "Сервер не пускает хост Opsentri: нужна строка в pg_hba.conf."),
    ("connection refused",
     "Сервер недоступен: проверь хост, порт и что PostgreSQL запущен."),
    ("could not translate host name",
     "Имя хоста не резолвится — проверь хост в подключении."),
    ("could not open output file",
     "Нельзя создать файл дампа: каталога нет или у пользователя Opsentri "
     "нет прав на запись в него."),
    ("permission denied for",
     "У пользователя БД нет прав на чтение части объектов — нужен владелец "
     "или роль с SELECT на эти таблицы."),
    ("no space left on device",
     "На диске с каталогом дампа закончилось место."),
    ("no matching tables were found",
     "Указанные таблицы не найдены — пиши их как schema.table."),
    ("no matching schemas were found",
     "Указанные схемы не найдены — проверь имена."),
    ("does not exist",
     "Объект или база не найдены — проверь имя БД, схемы и таблицы."),
)


def pg_error_hint(text):
    """Подсказка 'что делать' по тексту ошибки или лога; '' если не узнали."""
    low = (text or "").lower()

    for needle, hint in _PG_HINTS:
        if needle in low:
            return hint

    return ""


def launch_error_message(exc, cmd_path):
    """Ошибка до запуска процесса (нет бинаря и т.п.) -> текст для человека."""
    if isinstance(exc, FileNotFoundError):
        missing = getattr(exc, "filename", None)

        if missing and str(missing) != str(cmd_path):
            return "Файл или каталог не найден: {}".format(missing)

        return (
            "Не найден исполняемый файл: {}. Установи клиент PostgreSQL "
            "(pg_dump/pg_restore) на сервер Opsentri или укажи путь к нему."
        ).format(cmd_path)

    if isinstance(exc, PermissionError):
        return "Нет прав на запуск: {} ({})".format(cmd_path, exc)

    return str(exc)


def finalize_pg_job(job_id, job_type, config, items, log_text, rc,
                    command_text):
    """Единая финализация pg_dump/pg_restore (и раннер, и переподхват)."""
    ok = pg_outcome(rc, log_text) == "done"

    if job_type == "pg_dump":
        insert_backup(
            connection_id=int(config.get("connection_id") or 0),
            job_id=job_id,
            backup_timestamp=config.get("backup_timestamp"),
            dbname=config.get("dbname"),
            backup_type="data_only" if config.get("data_only")
            else ("metadata_only" if config.get("schema_only") else "full"),
            backup_dir=config.get("backup_dir") or "",
            status="done" if ok else "failed",
            tool="pg_dump",
        )

    if ok:
        for item in items:
            mark_item_done(item["id"])

        refresh_job_progress(job_id)
        mark_job_done(job_id)
        return

    errors = _extract_pg_errors(log_text)
    hint = pg_error_hint("\n".join(errors) or log_text)
    report = "{}Команда: {}\n\n{}\n\nХвост лога:\n{}".format(
        "Что делать: {}\n\n".format(hint) if hint else "",
        command_text,
        "\n".join(errors) or "(строк с ошибками не найдено, код {})".format(rc),
        (log_text or "")[-5000:],
    )

    for item in items:
        mark_item_failed(
            item["id"],
            ("\n".join(errors) or "{} rc={}".format(job_type, rc))[:2000],
        )

    refresh_job_progress(job_id)
    mark_job_failed(job_id, report[:12000])


def _run_pg_job(job_id, job_type, build):
    """Общий каркас раннеров pg_dump / pg_restore."""
    job = get_job(job_id)

    if not job:
        return

    config = json.loads(job.get("config_json") or "{}")
    mark_job_running(job_id)

    items = get_job_items(job_id)

    for item in items:
        mark_item_running(item["id"])

    refresh_job_progress(job_id)
    cmd = None

    try:
        conn_cfg = get_connection_by_id(int(config["connection_id"]))

        if not conn_cfg:
            raise Exception("Подключение не найдено")

        cmd = build(config, conn_cfg)
        env = build_pg_env(conn_cfg)

        def finalize(log_text, rc):
            finalize_pg_job(
                job_id, job_type, config, items, log_text, rc, " ".join(cmd),
            )

        _run_external_tool(job_id, cmd, env, finalize)

    except Exception as e:
        text = launch_error_message(e, cmd[0] if cmd else job_type)
        hint = pg_error_hint(text)

        for item in items:
            try:
                mark_item_failed(item["id"], text[:500])
            except Exception:
                pass

        refresh_job_progress(job_id)
        mark_job_failed(job_id, "{}{}{}".format(
            text,
            "\n\nЧто делать: " + hint if hint else "",
            "\n\nКоманда: " + " ".join(cmd) if cmd else "",
        )[:4000])


def run_pg_dump_job(job_id):
    def build(config, conn_cfg):
        if not config.get("dbname"):
            config["dbname"] = (
                conn_cfg.get("database_name") or conn_cfg.get("database")
            )

        # запуск по расписанию: метка генерируется на момент запуска,
        # чтобы каждый прогон писал новый файл
        if not config.get("backup_timestamp"):
            config["backup_timestamp"] = make_timestamp()

        cmd, _out = build_pg_dump_command(config)
        return cmd

    _run_pg_job(job_id, "pg_dump", build)


def run_pg_restore_job(job_id):
    def build(config, conn_cfg):
        return build_pg_restore_command(config)

    _run_pg_job(job_id, "pg_restore", build)


def resume_unfinished_pg_jobs():
    """Переподхват pg_dump/pg_restore после рестарта (pid + лог)."""
    import threading

    from job_manager import list_unfinished_jobs

    try:
        from modules.gpcopy import _watch_gpcopy_log, pid_alive
    except ImportError:
        from gpcopy import _watch_gpcopy_log, pid_alive

    handled = []

    try:
        unfinished = list_unfinished_jobs()
    except Exception:
        return handled

    for job in unfinished:
        job_type = job.get("job_type")

        if job_type not in ("pg_dump", "pg_restore"):
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

        def _resume(jid=job_id, jt=job_type, lp=log_path, p=pid,
                    it=items, cfg=config):
            log_text = _watch_gpcopy_log(jid, lp, [], pid=p)

            if is_stop_requested(jid):
                mark_job_cancelled(jid)
                refresh_job_progress(jid)
                return

            finalize_pg_job(
                jid, jt, cfg, it, log_text, None,
                "{} (переподхвачен после рестарта)".format(jt),
            )

        if pid and pid_alive(pid):
            threading.Thread(target=_resume, daemon=True).start()
        else:
            try:
                with open(log_path, "r", encoding="utf-8",
                          errors="replace") as f:
                    log_text = f.read()
            except Exception:
                log_text = ""

            finalize_pg_job(
                job_id, job_type, config, items, log_text, None,
                "{} (завершился, пока Opsentri был перезапущен)".format(
                    job_type
                ),
            )

    return handled
