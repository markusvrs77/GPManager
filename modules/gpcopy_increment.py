"""
gpcopy increment (watermark-append): догрузка только новых строк.

Находит max(watermark_column) в dest и копирует из source строки с
значением > watermark (append). Для append-only / лог-таблиц. Без
update/delete — для изменяемых таблиц используется key-upsert (gpcopy_sync).
"""

from __future__ import print_function

import os
import json
import time
import tempfile
import traceback
import subprocess

try:
    from job_manager import (
        get_job, get_job_items, mark_job_running, mark_item_running,
        refresh_job_progress, is_stop_requested, clear_stop_flag,
    )
except ImportError:
    from modules.job_manager import (
        get_job, get_job_items, mark_job_running, mark_item_running,
        refresh_job_progress, is_stop_requested, clear_stop_flag,
    )

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id

try:
    from modules.gpcopy import (
        quote_ident, sql_literal, build_gpcopy_command,
        open_psycopg2_connection_by_cfg, get_conn_dbname, get_conn_host,
        get_conn_port, get_conn_user, safe_mark_job_failed,
        safe_mark_job_cancelled, safe_mark_item_failed, safe_mark_item_done,
        get_item_value, DEFAULT_GPCOPY_PATH,
    )
except ImportError:
    from gpcopy import (
        quote_ident, sql_literal, build_gpcopy_command,
        open_psycopg2_connection_by_cfg, get_conn_dbname, get_conn_host,
        get_conn_port, get_conn_user, safe_mark_job_failed,
        safe_mark_job_cancelled, safe_mark_item_failed, safe_mark_item_done,
        get_item_value, DEFAULT_GPCOPY_PATH,
    )


def _watermark_literal(value):
    """Число — как есть; всё остальное — экранированный SQL-литерал."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        return str(value)

    return sql_literal(value)


def build_increment_items(tables, watermarks, source_db, dest_db):
    """
    Чистая функция: строит include-table-json items.

    tables      — [{schema, table, watermark_column}]
    watermarks  — {(schema, table): value | None}
    Возвращает [{source, dest, sql}].
    """
    if not tables:
        raise ValueError("Не выбраны таблицы для increment")

    items = []

    for entry in tables:
        schema = entry.get("schema") or entry.get("schema_name")
        table = entry.get("table") or entry.get("table_name")
        column = entry.get("watermark_column")

        if not schema or not table:
            continue

        if not column:
            raise ValueError(
                "watermark_column обязателен для {}.{}".format(schema, table)
            )

        full = "{}.{}".format(quote_ident(schema), quote_ident(table))
        watermark = watermarks.get((schema, table))

        if watermark is None:
            sql = "SELECT * FROM {}".format(full)
        else:
            sql = "SELECT * FROM {} WHERE {} > {}".format(
                full, quote_ident(column), _watermark_literal(watermark)
            )

        items.append({
            "source": "{}.{}.{}".format(source_db, schema, table),
            "dest": "{}.{}.{}".format(dest_db, schema, table),
            "sql": sql,
        })

    if not items:
        raise ValueError("Нет валидных таблиц для increment")

    return items


def get_dest_watermark(dest_cfg, schema, table, column):
    """max(column) в dest; None если таблицы нет / она пуста."""
    conn = open_psycopg2_connection_by_cfg(dest_cfg)

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max({}) FROM {}.{}".format(
                    quote_ident(column), quote_ident(schema), quote_ident(table)
                )
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception:
        # Таблицы может не быть в dest — тогда это полная догрузка.
        return None
    finally:
        conn.close()


def build_increment_include_json_file(config):
    source_cfg = get_connection_by_id(int(config["source_connection_id"]))
    dest_cfg = get_connection_by_id(int(config["dest_connection_id"]))

    if not source_cfg or not dest_cfg:
        raise ValueError("source/dest connection not found")

    source_db = config.get("source_db") or get_conn_dbname(source_cfg)
    dest_db = config.get("dest_db") or get_conn_dbname(dest_cfg)

    tables = config.get("tables") or []

    watermarks = {}
    for entry in tables:
        schema = entry.get("schema") or entry.get("schema_name")
        table = entry.get("table") or entry.get("table_name")
        column = entry.get("watermark_column")

        if schema and table and column:
            watermarks[(schema, table)] = get_dest_watermark(
                dest_cfg, schema, table, column
            )

    items = build_increment_items(tables, watermarks, source_db, dest_db)

    fd, path = tempfile.mkstemp(prefix="gpcopy_increment_", suffix=".json", text=True)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    return path


def run_gpcopy_increment_job(job_id):
    include_json_file = None
    started = time.time()

    try:
        job = get_job(job_id)
        if not job:
            raise Exception("Job not found: {}".format(job_id))

        config = json.loads(get_item_value(job, "config_json") or "{}")

        source_cfg = get_connection_by_id(int(config["source_connection_id"]))
        dest_cfg = get_connection_by_id(int(config["dest_connection_id"]))

        items = get_job_items(job_id)
        if not items:
            raise Exception("No job items found")

        clear_stop_flag(job_id)
        mark_job_running(job_id)

        include_json_file = build_increment_include_json_file(config)

        gpcopy_path = (
            config.get("gpcopy_path")
            or os.environ.get("GPCOPY_PATH")
            or DEFAULT_GPCOPY_PATH
        )

        cmd = build_gpcopy_command(
            gpcopy_path=gpcopy_path,
            source_host=get_conn_host(source_cfg),
            dest_host=get_conn_host(dest_cfg),
            source_port=get_conn_port(source_cfg),
            dest_port=get_conn_port(dest_cfg),
            dest_user=get_conn_user(dest_cfg),
            include_json_file=include_json_file,
            jobs=int(config.get("jobs") or 4),
            append=True,
        )

        for item in items:
            mark_item_running(get_item_value(item, "id"))
        refresh_job_progress(job_id)

        if is_stop_requested(job_id):
            safe_mark_job_cancelled(job_id, "Stop requested before increment start")
            return

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        stdout_data, stderr_data = process.communicate()
        rc = process.returncode
        duration = time.time() - started

        if rc == 0:
            for item in items:
                safe_mark_item_done(get_item_value(item, "id"), duration_seconds=duration)
            refresh_job_progress(job_id)
            from job_manager import mark_job_done
            mark_job_done(job_id)
        else:
            error_text = stderr_data or stdout_data or "gpcopy rc={}".format(rc)
            for item in items:
                safe_mark_item_failed(
                    get_item_value(item, "id"),
                    error_message=error_text[:1000], duration_seconds=duration,
                )
            refresh_job_progress(job_id)
            safe_mark_job_failed(job_id, error_text[:4000])

    except Exception as e:
        err = "{}\n{}".format(e, traceback.format_exc())
        try:
            safe_mark_job_failed(job_id, err[:4000])
        except Exception:
            pass
    finally:
        if include_json_file and os.path.exists(include_json_file):
            try:
                os.remove(include_json_file)
            except Exception:
                pass
