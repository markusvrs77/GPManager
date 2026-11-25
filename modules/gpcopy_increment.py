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
        get_job, get_job_items, mark_job_running,
        refresh_job_progress, is_stop_requested, clear_stop_flag,
        mark_job_cancelled, set_job_runtime,
    )
except ImportError:
    from modules.job_manager import (
        get_job, get_job_items, mark_job_running,
        refresh_job_progress, is_stop_requested, clear_stop_flag,
        mark_job_cancelled, set_job_runtime,
    )

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id

try:
    from modules.gpcopy import (
        quote_ident, sql_literal, build_gpcopy_command, write_dest_mapping_file, pick_rpc_port,
        open_psycopg2_connection_by_cfg, get_conn_dbname, get_conn_host,
        get_conn_port, get_conn_user, safe_mark_job_failed,
        safe_mark_job_cancelled, safe_mark_item_failed,
        get_item_value, DEFAULT_GPCOPY_PATH, job_targets, gpcopy_full_name,
        prepare_mapped_targets, _job_log_path, _watch_gpcopy_log,
        finalize_gpcopy_job, owner_item_keys,
    )
except ImportError:
    from gpcopy import (
        quote_ident, sql_literal, build_gpcopy_command, write_dest_mapping_file, pick_rpc_port,
        open_psycopg2_connection_by_cfg, get_conn_dbname, get_conn_host,
        get_conn_port, get_conn_user, safe_mark_job_failed,
        safe_mark_job_cancelled, safe_mark_item_failed,
        get_item_value, DEFAULT_GPCOPY_PATH, job_targets, gpcopy_full_name,
        prepare_mapped_targets, _job_log_path, _watch_gpcopy_log,
        finalize_gpcopy_job, owner_item_keys,
    )

try:
    from modules.sync_targets import target_of
except ImportError:
    from sync_targets import target_of


def _watermark_literal(value):
    """Число — как есть; всё остальное — экранированный SQL-литерал."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        return str(value)

    return sql_literal(value)


def build_increment_items(tables, watermarks, source_db, dest_db,
                          targets=None):
    """
    Чистая функция: строит include-table-json items.

    tables      — [{schema, table, watermark_column}]
    watermarks  — {(schema, table): value | None} (ключ — имя источника)
    targets     — карта «источник -> цель»; без неё dest одноимённый
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

        dst_schema, dst_table = target_of(targets, schema, table)

        if (dst_schema, dst_table) == (schema, table):
            dest = "{}.{}.{}".format(dest_db, schema, table)
        else:
            # цель из карты: имя схемы источника может быть с заглавными —
            # gpcopy понимает такое только в кавычках
            dest = gpcopy_full_name(dest_db, dst_schema, dst_table)

        items.append({
            "source": "{}.{}.{}".format(source_db, schema, table),
            "dest": dest,
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
    targets = config.get("targets") or {}

    watermarks = {}
    for entry in tables:
        schema = entry.get("schema") or entry.get("schema_name")
        table = entry.get("table") or entry.get("table_name")
        column = entry.get("watermark_column")

        if schema and table and column:
            # отметку читаем там, куда грузим, — в цели
            dst_schema, dst_table = target_of(targets, schema, table)
            watermarks[(schema, table)] = get_dest_watermark(
                dest_cfg, dst_schema, dst_table, column
            )

    items = build_increment_items(tables, watermarks, source_db, dest_db,
                                  targets)

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

        # карта «источник -> цель»: проверка на выборе задачи и цель в
        # приёмнике до gpcopy (нет — создаём по источнику)
        def _pair(entry):
            return (entry.get("schema") or entry.get("schema_name"),
                    entry.get("table") or entry.get("table_name"))

        tables = config.get("tables") or []
        targets = job_targets(config, [_pair(t) for t in tables])
        config["targets"] = targets

        if targets:
            target_errors = prepare_mapped_targets(
                config["source_connection_id"], config["dest_connection_id"],
                targets, [_pair(t) for t in tables])

            if target_errors:
                kept_items = []

                for item in items:
                    pair = (get_item_value(item, "schema_name"),
                            get_item_value(item, "table_name"))

                    if pair in target_errors:
                        safe_mark_item_failed(
                            get_item_value(item, "id"),
                            error_message=target_errors[pair][:2000])
                    else:
                        kept_items.append(item)

                items = kept_items
                config["tables"] = [
                    t for t in tables if _pair(t) not in target_errors]
                refresh_job_progress(job_id)

                if not items or not config["tables"]:
                    raise Exception(
                        "Не удалось подготовить цели в приёмнике:\n{}".format(
                            "\n".join(target_errors.values())))

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
            dest_mapping_file=write_dest_mapping_file(dest_cfg),
            rpc_port=pick_rpc_port(),
        )

        if is_stop_requested(job_id):
            safe_mark_job_cancelled(job_id, "Stop requested before increment start")
            return

        # общий каркас gpcopy (как у полного копирования и партиций):
        # лог в файл, live-статусы, стоп по кнопке, переподхват после
        # рестарта. Итог — по каждой таблице: упавшая одна таблица больше
        # не красит ошибкой все остальные
        log_path = _job_log_path(job_id)
        log_handle = open(log_path, "w", encoding="utf-8", errors="replace")

        popen_kwargs = {
            "stdout": log_handle,
            "stderr": subprocess.STDOUT,
            "universal_newlines": True,
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

        log_text = _watch_gpcopy_log(
            job_id, log_path, owner_item_keys(items, targets),
            process=process)

        if is_stop_requested(job_id):
            mark_job_cancelled(job_id)
            refresh_job_progress(job_id)
            return

        finalize_gpcopy_job(
            job_id, items, process.returncode, log_text, "",
            " ".join(cmd), time.time() - started, config,
        )

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
