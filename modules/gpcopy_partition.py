"""
gpcopy partition-diff: копирует только те leaf-партиции, что отличаются
от прода по числу строк (COUNT(*) source != dest, или партиция отсутствует
в dest). Определение leaf-партиций — рекурсивный CTE по pg_inherits (тот же
паттерн, что modules/reorganize.py:get_reorganize_targets).
"""

from __future__ import print_function

import os
import json
import time
import traceback
import subprocess

from psycopg2.extras import RealDictCursor

try:
    from job_manager import (
        get_job, mark_job_running, mark_job_done, is_stop_requested,
        clear_stop_flag,
    )
except ImportError:
    from modules.job_manager import (
        get_job, mark_job_running, mark_job_done, is_stop_requested,
        clear_stop_flag,
    )

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id

try:
    from modules.gpcopy import (
        quote_ident, build_gpcopy_command, open_psycopg2_connection_by_cfg,
        get_conn_dbname, get_conn_host, get_conn_port, get_conn_user,
        make_include_table_file, safe_mark_job_failed, get_item_value,
        DEFAULT_GPCOPY_PATH,
    )
except ImportError:
    from gpcopy import (
        quote_ident, build_gpcopy_command, open_psycopg2_connection_by_cfg,
        get_conn_dbname, get_conn_host, get_conn_port, get_conn_user,
        make_include_table_file, safe_mark_job_failed, get_item_value,
        DEFAULT_GPCOPY_PATH,
    )


_LEAF_CTE = """
    WITH RECURSIVE tree AS (
        SELECT c.oid, n.nspname AS schema_name, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        UNION ALL
        SELECT child.oid, cn.nspname, child.relname
        FROM tree
        JOIN pg_inherits i ON i.inhparent = tree.oid
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_namespace cn ON cn.oid = child.relnamespace
    )
    SELECT t.schema_name, t.table_name
    FROM tree t
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_inherits i2 WHERE i2.inhparent = t.oid
    )
    ORDER BY t.schema_name, t.table_name
"""


def list_leaf_partitions(conn, schema, table):
    """Возвращает [(schema, table)] leaf-партиций (или саму таблицу, если не партиц.)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(_LEAF_CTE, (schema, table))
        rows = cur.fetchall()

    return [(r["schema_name"], r["table_name"]) for r in rows]


def _count_rows(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM {}.{}".format(
                quote_ident(schema), quote_ident(table)
            )
        )
        return int(cur.fetchone()[0])


def classify_partition_diff(source_counts, dest_counts):
    """
    Чистая функция. source_counts / dest_counts — {partition_name: count}.
    Возвращает [{partition, src_count, dest_count, action}].
    action: copy_missing | copy_changed | skip.
    """
    rows = []

    for partition, src_count in source_counts.items():
        if partition not in dest_counts:
            action = "copy_missing"
            dest_count = None
        elif dest_counts[partition] != src_count:
            action = "copy_changed"
            dest_count = dest_counts[partition]
        else:
            action = "skip"
            dest_count = dest_counts[partition]

        rows.append({
            "partition": partition,
            "src_count": src_count,
            "dest_count": dest_count,
            "action": action,
        })

    return rows


def partitions_to_copy(diff_rows):
    """Имена партиций с action != skip."""
    return [r["partition"] for r in diff_rows if r["action"] != "skip"]


def diff_partitions(source_cfg, dest_cfg, schema, table):
    """Открывает соединения, считает строки leaf-партиций с обеих сторон, классифицирует."""
    source_conn = open_psycopg2_connection_by_cfg(source_cfg)
    dest_conn = open_psycopg2_connection_by_cfg(dest_cfg)

    try:
        source_leaves = list_leaf_partitions(source_conn, schema, table)

        source_counts = {}
        for s, t in source_leaves:
            source_counts[t] = _count_rows(source_conn, s, t)

        dest_counts = {}
        for s, t in source_leaves:
            try:
                dest_counts[t] = _count_rows(dest_conn, s, t)
            except Exception:
                # Партиции нет в dest — останется missing.
                dest_conn.rollback()

        return classify_partition_diff(source_counts, dest_counts), source_leaves

    finally:
        source_conn.close()
        dest_conn.close()


def run_gpcopy_partition_diff_job(job_id):
    include_file = None
    started = time.time()

    try:
        job = get_job(job_id)
        if not job:
            raise Exception("Job not found: {}".format(job_id))

        config = json.loads(get_item_value(job, "config_json") or "{}")

        source_cfg = get_connection_by_id(int(config["source_connection_id"]))
        dest_cfg = get_connection_by_id(int(config["dest_connection_id"]))

        tables = config.get("tables") or []
        if not tables:
            raise Exception("tables is empty")

        clear_stop_flag(job_id)
        mark_job_running(job_id)

        # Собираем список отличающихся leaf-партиций по всем выбранным таблицам.
        copy_items = []
        for entry in tables:
            schema = entry.get("schema") or entry.get("schema_name")
            table = entry.get("table") or entry.get("table_name")

            diff_rows, leaves = diff_partitions(source_cfg, dest_cfg, schema, table)
            leaf_by_name = {t: (s, t) for (s, t) in leaves}

            for name in partitions_to_copy(diff_rows):
                s, t = leaf_by_name[name]
                copy_items.append({"schema_name": s, "table_name": t})

        if is_stop_requested(job_id):
            from job_manager import mark_job_cancelled
            mark_job_cancelled(job_id)
            return

        if not copy_items:
            # Всё совпадает — копировать нечего, это успех.
            mark_job_done(job_id)
            return

        source_db = get_conn_dbname(source_cfg)
        include_file = make_include_table_file(copy_items, source_db)

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
            include_tables_file=include_file,
            jobs=int(config.get("jobs") or 4),
            truncate=True,
        )

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        stdout_data, stderr_data = process.communicate()
        rc = process.returncode

        if rc == 0:
            mark_job_done(job_id)
        else:
            error_text = stderr_data or stdout_data or "gpcopy rc={}".format(rc)
            safe_mark_job_failed(job_id, error_text[:4000])

    except Exception as e:
        err = "{}\n{}".format(e, traceback.format_exc())
        try:
            safe_mark_job_failed(job_id, err[:4000])
        except Exception:
            pass
    finally:
        if include_file and os.path.exists(include_file):
            try:
                os.remove(include_file)
            except Exception:
                pass
