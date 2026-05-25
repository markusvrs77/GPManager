from __future__ import print_function

import re
import os
import json
import time
import tempfile
import traceback
import subprocess

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from job_manager import (
        get_job,
        get_job_items,
        mark_job_running,
        mark_job_done,
        mark_job_failed,
        mark_job_cancelled,
        mark_item_running,
        mark_item_done,
        mark_item_failed,
        mark_item_skipped,
        refresh_job_progress,
        is_stop_requested,
        clear_stop_flag,
    )
except ImportError:
    from modules.job_manager import (
        get_job,
        get_job_items,
        mark_job_running,
        mark_job_done,
        mark_job_failed,
        mark_job_cancelled,
        mark_item_running,
        mark_item_done,
        mark_item_failed,
        mark_item_skipped,
        refresh_job_progress,
        is_stop_requested,
        clear_stop_flag,
    )

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id


DEFAULT_GPCOPY_PATH = "/usr/local/gpdb/greenplum-db/bin/gpcopy"


# ------------------------------------------------------------
# Common helpers
# ------------------------------------------------------------

def conn_get(conn, key, default=None):
    if conn is None:
        return default

    if isinstance(conn, dict):
        return conn.get(key, default)

    try:
        return conn[key]
    except Exception:
        return getattr(conn, key, default)


def get_conn_host(conn):
    return (
        conn_get(conn, "host")
        or conn_get(conn, "hostname")
        or conn_get(conn, "ip")
    )


def get_conn_port(conn):
    return (
        conn_get(conn, "port")
        or conn_get(conn, "db_port")
        or 5432
    )


def get_conn_dbname(conn):
    return (
        conn_get(conn, "database")
        or conn_get(conn, "dbname")
        or conn_get(conn, "db_name")
        or conn_get(conn, "database_name")
        or conn_get(conn, "db")
    )


def get_conn_user(conn):
    return (
        conn_get(conn, "username")
        or conn_get(conn, "user")
        or conn_get(conn, "login")
        or conn_get(conn, "db_user")
        or "gpadmin"
    )


def get_conn_password(conn):
    return (
        conn_get(conn, "password")
        or conn_get(conn, "passwd")
        or conn_get(conn, "db_password")
        or ""
    )


def open_psycopg2_connection_by_cfg(conn_cfg):
    host = get_conn_host(conn_cfg)
    port = get_conn_port(conn_cfg)
    dbname = get_conn_dbname(conn_cfg)
    user = get_conn_user(conn_cfg)
    password = get_conn_password(conn_cfg)

    if not host:
        raise Exception("Connection host is empty")

    if not dbname:
        raise Exception("Connection database/dbname is empty")

    return psycopg2.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
    )


def quote_ident(name):
    return '"' + str(name).replace('"', '""') + '"'


def validate_identifier_with_dollar(name):
    """
    Разрешаем обычные имена колонок и имена с $, например date_change$.
    """
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_$]*$", name or ""))


def quote_table_for_include(schema_name, table_name):
    """
    gpcopy include-table-file обычно принимает schema.table.
    """
    return "{}.{}".format(schema_name, table_name)


def get_item_value(item, key, default=None):
    if isinstance(item, dict):
        return item.get(key, default)

    try:
        return item[key]
    except Exception:
        return getattr(item, key, default)


def safe_mark_job_failed(job_id, error_message):
    try:
        mark_job_failed(job_id, error_message)
    except TypeError:
        mark_job_failed(job_id)


def safe_mark_job_cancelled(job_id, error_message=None):
    try:
        mark_job_cancelled(job_id, error_message)
    except TypeError:
        mark_job_cancelled(job_id)


def safe_mark_item_failed(item_id, error_message=None, duration_seconds=None):
    try:
        mark_item_failed(
            item_id,
            error_message=error_message,
            duration_seconds=duration_seconds,
        )
    except TypeError:
        try:
            mark_item_failed(item_id, error_message)
        except TypeError:
            mark_item_failed(item_id)


def safe_mark_item_done(item_id, duration_seconds=None):
    try:
        mark_item_done(item_id, duration_seconds=duration_seconds)
    except TypeError:
        mark_item_done(item_id)


# ------------------------------------------------------------
# Date columns API
# ------------------------------------------------------------

def get_date_columns_for_table(connection_id, schema_name, table_name):
    connection = get_connection_by_id(int(connection_id))

    if not connection:
        raise Exception("Connection not found: {}".format(connection_id))

    sql = """
        SELECT
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND (
                data_type IN (
                    'date',
                    'timestamp without time zone',
                    'timestamp with time zone'
                )
                OR udt_name IN ('date', 'timestamp', 'timestamptz')
          )
        ORDER BY ordinal_position
    """

    conn = open_psycopg2_connection_by_cfg(connection)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (schema_name, table_name))
            rows = cur.fetchall()
    finally:
        conn.close()

    return [dict(row) for row in rows]


# ------------------------------------------------------------
# Include file for normal gpcopy
# ------------------------------------------------------------

def make_include_table_file(items):
    fd, path = tempfile.mkstemp(prefix="gpcopy_include_", suffix=".txt")
    os.close(fd)

    with open(path, "w") as f:
        for item in items:
            schema_name = (
                get_item_value(item, "schema_name")
                or get_item_value(item, "schema")
            )
            table_name = (
                get_item_value(item, "table_name")
                or get_item_value(item, "table")
            )

            if not schema_name or not table_name:
                continue

            f.write(quote_table_for_include(schema_name, table_name))
            f.write("\n")

    return path


# ------------------------------------------------------------
# Include JSON for gpcopy by date
# ------------------------------------------------------------

def build_gpcopy_date_include_json_preview(config):
    source_connection_id = (
        config.get("source_connection_id")
        or config.get("connection_id")
    )

    dest_connection_id = (
        config.get("dest_connection_id")
        or config.get("destination_connection_id")
    )

    if not source_connection_id:
        raise ValueError("source_connection_id is required")

    if not dest_connection_id:
        raise ValueError("dest_connection_id/destination_connection_id is required")

    source_connection = get_connection_by_id(int(source_connection_id))
    dest_connection = get_connection_by_id(int(dest_connection_id))

    if not source_connection:
        raise ValueError("Source connection not found: {}".format(source_connection_id))

    if not dest_connection:
        raise ValueError("Destination connection not found: {}".format(dest_connection_id))

    source_dbname = get_conn_dbname(source_connection)
    dest_dbname = get_conn_dbname(dest_connection)

    if not source_dbname:
        raise ValueError("Source database/dbname is empty")

    if not dest_dbname:
        raise ValueError("Destination database/dbname is empty")

    selected_tables = config.get("selected_tables") or []
    target_schema = (config.get("target_schema") or "").strip()

    date_filter_column = (config.get("date_filter_column") or "").strip()
    date_from = (config.get("date_from") or "").strip()
    date_to = (config.get("date_to") or "").strip()

    if not selected_tables:
        raise ValueError("Не выбраны таблицы")

    if not date_filter_column:
        raise ValueError("date_filter_column обязателен")

    if not validate_identifier_with_dollar(date_filter_column):
        raise ValueError("Некорректная колонка даты: {}".format(date_filter_column))

    if not date_from or not date_to:
        raise ValueError("date_from и date_to обязательны")

    items = []
    seen = set()

    for table_item in selected_tables:
        schema_name = (
            table_item.get("schema")
            or table_item.get("schema_name")
        )
        table_name = (
            table_item.get("table")
            or table_item.get("table_name")
        )

        if not schema_name or not table_name:
            continue

        key = (schema_name, table_name)

        if key in seen:
            continue

        seen.add(key)

        dest_schema = target_schema or schema_name

        # Формат, который у тебя уже сработал:
        # source: adb.schema.table
        # dest:   adb.schema.table
        source_json = "{}.{}.{}".format(
            source_dbname,
            schema_name,
            table_name,
        )

        dest_json = "{}.{}.{}".format(
            dest_dbname,
            dest_schema,
            table_name,
        )

        sql_table_name = "{}.{}".format(
            quote_ident(schema_name),
            quote_ident(table_name),
        )

        sql_column_name = quote_ident(date_filter_column)

        sql = (
            "SELECT * FROM {table_name} "
            "WHERE {column_name} >= '{date_from}' "
            "AND {column_name} < '{date_to}'"
        ).format(
            table_name=sql_table_name,
            column_name=sql_column_name,
            date_from=date_from,
            date_to=date_to,
        )

        items.append({
            "source": source_json,
            "dest": dest_json,
            "sql": sql,
        })

    if not items:
        raise ValueError("Нет таблиц для gpcopy include-table-json")

    return items


def build_gpcopy_date_include_json_file(config):
    items = build_gpcopy_date_include_json_preview(config)

    fd, path = tempfile.mkstemp(
        prefix="gpcopy_include_date_",
        suffix=".json",
        text=True,
    )

    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    return path


# ------------------------------------------------------------
# Command builder
# ------------------------------------------------------------

def build_gpcopy_command(gpcopy_path,
    source_host,
    dest_host,
    include_json_path=None,
    include_table_file=None,
    include_table=None,
    source_db=None,
    dest_db=None,
    dest_user=None,
    jobs=4,
    on_segment_threshold=-1,
    append=False,
    truncate=False,
    drop=False,
    skip_existing=False,
    analyze=False,
    dry_run=False,
    validate_count=False,
    no_ownership=True,
    extra_args=None,
):
    cmd = [
        gpcopy_path,
        "--source-host", str(source_host),
        "--dest-host", str(dest_host),
    ]

    # В gpcopy 2.7.0 база источника задаётся через --dbname / -d
    if source_db:
        cmd.extend(["--dbname", str(source_db)])

    # База назначения задаётся через --dest-dbname / -D
    if dest_db:
        cmd.extend(["--dest-dbname", str(dest_db)])

    # В твоей версии есть только --dest-user, source-user нет
    if dest_user:
        cmd.extend(["--dest-user", str(dest_user)])

    cmd.extend(["--jobs", str(jobs)])
    cmd.extend(["--on-segment-threshold", str(on_segment_threshold)])

    if include_json_path:
        cmd.extend(["--include-table-json", str(include_json_path)])
    elif include_table_file:
        cmd.extend(["--include-table-file", str(include_table_file)])
    elif include_table:
        cmd.extend(["--include-table", str(include_table)])

    if append:
        cmd.append("--append")

    if truncate:
        cmd.append("--truncate")

    if drop:
        cmd.append("--drop")

    if skip_existing:
        cmd.append("--skip-existing")

    if analyze:
        cmd.append("--analyze")

    if dry_run:
        cmd.append("--dry-run")

    if validate_count:
        cmd.extend(["--validate", "count"])

    if no_ownership:
        cmd.append("--no-ownership")

    if extra_args:
        if isinstance(extra_args, list):
            cmd.extend(extra_args)
        elif isinstance(extra_args, str) and extra_args.strip():
            cmd.extend(extra_args.strip().split())

    return cmd


# ------------------------------------------------------------
# Main job runner
# ------------------------------------------------------------

def run_gpcopy_job(job_id):
    include_file = None
    include_json_file = None
    started = time.time()

    try:
        job = get_job(job_id)

        if not job:
            raise Exception("Job not found: {}".format(job_id))

        config_json = get_item_value(job, "config_json")
        config = json.loads(config_json or "{}")

        source_connection_id = (
            config.get("source_connection_id")
            or config.get("connection_id")
        )

        dest_connection_id = (
            config.get("dest_connection_id")
            or config.get("destination_connection_id")
        )

        if not source_connection_id:
            raise Exception("source_connection_id is required")

        if not dest_connection_id:
            raise Exception("dest_connection_id/destination_connection_id is required")

        source_connection = get_connection_by_id(int(source_connection_id))
        dest_connection = get_connection_by_id(int(dest_connection_id))

        if not source_connection:
            raise Exception("Source connection not found: {}".format(source_connection_id))

        if not dest_connection:
            raise Exception("Destination connection not found: {}".format(dest_connection_id))

        items = get_job_items(job_id)

        if not items:
            raise Exception("No job items found")

        clear_stop_flag(job_id)
        mark_job_running(job_id)

        # ------------------------------------------------------------
        # Build gpcopy runtime options
        # ------------------------------------------------------------

        mode = config.get("mode") or ""

        gpcopy_path = (
                config.get("gpcopy_path")
                or os.environ.get("GPCOPY_PATH")
                or DEFAULT_GPCOPY_PATH
        )

        source_host = get_conn_host(source_connection)
        dest_host = get_conn_host(dest_connection)

        source_db = (
                config.get("source_db")
                or config.get("source_dbname")
                or get_conn_dbname(source_connection)
        )

        dest_db = (
                config.get("dest_db")
                or config.get("dest_dbname")
                or get_conn_dbname(dest_connection)
        )

        dest_user = (
                config.get("dest_user")
                or get_conn_user(dest_connection)
        )

        jobs = int(config.get("jobs") or 4)

        on_segment_threshold = int(
            config.get("on_segment_threshold")
            if config.get("on_segment_threshold") is not None
            else -1
        )

        append = bool(config.get("append"))
        truncate = bool(config.get("truncate"))
        drop = bool(config.get("drop"))
        skip_existing = bool(config.get("skip_existing"))
        analyze = bool(config.get("analyze"))
        dry_run = bool(config.get("dry_run"))
        validate_count = bool(config.get("validate_count"))

        no_ownership = config.get("no_ownership")
        if no_ownership is None:
            no_ownership = True
        else:
            no_ownership = bool(no_ownership)

        extra_args = config.get("extra_args") or []

        if not gpcopy_path:
            raise Exception("gpcopy_path is empty")

        if not source_host:
            raise Exception("Source host is empty")

        if not dest_host:
            raise Exception("Destination host is empty")

        if not source_db:
            raise Exception("Source database/dbname is empty")

        if not dest_db:
            raise Exception("Destination database/dbname is empty")

        if mode == "date_filter":
            include_json_file = build_gpcopy_date_include_json_file(config)
        else:
            include_file = make_include_table_file(items)

        cmd = build_gpcopy_command(
            gpcopy_path=gpcopy_path,
            source_host=source_host,
            dest_host=dest_host,
            source_db=source_db,
            dest_db=dest_db,
            dest_user=dest_user,
            include_json_path=include_json_file,
            include_table_file=include_file,
            jobs=jobs,
            on_segment_threshold=on_segment_threshold,
            append=append,
            truncate=truncate,
            drop=drop,
            skip_existing=skip_existing,
            analyze=analyze,
            dry_run=dry_run,
            validate_count=validate_count,
            no_ownership=no_ownership,
            extra_args=extra_args,
        )

        command_text = " ".join(cmd)

        for item in items:
            item_id = get_item_value(item, "id")
            mark_item_running(item_id)

        refresh_job_progress(job_id)

        if is_stop_requested(job_id):
            safe_mark_job_cancelled(job_id, "Stop requested before gpcopy start")
            refresh_job_progress(job_id)
            return

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )

        stdout_data, stderr_data = process.communicate()
        rc = process.returncode

        duration = time.time() - started

        if is_stop_requested(job_id):
            safe_mark_job_cancelled(job_id, "Stop requested")
            refresh_job_progress(job_id)
            return

        if rc == 0:
            for item in items:
                item_id = get_item_value(item, "id")
                safe_mark_item_done(item_id, duration_seconds=duration)

            refresh_job_progress(job_id)
            mark_job_done(job_id)

        else:
            error_text = stderr_data or stdout_data or "gpcopy failed with rc={}".format(rc)

            full_error = (
                "Command:\n{command}\n\n"
                "Return code: {rc}\n\n"
                "STDOUT:\n{stdout}\n\n"
                "STDERR:\n{stderr}"
            ).format(
                command=command_text,
                rc=rc,
                stdout=stdout_data,
                stderr=stderr_data,
            )

            for item in items:
                item_id = get_item_value(item, "id")
                safe_mark_item_failed(
                    item_id,
                    error_message=error_text[:1000],
                    duration_seconds=duration,
                )

            refresh_job_progress(job_id)
            safe_mark_job_failed(job_id, full_error[:4000])

    except Exception as e:
        err = "{}\n{}".format(str(e), traceback.format_exc())

        try:
            safe_mark_job_failed(job_id, err[:4000])
        except Exception:
            pass

        try:
            items = get_job_items(job_id)

            for item in items:
                item_id = get_item_value(item, "id")
                status = get_item_value(item, "status")

                if status in ("queued", "running"):
                    safe_mark_item_failed(
                        item_id,
                        error_message=str(e),
                    )

            refresh_job_progress(job_id)

        except Exception:
            pass

    finally:
        if include_file and os.path.exists(include_file):
            try:
                os.remove(include_file)
            except Exception:
                pass

        if include_json_file and os.path.exists(include_json_file):
            try:
                os.remove(include_json_file)
            except Exception:
                pass