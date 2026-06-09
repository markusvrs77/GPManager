import os
import json
import time
import tempfile
import subprocess
import traceback
import psycopg2
from psycopg2.extras import RealDictCursor
import sqlite3
from datetime import datetime

from job_manager import (
    get_job,
    get_job_items,
    mark_job_running,
    mark_job_done,
    mark_item_running,
    refresh_job_progress,
)


def get_item_value(obj, key, default=None):
    """
    Универсально достаёт значение из dict или sqlite3.Row.
    """
    if obj is None:
        return default

    if isinstance(obj, dict):
        return obj.get(key, default)

    try:
        return obj[key]
    except Exception:
        return default


try:
    from job_manager import safe_mark_job_failed
except ImportError:
    def safe_mark_job_failed(job_id, message):
        try:
            from job_manager import mark_job_failed
            return mark_job_failed(job_id, message)
        except Exception:
            print(f"[gpcopy_sync] job {job_id} failed: {message}")


try:
    from job_manager import safe_mark_item_done
except ImportError:
    def safe_mark_item_done(item_id, message=None):
        try:
            from job_manager import mark_item_done
            return mark_item_done(item_id, message)
        except Exception:
            print(f"[gpcopy_sync] item {item_id} done")


try:
    from job_manager import safe_mark_item_failed
except ImportError:
    def safe_mark_item_failed(item_id, message):
        try:
            from job_manager import mark_item_failed
            return mark_item_failed(item_id, message)
        except Exception:
            print(f"[gpcopy_sync] item {item_id} failed: {message}")


try:
    from job_manager import is_stop_requested
except ImportError:
    def is_stop_requested(job_id):
        return False


try:
    from job_manager import safe_mark_job_cancelled
except ImportError:
    def safe_mark_job_cancelled(job_id, message="Cancelled"):
        try:
            from job_manager import mark_job_cancelled
            return mark_job_cancelled(job_id, message)
        except Exception:
            print(f"[gpcopy_sync] job {job_id} cancelled: {message}")


from db import get_connection_by_id


DEFAULT_GPCOPY_PATH = "/usr/local/gpdb/greenplum-db/bin/gpcopy"


def run_gpcopy_sync_job(job_id):
    """
    Реальный Prod -> Test sync:
    1. Создаёт staging table на TEST
    2. Через gpcopy грузит PROD -> TEST staging
    3. На TEST делает DELETE / UPDATE / INSERT по key_columns
    """

    try:
        print(f"[gpcopy_sync] starting job {job_id}")

        sync_update_job_status(job_id, "running", "GPCOPY SYNC started")

        job = get_job(job_id)

        if not job:
            raise Exception(f"Job not found: {job_id}")

        config_json = get_item_value(job, "config_json", "{}")

        try:
            config = json.loads(config_json or "{}")
        except Exception:
            config = {}

        source_connection_id = config.get("source_connection_id")
        dest_connection_id = config.get("dest_connection_id")
        table_configs = config.get("table_configs") or []
        gpcopy_path = config.get("gpcopy_path") or DEFAULT_GPCOPY_PATH
        jobs = int(config.get("jobs") or 4)

        if not source_connection_id:
            raise Exception("source_connection_id is empty")

        if not dest_connection_id:
            raise Exception("dest_connection_id is empty")

        if not table_configs:
            raise Exception("table_configs is empty")

        source_host = get_conn_host(source_connection_id)
        dest_host = get_conn_host(dest_connection_id)
        source_port = get_conn_port(source_connection_id)
        dest_port = get_conn_port(dest_connection_id)
        dest_user = get_conn_user(dest_connection_id)

        source_db = get_conn_dbname(source_connection_id)
        dest_db = get_conn_dbname(dest_connection_id)

        items = get_job_items(job_id) or []

        if not items:
            raise Exception("job_items is empty")

        source_conn = open_conn(source_connection_id)
        target_conn = open_conn(dest_connection_id)
        target_conn.autocommit = False

        try:
            for item in items:
                item_id = get_item_value(item, "id")
                schema_name = get_item_value(item, "schema_name", "")
                table_name = get_item_value(item, "table_name", "")

                try:
                    if is_stop_requested(job_id):
                        if item_id:
                            sync_update_item_status(item_id, "cancelled", "Stop requested")

                        sync_update_job_status(job_id, "cancelled", "Stop requested")
                        refresh_job_progress(job_id)
                        return

                    if item_id:
                        sync_update_item_status(item_id, "running")

                    print(f"[gpcopy_sync] processing {schema_name}.{table_name}")

                    table_cfg = None

                    for cfg in table_configs:
                        cfg_schema = cfg.get("schema") or ""
                        cfg_table = cfg.get("table") or ""

                        if cfg_schema == schema_name and cfg_table == table_name:
                            table_cfg = cfg
                            break

                    if not table_cfg:
                        raise Exception(f"Config not found for {schema_name}.{table_name}")

                    source_schema, source_table = split_table_name(table_cfg["source"])
                    target_schema, target_table = split_table_name(table_cfg["target"])

                    key_columns = table_cfg.get("key_columns") or []
                    compare_columns = table_cfg.get("compare_columns") or ["*"]
                    delete_missing = bool(table_cfg.get("delete_missing"))

                    if not key_columns:
                        raise Exception(f"Key columns empty for {schema_name}.{table_name}")

                    common_cols, compare_cols = normalize_columns(
                        source_conn,
                        target_conn,
                        source_schema,
                        source_table,
                        target_schema,
                        target_table,
                        key_columns,
                        compare_columns,
                    )

                    if not common_cols:
                        raise Exception("No common columns found")

                    stage_schema = "gpmanager_sync_stage"
                    stage_table = safe_stage_name(target_schema, target_table, job_id)

                    target_full = full_table(target_schema, target_table)
                    stage_full = full_table(stage_schema, stage_table)

                    # 1. Create stage schema/table on TEST
                    print(f"[gpcopy_sync] create staging {stage_schema}.{stage_table}")

                    run_sql_execute(
                        target_conn,
                        f"CREATE SCHEMA IF NOT EXISTS {qident(stage_schema)}"
                    )

                    run_sql_execute(
                        target_conn,
                        f"DROP TABLE IF EXISTS {stage_full}"
                    )

                    run_sql_execute(
                        target_conn,
                        f"""
                        CREATE TABLE {stage_full}
                        AS
                        SELECT {build_cols_list(common_cols)}
                        FROM {target_full}
                        WHERE 1 = 0
                        DISTRIBUTED RANDOMLY
                        """
                    )

                    target_conn.commit()

                    # 2. Build include-table-json for gpcopy
                    source_cols_sql = build_cols_list(common_cols)
                    source_sql = f"""
                        SELECT {source_cols_sql}
                        FROM {full_table(source_schema, source_table)}
                    """

                    include_json = [
                        {
                            "source": f"{source_db}.{source_schema}.{source_table}",
                            "dest": f"{dest_db}.{stage_schema}.{stage_table}",
                            "sql": source_sql,
                        }
                    ]

                    fd, include_json_file = tempfile.mkstemp(
                        prefix=f"gpcopy_sync_{job_id}_",
                        suffix=".json",
                        text=True,
                    )

                    try:
                        with os.fdopen(fd, "w", encoding="utf-8") as f:
                            json.dump(include_json, f, ensure_ascii=False, indent=2)

                        cmd = build_gpcopy_sync_command(
                            gpcopy_path=gpcopy_path,
                            source_host=source_host,
                            dest_host=dest_host,
                            source_port=source_port,
                            dest_port=dest_port,
                            dest_user=dest_user,
                            include_json_file=include_json_file,
                            jobs=jobs,
                        )

                        print("[gpcopy_sync] command:", " ".join(cmd))

                        proc = subprocess.run(
                            cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True,
                        )

                        print(proc.stdout)

                        if proc.returncode != 0:
                            raise Exception(f"gpcopy failed with code {proc.returncode}: {proc.stdout[-4000:]}")

                    finally:
                        try:
                            os.remove(include_json_file)
                        except Exception:
                            pass

                    # 3. Проверяем, сколько строк пришло в staging
                    stage_count_row = run_sql_fetch_one(
                        target_conn,
                        f"SELECT COUNT(*) AS cnt FROM {stage_full}"
                    )

                    stage_count = int(stage_count_row.get("cnt") or 0)

                    print(f"[gpcopy_sync] staging rows: {stage_count}")

                    # 4. Проверяем дубли ключа в staging
                    key_cols_expr = build_cols_list(key_columns)

                    duplicate_row = run_sql_fetch_one(
                        target_conn,
                        f"""
                        SELECT COUNT(*) AS duplicate_keys
                        FROM (
                            SELECT {key_cols_expr}
                            FROM {stage_full}
                            GROUP BY {key_cols_expr}
                            HAVING COUNT(*) > 1
                            LIMIT 1
                        ) x
                        """
                    )

                    duplicate_keys = int(duplicate_row.get("duplicate_keys") or 0)

                    if duplicate_keys > 0:
                        raise Exception(
                            f"Duplicate key detected in source/staging for {schema_name}.{table_name}. "
                            f"Key columns: {', '.join(key_columns)}"
                        )

                    key_cond = build_key_condition("t", "s", key_columns)

                    target_hash = build_hash_expr("t", compare_cols)
                    stage_hash = build_hash_expr("s", compare_cols)

                    insert_cols = build_cols_list(common_cols)
                    insert_select_cols = build_cols_list(common_cols, "s")

                    update_set = ", ".join([
                        f"{qident(c)} = s.{qident(c)}"
                        for c in compare_cols
                    ])

                    # 5. Preview counts
                    insert_count = int(run_sql_fetch_one(
                        target_conn,
                        f"""
                        SELECT COUNT(*) AS cnt
                        FROM {stage_full} s
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {target_full} t
                            WHERE {key_cond}
                        )
                        """
                    ).get("cnt") or 0)

                    update_count = int(run_sql_fetch_one(
                        target_conn,
                        f"""
                        SELECT COUNT(*) AS cnt
                        FROM {target_full} t
                        JOIN {stage_full} s
                          ON {key_cond}
                        WHERE {target_hash} <> {stage_hash}
                        """
                    ).get("cnt") or 0)

                    delete_count = 0

                    if delete_missing:
                        delete_count = int(run_sql_fetch_one(
                            target_conn,
                            f"""
                            SELECT COUNT(*) AS cnt
                            FROM {target_full} t
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM {stage_full} s
                                WHERE {key_cond}
                            )
                            """
                        ).get("cnt") or 0)

                    print(
                        f"[gpcopy_sync] diff {schema_name}.{table_name}: "
                        f"insert={insert_count}, update={update_count}, delete={delete_count}"
                    )

                    # 6. DELETE missing
                    if delete_missing:
                        run_sql_execute(
                            target_conn,
                            f"""
                            DELETE FROM {target_full} t
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM {stage_full} s
                                WHERE {key_cond}
                            )
                            """
                        )

                    # 7. UPDATE changed
                    if update_set:
                        run_sql_execute(
                            target_conn,
                            f"""
                            UPDATE {target_full} t
                            SET {update_set}
                            FROM {stage_full} s
                            WHERE {key_cond}
                              AND {target_hash} <> {stage_hash}
                            """
                        )

                    # 8. INSERT new
                    run_sql_execute(
                        target_conn,
                        f"""
                        INSERT INTO {target_full} ({insert_cols})
                        SELECT {insert_select_cols}
                        FROM {stage_full} s
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {target_full} t
                            WHERE {key_cond}
                        )
                        """
                    )

                    target_conn.commit()

                    # 9. Drop staging
                    try:
                        run_sql_execute(
                            target_conn,
                            f"DROP TABLE IF EXISTS {stage_full}"
                        )
                        target_conn.commit()
                    except Exception:
                        target_conn.rollback()

                    done_message = (
                        f"stage={stage_count}; "
                        f"insert={insert_count}; "
                        f"update={update_count}; "
                        f"delete={delete_count}"
                    )

                    if item_id:
                        sync_update_item_status(item_id, "done", done_message)

                    refresh_job_progress(job_id)

                except Exception as item_error:
                    target_conn.rollback()

                    print(f"[gpcopy_sync] item failed: {schema_name}.{table_name}: {item_error}")
                    traceback.print_exc()

                    if item_id:
                        sync_update_item_status(item_id, "failed", str(item_error))

                    refresh_job_progress(job_id)

            refresh_job_progress(job_id)

            # Проверяем failed items
            conn = gpmanager_sqlite_conn()

            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM job_items
                    WHERE job_id = ?
                      AND status = 'failed'
                    """,
                    (job_id,)
                )
                failed_count = cur.fetchone()["cnt"]
            finally:
                conn.close()

            if failed_count:
                sync_update_job_status(job_id, "failed", f"Finished with {failed_count} failed items")
            else:
                sync_update_job_status(job_id, "done", "GPCOPY SYNC done")

            refresh_job_progress(job_id)

            print(f"[gpcopy_sync] job {job_id} finished")

        finally:
            source_conn.close()
            target_conn.close()

    except Exception as e:
        traceback.print_exc()
        sync_update_job_status(job_id, "failed", str(e))
        refresh_job_progress(job_id)

def qident(name):
    return '"' + str(name).replace('"', '""') + '"'


def split_table_name(full_name):
    parts = full_name.split(".")

    if len(parts) == 2:
        return parts[0], parts[1]

    if len(parts) == 3:
        return parts[1], parts[2]

    raise Exception("Invalid table name: {}".format(full_name))


def conn_value(cfg, *names):
    try:
        cfg = dict(cfg)
    except Exception:
        pass

    for name in names:
        if isinstance(cfg, dict) and cfg.get(name):
            return cfg.get(name)

    return None


def open_conn(connection_id):
    cfg = get_connection_by_id(int(connection_id))

    if not cfg:
        raise Exception("Connection not found: {}".format(connection_id))

    host = conn_value(cfg, "host", "hostname", "server")
    port = conn_value(cfg, "port", "db_port") or 5432
    dbname = conn_value(cfg, "database_name", "database", "dbname", "db_name")
    user = conn_value(cfg, "username", "user", "login")
    password = conn_value(cfg, "password", "passwd") or ""

    if not host:
        raise Exception("Connection host is empty")

    if not dbname:
        raise Exception("Connection dbname is empty")

    if not user:
        raise Exception("Connection user is empty")

    return psycopg2.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
    )


def get_conn_dbname(connection_id):
    cfg = get_connection_by_id(int(connection_id))
    return conn_value(cfg, "database_name", "database", "dbname", "db_name")


def get_conn_host(connection_id):
    cfg = get_connection_by_id(int(connection_id))
    return conn_value(cfg, "host", "hostname", "server")


def get_conn_user(connection_id):
    cfg = get_connection_by_id(int(connection_id))
    return conn_value(cfg, "username", "user", "login") or "gpadmin"


def get_table_columns(conn, schema_name, table_name):
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (schema_name, table_name))
        return [r["column_name"] for r in cur.fetchall()]


def normalize_columns(source_conn, target_conn, source_schema, source_table, target_schema, target_table, key_columns, compare_columns):
    source_cols = get_table_columns(source_conn, source_schema, source_table)
    target_cols = get_table_columns(target_conn, target_schema, target_table)

    common_cols = [c for c in source_cols if c in target_cols]

    for key in key_columns:
        if key not in common_cols:
            raise Exception("Key column {} not found in both tables".format(key))

    if compare_columns == ["*"]:
        compare_cols = [c for c in common_cols if c not in key_columns]
    else:
        compare_cols = [c for c in compare_columns if c in common_cols and c not in key_columns]

    if not compare_cols:
        raise Exception("No compare columns found")

    return common_cols, compare_cols


def build_key_condition(left_alias, right_alias, key_columns):
    return " AND ".join([
        "{}.{} IS NOT DISTINCT FROM {}.{}".format(
            left_alias,
            qident(c),
            right_alias,
            qident(c),
        )
        for c in key_columns
    ])


def build_hash_expr(alias, columns):
    concat_parts = [
        "COALESCE({}.{}::text, '<NULL>')".format(alias, qident(c))
        for c in columns
    ]

    return "md5(CONCAT_WS('|', {}))".format(", ".join(concat_parts))


def preview_gpcopy_sync(data):
    source_connection_id = data.get("source_connection_id")
    dest_connection_id = data.get("dest_connection_id")
    table_configs = data.get("table_configs") or []

    if not source_connection_id:
        raise Exception("source_connection_id is required")

    if not dest_connection_id:
        raise Exception("dest_connection_id is required")

    result = {
        "ok": True,
        "tables": [],
        "total_insert": 0,
        "total_update": 0,
        "total_delete": 0,
    }

    source_conn = open_conn(source_connection_id)
    target_conn = open_conn(dest_connection_id)

    try:
        for cfg in table_configs:
            source_schema, source_table = split_table_name(cfg["source"])
            target_schema, target_table = split_table_name(cfg["target"])

            key_columns = cfg.get("key_columns") or []
            compare_columns = cfg.get("compare_columns") or ["*"]
            delete_missing = bool(cfg.get("delete_missing"))

            common_cols, compare_cols = normalize_columns(
                source_conn,
                target_conn,
                source_schema,
                source_table,
                target_schema,
                target_table,
                key_columns,
                compare_columns,
            )

            # preview без gpcopy: считаем по dblink невозможно без extension.
            # Поэтому preview здесь только валидирует структуру.
            item = {
                "source": cfg["source"],
                "target": cfg["target"],
                "key_columns": key_columns,
                "compare_columns": compare_cols,
                "delete_missing": delete_missing,
                "insert_count": None,
                "update_count": None,
                "delete_count": None,
                "message": "Structure validated. Counts will be calculated during apply after staging load.",
            }

            result["tables"].append(item)

        return result

    finally:
        source_conn.close()
        target_conn.close()


def gpmanager_sqlite_path():
    db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "instance",
        "gp_reorganize_center.sqlite3"
    )

    if not os.path.exists(db_path):
        db_path = os.path.join("instance", "gp_reorganize_center.sqlite3")

    return db_path


def gpmanager_sqlite_conn():
    conn = sqlite3.connect(gpmanager_sqlite_path())
    conn.row_factory = sqlite3.Row
    return conn


def sync_update_job_status(job_id, status, message=None):
    conn = gpmanager_sqlite_conn()

    try:
        cur = conn.cursor()

        cur.execute("PRAGMA table_info(jobs)")
        cols = [row["name"] for row in cur.fetchall()]

        set_parts = ["status = ?"]
        params = [status]

        if "message" in cols:
            set_parts.append("message = COALESCE(?, message)")
            params.append(message)

        elif "error_message" in cols and message:
            set_parts.append("error_message = ?")
            params.append(message)

        if "updated_at" in cols:
            set_parts.append("updated_at = CURRENT_TIMESTAMP")

        params.append(job_id)

        sql = f"""
            UPDATE jobs
            SET {", ".join(set_parts)}
            WHERE id = ?
        """

        cur.execute(sql, params)
        conn.commit()

    finally:
        conn.close()


def sync_update_item_status(item_id, status, error_message=None):
    conn = gpmanager_sqlite_conn()

    try:
        cur = conn.cursor()

        cur.execute("PRAGMA table_info(job_items)")
        cols = [row["name"] for row in cur.fetchall()]

        set_parts = ["status = ?"]
        params = [status]

        if "error_message" in cols:
            set_parts.append("error_message = ?")
            params.append(error_message)

        if status == "running":
            if "started_at" in cols:
                set_parts.append("started_at = COALESCE(started_at, CURRENT_TIMESTAMP)")

        if status in ("done", "failed", "skipped", "cancelled"):
            if "finished_at" in cols:
                set_parts.append("finished_at = CURRENT_TIMESTAMP")

            if "duration_seconds" in cols and "started_at" in cols:
                set_parts.append("""
                    duration_seconds = CASE
                        WHEN started_at IS NOT NULL
                        THEN CAST((julianday(CURRENT_TIMESTAMP) - julianday(started_at)) * 86400 AS INTEGER)
                        ELSE duration_seconds
                    END
                """)

        if "updated_at" in cols:
            set_parts.append("updated_at = CURRENT_TIMESTAMP")

        params.append(item_id)

        sql = f"""
            UPDATE job_items
            SET {", ".join(set_parts)}
            WHERE id = ?
        """

        cur.execute(sql, params)
        conn.commit()

    finally:
        conn.close()

def safe_stage_name(schema_name, table_name, job_id):
    raw = f"stg_{job_id}_{schema_name}_{table_name}"
    safe = ""

    for ch in raw:
        if ch.isalnum() or ch == "_":
            safe += ch
        else:
            safe += "_"

    return safe[:60]


def full_table(schema_name, table_name):
    return f"{qident(schema_name)}.{qident(table_name)}"


def build_cols_list(columns, alias=None):
    if alias:
        return ", ".join([f"{alias}.{qident(c)}" for c in columns])

    return ", ".join([qident(c) for c in columns])


def run_sql_fetch_one(conn, sql, params=None):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or [])
        row = cur.fetchone()
        return dict(row) if row else {}


def run_sql_execute(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])


def get_conn_port(connection_id):
    cfg = get_connection_by_id(int(connection_id))
    return conn_value(cfg, "port", "db_port") or 5432


def build_gpcopy_sync_command(
    gpcopy_path,
    source_host,
    dest_host,
    source_port,
    dest_port,
    dest_user,
    include_json_file,
    jobs=4,
):
    cmd = [
        gpcopy_path,
        "--source-host", str(source_host),
        "--source-port", str(source_port),
        "--dest-host", str(dest_host),
        "--dest-port", str(dest_port),
        "--dest-user", str(dest_user),
        "--include-table-json", include_json_file,
        "--append",
        "--jobs", str(jobs),
        "--on-segment-threshold", "-1",
    ]

    return cmd