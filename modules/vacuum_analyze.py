from __future__ import print_function

import json
import time
import traceback
import psycopg2

from psycopg2 import sql

from modules.connections import get_connection_by_id

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
    is_stop_requested,
    clear_stop_flag,
    refresh_job_progress,
)


def conn_get(connection, key, default=None):
    try:
        value = connection.get(key)
        if value is not None:
            return value
    except Exception:
        pass

    try:
        value = connection[key]
        if value is not None:
            return value
    except Exception:
        pass

    try:
        value = getattr(connection, key)
        if value is not None:
            return value
    except Exception:
        pass

    return default


def open_gp_connection(connection):
    host = (
        conn_get(connection, "host")
        or conn_get(connection, "hostname")
        or conn_get(connection, "server")
        or conn_get(connection, "ip")
    )

    port = (
        conn_get(connection, "port")
        or conn_get(connection, "db_port")
        or 5432
    )

    database = (
        conn_get(connection, "database")
        or conn_get(connection, "dbname")
        or conn_get(connection, "db_name")
        or conn_get(connection, "database_name")
        or conn_get(connection, "databaseName")
        or conn_get(connection, "db")
    )

    user = (
        conn_get(connection, "username")
        or conn_get(connection, "user")
        or conn_get(connection, "login")
        or conn_get(connection, "db_user")
    )

    password = (
        conn_get(connection, "password")
        or conn_get(connection, "passwd")
        or conn_get(connection, "db_password")
    )

    if not host:
        raise Exception("Connection host is empty. Connection data: {}".format(connection))

    if not database:
        raise Exception("Connection database/dbname is empty. Connection data: {}".format(connection))

    if not user:
        raise Exception("Connection username/user is empty. Connection data: {}".format(connection))

    return psycopg2.connect(
        host=host,
        port=int(port),
        dbname=database,
        user=user,
        password=password,
    )


def quote_table(schema_name, table_name):
    return sql.SQL("{}.{}").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )


def build_vacuum_sql(schema_name, table_name, action):
    action = str(action or "").upper().strip()
    table_ident = quote_table(schema_name, table_name)

    if action == "VACUUM":
        return sql.SQL("VACUUM {}").format(table_ident)

    if action == "VACUUM_FULL":
        return sql.SQL("VACUUM FULL {}").format(table_ident)

    if action == "ANALYZE":
        return sql.SQL("ANALYZE {}").format(table_ident)

    if action == "VACUUM_ANALYZE":
        return sql.SQL("VACUUM ANALYZE {}").format(table_ident)

    if action == "VACUUM_FREEZE":
        return sql.SQL("VACUUM FREEZE {}").format(table_ident)

    raise ValueError("Unknown vacuum/analyze action: {}".format(action))


def parse_job_config(job):
    config = job.get("config")

    if isinstance(config, dict):
        return config

    config_json = job.get("config_json")

    if not config_json:
        return {}

    try:
        return json.loads(config_json)
    except Exception:
        return {}


def run_vacuum_analyze_job(job_id):
    job = get_job(job_id)

    if not job:
        return

    connection_id = job.get("connection_id")
    config = parse_job_config(job)

    action = str(config.get("action") or "VACUUM_ANALYZE").upper().strip()

    conn = None

    try:
        mark_job_running(job_id)

        connection = get_connection_by_id(connection_id)

        if not connection:
            raise Exception("Connection not found: {}".format(connection_id))

        conn = open_gp_connection(connection)

        # VACUUM нельзя выполнять внутри транзакции
        conn.autocommit = True

        items = get_job_items(job_id)

        for item in items:
            item_id = item["id"]
            schema_name = item["schema_name"]
            table_name = item["table_name"]

            if is_stop_requested(job_id):
                mark_item_skipped(item_id, "Job stopped by user")
                continue

            mark_item_running(item_id)

            try:
                item_action = (
                        item.get("action")
                        or action
                        or "VACUUM_ANALYZE"
                )

                item_action = str(item_action).upper().strip()

                query = build_vacuum_sql(
                    schema_name=schema_name,
                    table_name=table_name,
                    action=item_action,
                )

                with conn.cursor() as cur:
                    cur.execute(query)

                mark_item_done(item_id)

            except Exception as e:
                mark_item_failed(item_id, str(e))

            refresh_job_progress(job_id)

        if is_stop_requested(job_id):
            mark_job_cancelled(job_id)
        else:
            mark_job_done(job_id)

    except Exception as e:
        mark_job_failed(job_id, str(e))
        traceback.print_exc()

    finally:
        clear_stop_flag(job_id)

        if conn:
            try:
                conn.close()
            except Exception:
                pass