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


# ------------------------------------------------------------
# Быстрый diff по статистике каталога (reltuples, один запрос
# на сторону для всех таблиц сразу) + батч-COUNT для точного режима.
# ------------------------------------------------------------

_LEAF_STATS_CTE = """
    WITH RECURSIVE tree AS (
        SELECT c.oid, c.oid AS root_oid,
               n.nspname AS root_schema, c.relname AS root_table
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE (n.nspname, c.relname) IN %s
        UNION ALL
        SELECT child.oid, tree.root_oid, tree.root_schema, tree.root_table
        FROM tree
        JOIN pg_inherits i ON i.inhparent = tree.oid
        JOIN pg_class child ON child.oid = i.inhrelid
    )
    SELECT t.root_schema, t.root_table,
           n.nspname AS leaf_schema, c.relname AS leaf_table,
           c.reltuples::bigint AS rows
    FROM tree t
    JOIN pg_class c ON c.oid = t.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_inherits i2 WHERE i2.inhparent = t.oid
    )
    ORDER BY t.root_schema, t.root_table, c.relname
"""


def fetch_leaf_stats(conn, tables):
    """
    Один запрос: leaf-партиции и reltuples для ВСЕХ переданных таблиц.
    tables: [(schema, table)].
    -> {(root_schema, root_table): {leaf_table: {"schema","table","rows"}}}
    Непартиционированная таблица возвращается сама как единственный leaf.
    """
    if not tables:
        return {}

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(_LEAF_STATS_CTE, (tuple(tables),))
        rows = cur.fetchall()

    out = {}
    for r in rows:
        root = (r["root_schema"], r["root_table"])
        out.setdefault(root, {})[r["leaf_table"]] = {
            "schema": r["leaf_schema"],
            "table": r["leaf_table"],
            "rows": int(r["rows"]),
        }
    return out


def classify_stats_maps(src_stats, dest_stats):
    """
    Чистая функция: покорневая классификация по статистике.
    -> {root_key: [{partition, src_count, dest_count, action}]}
    """
    out = {}
    for root, leaves in src_stats.items():
        src_counts = {name: info["rows"] for name, info in leaves.items()}
        dest_counts = {
            name: info["rows"]
            for name, info in (dest_stats.get(root) or {}).items()
        }
        out[root] = classify_partition_diff(src_counts, dest_counts)
    return out


def build_batched_count_sql(leaves, chunk=50):
    """
    SQL-чанки для точного пересчёта: один запрос считает до `chunk` партиций
    через UNION ALL (вместо round-trip на каждую).
    leaves: [(schema, table)] -> ["SELECT 'p1', count(*) FROM ... UNION ALL ...", ...]
    """
    chunks = []
    for i in range(0, len(leaves), chunk):
        parts = [
            "SELECT '{}' AS partition, count(*) AS cnt FROM {}.{}".format(
                t.replace("'", "''"), quote_ident(s), quote_ident(t)
            )
            for s, t in leaves[i:i + chunk]
        ]
        chunks.append(" UNION ALL ".join(parts))
    return chunks


def fetch_counts_batched(conn, leaves, chunk=50):
    """Точные COUNT(*) батчами. Несуществующие партиции просто пропускаются."""
    counts = {}
    for sql in build_batched_count_sql(leaves, chunk):
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                for name, cnt in cur.fetchall():
                    counts[name] = int(cnt)
        except Exception:
            conn.rollback()
            # чанк с несуществующей таблицей — падаем на поштучный счёт
            for s, t in leaves:
                if t in counts:
                    continue
                try:
                    counts[t] = _count_rows(conn, s, t)
                except Exception:
                    conn.rollback()
    return counts


def diff_partitions_stats(source_cfg, dest_cfg, tables, exact=False):
    """
    Diff для пачки таблиц: по reltuples (мгновенно) или точный (COUNT батчами).
    tables: [(schema, table)].
    -> ({root_key: diff_rows}, {root_key: {leaf_name: (schema, table)}})
    """
    source_conn = open_psycopg2_connection_by_cfg(source_cfg)
    dest_conn = open_psycopg2_connection_by_cfg(dest_cfg)

    try:
        src_stats = fetch_leaf_stats(source_conn, tables)
        dest_stats = fetch_leaf_stats(dest_conn, tables)

        leaves_by_root = {
            root: {name: (info["schema"], info["table"])
                   for name, info in leaves.items()}
            for root, leaves in src_stats.items()
        }

        if not exact:
            return classify_stats_maps(src_stats, dest_stats), leaves_by_root

        # Точный режим: reltuples заменяем настоящими COUNT(*) батчами.
        all_leaves = [
            (info["schema"], info["table"])
            for leaves in src_stats.values()
            for info in leaves.values()
        ]
        src_counts_flat = fetch_counts_batched(source_conn, all_leaves)
        dest_leaves = [
            lv for root, leaves in leaves_by_root.items()
            for name, lv in leaves.items()
            if name in (dest_stats.get(root) or {})
        ]
        dest_counts_flat = fetch_counts_batched(dest_conn, dest_leaves)

        out = {}
        for root, leaves in src_stats.items():
            src_counts = {n: src_counts_flat.get(n, l["rows"])
                          for n, l in leaves.items()}
            dest_present = dest_stats.get(root) or {}
            dest_counts = {n: dest_counts_flat[n]
                           for n in dest_present if n in dest_counts_flat}
            out[root] = classify_partition_diff(src_counts, dest_counts)
        return out, leaves_by_root

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

        explicit = config.get("partitions") or []

        if explicit:
            # UI уже показал diff и пользователь отметил, что переливать.
            copy_items = [
                {"schema_name": p.get("schema") or p.get("schema_name"),
                 "table_name": p.get("table") or p.get("table_name")}
                for p in explicit
            ]
        else:
            # Diff по всем таблицам сразу: stats (reltuples, мгновенно) —
            # дефолт; count_mode="exact" пересчитывает COUNT(*) батчами.
            exact = (config.get("count_mode") or "stats") == "exact"
            roots = [
                (entry.get("schema") or entry.get("schema_name"),
                 entry.get("table") or entry.get("table_name"))
                for entry in tables
            ]

            diff_by_root, leaves_by_root = diff_partitions_stats(
                source_cfg, dest_cfg, roots, exact=exact,
            )

            copy_items = []
            for root, diff_rows in diff_by_root.items():
                leaf_by_name = leaves_by_root.get(root) or {}
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
