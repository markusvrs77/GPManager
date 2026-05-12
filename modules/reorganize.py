import time
import traceback

from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from modules.connections import open_gp_connection

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


def normalize_distkey(distkey):
    """
    Greenplum 7.3 gp_distribution_policy.distkey может прийти по-разному:
    - None
    - []
    - [1, 2]
    - '{1,2}'
    - '1'
    - int
    """

    if distkey is None:
        return []

    if isinstance(distkey, (list, tuple)):
        result = []
        for x in distkey:
            try:
                result.append(int(x))
            except Exception:
                pass
        return result

    if isinstance(distkey, int):
        return [distkey]

    s = str(distkey).strip()

    if not s:
        return []

    if s.startswith("{") and s.endswith("}"):
        inner = s[1:-1].strip()

        if not inner:
            return []

        result = []

        for x in inner.split(","):
            x = x.strip()
            if not x:
                continue

            try:
                result.append(int(x))
            except Exception:
                pass

        return result

    try:
        return [int(s)]
    except Exception:
        return []


def get_table_metadata(conn, schema_name, table_name):
    """
    Получаем метаданные таблицы для Greenplum 7.3.

    Возвращает:
    {
        oid,
        policytype,
        distkey,
        access_method,
        total_bytes
    }

    policytype:
    - r = replicated
    - если distkey есть = hash distributed
    - если distkey пустой = randomly distributed
    """

    query = """
        SELECT
            c.oid,
            dp.policytype,
            dp.distkey,
            am.amname,
            pg_total_relation_size(c.oid) AS total_bytes
        FROM pg_class c
        JOIN pg_namespace n
            ON n.oid = c.relnamespace
        LEFT JOIN gp_distribution_policy dp
            ON dp.localoid = c.oid
        LEFT JOIN pg_am am
            ON am.oid = c.relam
        WHERE n.nspname = %s
          AND c.relname = %s
        LIMIT 1
    """

    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        row = cur.fetchone()

    if not row:
        return None

    oid, policytype, distkey_raw, amname, total_bytes = row

    distkey = normalize_distkey(distkey_raw)

    access_method = "HEAP"

    if amname:
        am = amname.lower()

        if am in ("ao_row", "ao", "ao_row_plain"):
            access_method = "AO"
        elif am in ("ao_column", "aocs", "ao_column_plain", "columnar"):
            access_method = "AOCO"
        elif am == "heap":
            access_method = "HEAP"
        else:
            access_method = amname.upper()

    return {
        "oid": oid,
        "policytype": policytype,
        "distkey": distkey,
        "access_method": access_method,
        "total_bytes": total_bytes or 0,
    }


def get_distribution_columns(conn, oid, distkey):
    """
    По номерам колонок из gp_distribution_policy.distkey получаем имена колонок.
    """

    if not distkey:
        return []

    arr = []

    for x in distkey:
        try:
            arr.append(int(x))
        except Exception:
            pass

    if not arr:
        return []

    query = """
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = %s
          AND attnum = ANY(%s)
        ORDER BY array_position(%s::smallint[], attnum)
    """

    with conn.cursor() as cur:
        cur.execute(query, (oid, arr, arr))
        rows = cur.fetchall()

    return [r[0] for r in rows]


def get_distribution_type(policytype, dist_cols):
    if policytype == "r":
        return "REPLICATED"

    if dist_cols:
        return "HASH"

    return "RANDOM"


def build_reorganize_sql(schema_name, table_name, policytype, dist_cols):
    """
    Важно:
    HASH таблицы надо реорганизовать с сохранением DISTRIBUTED BY.
    RANDOM / REPLICATED — без DISTRIBUTED BY.

    HASH:
        ALTER TABLE schema.table
        SET WITH (REORGANIZE=true)
        DISTRIBUTED BY (col1, col2)

    RANDOM / REPLICATED:
        ALTER TABLE schema.table
        SET WITH (REORGANIZE=true)
    """

    if policytype == "r":
        return sql.SQL(
            "ALTER TABLE {}.{} SET WITH (REORGANIZE=true)"
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )

    if dist_cols:
        return sql.SQL(
            "ALTER TABLE {}.{} SET WITH (REORGANIZE=true) DISTRIBUTED BY ({})"
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join([sql.Identifier(c) for c in dist_cols]),
        )

    return sql.SQL(
        "ALTER TABLE {}.{} SET WITH (REORGANIZE=true)"
    ).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )


def get_reorganize_targets(connection_id, schema_name, table_name):
    """
    Если таблица обычная — возвращает её саму.
    Если таблица parent partition — возвращает leaf partitions.

    UI показывает parent tables.
    Backend выполняет REORGANIZE по leaf partitions.
    """

    conn = open_gp_connection(connection_id)

    query = """
        WITH RECURSIVE tree AS (
            SELECT
                c.oid,
                n.nspname AS schema_name,
                c.relname AS table_name,
                0 AS level_no
            FROM pg_class c
            JOIN pg_namespace n
                ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s

            UNION ALL

            SELECT
                child.oid,
                child_ns.nspname AS schema_name,
                child.relname AS table_name,
                tree.level_no + 1 AS level_no
            FROM tree
            JOIN pg_inherits i
                ON i.inhparent = tree.oid
            JOIN pg_class child
                ON child.oid = i.inhrelid
            JOIN pg_namespace child_ns
                ON child_ns.oid = child.relnamespace
        )
        SELECT
            t.oid,
            t.schema_name,
            t.table_name,
            t.level_no,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM pg_inherits i2
                    WHERE i2.inhparent = t.oid
                )
                THEN false
                ELSE true
            END AS is_leaf
        FROM tree t
        ORDER BY
            t.level_no,
            t.schema_name,
            t.table_name
    """

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (schema_name, table_name))
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    leaf_rows = []

    for row in rows:
        if row["is_leaf"]:
            leaf_rows.append(
                {
                    "schema_name": row["schema_name"],
                    "table_name": row["table_name"],
                }
            )

    return leaf_rows


def reorganize_table(connection_id, schema_name, table_name):
    """
    Выполняет REORGANIZE одной таблицы/partition.

    Использует логику:
    - определить distribution
    - если HASH, сохранить DISTRIBUTED BY (...)
    - если RANDOM/REPLICATED, выполнить простой REORGANIZE
    """

    started_at = time.time()
    conn = open_gp_connection(connection_id)

    try:
        meta = get_table_metadata(conn, schema_name, table_name)

        if not meta:
            return {
                "ok": False,
                "status": "NOT_FOUND",
                "message": "Table not found",
                "schema_name": schema_name,
                "table_name": table_name,
                "duration_sec": round(time.time() - started_at, 2),
            }

        dist_cols = get_distribution_columns(
            conn=conn,
            oid=meta["oid"],
            distkey=meta["distkey"],
        )

        distribution_type = get_distribution_type(
            policytype=meta["policytype"],
            dist_cols=dist_cols,
        )

        query = build_reorganize_sql(
            schema_name=schema_name,
            table_name=table_name,
            policytype=meta["policytype"],
            dist_cols=dist_cols,
        )

        with conn.cursor() as cur:
            cur.execute(query)

        conn.commit()

        return {
            "ok": True,
            "status": "SUCCESS",
            "message": "REORGANIZE completed",
            "schema_name": schema_name,
            "table_name": table_name,
            "distribution_type": distribution_type,
            "distribution_columns": dist_cols,
            "access_method": meta["access_method"],
            "total_bytes": meta["total_bytes"],
            "duration_sec": round(time.time() - started_at, 2),
        }

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass

        return {
            "ok": False,
            "status": "FAILED",
            "message": str(e),
            "traceback": traceback.format_exc(),
            "schema_name": schema_name,
            "table_name": table_name,
            "duration_sec": round(time.time() - started_at, 2),
        }

    finally:
        conn.close()


def run_reorganize_job(job_id):
    """
    Запуск REORGANIZE job из web UI.

    job_items уже должны содержать реальные targets:
    - обычные таблицы
    - leaf partitions
    """

    job = get_job(job_id)

    if not job:
        return

    connection_id = job["connection_id"]
    items = get_job_items(job_id)

    try:
        mark_job_running(job_id)

        for item in items:
            if is_stop_requested(job_id):
                mark_item_skipped(
                    item["id"],
                    "Job stopped by user",
                )
                refresh_job_progress(job_id)
                continue

            schema_name = item["schema_name"]
            table_name = item["table_name"]

            try:
                mark_item_running(
                    item["id"],
                    worker_id=1,
                )

                result = reorganize_table(
                    connection_id=connection_id,
                    schema_name=schema_name,
                    table_name=table_name,
                )

                if result.get("ok"):
                    mark_item_done(item["id"])
                else:
                    message = result.get("message") or "REORGANIZE failed"
                    mark_item_failed(item["id"], message)

            except Exception as e:
                mark_item_failed(
                    item["id"],
                    str(e),
                )

            refresh_job_progress(job_id)

        if is_stop_requested(job_id):
            mark_job_cancelled(job_id)
        else:
            mark_job_done(job_id)

    except Exception as e:
        mark_job_failed(job_id, str(e))

    finally:
        clear_stop_flag(job_id)


def build_vacuum_sql(schema_name, table_name, vacuum_type):
    """
    Дополнительно оставляем VACUUM helper в этом модуле.
    Потом его можно использовать для модуля Vacuum.

    vacuum_type:
    - analyze
    - full
    - freeze
    - full_analyze
    - freeze_analyze
    """

    vacuum_type = vacuum_type.lower().strip()

    if vacuum_type == "analyze":
        command = "VACUUM ANALYZE {}.{}"
    elif vacuum_type == "full":
        command = "VACUUM FULL {}.{}"
    elif vacuum_type == "freeze":
        command = "VACUUM FREEZE {}.{}"
    elif vacuum_type == "full_analyze":
        command = "VACUUM FULL ANALYZE {}.{}"
    elif vacuum_type == "freeze_analyze":
        command = "VACUUM FREEZE ANALYZE {}.{}"
    else:
        raise ValueError("Unknown vacuum type: {}".format(vacuum_type))

    return sql.SQL(command).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )


def vacuum_table(connection_id, schema_name, table_name, vacuum_type):
    """
    VACUUM должен выполняться с autocommit=True.
    """

    started_at = time.time()
    conn = open_gp_connection(connection_id)

    old_autocommit = conn.autocommit

    try:
        conn.autocommit = True

        query = build_vacuum_sql(
            schema_name=schema_name,
            table_name=table_name,
            vacuum_type=vacuum_type,
        )

        with conn.cursor() as cur:
            cur.execute(query)

        return {
            "ok": True,
            "status": "SUCCESS",
            "message": "VACUUM {} completed".format(vacuum_type),
            "schema_name": schema_name,
            "table_name": table_name,
            "vacuum_type": vacuum_type,
            "duration_sec": round(time.time() - started_at, 2),
        }

    except Exception as e:
        return {
            "ok": False,
            "status": "FAILED",
            "message": str(e),
            "traceback": traceback.format_exc(),
            "schema_name": schema_name,
            "table_name": table_name,
            "vacuum_type": vacuum_type,
            "duration_sec": round(time.time() - started_at, 2),
        }

    finally:
        try:
            conn.autocommit = old_autocommit
        except Exception:
            pass

        conn.close()


def find_unique_column_without_index(conn, schema_name, table_name, max_candidates=20):
    """
    Ищет обычную колонку без unique index, которая фактически уникальна.

    Проверка:
        count(*) = count(column) = count(distinct column)

    Чтобы не проверять все подряд тяжёлые колонки, сначала берём кандидатов из pg_stats.
    Приоритет:
    - NOT NULL
    - n_distinct близко к -1 или большое
    - простые типы: int, bigint, numeric, text, varchar, date, timestamp, uuid и т.д.
    """

    candidate_query = """
        WITH table_info AS (
            SELECT
                c.oid,
                c.reltuples::bigint AS estimated_rows
            FROM pg_class c
            JOIN pg_namespace n
                ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
            LIMIT 1
        ),
        cols AS (
            SELECT
                a.attname,
                a.attnotnull,
                t.typname,
                s.n_distinct,
                COALESCE(s.null_frac, 1) AS null_frac,
                ti.estimated_rows
            FROM table_info ti
            JOIN pg_attribute a
                ON a.attrelid = ti.oid
            JOIN pg_type t
                ON t.oid = a.atttypid
            LEFT JOIN pg_stats s
                ON s.schemaname = %s
               AND s.tablename = %s
               AND s.attname = a.attname
            WHERE a.attnum > 0
              AND a.attisdropped = false
              AND t.typname IN (
                    'int2',
                    'int4',
                    'int8',
                    'numeric',
                    'float4',
                    'float8',
                    'text',
                    'varchar',
                    'bpchar',
                    'date',
                    'timestamp',
                    'timestamptz',
                    'uuid'
              )
        )
        SELECT
            attname,
            attnotnull,
            typname,
            n_distinct,
            null_frac,
            estimated_rows
        FROM cols
        ORDER BY
            attnotnull DESC,
            CASE
                WHEN n_distinct = -1 THEN 1
                WHEN n_distinct < 0 THEN 2
                WHEN n_distinct > 0 THEN 3
                ELSE 4
            END,
            abs(COALESCE(n_distinct, 0)) DESC,
            attname
        LIMIT %s
    """

    with conn.cursor() as cur:
        cur.execute(
            candidate_query,
            (
                schema_name,
                table_name,
                schema_name,
                table_name,
                max_candidates,
            ),
        )
        candidates = cur.fetchall()

    if not candidates:
        return None

    for row in candidates:
        attname = row[0]
        attnotnull = row[1]
        typname = row[2]
        n_distinct = row[3]
        null_frac = row[4]
        estimated_rows = row[5]

        """
        Если статистика явно говорит, что null есть, такую колонку лучше не брать.
        Но если статистика отсутствует, всё равно проверим точно.
        """
        if null_frac is not None and float(null_frac) > 0:
            continue

        check_query = sql.SQL(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT({col}) AS not_null_rows,
                COUNT(DISTINCT {col}) AS distinct_rows
            FROM {schema}.{table}
            """
        ).format(
            col=sql.Identifier(attname),
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
        )

        try:
            with conn.cursor() as cur:
                cur.execute(check_query)
                check_row = cur.fetchone()

            total_rows = int(check_row[0] or 0)
            not_null_rows = int(check_row[1] or 0)
            distinct_rows = int(check_row[2] or 0)

            if total_rows > 0 and total_rows == not_null_rows and total_rows == distinct_rows:
                return {
                    "column": attname,
                    "type": typname,
                    "total_rows": total_rows,
                    "distinct_rows": distinct_rows,
                    "estimated_rows": estimated_rows,
                    "n_distinct": n_distinct,
                    "reason": "Column is unique by data scan: count(*) = count(column) = count(distinct column)",
                }

        except Exception:
            """
            Если конкретная колонка не проверилась — идём дальше.
            Например, тип не поддержал count(distinct) или была ошибка выполнения.
            """
            continue

    return None


def get_distribution_recommendation(connection_id, schema_name, table_name):
    """
    Рекомендация новой distribution для Greenplum 7.3.

    Приоритет:
    1. PRIMARY KEY из одной NOT NULL колонки
    2. UNIQUE INDEX из одной NOT NULL колонки
    3. RANDOMLY
    """

    conn = open_gp_connection(connection_id)

    try:
        meta = get_table_metadata(conn, schema_name, table_name)

        if not meta:
            return {
                "ok": False,
                "status": "NOT_FOUND",
                "message": "Table not found",
                "schema_name": schema_name,
                "table_name": table_name,
            }

        current_dist_cols = get_distribution_columns(
            conn=conn,
            oid=meta["oid"],
            distkey=meta["distkey"],
        )

        current_distribution_type = get_distribution_type(
            policytype=meta["policytype"],
            dist_cols=current_dist_cols,
        )

        current_distribution = {
            "type": current_distribution_type,
            "columns": current_dist_cols,
        }

        query = """
            WITH unique_indexes AS (
                SELECT
                    i.indrelid,
                    i.indexrelid,
                    idx.relname AS index_name,
                    i.indisprimary,
                    i.indisunique,
                    i.indkey,
                    array_agg(a.attname ORDER BY k.ord) AS columns,
                    count(*) AS column_count,
                    bool_and(a.attnotnull) AS all_not_null
                FROM pg_index i
                JOIN pg_class t
                    ON t.oid = i.indrelid
                JOIN pg_namespace n
                    ON n.oid = t.relnamespace
                JOIN pg_class idx
                    ON idx.oid = i.indexrelid
                JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                    ON true
                JOIN pg_attribute a
                    ON a.attrelid = t.oid
                   AND a.attnum = k.attnum
                WHERE n.nspname = %s
                  AND t.relname = %s
                  AND i.indisunique = true
                  AND i.indisvalid = true
                  AND i.indisready = true
                  AND i.indpred IS NULL
                  AND i.indexprs IS NULL
                  AND a.attisdropped = false
                GROUP BY
                    i.indrelid,
                    i.indexrelid,
                    idx.relname,
                    i.indisprimary,
                    i.indisunique,
                    i.indkey
            )
            SELECT
                index_name,
                indisprimary,
                columns,
                column_count,
                all_not_null
            FROM unique_indexes
            WHERE column_count = 1
              AND all_not_null = true
            ORDER BY
                indisprimary DESC,
                index_name
            LIMIT 1
        """

        with conn.cursor() as cur:
            cur.execute(query, (schema_name, table_name))
            row = cur.fetchone()

        if row:
            index_name, indisprimary, columns, column_count, all_not_null = row

            recommended_columns = list(columns)

            already_same = (
                current_distribution_type == "HASH"
                and current_dist_cols == recommended_columns
            )

            reason = "PRIMARY KEY unique not null column"
            if not indisprimary:
                reason = "UNIQUE INDEX unique not null column"

            return {
                "ok": True,
                "schema_name": schema_name,
                "table_name": table_name,
                "current_distribution": current_distribution,
                "recommendation_type": "HASH",
                "recommended_columns": recommended_columns,
                "recommended_sql_preview": build_distribution_sql_preview(
                    schema_name=schema_name,
                    table_name=table_name,
                    distribution_type="HASH",
                    columns=recommended_columns,
                ),
                "reason": reason,
                "source_index": index_name,
                "already_same": already_same,
                "message": "Recommended DISTRIBUTED BY ({})".format(
                    ", ".join(recommended_columns)
                ),
            }

        unique_column = find_unique_column_without_index(
            conn=conn,
            schema_name=schema_name,
            table_name=table_name,
            max_candidates=20,
        )

        if unique_column:
            recommended_columns = [unique_column["column"]]

            already_same = (
                current_distribution_type == "HASH"
                and current_dist_cols == recommended_columns
            )

            return {
                "ok": True,
                "schema_name": schema_name,
                "table_name": table_name,
                "current_distribution": current_distribution,
                "recommendation_type": "HASH",
                "recommended_columns": recommended_columns,
                "recommended_sql_preview": build_distribution_sql_preview(
                    schema_name=schema_name,
                    table_name=table_name,
                    distribution_type="HASH",
                    columns=recommended_columns,
                ),
                "reason": unique_column["reason"],
                "source_index": None,
                "source_column": unique_column["column"],
                "source_column_type": unique_column["type"],
                "total_rows": unique_column["total_rows"],
                "distinct_rows": unique_column["distinct_rows"],
                "already_same": already_same,
                "message": "Recommended DISTRIBUTED BY ({}) based on data uniqueness scan".format(
                    unique_column["column"]
                ),
            }

        already_random = current_distribution_type == "RANDOM"

        return {
            "ok": True,
            "schema_name": schema_name,
            "table_name": table_name,
            "current_distribution": current_distribution,
            "recommendation_type": "RANDOM",
            "recommended_columns": [],
            "recommended_sql_preview": build_distribution_sql_preview(
                schema_name=schema_name,
                table_name=table_name,
                distribution_type="RANDOM",
                columns=[],
            ),
            "reason": "No PRIMARY KEY, UNIQUE INDEX, or unique column found by data scan",
            "source_index": None,
            "source_column": None,
            "already_same": already_random,
            "message": "Recommended DISTRIBUTED RANDOMLY",
        }

    except Exception as e:
        return {
            "ok": False,
            "schema_name": schema_name,
            "table_name": table_name,
            "status": "FAILED",
            "message": str(e),
            "traceback": traceback.format_exc(),
        }

    finally:
        conn.close()


def build_distribution_sql_preview(schema_name, table_name, distribution_type, columns):
    if distribution_type == "HASH":
        cols = ", ".join(['"{}"'.format(c) for c in columns])

        return 'ALTER TABLE "{}"."{}" SET WITH (REORGANIZE=true) DISTRIBUTED BY ({});'.format(
            schema_name,
            table_name,
            cols,
        )

    return 'ALTER TABLE "{}"."{}" SET WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY;'.format(
        schema_name,
        table_name,
    )


def build_apply_distribution_sql(schema_name, table_name, distribution_type, columns):
    """
    Меняет distribution и сразу делает REORGANIZE.
    """

    distribution_type = distribution_type.upper().strip()

    if distribution_type == "HASH":
        if not columns:
            raise ValueError("HASH distribution requires columns")

        return sql.SQL(
            "ALTER TABLE {}.{} SET WITH (REORGANIZE=true) DISTRIBUTED BY ({})"
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join([sql.Identifier(c) for c in columns]),
        )

    if distribution_type == "RANDOM":
        return sql.SQL(
            "ALTER TABLE {}.{} SET WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY"
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )

    raise ValueError("Unknown distribution_type: {}".format(distribution_type))


def apply_distribution_and_reorganize(connection_id, schema_name, table_name, distribution_type, columns=None):
    """
    Выполняет:
    ALTER TABLE ... SET WITH (REORGANIZE=true) DISTRIBUTED BY (...)
    или
    ALTER TABLE ... SET WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY
    """

    if columns is None:
        columns = []

    started_at = time.time()
    conn = open_gp_connection(connection_id)

    try:
        query = build_apply_distribution_sql(
            schema_name=schema_name,
            table_name=table_name,
            distribution_type=distribution_type,
            columns=columns,
        )

        with conn.cursor() as cur:
            cur.execute(query)

        conn.commit()

        return {
            "ok": True,
            "status": "SUCCESS",
            "message": "Distribution changed and REORGANIZE completed",
            "schema_name": schema_name,
            "table_name": table_name,
            "distribution_type": distribution_type,
            "columns": columns,
            "duration_sec": round(time.time() - started_at, 2),
        }

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass

        return {
            "ok": False,
            "status": "FAILED",
            "message": str(e),
            "traceback": traceback.format_exc(),
            "schema_name": schema_name,
            "table_name": table_name,
            "distribution_type": distribution_type,
            "columns": columns,
            "duration_sec": round(time.time() - started_at, 2),
        }

    finally:
        conn.close()
