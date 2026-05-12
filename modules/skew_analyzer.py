from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from db import sqlite_cursor
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


def analyze_table_skew(connection_id, schema_name, table_name, job_id=None):
    query = sql.SQL(
        """
        SELECT
            gp_segment_id,
            count(*)::bigint AS row_count
        FROM {}.{}
        GROUP BY gp_segment_id
        ORDER BY gp_segment_id
        """
    ).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )

    conn = open_gp_connection(connection_id)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
    finally:
        conn.close()

    segment_rows = {}

    for row in rows:
        segment_rows[int(row["gp_segment_id"])] = int(row["row_count"])

    if not segment_rows:
        result = {
            "schema_name": schema_name,
            "table_name": table_name,
            "total_rows": 0,
            "segment_count": 0,
            "avg_rows": 0,
            "max_rows": 0,
            "min_rows": 0,
            "skew_ratio": 0,
            "empty_segments": 0,
            "status": "EMPTY",
            "segments": [],
        }
        save_skew_result(connection_id, result, job_id=job_id)
        return result

    total_rows = sum(segment_rows.values())
    segment_count = len(segment_rows)
    avg_rows = total_rows / float(segment_count) if segment_count else 0
    max_rows = max(segment_rows.values())
    min_rows = min(segment_rows.values())

    if avg_rows > 0:
        skew_ratio = max_rows / avg_rows
    else:
        skew_ratio = 0

    empty_segments = len([v for v in segment_rows.values() if v == 0])

    if total_rows == 0:
        status = "EMPTY"
    elif skew_ratio < 1.5:
        status = "OK"
    elif skew_ratio < 3.0:
        status = "WARNING"
    else:
        status = "CRITICAL"

    result = {
        "schema_name": schema_name,
        "table_name": table_name,
        "total_rows": total_rows,
        "segment_count": segment_count,
        "avg_rows": round(avg_rows, 2),
        "max_rows": max_rows,
        "min_rows": min_rows,
        "skew_ratio": round(skew_ratio, 4),
        "empty_segments": empty_segments,
        "status": status,
        "segments": [
            {
                "gp_segment_id": seg_id,
                "row_count": row_count,
            }
            for seg_id, row_count in sorted(segment_rows.items())
        ],
    }

    save_skew_result(connection_id, result, job_id=job_id)

    return result


def analyze_tables_skew(connection_id, tables):
    results = []

    for item in tables:
        schema_name = item.get("schema")
        table_name = item.get("table")

        if not schema_name or not table_name:
            results.append(
                {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "status": "FAILED",
                    "error": "schema/table is empty",
                }
            )
            continue

        try:
            result = analyze_table_skew(
                connection_id=connection_id,
                schema_name=schema_name,
                table_name=table_name,
            )
            results.append(result)

        except Exception as e:
            results.append(
                {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "status": "FAILED",
                    "error": str(e),
                }
            )

    return results


def run_skew_job(job_id):
    job = get_job(job_id)

    if not job:
        return

    connection_id = job["connection_id"]
    items = get_job_items(job_id)

    try:
        mark_job_running(job_id)

        for item in items:
            if is_stop_requested(job_id):
                mark_item_skipped(item["id"], "Job stopped by user")
                continue

            schema_name = item["schema_name"]
            table_name = item["table_name"]

            if not schema_name or not table_name:
                mark_item_failed(item["id"], "schema/table is empty")
                refresh_job_progress(job_id)
                continue

            try:
                mark_item_running(item["id"], worker_id=1)

                analyze_table_skew(
                    connection_id=connection_id,
                    schema_name=schema_name,
                    table_name=table_name,
                    job_id=job_id,
                )

                mark_item_done(item["id"])

            except Exception as e:
                mark_item_failed(item["id"], str(e))

            refresh_job_progress(job_id)

        if is_stop_requested(job_id):
            mark_job_cancelled(job_id)
        else:
            mark_job_done(job_id)

    except Exception as e:
        mark_job_failed(job_id, str(e))

    finally:
        clear_stop_flag(job_id)


def save_skew_result(connection_id, result, job_id=None):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO skew_results (
                job_id,
                connection_id,
                schema_name,
                table_name,
                total_rows,
                segment_count,
                avg_rows,
                max_rows,
                min_rows,
                skew_ratio,
                empty_segments,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                connection_id,
                result.get("schema_name"),
                result.get("table_name"),
                result.get("total_rows"),
                result.get("segment_count"),
                result.get("avg_rows"),
                result.get("max_rows"),
                result.get("min_rows"),
                result.get("skew_ratio"),
                result.get("empty_segments"),
                result.get("status"),
            ),
        )

        skew_result_id = cur.lastrowid

        segments = result.get("segments") or []

        for segment in segments:
            cur.execute(
                """
                INSERT INTO skew_result_segments (
                    skew_result_id,
                    job_id,
                    connection_id,
                    schema_name,
                    table_name,
                    gp_segment_id,
                    row_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    skew_result_id,
                    job_id,
                    connection_id,
                    result.get("schema_name"),
                    result.get("table_name"),
                    segment.get("gp_segment_id"),
                    segment.get("row_count"),
                ),
            )

        return skew_result_id


def get_last_skew_results(limit=500):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM jobs
            WHERE job_type = 'skew'
            ORDER BY id DESC
            LIMIT 1
            """
        )
        job_row = cur.fetchone()

        latest_job_id = job_row["id"] if job_row else None

        if latest_job_id:
            cur.execute(
                """
                SELECT
                    id,
                    job_id,
                    connection_id,
                    schema_name,
                    table_name,
                    total_rows,
                    segment_count,
                    avg_rows,
                    max_rows,
                    min_rows,
                    skew_ratio,
                    empty_segments,
                    status,
                    checked_at
                FROM skew_results
                WHERE job_id = ?
                ORDER BY
                    CASE status
                        WHEN 'CRITICAL' THEN 1
                        WHEN 'WARNING' THEN 2
                        WHEN 'OK' THEN 3
                        WHEN 'EMPTY' THEN 4
                        WHEN 'FAILED' THEN 5
                        ELSE 6
                    END,
                    skew_ratio DESC,
                    total_rows DESC,
                    schema_name,
                    table_name
                LIMIT ?
                """,
                (latest_job_id, limit),
            )
        else:
            cur.execute(
                """
                SELECT
                    id,
                    job_id,
                    connection_id,
                    schema_name,
                    table_name,
                    total_rows,
                    segment_count,
                    avg_rows,
                    max_rows,
                    min_rows,
                    skew_ratio,
                    empty_segments,
                    status,
                    checked_at
                FROM skew_results
                ORDER BY
                    CASE status
                        WHEN 'CRITICAL' THEN 1
                        WHEN 'WARNING' THEN 2
                        WHEN 'OK' THEN 3
                        WHEN 'EMPTY' THEN 4
                        WHEN 'FAILED' THEN 5
                        ELSE 6
                    END,
                    skew_ratio DESC,
                    total_rows DESC,
                    checked_at DESC
                LIMIT ?
                """,
                (limit,),
            )

        return [dict(row) for row in cur.fetchall()]


def get_skew_results_by_job(job_id):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                job_id,
                connection_id,
                schema_name,
                table_name,
                total_rows,
                segment_count,
                avg_rows,
                max_rows,
                min_rows,
                skew_ratio,
                empty_segments,
                status,
                checked_at
            FROM skew_results
            WHERE job_id = ?
            ORDER BY skew_ratio DESC, total_rows DESC, schema_name, table_name
            """,
            (job_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_skew_summary_by_job(job_id):
    results = get_skew_results_by_job(job_id)

    status_counts = {
        "OK": 0,
        "WARNING": 0,
        "CRITICAL": 0,
        "EMPTY": 0,
        "FAILED": 0,
        "INTERRUPTED": 0,
    }

    max_skew = 0
    skew_sum = 0
    skew_count = 0

    for row in results:
        status = row.get("status") or "FAILED"
        if status not in status_counts:
            status_counts[status] = 0
        status_counts[status] += 1

        skew_ratio = row.get("skew_ratio")
        if skew_ratio is not None:
            try:
                skew_ratio = float(skew_ratio)
                skew_sum += skew_ratio
                skew_count += 1
                if skew_ratio > max_skew:
                    max_skew = skew_ratio
            except Exception:
                pass

    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS interrupted_count
            FROM job_items
            WHERE job_id = ?
              AND status = 'interrupted'
            """,
            (job_id,),
        )

        row = cur.fetchone()
        interrupted_count = row["interrupted_count"] if row else 0

    status_counts["INTERRUPTED"] = interrupted_count

    avg_skew = round(skew_sum / skew_count, 4) if skew_count else 0

    total_tables = sum(status_counts.values())

    return {
        "total_tables": len(results),
        "max_skew": round(max_skew, 4),
        "avg_skew": avg_skew,
        "status_counts": status_counts,
    }


def get_skew_result_segments(skew_result_id):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT
                r.id AS skew_result_id,
                r.job_id,
                r.connection_id,
                r.schema_name,
                r.table_name,
                r.total_rows,
                r.segment_count,
                r.avg_rows,
                r.max_rows,
                r.min_rows,
                r.skew_ratio,
                r.empty_segments,
                r.status,
                r.checked_at
            FROM skew_results r
            WHERE r.id = ?
            """,
            (skew_result_id,),
        )

        result_row = cur.fetchone()

        if not result_row:
            return None

        cur.execute(
            """
            SELECT
                gp_segment_id,
                row_count
            FROM skew_result_segments
            WHERE skew_result_id = ?
            ORDER BY gp_segment_id
            """,
            (skew_result_id,),
        )

        segments = [dict(row) for row in cur.fetchall()]

    return {
        "result": dict(result_row),
        "segments": segments,
    }


def get_latest_problem_skew_results(limit=500):
    """
    Возвращает перекошенные таблицы из последнего skew job:
    CRITICAL и WARNING.
    Используется на странице /reorganize.
    """

    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM jobs
            WHERE job_type = 'skew'
            ORDER BY id DESC
            LIMIT 1
            """
        )

        job_row = cur.fetchone()

        if not job_row:
            return []

        latest_job_id = job_row["id"]

        cur.execute(
            """
            SELECT
                id,
                job_id,
                connection_id,
                schema_name,
                table_name,
                total_rows,
                segment_count,
                avg_rows,
                max_rows,
                min_rows,
                skew_ratio,
                empty_segments,
                status,
                checked_at
            FROM skew_results
            WHERE job_id = ?
              AND status IN ('CRITICAL', 'WARNING')
            ORDER BY
                CASE status
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'WARNING' THEN 2
                    ELSE 3
                END,
                skew_ratio DESC,
                total_rows DESC,
                schema_name,
                table_name
            LIMIT ?
            """,
            (
                latest_job_id,
                limit,
            ),
        )

        return [dict(row) for row in cur.fetchall()]
