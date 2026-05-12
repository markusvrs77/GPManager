import json
import threading
from datetime import datetime

from db import sqlite_cursor


STOP_FLAGS = {}
RUNNING_THREADS = {}


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def create_job(job_type, connection_id, config):
    total_items = len(config.get("tables", []))

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO jobs (
                job_type,
                status,
                connection_id,
                config_json,
                total_items,
                done_items,
                failed_items,
                skipped_items,
                progress_percent,
                started_at
            )
            VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0, ?)
            """,
            (
                job_type,
                "queued",
                connection_id,
                json.dumps(config, ensure_ascii=False),
                total_items,
                now_str(),
            ),
        )

        job_id = cur.lastrowid

        for item in config.get("tables", []):
            cur.execute(
                """
                INSERT INTO job_items (
                    job_id,
                    schema_name,
                    table_name,
                    action,
                    status
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    item.get("schema"),
                    item.get("table"),
                    job_type,
                    "queued",
                ),
            )

        return job_id


def get_job(job_id):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                job_type,
                status,
                connection_id,
                config_json,
                total_items,
                done_items,
                failed_items,
                skipped_items,
                progress_percent,
                started_at,
                finished_at,
                error_message,
                log_file
            FROM jobs
            WHERE id = ?
            """,
            (job_id,),
        )
        row = cur.fetchone()

    return dict(row) if row else None


def get_job_items(job_id):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                job_id,
                schema_name,
                table_name,
                action,
                status,
                worker_id,
                started_at,
                finished_at,
                duration_seconds,
                error_message
            FROM job_items
            WHERE job_id = ?
            ORDER BY id
            """,
            (job_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def mark_job_running(job_id):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'running'
            WHERE id = ?
            """,
            (job_id,),
        )


def mark_job_done(job_id):
    refresh_job_progress(job_id)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'done',
                finished_at = ?
            WHERE id = ?
              AND status != 'cancelled'
            """,
            (
                now_str(),
                job_id,
            ),
        )


def mark_job_failed(job_id, error_message):
    refresh_job_progress(job_id)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'failed',
                finished_at = ?,
                error_message = ?
            WHERE id = ?
            """,
            (
                now_str(),
                str(error_message),
                job_id,
            ),
        )


def mark_job_cancelled(job_id):
    refresh_job_progress(job_id)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'cancelled',
                finished_at = ?
            WHERE id = ?
            """,
            (
                now_str(),
                job_id,
            ),
        )


def set_stop_flag(job_id):
    STOP_FLAGS[int(job_id)] = True

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'stopping'
            WHERE id = ?
              AND status IN ('queued', 'running')
            """,
            (job_id,),
        )


def is_stop_requested(job_id):
    return STOP_FLAGS.get(int(job_id), False)


def clear_stop_flag(job_id):
    STOP_FLAGS.pop(int(job_id), None)


def mark_item_running(item_id, worker_id=None):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE job_items
            SET status = 'running',
                worker_id = ?,
                started_at = ?
            WHERE id = ?
            """,
            (
                worker_id,
                now_str(),
                item_id,
            ),
        )


def mark_item_done(item_id):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE job_items
            SET status = 'done',
                finished_at = ?,
                duration_seconds =
                    CASE
                        WHEN started_at IS NOT NULL
                        THEN ROUND((julianday(?) - julianday(started_at)) * 86400, 2)
                        ELSE NULL
                    END
            WHERE id = ?
            """,
            (
                now_str(),
                now_str(),
                item_id,
            ),
        )


def mark_item_failed(item_id, error_message):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE job_items
            SET status = 'failed',
                finished_at = ?,
                duration_seconds =
                    CASE
                        WHEN started_at IS NOT NULL
                        THEN ROUND((julianday(?) - julianday(started_at)) * 86400, 2)
                        ELSE NULL
                    END,
                error_message = ?
            WHERE id = ?
            """,
            (
                now_str(),
                now_str(),
                str(error_message),
                item_id,
            ),
        )


def mark_item_skipped(item_id, reason):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE job_items
            SET status = 'skipped',
                finished_at = ?,
                error_message = ?
            WHERE id = ?
            """,
            (
                now_str(),
                reason,
                item_id,
            ),
        )


def refresh_job_progress(job_id):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_items,
                SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done_items,
                SUM(CASE WHEN status IN ('failed', 'interrupted') THEN 1 ELSE 0 END) AS failed_items,
                SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped_items
            FROM job_items
            WHERE job_id = ?
            """,
            (job_id,),
        )

        stats = dict(cur.fetchone())

        total_items = stats.get("total_items") or 0
        done_items = stats.get("done_items") or 0
        failed_items = stats.get("failed_items") or 0
        skipped_items = stats.get("skipped_items") or 0

        finished_items = done_items + failed_items + skipped_items

        if total_items > 0:
            progress_percent = round((finished_items / float(total_items)) * 100, 2)
        else:
            progress_percent = 0

        cur.execute(
            """
            UPDATE jobs
            SET total_items = ?,
                done_items = ?,
                failed_items = ?,
                skipped_items = ?,
                progress_percent = ?
            WHERE id = ?
            """,
            (
                total_items,
                done_items,
                failed_items,
                skipped_items,
                progress_percent,
                job_id,
            ),
        )


def run_background_job(job_id, target_func):
    thread = threading.Thread(
        target=target_func,
        args=(job_id,),
    )
    thread.daemon = True
    thread.start()

    RUNNING_THREADS[int(job_id)] = thread

    return thread


def get_latest_job(job_type=None):
    with sqlite_cursor() as cur:
        if job_type:
            cur.execute(
                """
                SELECT
                    id,
                    job_type,
                    status,
                    connection_id,
                    config_json,
                    total_items,
                    done_items,
                    failed_items,
                    skipped_items,
                    progress_percent,
                    started_at,
                    finished_at,
                    error_message,
                    log_file
                FROM jobs
                WHERE job_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (job_type,),
            )
        else:
            cur.execute(
                """
                SELECT
                    id,
                    job_type,
                    status,
                    connection_id,
                    config_json,
                    total_items,
                    done_items,
                    failed_items,
                    skipped_items,
                    progress_percent,
                    started_at,
                    finished_at,
                    error_message,
                    log_file
                FROM jobs
                ORDER BY id DESC
                LIMIT 1
                """
            )

        row = cur.fetchone()

    return dict(row) if row else None

def mark_interrupted_jobs_on_startup():
    """
    При старте приложения переводит зависшие jobs в interrupted.
    Нужно на случай, если Flask был перезапущен во время выполнения background thread.
    """

    interrupted_job_ids = []

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            SELECT id
            FROM jobs
            WHERE status IN ('queued', 'running', 'stopping')
            """
        )

        rows = cur.fetchall()
        interrupted_job_ids = [int(row["id"]) for row in rows]

        if not interrupted_job_ids:
            return []

        for job_id in interrupted_job_ids:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'interrupted',
                    finished_at = ?,
                    error_message =
                        CASE
                            WHEN error_message IS NULL OR error_message = ''
                            THEN 'Application restarted while job was running'
                            ELSE error_message
                        END
                WHERE id = ?
                """,
                (
                    now_str(),
                    job_id,
                ),
            )

            cur.execute(
                """
                UPDATE job_items
                SET status = 'interrupted',
                    finished_at = ?,
                    error_message =
                        CASE
                            WHEN error_message IS NULL OR error_message = ''
                            THEN 'Application restarted while item was running'
                            ELSE error_message
                        END
                WHERE job_id = ?
                  AND status IN ('queued', 'running')
                """,
                (
                    now_str(),
                    job_id,
                ),
            )

    for job_id in interrupted_job_ids:
        refresh_job_progress(job_id)

    return interrupted_job_ids


def get_active_jobs(job_type=None):
    with sqlite_cursor() as cur:
        if job_type:
            cur.execute(
                """
                SELECT
                    id,
                    job_type,
                    status,
                    connection_id,
                    config_json,
                    total_items,
                    done_items,
                    failed_items,
                    skipped_items,
                    progress_percent,
                    started_at,
                    finished_at,
                    error_message,
                    log_file
                FROM jobs
                WHERE job_type = ?
                  AND status IN ('queued', 'running', 'stopping')
                ORDER BY id DESC
                """,
                (job_type,),
            )
        else:
            cur.execute(
                """
                SELECT
                    id,
                    job_type,
                    status,
                    connection_id,
                    config_json,
                    total_items,
                    done_items,
                    failed_items,
                    skipped_items,
                    progress_percent,
                    started_at,
                    finished_at,
                    error_message,
                    log_file
                FROM jobs
                WHERE status IN ('queued', 'running', 'stopping')
                ORDER BY id DESC
                """
            )

        return [dict(row) for row in cur.fetchall()]

def create_job_items(job_id, items):
    """
    Создаёт job_items для job.
    items пример:
    [
        {
            "schema_name": "dwh",
            "table_name": "table1",
            "action": "REORGANIZE"
        }
    ]
    """

    from db import sqlite_cursor

    with sqlite_cursor(commit=True) as cur:
        for item in items:
            cur.execute(
                """
                INSERT INTO job_items (
                    job_id,
                    schema_name,
                    table_name,
                    action,
                    status
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    item.get("schema_name"),
                    item.get("table_name"),
                    item.get("action", "REORGANIZE"),
                    "queued",
                ),
            )

        cur.execute(
            """
            UPDATE jobs
            SET
                total_items = ?,
                done_items = 0,
                failed_items = 0,
                skipped_items = 0,
                progress_percent = 0
            WHERE id = ?
            """,
            (
                len(items),
                job_id,
            ),
        )

