import json
import threading
from datetime import datetime
import os
import sqlite3
from db import sqlite_cursor


STOP_FLAGS = {}
RUNNING_THREADS = {}

def get_job_manager_db_connection():
    """
    SQLite connection для job_manager.
    Используем стандартный путь instance/gp_reorganize_center.sqlite3.
    """

    db_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "instance",
        "gp_reorganize_center.sqlite3"
    )

    if not os.path.exists(db_path):
        db_path = os.path.join("instance", "gp_reorganize_center.sqlite3")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    return conn

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def create_job(job_type, connection_id, config):
    if config is None:
        config = {}

    tables = config.get("tables") or []
    total_items = len(tables)

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

        item_action = (
            config.get("action")
            or config.get("item_action")
            or job_type
        )

        item_action = str(item_action).upper().strip()

        for item in tables:
            if isinstance(item, dict):
                schema_name = item.get("schema") or item.get("schema_name")
                table_name = item.get("table") or item.get("table_name")
            else:
                continue

            if not schema_name or not table_name:
                continue

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
                    schema_name,
                    table_name,
                    item_action,
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
    conn = get_job_manager_db_connection()

    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT stop_requested
            FROM jobs
            WHERE id = ?
            """,
            (job_id,)
        )

        row = cur.fetchone()

        if not row:
            return False

        return bool(row["stop_requested"])

    finally:
        conn.close()


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

# ============================================================
# Compatibility helpers
# Нужны для старых/новых модулей: vacuum_analyze.py, gpcopy.py
# ============================================================

def update_job_status(job_id, status, error_message=None, log_file=None):
    """
    Универсальное обновление статуса job.
    Поддерживает старый стиль вызова из модулей:
        update_job_status(job_id, "running")
        update_job_status(job_id, "failed", "error text")
        update_job_status(job_id, "done")
    """

    status = str(status).lower()

    if status == "running":
        return mark_job_running(job_id)

    if status == "done":
        return mark_job_done(job_id)

    if status == "failed":
        return mark_job_failed(job_id, error_message or "")

    if status == "cancelled":
        return mark_job_cancelled(job_id)

    if status == "stopping":
        return set_stop_flag(job_id)

    # fallback для interrupted / queued / любых других статусов
    with sqlite_cursor(commit=True) as cur:
        if status in ("done", "failed", "cancelled", "interrupted"):
            cur.execute(
                """
                UPDATE jobs
                SET status = ?,
                    finished_at = ?,
                    error_message = COALESCE(?, error_message),
                    log_file = COALESCE(?, log_file)
                WHERE id = ?
                """,
                (
                    status,
                    now_str(),
                    str(error_message) if error_message else None,
                    log_file,
                    job_id,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE jobs
                SET status = ?,
                    error_message = COALESCE(?, error_message),
                    log_file = COALESCE(?, log_file)
                WHERE id = ?
                """,
                (
                    status,
                    str(error_message) if error_message else None,
                    log_file,
                    job_id,
                ),
            )

    refresh_job_progress(job_id)


def request_stop_job(job_id):
    conn = get_job_manager_db_connection()

    try:
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE jobs
            SET stop_requested = 1,
                status = CASE
                    WHEN status IN ('queued', 'running') THEN 'stopping'
                    ELSE status
                END
            WHERE id = ?
            """,
            (job_id,)
        )

        conn.commit()

    finally:
        conn.close()


def update_job_item_status(item_id, status, error_message=None, worker_id=None):
    """
    Универсальное обновление статуса job_items.
    Нужно, если модуль gpcopy.py вызывает update_job_item_status().
    """

    status = str(status).lower()

    if status == "running":
        return mark_item_running(item_id, worker_id=worker_id)

    if status == "done":
        return mark_item_done(item_id)

    if status == "failed":
        return mark_item_failed(item_id, error_message or "")

    if status == "skipped":
        return mark_item_skipped(item_id, error_message or "Skipped")

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE job_items
            SET status = ?,
                finished_at = ?,
                error_message = COALESCE(?, error_message)
            WHERE id = ?
            """,
            (
                status,
                now_str(),
                str(error_message) if error_message else None,
                item_id,
            ),
        )


def get_job_status(job_id):
    """
    Возвращает только статус job.
    """
    job = get_job(job_id)
    if not job:
        return None
    return job.get("status")