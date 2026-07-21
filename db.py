import sqlite3
from contextlib import contextmanager

import config


def get_sqlite_connection():
    # Resolve config.SQLITE_DB_PATH at call time so tests can redirect it.
    conn = sqlite3.connect(config.SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@contextmanager
def sqlite_cursor(commit=False):
    conn = get_sqlite_connection()
    try:
        cur = conn.cursor()
        yield cur
        if commit:
            conn.commit()
    finally:
        conn.close()


def ensure_column_exists(table_name: str, column_name: str, alter_sql: str):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(f"PRAGMA table_info({table_name})")
        columns = [row["name"] for row in cur.fetchall()]

        if column_name not in columns:
            cur.execute(alter_sql)


def init_db():
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                host TEXT NOT NULL,
                port INTEGER NOT NULL DEFAULT 5432,
                database_name TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                connection_id INTEGER,
                config_json TEXT,
                total_items INTEGER DEFAULT 0,
                done_items INTEGER DEFAULT 0,
                failed_items INTEGER DEFAULT 0,
                skipped_items INTEGER DEFAULT 0,
                progress_percent REAL DEFAULT 0,
                started_at TEXT,
                finished_at TEXT,
                error_message TEXT,
                log_file TEXT,
                stop_requested INTEGER DEFAULT 0
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                schema_name TEXT,
                table_name TEXT,
                action TEXT,
                status TEXT NOT NULL,
                worker_id INTEGER,
                started_at TEXT,
                finished_at TEXT,
                duration_seconds REAL,
                error_message TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS skew_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER,
                connection_id INTEGER,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                total_rows INTEGER,
                segment_count INTEGER,
                avg_rows REAL,
                max_rows INTEGER,
                min_rows INTEGER,
                skew_ratio REAL,
                empty_segments INTEGER,
                status TEXT,
                checked_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS skew_result_segments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                skew_result_id INTEGER NOT NULL,
                job_id INTEGER,
                connection_id INTEGER,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                gp_segment_id INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                checked_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_skew_result_segments_result_id
            ON skew_result_segments(skew_result_id)
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_skew_result_segments_job_table
            ON skew_result_segments(job_id, schema_name, table_name)
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_jobs_type_status
            ON jobs(job_type, status)
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_job_items_job_id_status
            ON job_items(job_id, status)
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_skew_results_job_id
            ON skew_results(job_id)
            """
        )

        # --- Scheduler (spec: docs/superpowers/specs/2026-07-21-scheduler-cron-design.md §3) ---

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                job_type TEXT NOT NULL,
                config_json TEXT,
                cron_expr TEXT NOT NULL,
                timezone TEXT,
                overlap_policy TEXT NOT NULL DEFAULT 'skip',
                max_retries INTEGER NOT NULL DEFAULT 0,
                retry_delay_seconds INTEGER NOT NULL DEFAULT 0,
                notify_on TEXT NOT NULL DEFAULT 'failure',
                notify_channel_ids TEXT,
                next_run_at TEXT,
                last_run_at TEXT,
                last_status TEXT,
                last_job_id INTEGER,
                last_error TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                fired_at TEXT,
                run_date TEXT,
                job_id INTEGER,
                status TEXT NOT NULL,
                attempt_no INTEGER NOT NULL DEFAULT 0,
                error TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_id
            ON schedule_runs(schedule_id, status)
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduler_lock (
                id INTEGER PRIMARY KEY,
                holder TEXT,
                heartbeat_at TEXT,
                expires_at TEXT
            )
            """
        )

        cur.execute(
            """
            INSERT OR IGNORE INTO scheduler_lock (id, holder, heartbeat_at, expires_at)
            VALUES (1, NULL, NULL, NULL)
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS notification_channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                config_json TEXT,
                enabled INTEGER NOT NULL DEFAULT 1
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS table_sets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                connection_id INTEGER,
                tables_json TEXT,
                rules_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT
            )
            """
        )

    ensure_column_exists(
        "skew_results",
        "job_id",
        "ALTER TABLE skew_results ADD COLUMN job_id INTEGER"
    )

    ensure_column_exists(
        "jobs",
        "stop_requested",
        "ALTER TABLE jobs ADD COLUMN stop_requested INTEGER DEFAULT 0"
    )


def get_connection_by_id(connection_id):
    """
    Возвращает подключение по id из SQLite.
    Не зависит от list_connections().
    """

    connection_id = int(connection_id)

    # Единый источник пути к БД (config.SQLITE_DB_PATH), чтобы тесты могли его подменить.
    conn = get_sqlite_connection()

    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT *
            FROM connections
            WHERE id = ?
            """,
            (connection_id,)
        )

        row = cur.fetchone()

        if not row:
            return None

        return dict(row)

    finally:
        conn.close()