import psycopg2
from psycopg2.extras import RealDictCursor

from db import sqlite_cursor


def list_connections():
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                name,
                host,
                port,
                database_name,
                username,
                CASE
                    WHEN password IS NULL OR password = '' THEN ''
                    ELSE '********'
                END AS password_masked,
                COALESCE(db_type, 'greenplum') AS db_type,
                COALESCE(role, 'both') AS role,
                created_at,
                updated_at
            FROM connections
            ORDER BY id DESC
            """
        )
        return [dict(row) for row in cur.fetchall()]


def get_connection_by_id(connection_id: int):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                name,
                host,
                port,
                database_name,
                username,
                password,
                COALESCE(db_type, 'greenplum') AS db_type,
                COALESCE(role, 'both') AS role,
                created_at,
                updated_at
            FROM connections
            WHERE id = ?
            """,
            (connection_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def create_connection(data: dict):
    name = data.get("name", "").strip()
    host = data.get("host", "").strip()
    port = int(data.get("port") or 5432)
    database_name = data.get("database_name", "").strip()
    username = data.get("username", "").strip()
    password = data.get("password", "")

    db_type = str(data.get("db_type") or "greenplum").strip().lower()
    if db_type not in ("greenplum", "postgres", "mysql", "oracle"):
        db_type = "greenplum"

    # роль коннектора: источник, назначение или универсальный
    role = str(data.get("role") or "both").strip().lower()
    if role not in ("source", "dest", "both"):
        role = "both"

    if not name:
        raise ValueError("Название подключения обязательно")
    if not host:
        raise ValueError("Host обязателен")
    if not database_name:
        raise ValueError("Database обязателен")
    if not username:
        raise ValueError("Username обязателен")

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO connections (
                name,
                host,
                port,
                database_name,
                username,
                password,
                db_type,
                role
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                host,
                port,
                database_name,
                username,
                password,
                db_type,
                role,
            ),
        )
        return cur.lastrowid


def delete_connection(connection_id: int):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            DELETE FROM connections
            WHERE id = ?
            """,
            (connection_id,),
        )


def test_gp_connection(connection_id: int):
    cfg = get_connection_by_id(connection_id)

    if not cfg:
        return {
            "ok": False,
            "message": "Подключение не найдено",
        }

    try:
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database_name"],
            user=cfg["username"],
            password=cfg["password"],
            connect_timeout=10,
        )

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    version() AS version,
                    current_database() AS database_name,
                    current_user AS current_user
                """
            )
            info = cur.fetchone()

        conn.close()

        return {
            "ok": True,
            "message": "Подключение успешно",
            "info": dict(info),
        }

    except Exception as e:
        return {
            "ok": False,
            "message": str(e),
        }


def open_gp_connection(connection_id: int):
    cfg = get_connection_by_id(connection_id)

    if not cfg:
        raise ValueError(f"Connection id={connection_id} not found")

    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database_name"],
        user=cfg["username"],
        password=cfg["password"],
        connect_timeout=15,
    )
