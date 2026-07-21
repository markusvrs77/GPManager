"""
Каталог таблиц для массовых операций gpcopy (10k+ таблиц):

- серверный кэш плоского списка (schema, table) с TTL — без ручного Load;
- поиск/маска/вставка списка — выбор тысяч таблиц без рендера дерева;
- bulk-резолв колонок (даты/watermark) по приоритетному списку одним запросом;
- автодетект PK для sync-режима;
- именованные наборы таблиц (table_sets) для переиспользования в запусках
  и расписаниях.
"""

import json
import time
import fnmatch
import threading

from psycopg2.extras import RealDictCursor

from db import sqlite_cursor

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id

try:
    from modules.gpcopy import open_psycopg2_connection_by_cfg
except ImportError:
    from gpcopy import open_psycopg2_connection_by_cfg


CACHE_TTL_SECONDS = 600

_cache = {}
_cache_lock = threading.Lock()

_SYSTEM_SCHEMAS = (
    "pg_catalog", "information_schema", "gp_toolkit", "pg_toast",
    "pg_aoseg", "pg_bitmapindex", "gpmanager_sync_stage",
)


# ------------------------------------------------------------
# Каталог (кэш плоского списка таблиц)
# ------------------------------------------------------------

def _fetch_tables(connection_id):
    cfg = get_connection_by_id(int(connection_id))

    if not cfg:
        raise ValueError("Connection not found: {}".format(connection_id))

    conn = open_psycopg2_connection_by_cfg(cfg)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE schemaname NOT IN %s
                ORDER BY schemaname, tablename
                """,
                (_SYSTEM_SCHEMAS,),
            )
            return [(r[0], r[1]) for r in cur.fetchall()]
    finally:
        conn.close()


def get_catalog(connection_id, force=False):
    """Список (schema, table) из кэша; обновляет при истёкшем TTL или force."""
    key = int(connection_id)
    now = time.time()

    with _cache_lock:
        entry = _cache.get(key)

        if not force and entry and now - entry["ts"] < CACHE_TTL_SECONDS:
            return entry["tables"], entry["ts"]

    tables = _fetch_tables(key)

    with _cache_lock:
        _cache[key] = {"tables": tables, "ts": now}

    return tables, now


def catalog_summary(tables):
    """Счётчики по схемам: [{schema, total}]."""
    counts = {}

    for schema, _table in tables:
        counts[schema] = counts.get(schema, 0) + 1

    return [
        {"schema": s, "total": counts[s]}
        for s in sorted(counts)
    ]


def search_tables(tables, query, limit=200):
    """Подстрочный поиск по 'schema.table' (без учёта регистра)."""
    query = (query or "").strip().lower()

    if not query:
        return []

    out = []

    for schema, table in tables:
        if query in "{}.{}".format(schema, table).lower():
            out.append((schema, table))

            if len(out) >= int(limit):
                break

    return out


# ------------------------------------------------------------
# Массовый выбор — чистые функции
# ------------------------------------------------------------

def match_mask(tables, mask):
    """fnmatch-маска по 'schema.table': dwh.fact_* / *.dim_* / точное имя."""
    mask = (mask or "").strip().lower()

    if not mask:
        return []

    return [
        (schema, table)
        for schema, table in tables
        if fnmatch.fnmatch("{}.{}".format(schema, table).lower(), mask)
    ]


def parse_table_list(text, tables):
    """
    Разбирает вставленный список 'schema.table' построчно.
    Возвращает (valid: [(schema, table)], invalid: [строка]).
    """
    known = {(s.lower(), t.lower()): (s, t) for s, t in tables}
    valid = []
    invalid = []
    seen = set()

    for raw_line in (text or "").splitlines():
        line = raw_line.strip()

        if not line:
            continue

        if "." not in line:
            invalid.append(line)
            continue

        schema, table = line.split(".", 1)
        key = (schema.strip().lower(), table.strip().lower())

        if key in known:
            if key not in seen:
                seen.add(key)
                valid.append(known[key])
        else:
            invalid.append(line)

    return valid, invalid


# ------------------------------------------------------------
# Bulk-резолв колонок и PK
# ------------------------------------------------------------

def pick_columns(columns_by_table, priority):
    """
    Чистая функция: для каждой таблицы — первая колонка из priority,
    которая у неё есть. Возвращает (resolved: {key: column}, missing: [key]).
    """
    resolved = {}
    missing = []

    for key, columns in columns_by_table.items():
        column_set = set(columns)
        found = None

        for candidate in priority:
            if candidate in column_set:
                found = candidate
                break

        if found:
            resolved[key] = found
        else:
            missing.append(key)

    return resolved, sorted(missing)


def fetch_columns_for_candidates(connection_id, tables, candidate_columns):
    """
    Один запрос: какие из candidate_columns есть у каждой выбранной таблицы.
    Возвращает {(schema, table): [column, ...]} (только выбранные таблицы).
    """
    if not tables or not candidate_columns:
        return {}

    cfg = get_connection_by_id(int(connection_id))
    conn = open_psycopg2_connection_by_cfg(cfg)

    wanted = {(s, t) for s, t in tables}
    schemas = sorted({s for s, _t in wanted})

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_schema, table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = ANY(%s)
                  AND column_name = ANY(%s)
                """,
                (schemas, list(candidate_columns)),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    out = {key: [] for key in wanted}

    for schema, table, column in rows:
        key = (schema, table)

        if key in out:
            out[key].append(column)

    return out


def fetch_primary_keys(connection_id, tables):
    """
    Автодетект PK: {(schema, table): [pk_column, ...]} одним запросом.
    """
    if not tables:
        return {}

    cfg = get_connection_by_id(int(connection_id))
    conn = open_psycopg2_connection_by_cfg(cfg)

    wanted = {(s, t) for s, t in tables}
    schemas = sorted({s for s, _t in wanted})

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT n.nspname, c.relname, a.attname
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a
                  ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
                WHERE i.indisprimary
                  AND n.nspname = ANY(%s)
                ORDER BY n.nspname, c.relname, a.attnum
                """,
                (schemas,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    out = {}

    for schema, table, column in rows:
        key = (schema, table)

        if key in wanted:
            out.setdefault(key, []).append(column)

    return out


# ------------------------------------------------------------
# Table sets (именованные наборы)
# ------------------------------------------------------------

def create_table_set(data):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO table_sets (name, connection_id, tables_json, rules_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                data.get("name") or "set",
                data.get("connection_id"),
                json.dumps(data.get("tables") or [], ensure_ascii=False),
                json.dumps(data.get("rules") or {}, ensure_ascii=False),
            ),
        )
        return cur.lastrowid


def _row_to_set(row):
    out = dict(row)
    out["tables"] = json.loads(out.pop("tables_json") or "[]")
    out["rules"] = json.loads(out.pop("rules_json") or "{}")
    return out


def get_table_set(set_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM table_sets WHERE id = ?", (set_id,))
        row = cur.fetchone()

    return _row_to_set(row) if row else None


def list_table_sets(connection_id=None):
    with sqlite_cursor() as cur:
        if connection_id:
            cur.execute(
                "SELECT * FROM table_sets WHERE connection_id = ? ORDER BY name",
                (connection_id,),
            )
        else:
            cur.execute("SELECT * FROM table_sets ORDER BY name")

        return [_row_to_set(r) for r in cur.fetchall()]


def delete_table_set(set_id):
    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM table_sets WHERE id = ?", (set_id,))
