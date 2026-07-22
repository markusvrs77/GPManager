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


_part_cache = {}


def fetch_partition_pairs(connection_id):
    """
    Пары (child -> parent) из pg_inherits для всего кластера, с TTL-кэшем.
    Возвращает dict {(schema, table): (parent_schema, parent_table)}.
    """
    key = int(connection_id)
    now = time.time()

    with _cache_lock:
        entry = _part_cache.get(key)
        if entry and now - entry["ts"] < CACHE_TTL_SECONDS:
            return entry["pairs"]

    cfg = get_connection_by_id(key)
    if not cfg:
        raise ValueError("Connection not found: {}".format(connection_id))

    conn = open_psycopg2_connection_by_cfg(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cn.nspname, cc.relname, pn.nspname, pc.relname
                FROM pg_inherits i
                JOIN pg_class cc ON cc.oid = i.inhrelid
                JOIN pg_namespace cn ON cn.oid = cc.relnamespace
                JOIN pg_class pc ON pc.oid = i.inhparent
                JOIN pg_namespace pn ON pn.oid = pc.relnamespace
                WHERE cn.nspname NOT IN %s
                """,
                (_SYSTEM_SCHEMAS,),
            )
            pairs = {(r[0], r[1]): (r[2], r[3]) for r in cur.fetchall()}
    finally:
        conn.close()

    with _cache_lock:
        _part_cache[key] = {"pairs": pairs, "ts": now}

    return pairs


def classify_partition_roles(tables, child_parent):
    """
    Роли таблиц по pg_inherits (чистая функция).

    tables: [(schema, table)], child_parent: {(s,t): (parent_s, parent_t)}.
    -> {(s,t): {"kind": "regular"|"parent"|"partition",
                "root": (s,t)|None, "partitions": int}}
    Субпартиции сворачиваются к корневому родителю.
    """
    parents = set(child_parent.values())

    def find_root(key):
        seen = set()
        while key in child_parent and key not in seen:
            seen.add(key)
            key = child_parent[key]
        return key

    roles = {}
    root_counts = {}

    for key in tables:
        if key in child_parent:
            root = find_root(key)
            roles[key] = {"kind": "partition", "root": root, "partitions": 0}
            root_counts[root] = root_counts.get(root, 0) + 1
        elif key in parents:
            roles[key] = {"kind": "parent", "root": None, "partitions": 0}
        else:
            roles[key] = {"kind": "regular", "root": None, "partitions": 0}

    for root, n in root_counts.items():
        if root in roles:
            roles[root]["partitions"] = n

    return roles


def schema_tables_with_roles(connection_id, schema):
    """
    Таблицы одной схемы с ролями для UI-селектора:
    [{"table", "kind", "partitions", "parent"}], сортировка по имени.
    """
    tables, _ts = get_catalog(connection_id)
    schema_tables = [(s, t) for s, t in tables if s == schema]

    pairs = fetch_partition_pairs(connection_id)
    roles = classify_partition_roles(schema_tables, pairs)

    out = []
    for s, t in schema_tables:
        info = roles[(s, t)]
        out.append({
            "table": t,
            "kind": info["kind"],
            "partitions": info["partitions"],
            "parent": info["root"][1] if info["root"] else None,
        })

    out.sort(key=lambda r: r["table"])
    return out


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


# ------------------------------------------------------------
# Умный подбор ключей и колонок:
# PK -> уникальный индекс -> вычисление уникальности по данным;
# приоритетный список дат -> фолбэк на любую date/timestamp колонку.
# ------------------------------------------------------------

def resolve_keys_hierarchy(tables, pk_map, unique_map):
    """
    Чистая функция. Для каждой таблицы: PK, иначе кратчайший уникальный
    индекс, иначе — в unresolved (кандидат на вычисление по данным).
    """
    resolved = {}
    unresolved = []

    for key in tables:
        if key in pk_map and pk_map[key]:
            resolved[key] = {"columns": pk_map[key], "source": "pk"}
        elif key in unique_map and unique_map[key]:
            shortest = sorted(unique_map[key], key=len)[0]
            resolved[key] = {"columns": shortest, "source": "unique_index"}
        else:
            unresolved.append(key)

    return resolved, unresolved


_METRIC_TYPES = ("numeric", "decimal", "double", "real", "float", "money")


def _candidate_score(name, data_type):
    lowered = name.lower()
    type_lowered = (data_type or "").lower()

    for metric in _METRIC_TYPES:
        if metric in type_lowered:
            return None  # метрики не бывают ключами

    if lowered == "id":
        name_score = 100
    elif "guid" in lowered or "uuid" in lowered:
        name_score = 80
    elif lowered.endswith("_id") or lowered.startswith("id_"):
        name_score = 60
    elif any(p in lowered for p in ("code", "key", "hash", "num")):
        name_score = 50
    else:
        name_score = 10

    if "uuid" in type_lowered:
        type_score = 30
    elif "int" in type_lowered:
        type_score = 20
    elif "char" in type_lowered or "text" in type_lowered:
        type_score = 10
    else:
        type_score = 0

    return name_score + type_score


def choose_candidate_columns(columns_with_types, limit=5):
    """Ранжирует колонки-кандидаты на уникальность (id/uuid/code раньше)."""
    scored = []

    for name, data_type in columns_with_types:
        score = _candidate_score(name, data_type)

        if score is not None:
            scored.append((score, name))

    scored.sort(key=lambda x: -x[0])
    return [name for _score, name in scored[:int(limit)]]


def pick_columns_with_fallback(columns_by_table, priority, date_cols_by_table):
    """
    Приоритетный список -> фолбэк: первая date/timestamp колонка таблицы.
    Возвращает (resolved: {key: {column, via}}, missing: [key]).
    """
    base_resolved, base_missing = pick_columns(columns_by_table, priority)

    resolved = {
        key: {"column": column, "via": "priority"}
        for key, column in base_resolved.items()
    }
    missing = []

    for key in base_missing:
        fallback = (date_cols_by_table or {}).get(key) or []

        if fallback:
            resolved[key] = {"column": fallback[0], "via": "fallback_date"}
        else:
            missing.append(key)

    return resolved, sorted(missing)


def fetch_unique_indexes(connection_id, tables):
    """
    (pk_map, unique_map) одним запросом: pk_map={key: [cols]},
    unique_map={key: [[cols], ...]} — уникальные индексы без PK.
    """
    if not tables:
        return {}, {}

    cfg = get_connection_by_id(int(connection_id))
    conn = open_psycopg2_connection_by_cfg(cfg)

    wanted = {(s, t) for s, t in tables}
    schemas = sorted({s for s, _t in wanted})

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT n.nspname, c.relname, i.indexrelid::bigint,
                       i.indisprimary, a.attname
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a
                  ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
                WHERE i.indisunique
                  AND n.nspname = ANY(%s)
                ORDER BY n.nspname, c.relname, i.indexrelid, a.attnum
                """,
                (schemas,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    by_index = {}

    for schema, table, index_oid, is_primary, column in rows:
        key = (schema, table)

        if key not in wanted:
            continue

        by_index.setdefault((key, index_oid, bool(is_primary)), []).append(column)

    pk_map = {}
    unique_map = {}

    for (key, _oid, is_primary), columns in by_index.items():
        if is_primary:
            pk_map[key] = columns
        else:
            unique_map.setdefault(key, []).append(columns)

    return pk_map, unique_map


_DATE_TYPES = (
    "date", "timestamp without time zone", "timestamp with time zone",
)


def fetch_date_columns_bulk(connection_id, tables):
    """{key: [date/timestamp колонки по ordinal]} одним запросом."""
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
                SELECT table_schema, table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = ANY(%s)
                  AND data_type = ANY(%s)
                ORDER BY table_schema, table_name, ordinal_position
                """,
                (schemas, list(_DATE_TYPES)),
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


def fetch_columns_with_types(connection_id, schema, table):
    cfg = get_connection_by_id(int(connection_id))
    conn = open_psycopg2_connection_by_cfg(cfg)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            )
            return [(r[0], r[1]) for r in cur.fetchall()]
    finally:
        conn.close()


def _qident(name):
    return '"' + str(name).replace('"', '""') + '"'


def probe_unique_column(connection_id, schema, table, limit_candidates=5,
                        statement_timeout_ms=120000):
    """
    Вычисление уникальной колонки по данным (когда нет ни PK, ни индексов):
    кандидаты по эвристике, для каждого count(*) == count(col) ==
    count(distinct col). Останавливается на первом уникальном.

    statement_timeout_ms ограничивает каждый пробный запрос на кластере —
    гигантская таблица не повиснет навсегда; кандидат с таймаутом
    помечается timeout=True и пропускается.
    """
    columns = fetch_columns_with_types(connection_id, schema, table)
    candidates = choose_candidate_columns(columns, limit=limit_candidates)

    if not candidates:
        return {"column": None, "checked": []}

    cfg = get_connection_by_id(int(connection_id))
    conn = open_psycopg2_connection_by_cfg(cfg)

    checked = []

    try:
        if statement_timeout_ms:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = {}".format(
                    int(statement_timeout_ms)))

        for candidate in candidates:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT count(*), count({c}), count(DISTINCT {c}) FROM {s}.{t}".format(
                            c=_qident(candidate),
                            s=_qident(schema),
                            t=_qident(table),
                        )
                    )
                    total, non_null, distinct = cur.fetchone()
            except Exception:
                # statement_timeout или другая ошибка — честно пропускаем кандидата
                conn.rollback()
                checked.append({"column": candidate, "timeout": True, "unique": False})
                continue

            is_unique = bool(total) and total == non_null == distinct
            checked.append({
                "column": candidate,
                "rows": int(total),
                "nulls": int(total - non_null),
                "distinct": int(distinct),
                "unique": is_unique,
            })

            if is_unique:
                return {"column": candidate, "checked": checked}

        return {"column": None, "checked": checked}

    finally:
        conn.close()
