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

# ------------------------------------------------------------
# сохранённые ключи синхронизации
# ------------------------------------------------------------

def save_sync_key(connection_id, schema, table, columns, source):
    """Запомнить ключ таблицы, чтобы не искать его заново."""
    if not columns:
        return

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO sync_keys (
                connection_id, schema_name, table_name, columns_json, source,
                found_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(connection_id, schema_name, table_name) DO UPDATE SET
                columns_json = excluded.columns_json,
                source = excluded.source,
                found_at = excluded.found_at
            """,
            (int(connection_id), schema, table,
             json.dumps(list(columns), ensure_ascii=False), source),
        )


def load_sync_keys(connection_id, tables=None):
    """
    Сохранённые ключи: {(schema, table): {"columns": [...], "source": str,
    "found_at": str}}. tables — ограничить выборку (список пар).
    """
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT schema_name, table_name, columns_json, source, found_at
            FROM sync_keys
            WHERE connection_id = ?
            """,
            (int(connection_id),),
        )
        rows = cur.fetchall()

    wanted = set(tables or [])
    out = {}

    for row in rows:
        key = (row["schema_name"], row["table_name"])

        if wanted and key not in wanted:
            continue

        try:
            columns = json.loads(row["columns_json"])
        except Exception:
            continue

        if columns:
            out[key] = {"columns": columns, "source": row["source"],
                        "found_at": row["found_at"]}

    return out


def forget_sync_key(connection_id, schema, table):
    """Забыть сохранённый ключ — чтобы искать заново."""
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            DELETE FROM sync_keys
            WHERE connection_id = ? AND schema_name = ? AND table_name = ?
            """,
            (int(connection_id), schema, table),
        )

        return cur.rowcount


# ------------------------------------------------------------
# размеры таблиц: подсказка, что грузить целиком тяжело
# ------------------------------------------------------------

def fetch_table_sizes(connection_id, tables):
    """
    {(schema, table): {"bytes": n, "rows": m}} — размер на диске и оценка
    строк. В Greenplum данные лежат на сегментах, поэтому размер берём
    через gp_dist_random и суммируем по всему дереву партиций; на обычном
    Postgres хватает pg_total_relation_size на мастере.
    """
    if not tables:
        return {}

    cfg = get_connection_by_id(int(connection_id))

    if not cfg:
        raise ValueError("Connection not found: {}".format(connection_id))

    conn = open_psycopg2_connection_by_cfg(cfg)
    out = {}

    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '60s'")

            pairs = list(tables)
            owner = {}          # oid потомка -> (schema, table) корня
            all_oids = []

            for i in range(0, len(pairs), 200):
                chunk = pairs[i:i + 200]
                placeholders = ", ".join(["(%s, %s)"] * len(chunk))
                params = [v for pair in chunk for v in pair]

                # корни и все их партиции одним проходом
                cur.execute(
                    """
                    WITH RECURSIVE roots AS (
                        SELECT c.oid, n.nspname AS s, c.relname AS t,
                               c.reltuples
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE (n.nspname, c.relname) IN ({})
                    ),
                    tree AS (
                        SELECT oid, s, t, reltuples FROM roots
                        UNION ALL
                        SELECT ch.oid, tr.s, tr.t, ch.reltuples
                        FROM tree tr
                        JOIN pg_inherits i ON i.inhparent = tr.oid
                        JOIN pg_class ch ON ch.oid = i.inhrelid
                    )
                    SELECT oid, s, t, reltuples FROM tree
                    """.format(placeholders),
                    params,
                )

                for oid, schema, table, reltuples in cur.fetchall():
                    owner[oid] = (schema, table)
                    all_oids.append(oid)

                    entry = out.setdefault((schema, table),
                                           {"bytes": 0, "rows": 0})
                    entry["rows"] += max(0, int(reltuples or 0))

            if not all_oids:
                return out

            # размер: на мастере он нулевой, поэтому спрашиваем сегменты
            sizes = {}
            distributed = True

            for i in range(0, len(all_oids), 500):
                chunk = all_oids[i:i + 500]
                values = ", ".join("({}::oid)".format(int(o)) for o in chunk)

                try:
                    cur.execute(
                        """
                        SELECT c.oid,
                               sum(pg_total_relation_size(c.oid))::bigint
                        FROM gp_dist_random('pg_class') c
                        WHERE c.oid IN (SELECT v.o FROM (VALUES {}) v(o))
                        GROUP BY c.oid
                        """.format(values)
                    )
                except Exception:
                    conn.rollback()
                    distributed = False
                    break

                for oid, size in cur.fetchall():
                    sizes[oid] = int(size or 0)

            if not distributed:
                # обычный Postgres: всё лежит на этом же сервере
                cur.execute("SET statement_timeout = '60s'")

                for i in range(0, len(all_oids), 500):
                    chunk = all_oids[i:i + 500]
                    values = ", ".join(
                        "({}::oid)".format(int(o)) for o in chunk)

                    cur.execute(
                        """
                        SELECT v.o, pg_total_relation_size(v.o)
                        FROM (VALUES {}) v(o)
                        """.format(values)
                    )

                    for oid, size in cur.fetchall():
                        sizes[oid] = int(size or 0)

            for oid, size in sizes.items():
                key = owner.get(oid)

                if key:
                    out[key]["bytes"] += size
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return out


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


def filter_candidates_by_stats(candidates, stats, reltuples):
    """
    Бесплатный отсев кандидатов по статистике ANALYZE (чистая функция).

    stats: {column: {"n_distinct": float, "null_frac": float}}.
    Семантика pg_stats.n_distinct: -1 = все значения уникальны,
    -f (0..1) = доля уникальных, положительное = оценка числа значений.

    Возвращает (keep, rejected): keep — сперва stats-уникальные, затем
    кандидаты без статистики; rejected — [{"column", "reason"}].
    """
    sure = []
    unknown = []
    rejected = []

    for col in candidates:
        st = stats.get(col)

        if st is None:
            unknown.append(col)
            continue

        if (st.get("null_frac") or 0) > 0:
            rejected.append({"column": col, "reason": "nulls"})
            continue

        nd = st.get("n_distinct") or 0

        if nd < 0:
            # доля уникальных значений
            if nd <= -0.99:
                sure.append(col)
            else:
                rejected.append({"column": col, "reason": "low_cardinality"})
        elif reltuples and reltuples > 0:
            if nd >= 0.99 * reltuples:
                sure.append(col)
            else:
                rejected.append({"column": col, "reason": "low_cardinality"})
        else:
            # сравнивать не с чем — проверим по данным
            unknown.append(col)

    return sure + unknown, rejected


_KEY_UNFIT_TYPES = ("boolean", "bool")


def rank_candidates_by_stats(columns_with_types, stats, reltuples, limit=8):
    """
    Кандидаты в уникальные колонки по статистике всех колонок таблицы.

    Жёсткий отсев — ТОЛЬКО по фактам, которые ANALYZE реально видел
    в сэмпле (строки настоящие, значит выводы точные):
      null_frac > 0 — в сэмпле были NULL;
      has_mcv       — какое-то значение встретилось в сэмпле дважды+
                      (most_common_vals непуст) = настоящий дубликат;
      bool          — непригодный тип.

    n_distinct — лишь ПОРЯДОК проверки: это оценка, и у по-настоящему
    уникальных колонок на больших таблицах она систематически занижается,
    поэтому отбрасывать по ней нельзя — только проверять данными раньше
    или позже. Колонки без статистики идут после (тай-брейк — имя/тип).

    stats: {column: {"n_distinct", "null_frac", "has_mcv"}}.
    Возвращает (keep, rejected: [{"column","reason"}]).
    """
    scored = []    # (uniqueness_estimate, name) — есть статистика
    unknown = []   # (heuristic_score, name) — статистики нет
    rejected = []

    for name, data_type in columns_with_types:
        if (data_type or "").lower() in _KEY_UNFIT_TYPES:
            rejected.append({"column": name, "reason": "type"})
            continue

        st = stats.get(name)

        if st is None:
            unknown.append((_candidate_score(name, data_type) or 0, name))
            continue

        if (st.get("null_frac") or 0) > 0:
            rejected.append({"column": name, "reason": "nulls"})
            continue

        if st.get("has_mcv"):
            rejected.append({"column": name, "reason": "duplicates"})
            continue

        nd = st.get("n_distinct") or 0

        if nd < 0:
            uniq = -nd  # доля уникальных значений (1.0 = все уникальны)
        elif reltuples and reltuples > 0:
            uniq = float(nd) / float(reltuples)
        else:
            unknown.append((_candidate_score(name, data_type) or 0, name))
            continue

        scored.append((uniq, name))

    scored.sort(key=lambda x: -x[0])
    unknown.sort(key=lambda x: -x[0])

    keep = [n for _s, n in scored] + [n for _s, n in unknown]
    return keep[:int(limit)], rejected


def probe_unique_column(connection_id, schema, table, limit_candidates=8,
                        statement_timeout_ms=120000, sample_rows=500000,
                        full_scan_max_rows=20000000):
    """
    Лёгкий поиск уникальной колонки (когда нет ни PK, ни индексов):

    1) pg_stats (бесплатно): кандидаты с NULL или низкой кардинальностью
       отсеиваются без сканов; n_distinct = -1 идёт первым.
    2) сэмпл (дёшево): NULL или дубликат в первых `sample_rows` строках —
       точный отказ.
    3) подтверждение одним GROUP BY-проходом — но только если таблица
       не больше full_scan_max_rows (по reltuples). Для гигантских таблиц
       полное доказательство = чтение всей таблицы, поэтому вердикт
       останавливается на "confidence": "sample" (статистика + сэмпл),
       и UI помечает такой ключ отдельным бейджем.

    Возвращает {"column", "confidence": "confirmed"|"sample"|None, "checked"}.
    """
    columns = fetch_columns_with_types(connection_id, schema, table)

    if not columns:
        return {"column": None, "confidence": None, "checked": []}

    cfg = get_connection_by_id(int(connection_id))
    conn = open_psycopg2_connection_by_cfg(cfg)

    checked = []

    def q(sql, params=None):
        with conn.cursor() as cur:
            cur.execute(sql, params)
            try:
                return cur.fetchall()
            except Exception:
                return []

    try:
        if statement_timeout_ms:
            q("SET statement_timeout = {}".format(int(statement_timeout_ms)))

        # пустая таблица — уникальность не доказать
        if not q("SELECT 1 FROM {}.{} LIMIT 1".format(_qident(schema), _qident(table))):
            return {"column": None, "checked": [{"reason": "empty_table"}]}

        # 1) статистика ANALYZE. Для иерархий наследования pg_stats держит
        # ДВЕ строки на колонку: inherited=t (вся иерархия партиций) и
        # inherited=f (только сама таблица). У партиционированного родителя
        # собственная строка пуста/бессмысленна — предпочитаем inherited=t.
        stats = {}
        for attname, nd, nf, inherited, has_mcv in q(
            "SELECT attname, n_distinct, null_frac, inherited, "
            "       most_common_vals IS NOT NULL AS has_mcv "
            "FROM pg_stats "
            "WHERE schemaname = %s AND tablename = %s",
            (schema, table),
        ):
            cur = stats.get(attname)
            if cur is None or (inherited and not cur.get("inherited")):
                stats[attname] = {
                    "n_distinct": float(nd),
                    "null_frac": float(nf),
                    "inherited": bool(inherited),
                    "has_mcv": bool(has_mcv),
                }
        # Размер: у партиционированного родителя reltuples = 0, поэтому
        # суммируем по всему дереву pg_inherits (root + все партиции).
        rel = q(
            """
            WITH RECURSIVE tree AS (
                SELECT c.oid FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                UNION ALL
                SELECT i.inhrelid FROM tree
                JOIN pg_inherits i ON i.inhparent = tree.oid
            )
            SELECT COALESCE(SUM(c.reltuples), 0)::bigint
            FROM tree JOIN pg_class c ON c.oid = tree.oid
            """,
            (schema, table),
        )
        reltuples = int(rel[0][0]) if rel else 0

        # кандидаты — по статистике ВСЕХ колонок, имя роли не играет
        keep, rejected = rank_candidates_by_stats(
            columns, stats, reltuples, limit=limit_candidates,
        )
        for r in rejected:
            checked.append({"column": r["column"], "unique": False,
                            "stage": "stats", "reason": r["reason"]})

        fq = "{}.{}".format(_qident(schema), _qident(table))

        # NULL и дубликаты ловятся ОДНИМ GROUP BY-проходом: NULL-группа
        # попадает под `c IS NULL`, дубликаты — под count(*) > 1.
        # Отдельный `WHERE c IS NULL LIMIT 1` убран: без NULL он был
        # полным сканом таблицы.
        def bad_group(source_sql):
            rows = q(
                "SELECT c FROM ({src}) s GROUP BY c "
                "HAVING count(*) > 1 OR c IS NULL LIMIT 1".format(src=source_sql),
            )
            if not rows:
                return None
            return "nulls" if rows[0][0] is None else "duplicate"

        # гигантская таблица: полный проход не делаем, вердикт по сэмплу
        huge = bool(full_scan_max_rows) and reltuples > int(full_scan_max_rows)

        for candidate in keep:
            c = _qident(candidate)
            try:
                # 2) дешёвый отсев: NULL или дубликат в первых sample_rows строках
                reason = bad_group(
                    "SELECT {c} AS c FROM {fq} LIMIT {n}".format(
                        c=c, fq=fq, n=int(sample_rows)),
                )
                if reason:
                    checked.append({"column": candidate, "unique": False,
                                    "stage": "sample", "reason": reason})
                    continue

                if huge:
                    # статистика говорит "уникальна", сэмпл чистый —
                    # честно возвращаем без полного доказательства
                    checked.append({"column": candidate, "unique": True,
                                    "stage": "sample",
                                    "rows_estimate": reltuples})
                    return {"column": candidate, "confidence": "sample",
                            "checked": checked}

                # 3) точное подтверждение: один агрегатный проход по всей таблице
                reason = bad_group("SELECT {c} AS c FROM {fq}".format(c=c, fq=fq))
                if reason:
                    checked.append({"column": candidate, "unique": False,
                                    "stage": "full", "reason": reason})
                    continue

                checked.append({"column": candidate, "unique": True,
                                "stage": "full"})
                return {"column": candidate, "confidence": "confirmed",
                        "checked": checked}

            except Exception:
                conn.rollback()
                checked.append({"column": candidate, "unique": False,
                                "timeout": True, "stage": "full"})
                continue

        return {"column": None, "confidence": None, "checked": checked}

    finally:
        conn.close()
