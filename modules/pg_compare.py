# -*- coding: utf-8 -*-
"""
Сравнение двух баз PostgreSQL (Postgres Toolkit → «Сравнение и разница»).

Задача pg_compare по каждой выбранной таблице считает строки в источнике
и приёмнике и сколько строк добавить, изменить и удалить:

  * источник отдаёт COPY (SELECT ключ::text..., md5(ROW(колонки)::text))
    TO STDOUT — по сети идут только ключи и хеши, источник только читается;
  * поток заливается во временную таблицу сессии приёмника (ANALYZE),
    подсчёт идёт в приёмнике за один проход его таблицы: FULL OUTER JOIN
    по ключу, без ключа — одна агрегация хешей обеих сторон;
  * work_mem поднимается SET LOCAL только в транзакции приёмника;
  * проверки дублей ключа пропускаются, где уникальность гарантирована
    каталогом (PK / годный уникальный индекс);
  * разные наборы колонок — structure_diff, данные не читаются.

Ключ: PK → уникальный индекс → сохранённый ключ (sync_keys), через
существующие функции table_catalog; ключ должен быть в обеих таблицах.
Результаты пишутся в SQLite pg_compare_results сразу после таблицы.
"""

import json
import queue
import threading

from psycopg2 import sql

from db import sqlite_cursor
from job_manager import (
    get_job,
    get_job_items,
    mark_item_done,
    mark_item_failed,
    mark_item_running,
    mark_item_skipped,
    mark_job_cancelled,
    mark_job_done,
    mark_job_failed,
    mark_job_running,
    now_str,
    refresh_job_progress,
)

import modules.pg_ranges as pg_ranges
import modules.table_catalog as table_catalog
from modules.pg_sync_common import (
    StopWatch,
    open_pg,
    row_hash_sql,
    stream_copy,
    table_column_types,
    table_columns,
)
from modules.sync_transport import job_config


STATUSES = ("same", "differs", "no_dest", "no_source", "structure_diff",
            "duplicate_keys", "error", "cancelled")

TEMP_TABLE = "pgcmp_src"

# сколько таблиц сравнивается одновременно: по паре соединений на воркер
PARALLEL_DEFAULT = 4
PARALLEL_MIN = 1
PARALLEL_MAX = 8
HASH_COLUMN = "h"

# work_mem транзакции приёмника на время сравнения (SET LOCAL — уходит
# с откатом): хеш-соединению и агрегации хешей хватает памяти без диска
WORK_MEM = "256MB"

# общий бюджет work_mem на всё сравнение: делится между воркерами, чтобы
# параллельность не умножала память приёмника; меньше минимума не даём
WORK_MEM_TOTAL_MB = 512
WORK_MEM_MIN_MB = 32


def worker_work_mem(workers):
    """work_mem одного воркера: бюджет / число воркеров, не меньше минимума."""
    per = WORK_MEM_TOTAL_MB // max(1, int(workers))
    return "%dMB" % max(WORK_MEM_MIN_MB, per)


# ------------------------------------------------------------------
# Раскрытие выбора
# ------------------------------------------------------------------

def _catalog(conn, schemas):
    """
    (схемы, которые есть; {(s, t)} таблиц r/p; {child: parent}) по
    списку схем одной стороны. Транзакция чтения закрывается откатом.
    """
    if not schemas:
        return set(), [], {}

    cur = conn.cursor()

    try:
        cur.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)",
            (list(schemas),),
        )
        present = {r[0] for r in cur.fetchall()}

        cur.execute(
            """
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname = ANY(%s)
            ORDER BY n.nspname, c.relname
            """,
            (list(schemas),),
        )
        relations = [(r[0], r[1]) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT cn.nspname, cc.relname, pn.nspname, pc.relname
            FROM pg_inherits i
            JOIN pg_class cc ON cc.oid = i.inhrelid
            JOIN pg_namespace cn ON cn.oid = cc.relnamespace
            JOIN pg_class pc ON pc.oid = i.inhparent
            JOIN pg_namespace pn ON pn.oid = pc.relnamespace
            WHERE cn.nspname = ANY(%s)
            """,
            (list(schemas),),
        )
        pairs = {(r[0], r[1]): (r[2], r[3]) for r in cur.fetchall()}
    finally:
        try:
            conn.rollback()
        except Exception:
            pass

    return present, relations, pairs


def _clean_name(value):
    return value.strip() if isinstance(value, str) else ""


def expand_selection(src_conn, dst_conn, schemas, tables):
    """
    Выбор «схемы целиком + отдельные таблицы» → [{schema, table, in_src,
    in_dst}]. Схема раскрывается в r/p-таблицы обеих сторон (таблица только
    в приёмнике → in_src=False). Отдельные таблицы сверяются с каталогом
    источника. Листья выбранного партиционированного родителя и повторы
    убираются. Неизвестные имена — ValueError.
    """
    schema_list = []
    for name in schemas or []:
        name = _clean_name(name)
        if name and name not in schema_list:
            schema_list.append(name)

    table_list = []
    for item in tables or []:
        item = item if isinstance(item, dict) else {}
        key = (_clean_name(item.get("schema")), _clean_name(item.get("table")))
        if not key[0] or not key[1]:
            raise ValueError("Таблица указана без схемы или имени")
        if key not in table_list:
            table_list.append(key)

    if not schema_list and not table_list:
        raise ValueError("Не выбраны ни схемы, ни таблицы")

    wanted = sorted(set(schema_list) | {s for s, _t in table_list})
    src_ns, src_rel, src_pairs = _catalog(src_conn, wanted)
    dst_ns, dst_rel, dst_pairs = _catalog(dst_conn, wanted)

    unknown = [s for s in schema_list if s not in src_ns]
    if unknown:
        raise ValueError("Схема не найдена в источнике: %s" % ", ".join(unknown))

    src_set = set(src_rel)
    dst_set = set(dst_rel)

    missing = ["%s.%s" % k for k in table_list if k not in src_set]
    if missing:
        raise ValueError(
            "Таблица не найдена в источнике: %s" % ", ".join(missing)
        )

    ordered = []

    for schema in schema_list:
        ordered.extend(k for k in src_rel if k[0] == schema)
        ordered.extend(k for k in dst_rel if k[0] == schema and k not in src_set)

    ordered.extend(table_list)

    child_parent = dict(dst_pairs)
    child_parent.update(src_pairs)
    kept, _covered = table_catalog.drop_covered_partitions(ordered,
                                                           child_parent)

    return [
        {"schema": s, "table": t, "in_src": (s, t) in src_set,
         "in_dst": (s, t) in dst_set}
        for s, t in kept
    ]


# ------------------------------------------------------------------
# SQL сравнения (чистые построители)
# ------------------------------------------------------------------

def _key_names(key_columns):
    return ["k%d" % i for i in range(len(key_columns))]


def _table_ident(schema, table):
    return sql.Identifier(schema, table)


def _where(where):
    """« WHERE <предикат>» (алиас таблицы — t) или пусто."""
    if where is None:
        return sql.SQL("")
    return sql.SQL(" WHERE {}").format(where)


def build_source_select(schema, table, key_columns, columns, where=None):
    """SELECT ключ::text..., md5(ROW(колонки)::text) FROM schema.table AS t
    [WHERE where]."""
    parts = [sql.SQL("{}::text").format(sql.Identifier("t", k))
             for k in key_columns]
    parts.append(row_hash_sql("t", columns))

    return sql.SQL("SELECT {} FROM {} AS {}{}").format(
        sql.SQL(", ").join(parts),
        _table_ident(schema, table),
        sql.Identifier("t"),
        _where(where),
    )


def build_temp_table_sql(key_columns):
    cols = [sql.SQL("{} text").format(sql.Identifier(n))
            for n in _key_names(key_columns) + [HASH_COLUMN]]

    return sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(
        sql.Identifier(TEMP_TABLE), sql.SQL(", ").join(cols)
    )


def build_drop_temp_sql():
    """Только временная схема сессии: одноимённую обычную таблицу не задеть."""
    return sql.SQL("DROP TABLE IF EXISTS {}").format(
        sql.Identifier("pg_temp", TEMP_TABLE)
    )


def build_analyze_temp_sql():
    """Статистика временной таблицы: без неё планировщик слеп к её размеру."""
    return sql.SQL("ANALYZE {}").format(sql.Identifier("pg_temp", TEMP_TABLE))


def build_work_mem_sql(work_mem=WORK_MEM):
    return sql.SQL("SET LOCAL work_mem = {}").format(sql.Literal(work_mem))


def build_dest_duplicate_sql(schema, table, key_columns, where=None):
    """Сколько значений ключа повторяется в таблице (приёмника)."""
    return sql.SQL(
        "SELECT count(*) FROM (SELECT 1 FROM {} AS {}{} GROUP BY {} "
        "HAVING count(*) > 1) AS x"
    ).format(
        _table_ident(schema, table),
        sql.Identifier("t"),
        _where(where),
        sql.SQL(", ").join(sql.Identifier("t", k) for k in key_columns),
    )


# PK или уникальный индекс приёмника ровно на ключевые колонки: не
# частичный, без выражений, валидный, все колонки NOT NULL — тогда
# дублей ключа в приёмнике быть не может и проверять их незачем
DEST_KEY_UNIQUE_SQL = """
    SELECT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
          AND i.indisunique AND i.indisvalid
          AND i.indpred IS NULL AND i.indexprs IS NULL
          AND i.indnkeyatts = cardinality(%s::text[])
          AND ARRAY(
                SELECT col.attname::text
                FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
                JOIN pg_attribute col
                  ON col.attrelid = c.oid AND col.attnum = k.attnum
                WHERE k.ord <= i.indnkeyatts AND col.attnotnull
                ORDER BY 1
              ) = ARRAY(SELECT unnest(%s::text[]) ORDER BY 1)
    )
"""

# ключ из этих источников уже уникален и NOT NULL в источнике
UNIQUE_KEY_SOURCES = ("pk", "unique_index")


def dest_key_is_unique(conn, schema, table, key_columns):
    """Есть ли в приёмнике PK / годный уникальный индекс ровно на ключе."""
    cur = conn.cursor()
    cur.execute(DEST_KEY_UNIQUE_SQL,
                (schema, table, list(key_columns), list(key_columns)))
    row = cur.fetchone()
    return bool(row and row[0])


def build_duplicate_sql(key_columns):
    """Сколько значений ключа повторяется в потоке источника."""
    return sql.SQL(
        "SELECT count(*) FROM (SELECT 1 FROM {} GROUP BY {} "
        "HAVING count(*) > 1) AS x"
    ).format(
        sql.Identifier(TEMP_TABLE),
        sql.SQL(", ").join(sql.Identifier(n) for n in _key_names(key_columns)),
    )


def build_count_sql(schema, table, key_columns, columns, where=None):
    """
    Одна строка (dst_rows, to_insert, to_update, to_delete) по временной
    таблице источника и таблице приёмника, за один проход приёмника.

    С ключом — FULL OUTER JOIN по ключу (сравнение «=», NULL-ключ пары
    не находит): нет пары в d — вставка, нет пары в s — удаление, пара с
    другим хешем — изменение. Хеш (md5) не бывает NULL, поэтому NULL
    в h означает отсутствие стороны.
    Без ключа — одна агрегация хешей обеих сторон: лишние копии хеша
    в источнике — вставка, в приёмнике — удаление (как EXCEPT ALL).
    """
    hash_expr = row_hash_sql("t", columns)
    src = sql.Identifier(TEMP_TABLE)
    h = sql.Identifier(HASH_COLUMN)
    t_alias = sql.Identifier("t")

    if not key_columns:
        sc, dc = sql.Identifier("sc"), sql.Identifier("dc")
        side = sql.Identifier("side")
        return sql.SQL(
            "SELECT sum({dc}) AS dst_rows, "
            "sum(greatest({sc} - {dc}, 0)) AS to_insert, 0 AS to_update, "
            "sum(greatest({dc} - {sc}, 0)) AS to_delete "
            "FROM (SELECT {h}, "
            "count(*) FILTER (WHERE {side} = 1) AS {sc}, "
            "count(*) FILTER (WHERE {side} = 2) AS {dc} "
            "FROM (SELECT {h}, 1 AS {side} FROM {src} "
            "UNION ALL SELECT {hash}, 2 FROM {tbl} AS {t}{w}) AS {u} "
            "GROUP BY {h}) AS {x}"
        ).format(sc=sc, dc=dc, side=side, h=h, src=src, hash=hash_expr,
                 tbl=_table_ident(schema, table), t=t_alias, w=_where(where),
                 u=sql.Identifier("u"), x=sql.Identifier("x"))

    names = _key_names(key_columns)
    casts = sql.SQL(", ").join(
        sql.SQL("{}::text AS {}").format(sql.Identifier("t", k),
                                         sql.Identifier(n))
        for k, n in zip(key_columns, names)
    )
    join = sql.SQL(" AND ").join(
        sql.SQL("{} = {}").format(sql.Identifier("s", n),
                                  sql.Identifier("d", n))
        for n in names
    )
    sh = sql.Identifier("s", HASH_COLUMN)
    dh = sql.Identifier("d", HASH_COLUMN)

    return sql.SQL(
        "SELECT count({dh}) AS dst_rows, "
        "count(*) FILTER (WHERE {dh} IS NULL) AS to_insert, "
        "count(*) FILTER (WHERE {sh} <> {dh}) AS to_update, "
        "count(*) FILTER (WHERE {sh} IS NULL) AS to_delete "
        "FROM {src} AS {s} FULL OUTER JOIN "
        "(SELECT {casts}, {hash} AS {h} FROM {tbl} AS {t}{w}) AS {d} "
        "ON {join}"
    ).format(
        dh=dh, sh=sh, src=src, s=sql.Identifier("s"), casts=casts,
        hash=hash_expr, h=h, tbl=_table_ident(schema, table), t=t_alias,
        w=_where(where),
        d=sql.Identifier("d"), join=join,
    )


# ------------------------------------------------------------------
# Сравнение одной таблицы
# ------------------------------------------------------------------

def _result(status, **values):
    row = {"status": status, "src_rows": None, "dst_rows": None,
           "to_insert": None, "to_update": None, "to_delete": None,
           "message": None}
    row.update(values)
    return row


def compare_table(src_conn, dst_conn, schema, table, key_columns,
                  key_source=None, work_mem=WORK_MEM, where=None):
    """
    Сравнение таблицы, которая есть в обеих базах.
    key_columns — [] для сравнения без ключа (мультимножество строк).
    key_source — откуда ключ ('pk' | 'unique_index' | 'sync_keys' | None):
    для 'pk' и 'unique_index' (годного по valid_unique_keys) дубли ключа
    в источнике не проверяются. Дубли в приёмнике не проверяются, если
    там есть PK / уникальный NOT NULL индекс ровно на ключе.
    work_mem — SET LOCAL для транзакции приёмника (раннер делит бюджет
    WORK_MEM_TOTAL_MB между воркерами).
    where — необязательный предикат (Composable, алиас таблицы t): сравнение
    только строк диапазона — выборка источника, подсчёт и дубли приёмника.
    -> {status, src_rows, dst_rows, to_insert, to_update, to_delete, message}
    Приёмник не меняется: временная таблица уходит вместе с откатом.
    """
    key_columns = list(key_columns or [])

    try:
        src_types = table_column_types(src_conn, schema, table)
        dst_types = table_column_types(dst_conn, schema, table)
        src_cols = list(src_types)
        dst_cols = list(dst_types)

        only_src = sorted(set(src_cols) - set(dst_cols))
        only_dst = sorted(set(dst_cols) - set(src_cols))
        # один и тот же текст значения у разных типов ещё не равенство
        retyped = sorted(c for c in set(src_cols) & set(dst_cols)
                         if src_types[c] != dst_types[c])

        if only_src or only_dst or retyped:
            parts = []
            if only_src:
                parts.append("только в источнике: %s" % ", ".join(only_src))
            if only_dst:
                parts.append("только в приёмнике: %s" % ", ".join(only_dst))
            if retyped:
                parts.append("тип отличается: %s" % ", ".join(
                    "%s (%s → %s)" % (c, src_types[c], dst_types[c])
                    for c in retyped))
            return _result(
                "structure_diff",
                message="Наборы колонок отличаются (%s). Проверьте DDL "
                        "в режиме «Перенос»." % "; ".join(parts),
            )

        absent = [k for k in key_columns if k not in src_cols]
        if absent:
            raise ValueError("Ключевых колонок нет в таблице: %s"
                             % ", ".join(absent))

        cur = dst_conn.cursor()
        cur.execute(build_work_mem_sql(work_mem))
        # остаток от прошлой таблицы, если её откат не прошёл
        cur.execute(build_drop_temp_sql())
        cur.execute(build_temp_table_sql(key_columns))

        src_rows = stream_copy(
            src_conn, dst_conn,
            build_source_select(schema, table, key_columns, src_cols,
                                where),
            sql.Identifier(TEMP_TABLE),
            _key_names(key_columns) + [HASH_COLUMN],
        )
        cur.execute(build_analyze_temp_sql())

        if key_columns and key_source not in UNIQUE_KEY_SOURCES:
            cur.execute(build_duplicate_sql(key_columns))
            duplicates = int(cur.fetchone()[0] or 0)

            if duplicates:
                return _result(
                    "duplicate_keys", src_rows=src_rows,
                    message="Ключ (%s) не уникален в источнике: "
                            "повторяется значений — %d"
                            % (", ".join(key_columns), duplicates),
                )

        if key_columns and not dest_key_is_unique(dst_conn, schema, table,
                                                  key_columns):
            cur.execute(build_dest_duplicate_sql(schema, table, key_columns,
                                                 where))
            duplicates = int(cur.fetchone()[0] or 0)

            if duplicates:
                return _result(
                    "duplicate_keys", src_rows=src_rows,
                    message="Ключ (%s) не уникален в приёмнике: "
                            "повторяется значений — %d"
                            % (", ".join(key_columns), duplicates),
                )

        cur.execute(build_count_sql(schema, table, key_columns, src_cols,
                                    where))
        dst_rows, to_insert, to_update, to_delete = [
            int(v or 0) for v in cur.fetchone()
        ]

        status = "same" if not (to_insert or to_update or to_delete) \
            else "differs"

        return _result(status, src_rows=src_rows, dst_rows=dst_rows,
                       to_insert=to_insert, to_update=to_update,
                       to_delete=to_delete)
    finally:
        for conn in (dst_conn, src_conn):
            try:
                conn.rollback()
            except Exception:
                pass


# ------------------------------------------------------------------
# Ключи
# ------------------------------------------------------------------

def resolve_key_candidates(source_id, tables):
    """
    {(s, t): [{"columns": [...], "source": "pk"|"unique_index"|"sync_keys"}]}
    в порядке PK → уникальный индекс → сохранённый ключ, по источнику.
    """
    tables = list(tables)
    if not tables:
        return {}

    pk_map, unique_map = table_catalog.fetch_unique_indexes(source_id, tables)
    resolved, _unresolved = table_catalog.resolve_keys_hierarchy(
        tables, pk_map, unique_map
    )
    saved = table_catalog.load_sync_keys(source_id, tables)

    out = {}

    for key in tables:
        found = []

        if key in resolved:
            found.append(dict(resolved[key]))

        for columns in sorted(unique_map.get(key) or [], key=len):
            if all(c["columns"] != columns for c in found):
                found.append({"columns": list(columns),
                              "source": "unique_index"})

        if key in saved:
            found.append({"columns": list(saved[key]["columns"]),
                          "source": "sync_keys"})

        out[key] = found

    return out


def valid_unique_keys(conn, schema, table):
    """
    Наборы колонок уникальных индексов источника, годных в ключ: не PK,
    не частичный, без выражений, все ключевые колонки NOT NULL. Иначе
    NULL-ключи и строки вне предиката ломают подсчёт разницы.
    -> [[колонка, ...]]
    """
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT array_agg(a.attname ORDER BY k.ord)
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            CROSS JOIN LATERAL unnest(i.indkey::int2[])
                 WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_attribute a
              ON a.attrelid = c.oid AND a.attnum = k.attnum
            WHERE n.nspname = %s AND c.relname = %s
              AND i.indisunique AND NOT i.indisprimary AND i.indisvalid
              AND i.indpred IS NULL AND i.indexprs IS NULL
              AND k.ord <= i.indnkeyatts
            GROUP BY i.indexrelid
            HAVING bool_and(a.attnotnull)
            """,
            (schema, table),
        )
        return [list(r[0]) for r in cur.fetchall() if r[0]]
    finally:
        try:
            conn.rollback()
        except Exception:
            pass


def pick_key(candidates, src_cols, dst_cols, valid_unique=None):
    """
    Первый ключ-кандидат, все колонки которого есть в обеих таблицах.
    Кандидат unique_index берётся, только если его набор есть в
    valid_unique (см. valid_unique_keys).
    """
    both = set(src_cols) & set(dst_cols)
    allowed = [set(cols) for cols in (valid_unique or [])]

    for candidate in candidates or []:
        columns = candidate.get("columns") or []
        if not columns or not all(c in both for c in columns):
            continue
        if candidate.get("source") == "unique_index" \
                and set(columns) not in allowed:
            continue
        return candidate

    return None


# ------------------------------------------------------------------
# Результаты в SQLite
# ------------------------------------------------------------------

def save_result(job_id, row):
    """Строка результата: schema, table, status, key_columns, key_source,
    src_rows, dst_rows, to_insert, to_update, to_delete, message."""
    status = row.get("status")
    if status not in STATUSES:
        raise ValueError("Неизвестный статус сравнения: %s" % status)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO pg_compare_results (
                job_id, schema_name, table_name, status, key_columns_json,
                key_source, src_rows, dst_rows, to_insert, to_update,
                to_delete, message, compared_at, chunked_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(job_id), row.get("schema"), row.get("table"), status,
                json.dumps(list(row.get("key_columns") or [])),
                row.get("key_source"),
                row.get("src_rows"), row.get("dst_rows"),
                row.get("to_insert"), row.get("to_update"),
                row.get("to_delete"),
                (str(row["message"])[:1000] if row.get("message") else None),
                now_str(),
                (json.dumps(row["chunked"]) if row.get("chunked") else None),
            ),
        )


def get_results(job_id):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT id, job_id, schema_name, table_name, status,
                   key_columns_json, key_source, src_rows, dst_rows,
                   to_insert, to_update, to_delete, message, compared_at,
                   chunked_json
            FROM pg_compare_results
            WHERE job_id = ?
            ORDER BY id
            """,
            (int(job_id),),
        )
        rows = [dict(r) for r in cur.fetchall()]

    out = []

    for r in rows:
        try:
            key_columns = json.loads(r.pop("key_columns_json") or "[]")
        except ValueError:
            key_columns = []

        r["schema"] = r.pop("schema_name")
        r["table"] = r.pop("table_name")
        r["key_columns"] = key_columns
        # сравнение по диапазонам: {checked, total, mismatched}, иначе None
        try:
            r["chunked"] = json.loads(r.pop("chunked_json") or "null")
        except ValueError:
            r["chunked"] = None
        out.append(r)

    return out


def latest_compare_job(src_id, dst_id):
    """Последняя задача pg_compare для пары подключений или None."""
    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT id, config_json FROM jobs WHERE job_type = 'pg_compare' "
            "ORDER BY id DESC"
        )
        rows = cur.fetchall()

    for row in rows:
        try:
            config = json.loads(row["config_json"] or "{}")
        except ValueError:
            continue

        try:
            same_pair = (int(config.get("source_connection_id")) == int(src_id)
                         and int(config.get("dest_connection_id")) == int(dst_id))
        except (TypeError, ValueError):
            continue

        if same_pair:
            return get_job(row["id"])

    return None


# ------------------------------------------------------------------
# Раннер
# ------------------------------------------------------------------

def _rollback(conns):
    """Откат всех соединений; -> индексы тех, чей откат не прошёл."""
    broken = []

    for index, conn in enumerate(conns):
        try:
            conn.rollback()
        except Exception:
            broken.append(index)

    return broken


def _reopen_broken(conns, broken, connection_ids, base=0):
    """
    Соединение, которое не откатилось, дальше не годится: закрываем и
    открываем заново на том же месте списка conns[base + index] (его видят
    StopWatch и finally раннера). Новое соединение кладётся в список сразу
    после открытия — до следующего open_pg, чтобы его падение не оставило
    открытое соединение вне списка. Индекс 0 — источник, он снова read-only.
    """
    for index in broken:
        try:
            conns[base + index].close()
        except Exception:
            pass
        conns[base + index] = open_pg(connection_ids[index],
                                      readonly=(index == 0))


def _cancel_rest(job_id):
    for rest in get_job_items(job_id):
        if rest["status"] in ("queued", "pending"):
            mark_item_skipped(rest["id"], "остановлено пользователем")

    refresh_job_progress(job_id)
    # строку, сравнение которой оборвал стоп, mark_job_cancelled закроет
    mark_job_cancelled(job_id)


def parse_parallel(value):
    """
    Число воркеров из запроса: None/пусто -> PARALLEL_DEFAULT.
    Не целое или вне PARALLEL_MIN..PARALLEL_MAX -> ValueError по-русски.
    """
    if value is None or value == "":
        return PARALLEL_DEFAULT

    if isinstance(value, bool) or isinstance(value, float):
        number = None
    else:
        try:
            number = int(str(value).strip())
        except (TypeError, ValueError):
            number = None

    if number is None or not PARALLEL_MIN <= number <= PARALLEL_MAX:
        raise ValueError(
            "Параллельность — целое число от %d до %d"
            % (PARALLEL_MIN, PARALLEL_MAX))

    return number


def _parallel_of(config):
    """parallel из config задачи; испорченное значение -> по умолчанию."""
    try:
        return parse_parallel(config.get("parallel"))
    except ValueError:
        return PARALLEL_DEFAULT


# ------------------------------------------------------------------
# Сравнение по диапазонам
# ------------------------------------------------------------------

# сколько ждать новой единицы, пока другие воркеры ещё могут их добавить
UNIT_WAIT = 0.05

# живой прогресс таблиц, сравниваемых по диапазонам: job_id → {(s, t): run}
_PROGRESS = {}
_PROGRESS_LOCK = threading.Lock()


def chunk_progress(job_id):
    """{(schema, table): {checked, total, mismatched}} идущих сейчас таблиц."""
    with _PROGRESS_LOCK:
        runs = list((_PROGRESS.get(int(job_id)) or {}).values())

    out = {}
    for run in runs:
        with run["lock"]:
            out[(run["schema"], run["table"])] = _chunked(run)
    return out


def _chunked(run):
    return {"checked": run["checked"], "total": run["total"],
            "mismatched": run["mismatched"]}


def _null_key_sql(schema, table, key_columns, pred):
    """Есть ли строки с NULL в какой-либо ключевой колонке (под предикатом)."""
    nulls = sql.SQL(" OR ").join(
        sql.SQL("{} IS NULL").format(sql.Identifier("t", k))
        for k in key_columns)
    where = nulls if pred is None else \
        sql.SQL("({}) AND ({})").format(pred, nulls)
    return sql.SQL("SELECT EXISTS (SELECT 1 FROM {} AS {} WHERE {})").format(
        _table_ident(schema, table), sql.Identifier("t"), where)


def _has_null_keys(conn, schema, table, key_columns, pred):
    # NULL-ключ пары не находит: построчно такие строки — вставка и удаление,
    # хотя контрольные суммы сторон совпадают
    cur = conn.cursor()
    cur.execute(_null_key_sql(schema, table, key_columns, pred))
    row = cur.fetchone()
    return bool(row and row[0])


def _duplicates(conn, schema, table, key_columns):
    cur = conn.cursor()
    cur.execute(build_dest_duplicate_sql(schema, table, key_columns))
    return int((cur.fetchone() or (0,))[0] or 0)


def _plan_chunks(src_conn, dst_conn, schema, table, key, src_cols, work_mem):
    """
    Подготовка таблицы к сравнению по диапазонам.
    -> None (сравнить как раньше), {"row": ...} (итог без нарезки) или
       {"column": ..., "ranges": [...]}.
    """
    key_columns = list(key["columns"]) if key else []
    key_source = key["source"] if key else None

    try:
        src_types = table_column_types(src_conn, schema, table)
        dst_types = table_column_types(dst_conn, schema, table)
        if src_types != dst_types \
                or any(k not in src_types for k in key_columns):
            # structure_diff / отказ по ключу даст compare_table
            return None

        if key_columns and key_source not in UNIQUE_KEY_SOURCES:
            dup = _duplicates(src_conn, schema, table, key_columns)
            if dup:
                return {"row": _result(
                    "duplicate_keys",
                    message="Ключ (%s) не уникален в источнике: повторяется "
                            "значений — %d" % (", ".join(key_columns), dup))}

        if key_columns and not dest_key_is_unique(dst_conn, schema, table,
                                                  key_columns):
            dup = _duplicates(dst_conn, schema, table, key_columns)
            if dup:
                return {"row": _result(
                    "duplicate_keys",
                    message="Ключ (%s) не уникален в приёмнике: повторяется "
                            "значений — %d" % (", ".join(key_columns), dup))}

        column = pg_ranges.pick_chunk_column(src_conn, dst_conn, schema,
                                             table, key_columns)

        if column is None:
            s = pg_ranges.range_checksum(src_conn, schema, table, src_cols,
                                         None)
            d = pg_ranges.range_checksum(dst_conn, schema, table, src_cols,
                                         None)
            null_keys = (key_columns and key_source not in UNIQUE_KEY_SOURCES
                         and _has_null_keys(src_conn, schema, table,
                                            key_columns, None))
            if s == d and not null_keys:
                return {"row": _result(
                    "same", src_rows=s[0], dst_rows=d[0], to_insert=0,
                    to_update=0, to_delete=0,
                    message="по диапазонам: колонки нарезки нет, "
                            "контрольная сумма таблицы совпала")}
            _rollback([src_conn, dst_conn])
            row = compare_table(src_conn, dst_conn, schema, table,
                                key_columns, key_source=key_source,
                                work_mem=work_mem)
            row["message"] = row.get("message") or (
                "по диапазонам: колонки нарезки нет, контрольная сумма "
                "не совпала — сравнено построчно")
            return {"row": row}

        ranges = pg_ranges.top_ranges(src_conn, schema, table, column)
        return {"column": column, "ranges": ranges}
    finally:
        _rollback([src_conn, dst_conn])


def _compare_one(src_conn, dst_conn, schema, table, info, candidates,
                 work_mem=WORK_MEM):
    """
    Итог таблицы ({"row": ...}) или план сравнения по диапазонам
    ({"key", "columns", "column", "ranges"}).
    """
    if not info.get("in_src", True):
        return {"row": _result("no_source",
                               message="Таблицы нет в источнике — не изменяется")}

    if not info.get("in_dst", True):
        return {"row": _result("no_dest", message="Таблицы нет в приёмнике")}

    src_cols = table_columns(src_conn, schema, table)
    dst_cols = table_columns(dst_conn, schema, table)
    valid_unique = None

    if any(c.get("source") == "unique_index" for c in candidates or []):
        valid_unique = valid_unique_keys(src_conn, schema, table)

    key = pick_key(candidates, src_cols, dst_cols, valid_unique)
    key_fields = {"key_columns": list(key["columns"]) if key else [],
                  "key_source": key["source"] if key else None}

    # транзакцию чтения закроет откат в _plan_chunks / compare_table
    chunk = pg_ranges.should_chunk(src_conn, schema, table)
    plan = None

    if chunk:
        plan = _plan_chunks(src_conn, dst_conn, schema, table, key, src_cols,
                            work_mem)

    if plan is None:
        row = compare_table(src_conn, dst_conn, schema, table,
                            key_fields["key_columns"],
                            key_source=key_fields["key_source"],
                            work_mem=work_mem)
        plan = {"row": row}

    if "row" in plan:
        plan["row"].update(key_fields)
        return plan

    return dict(plan, key=key_fields, columns=src_cols)


def save_range(job_id, run, rng, row):
    """Лист differs → pg_compare_ranges."""
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO pg_compare_ranges (
                job_id, schema_name, table_name, column_name, lo_json,
                hi_json, is_null_range, collate_c, depth, src_rows, dst_rows,
                to_insert, to_update, to_delete, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(job_id), run["schema"], run["table"],
                run["column"]["name"],
                json.dumps(pg_ranges.bound_to_json(rng.get("lo"))),
                json.dumps(pg_ranges.bound_to_json(rng.get("hi"))),
                1 if rng.get("is_null") else 0,
                1 if run["column"].get("collate_c") else 0,
                int(rng.get("depth") or 0),
                row.get("src_rows"), row.get("dst_rows"),
                row.get("to_insert"), row.get("to_update"),
                row.get("to_delete"), row.get("status"),
            ),
        )


def get_mismatched_ranges(job_id, schema, table):
    """
    Несовпавшие листья сравнения таблицы:
    [{column, lo, hi, is_null, depth, collate_c, src_rows, dst_rows,
      to_insert, to_update, to_delete, status}] в порядке записи.
    lo / hi — из JSON: целое как есть, прочее (numeric, дата, время, текст,
    uuid) — строкой; None — открытая граница.
    """
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT column_name, lo_json, hi_json, is_null_range, collate_c,
                   depth, src_rows, dst_rows, to_insert, to_update,
                   to_delete, status
            FROM pg_compare_ranges
            WHERE job_id = ? AND schema_name = ? AND table_name = ?
              AND status = 'differs'
            ORDER BY id
            """,
            (int(job_id), schema, table),
        )
        rows = [dict(r) for r in cur.fetchall()]

    out = []
    for r in rows:
        out.append({
            "column": r["column_name"],
            "lo": json.loads(r["lo_json"]) if r["lo_json"] else None,
            "hi": json.loads(r["hi_json"]) if r["hi_json"] else None,
            "is_null": bool(r["is_null_range"]),
            "depth": int(r["depth"] or 0),
            "collate_c": bool(r["collate_c"]),
            "src_rows": r["src_rows"], "dst_rows": r["dst_rows"],
            "to_insert": r["to_insert"], "to_update": r["to_update"],
            "to_delete": r["to_delete"], "status": r["status"],
        })
    return out


def _new_run(job_id, item, plan):
    key = plan["key"]
    run = {
        "job_id": job_id, "item": item, "schema": item["schema_name"],
        "table": item["table_name"], "key": key, "columns": plan["columns"],
        "column": plan["column"], "lock": threading.Lock(),
        "pending": len(plan["ranges"]), "total": len(plan["ranges"]),
        "checked": 0, "mismatched": 0, "error": None, "override": None,
        "cancelled": False, "closed": False,
        # NULL в ключе возможен только у ключа не из PK / уникального индекса
        "null_check": bool(key["key_columns"]) and
        key["key_source"] not in UNIQUE_KEY_SOURCES,
        "sums": {"src_rows": 0, "dst_rows": 0, "to_insert": 0,
                 "to_update": 0, "to_delete": 0},
    }
    with _PROGRESS_LOCK:
        _PROGRESS.setdefault(int(job_id), {})[(run["schema"],
                                               run["table"])] = run
    return run


def _forget_run(run):
    with _PROGRESS_LOCK:
        runs = _PROGRESS.get(int(run["job_id"])) or {}
        runs.pop((run["schema"], run["table"]), None)
        if not runs:
            _PROGRESS.pop(int(run["job_id"]), None)


def _finish_run(run, shared):
    """Все диапазоны таблицы закрыты: итог в pg_compare_results, item."""
    job_id, item = run["job_id"], run["item"]
    base = {"schema": run["schema"], "table": run["table"]}
    base.update(run["key"])
    chunked = _chunked(run)

    if run["error"]:
        with shared["lock"]:
            shared["failed"] += 1
        save_result(job_id, dict(base, status="error", chunked=chunked,
                                 message=run["error"]))
        mark_item_failed(item["id"], run["error"])
    else:
        sums = run["sums"]
        if run["override"]:
            row = dict(run["override"])
        else:
            changed = sums["to_insert"] or sums["to_update"] \
                or sums["to_delete"]
            row = _result("differs" if changed else "same", **sums)
            row["message"] = "по диапазонам: проверено %d, несовпавших %d" \
                % (run["checked"], run["mismatched"])
        save_result(job_id, dict(base, chunked=chunked, **row))
        mark_item_done(item["id"])

    run["closed"] = True
    _forget_run(run)
    refresh_job_progress(job_id)


def _range_done(run, shared):
    with run["lock"]:
        run["pending"] -= 1
        last = run["pending"] == 0 and not run["cancelled"]
    if last:
        _finish_run(run, shared)


def _put(work, shared, unit):
    with shared["lock"]:
        shared["outstanding"] += 1
    work.put(unit)


def _compare_range(job_id, slot, conns, ids, work, run, rng, watch, shared,
                   work_mem):
    """Одна единица «диапазон таблицы». -> False, если пришёл стоп."""
    schema, table = run["schema"], run["table"]
    column = run["column"]

    try:
        if run["error"] or run["cancelled"]:
            return True

        src_conn, dst_conn = conns[slot], conns[slot + 1]
        pred = pg_ranges.range_predicate("t", column["name"], rng,
                                         column.get("collate_c", False))
        s = pg_ranges.range_checksum(src_conn, schema, table, run["columns"],
                                     pred)
        d = pg_ranges.range_checksum(dst_conn, schema, table, run["columns"],
                                     pred)
        same = s == d and not (
            run["null_check"] and _has_null_keys(
                src_conn, schema, table, run["key"]["key_columns"], pred))
        _rollback([src_conn, dst_conn])

        subs = []
        if not same and max(s[0], d[0]) > pg_ranges.LEAF_ROWS:
            subs = pg_ranges.split_range(src_conn, schema, table, column, rng)
            _rollback([src_conn])

        if same:
            with run["lock"]:
                run["checked"] += 1
                run["sums"]["src_rows"] += s[0]
                run["sums"]["dst_rows"] += d[0]
        elif len(subs) > 1:
            with run["lock"]:
                run["checked"] += 1
                run["pending"] += len(subs)
                run["total"] += len(subs)
            for sub in subs:
                _put(work, shared, ("range", run, sub))
        else:
            row = compare_table(src_conn, dst_conn, schema, table,
                                run["key"]["key_columns"],
                                key_source=run["key"]["key_source"],
                                work_mem=work_mem, where=pred)
            with run["lock"]:
                run["checked"] += 1
                for name in run["sums"]:
                    run["sums"][name] += int(row.get(name) or 0)
                if row["status"] == "differs":
                    run["mismatched"] += 1
                elif row["status"] != "same" and not run["override"]:
                    run["override"] = row
            if row["status"] == "differs":
                save_range(job_id, run, rng, row)
        return True
    except Exception as e:
        broken = _rollback([conns[slot], conns[slot + 1]])

        if watch.stopped or watch.check():
            run["cancelled"] = True
            shared["cancelled"] = True
            return False

        with run["lock"]:
            if not run["error"]:
                where = "NULL" if rng.get("is_null") else "[%s, %s)" % (
                    rng.get("lo"), rng.get("hi"))
                run["error"] = ("Диапазон %s %s: %s"
                                % (column["name"], where, e))[:500]

        if broken:
            _reopen_broken(conns, broken, ids, base=slot)
        return True
    finally:
        _range_done(run, shared)


def _compare_table_unit(job_id, slot, conns, ids, work, item, info,
                        candidates, watch, shared, work_mem):
    """Единица «таблица». -> False, если пришёл стоп."""
    schema, table = item["schema_name"], item["table_name"]
    mark_item_running(item["id"])
    refresh_job_progress(job_id)

    base = {"schema": schema, "table": table}
    src_conn, dst_conn = conns[slot], conns[slot + 1]

    try:
        plan = _compare_one(
            src_conn, dst_conn, schema, table,
            info.get((schema, table), {}),
            candidates.get((schema, table)),
            work_mem=work_mem,
        )

        if "row" in plan:
            save_result(job_id, dict(base, **plan["row"]))
            mark_item_done(item["id"])
        else:
            run = _new_run(job_id, item, plan)
            with shared["lock"]:
                shared["runs"].append(run)
            for rng in plan["ranges"]:
                _put(work, shared, ("range", run, rng))
            return True
    except Exception as e:
        broken = _rollback([src_conn, dst_conn])

        # QueryCanceledError от conn.cancel() сторожа — это стоп;
        # строку закроет mark_job_cancelled в _cancel_rest
        if watch.stopped or watch.check():
            save_result(job_id, dict(
                base, status="cancelled",
                message="Сравнение остановлено пользователем",
            ))
            shared["cancelled"] = True
            return False

        with shared["lock"]:
            shared["failed"] += 1

        save_result(job_id, dict(base, status="error",
                                 message=str(e)[:500]))
        mark_item_failed(item["id"], str(e)[:500])

        if broken:
            # без живой пары воркер дальше не может; таблицы
            # доберут остальные
            _reopen_broken(conns, broken, ids, base=slot)

    refresh_job_progress(job_id)
    return True


def _compare_worker(job_id, slot, conns, ids, work, info, candidates,
                    watch, shared, work_mem=WORK_MEM):
    """
    Воркер: берёт единицы («таблица» или «диапазон таблицы») из общей
    очереди, пока все единицы не закрыты или не придёт стоп. Его пара
    соединений — conns[slot:slot + 2] (их же видит StopWatch); сломанные
    при откате переоткрываются на том же месте.
    """
    while True:
        try:
            unit = work.get(timeout=UNIT_WAIT)
        except queue.Empty:
            with shared["lock"]:
                idle = shared["outstanding"] <= 0
            if idle or shared["cancelled"] or watch.stopped:
                return
            continue

        try:
            if watch.check():
                # единица не начата: таблицу пометит skipped _cancel_rest,
                # начатую по диапазонам — раннер
                shared["cancelled"] = True
                return

            if unit[0] == "range":
                going = _compare_range(job_id, slot, conns, ids, work,
                                       unit[1], unit[2], watch, shared,
                                       work_mem)
            else:
                going = _compare_table_unit(job_id, slot, conns, ids, work,
                                            unit[1], info, candidates, watch,
                                            shared, work_mem)
            if not going:
                return
        finally:
            with shared["lock"]:
                shared["outstanding"] -= 1


def _run_worker(shared, *args):
    try:
        _compare_worker(*args)
    except Exception as e:
        with shared["lock"]:
            shared["fatal"].append(e)


def _cancel_runs(job_id, shared):
    """Таблицы по диапазонам, не закрытые к стопу, — строка cancelled."""
    for run in shared["runs"]:
        if run["closed"]:
            continue
        base = {"schema": run["schema"], "table": run["table"]}
        base.update(run["key"])
        save_result(job_id, dict(
            base, status="cancelled", chunked=_chunked(run),
            message="Сравнение остановлено пользователем",
        ))
        run["closed"] = True
        _forget_run(run)


def run_pg_compare_job(job_id):
    """
    Раннер job_type='pg_compare'. Config: source_connection_id,
    dest_connection_id, parallel (1..8, по умолчанию 4),
    tables=[{schema, table, in_src, in_dst}].
    Item на таблицу; таблицы разбирают min(parallel, таблиц) воркеров,
    у каждого своя пара соединений; результат пишется сразу после таблицы.
    """
    job = get_job(job_id)
    if not job:
        return

    config = job_config(job)
    mark_job_running(job_id)
    # все соединения всех воркеров: воркер slot держит conns[slot:slot+2];
    # этот же список отменяет StopWatch
    conns = []

    try:
        source_id = config.get("source_connection_id")
        dest_id = config.get("dest_connection_id")

        if not source_id or not dest_id:
            raise Exception("В задаче не указан источник или назначение")

        info = {(t.get("schema"), t.get("table")): t
                for t in config.get("tables") or []}
        items = get_job_items(job_id)

        both = [
            (it["schema_name"], it["table_name"]) for it in items
            if info.get((it["schema_name"], it["table_name"]), {})
                   .get("in_src", True)
            and info.get((it["schema_name"], it["table_name"]), {})
                   .get("in_dst", True)
        ]
        candidates = resolve_key_candidates(source_id, both)

        work = queue.Queue()
        pending = [it for it in items
                   if it.get("status") not in ("done", "failed", "skipped")]
        for item in pending:
            work.put(("table", item))

        # больших таблиц по диапазонам хватает на всех воркеров
        workers = max(1, _parallel_of(config))

        for _ in range(workers):
            conns.append(open_pg(source_id, readonly=True))
            conns.append(open_pg(dest_id))

        refresh_job_progress(job_id)
        shared = {"lock": threading.Lock(), "failed": 0, "fatal": [],
                  "cancelled": False, "outstanding": len(pending),
                  "runs": []}

        with StopWatch(job_id, conns) as watch:
            threads = [
                threading.Thread(
                    target=_run_worker,
                    args=(shared, job_id, slot * 2, conns, [source_id, dest_id],
                          work, info, candidates, watch, shared,
                          worker_work_mem(workers)),
                    name="pg_compare-%s-%d" % (job_id, slot),
                    daemon=True,
                )
                for slot in range(workers)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            if shared["cancelled"] or (watch.stopped and not work.empty()):
                _cancel_runs(job_id, shared)
                _cancel_rest(job_id)
                return

        if shared["fatal"]:
            raise shared["fatal"][0]

        if shared["failed"]:
            mark_job_failed(job_id, "%s таблиц(ы) не удалось сравнить"
                            % shared["failed"])
        else:
            mark_job_done(job_id)

    except Exception as e:
        _rollback(conns)
        mark_job_failed(job_id, str(e)[:500])
    finally:
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass
