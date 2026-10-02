# -*- coding: utf-8 -*-
"""
Сравнение двух баз PostgreSQL (Postgres Toolkit → «Сравнение и разница»).

Задача pg_compare по каждой выбранной таблице считает строки в источнике
и приёмнике и сколько строк добавить, изменить и удалить:

  * источник отдаёт COPY (SELECT ключ::text..., md5(ROW(колонки)::text))
    TO STDOUT — по сети идут только ключи и хеши, источник только читается;
  * поток заливается во временную таблицу сессии приёмника, подсчёт идёт
    в приёмнике соединением с его таблицей;
  * без ключа сравниваются мультимножества хешей (EXCEPT ALL);
  * разные наборы колонок — structure_diff, данные не читаются.

Ключ: PK → уникальный индекс → сохранённый ключ (sync_keys), через
существующие функции table_catalog; ключ должен быть в обеих таблицах.
Результаты пишутся в SQLite pg_compare_results сразу после таблицы.
"""

import json

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
HASH_COLUMN = "h"


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


def build_source_select(schema, table, key_columns, columns):
    """SELECT ключ::text..., md5(ROW(колонки)::text) FROM schema.table AS t."""
    parts = [sql.SQL("{}::text").format(sql.Identifier("t", k))
             for k in key_columns]
    parts.append(row_hash_sql("t", columns))

    return sql.SQL("SELECT {} FROM {} AS {}").format(
        sql.SQL(", ").join(parts),
        _table_ident(schema, table),
        sql.Identifier("t"),
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


def build_dest_duplicate_sql(schema, table, key_columns):
    """Сколько значений ключа повторяется в таблице приёмника."""
    return sql.SQL(
        "SELECT count(*) FROM (SELECT 1 FROM {} AS {} GROUP BY {} "
        "HAVING count(*) > 1) AS x"
    ).format(
        _table_ident(schema, table),
        sql.Identifier("t"),
        sql.SQL(", ").join(sql.Identifier("t", k) for k in key_columns),
    )


def build_duplicate_sql(key_columns):
    """Сколько значений ключа повторяется в потоке источника."""
    return sql.SQL(
        "SELECT count(*) FROM (SELECT 1 FROM {} GROUP BY {} "
        "HAVING count(*) > 1) AS x"
    ).format(
        sql.Identifier(TEMP_TABLE),
        sql.SQL(", ").join(sql.Identifier(n) for n in _key_names(key_columns)),
    )


def build_count_sql(schema, table, key_columns, columns):
    """
    Одна строка (dst_rows, to_insert, to_update, to_delete) по временной
    таблице источника и таблице приёмника.
    """
    hash_expr = row_hash_sql("t", columns)
    src = sql.Identifier(TEMP_TABLE)
    s_alias = sql.Identifier("s")
    d_alias = sql.Identifier("d")
    h = sql.Identifier(HASH_COLUMN)

    if not key_columns:
        return sql.SQL(
            "WITH d AS (SELECT {hash} AS {h} FROM {tbl} AS {t}) "
            "SELECT (SELECT count(*) FROM d) AS dst_rows, "
            "(SELECT count(*) FROM (SELECT {h} FROM {src} "
            "EXCEPT ALL SELECT {h} FROM d) AS x) AS to_insert, "
            "0 AS to_update, "
            "(SELECT count(*) FROM (SELECT {h} FROM d "
            "EXCEPT ALL SELECT {h} FROM {src}) AS x) AS to_delete"
        ).format(hash=hash_expr, h=h, tbl=_table_ident(schema, table),
                 t=sql.Identifier("t"), src=src)

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

    return sql.SQL(
        "WITH d AS (SELECT {casts}, {hash} AS {h} FROM {tbl} AS {t}) "
        "SELECT (SELECT count(*) FROM d) AS dst_rows, "
        "(SELECT count(*) FROM {src} AS {s} WHERE NOT EXISTS "
        "(SELECT 1 FROM d WHERE {join})) AS to_insert, "
        "(SELECT count(*) FROM {src} AS {s} JOIN d AS {d} ON {join} "
        "WHERE {sh} <> {dh}) AS to_update, "
        "(SELECT count(*) FROM d AS {d} WHERE NOT EXISTS "
        "(SELECT 1 FROM {src} AS {s} WHERE {join})) AS to_delete"
    ).format(
        casts=casts, hash=hash_expr, h=h, tbl=_table_ident(schema, table),
        t=sql.Identifier("t"), src=src, s=s_alias, d=d_alias, join=join,
        sh=sql.Identifier("s", HASH_COLUMN), dh=sql.Identifier("d", HASH_COLUMN),
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


def compare_table(src_conn, dst_conn, schema, table, key_columns):
    """
    Сравнение таблицы, которая есть в обеих базах.
    key_columns — [] для сравнения без ключа (мультимножество строк).
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
        # остаток от прошлой таблицы, если её откат не прошёл
        cur.execute(build_drop_temp_sql())
        cur.execute(build_temp_table_sql(key_columns))

        src_rows = stream_copy(
            src_conn, dst_conn,
            build_source_select(schema, table, key_columns, src_cols),
            sql.Identifier(TEMP_TABLE),
            _key_names(key_columns) + [HASH_COLUMN],
        )

        if key_columns:
            cur.execute(build_duplicate_sql(key_columns))
            duplicates = int(cur.fetchone()[0] or 0)

            if duplicates:
                return _result(
                    "duplicate_keys", src_rows=src_rows,
                    message="Ключ (%s) не уникален в источнике: "
                            "повторяется значений — %d"
                            % (", ".join(key_columns), duplicates),
                )

            cur.execute(build_dest_duplicate_sql(schema, table, key_columns))
            duplicates = int(cur.fetchone()[0] or 0)

            if duplicates:
                return _result(
                    "duplicate_keys", src_rows=src_rows,
                    message="Ключ (%s) не уникален в приёмнике: "
                            "повторяется значений — %d"
                            % (", ".join(key_columns), duplicates),
                )

        cur.execute(build_count_sql(schema, table, key_columns, src_cols))
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
              AND i.indisunique AND NOT i.indisprimary
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
                to_delete, message, compared_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )


def get_results(job_id):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT id, job_id, schema_name, table_name, status,
                   key_columns_json, key_source, src_rows, dst_rows,
                   to_insert, to_update, to_delete, message, compared_at
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

def _compare_one(src_conn, dst_conn, schema, table, info, candidates):
    if not info.get("in_src", True):
        return _result("no_source",
                       message="Таблицы нет в источнике — не изменяется")

    if not info.get("in_dst", True):
        return _result("no_dest", message="Таблицы нет в приёмнике")

    src_cols = table_columns(src_conn, schema, table)
    dst_cols = table_columns(dst_conn, schema, table)
    valid_unique = None

    if any(c.get("source") == "unique_index" for c in candidates or []):
        valid_unique = valid_unique_keys(src_conn, schema, table)

    key = pick_key(candidates, src_cols, dst_cols, valid_unique)

    row = compare_table(src_conn, dst_conn, schema, table,
                        key["columns"] if key else [])
    row["key_columns"] = list(key["columns"]) if key else []
    row["key_source"] = key["source"] if key else None
    return row


def _rollback(conns):
    """Откат всех соединений; -> индексы тех, чей откат не прошёл."""
    broken = []

    for index, conn in enumerate(conns):
        try:
            conn.rollback()
        except Exception:
            broken.append(index)

    return broken


def _reopen_broken(conns, broken, connection_ids):
    """
    Соединение, которое не откатилось, дальше не годится: закрываем и
    открываем заново на том же месте списка (его видит StopWatch).
    Индекс 0 — источник, он снова read-only.
    """
    for index in broken:
        try:
            conns[index].close()
        except Exception:
            pass
        conns[index] = open_pg(connection_ids[index], readonly=(index == 0))


def _cancel_rest(job_id):
    for rest in get_job_items(job_id):
        if rest["status"] in ("queued", "pending"):
            mark_item_skipped(rest["id"], "остановлено пользователем")

    refresh_job_progress(job_id)
    # строку, сравнение которой оборвал стоп, mark_job_cancelled закроет
    mark_job_cancelled(job_id)


def run_pg_compare_job(job_id):
    """
    Раннер job_type='pg_compare'. Config: source_connection_id,
    dest_connection_id, tables=[{schema, table, in_src, in_dst}].
    Item на таблицу; результат пишется сразу после таблицы.
    """
    job = get_job(job_id)
    if not job:
        return

    config = job_config(job)
    mark_job_running(job_id)
    conns = []

    try:
        source_id = config.get("source_connection_id")
        dest_id = config.get("dest_connection_id")

        if not source_id or not dest_id:
            raise Exception("В задаче не указан источник или назначение")

        src_conn = open_pg(source_id, readonly=True)
        conns.append(src_conn)
        dst_conn = open_pg(dest_id)
        conns.append(dst_conn)

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

        refresh_job_progress(job_id)
        failed = 0

        with StopWatch(job_id, conns) as watch:
            for item in items:
                if item.get("status") in ("done", "failed", "skipped"):
                    continue

                if watch.check():
                    _cancel_rest(job_id)
                    return

                schema, table = item["schema_name"], item["table_name"]
                mark_item_running(item["id"])
                refresh_job_progress(job_id)

                base = {"schema": schema, "table": table}

                try:
                    row = _compare_one(
                        src_conn, dst_conn, schema, table,
                        info.get((schema, table), {}),
                        candidates.get((schema, table)),
                    )
                    save_result(job_id, dict(base, **row))
                    mark_item_done(item["id"])
                except Exception as e:
                    broken = _rollback(conns)

                    # QueryCanceledError от conn.cancel() сторожа — это стоп
                    if watch.stopped or watch.check():
                        save_result(job_id, dict(
                            base, status="cancelled",
                            message="Сравнение остановлено пользователем",
                        ))
                        _cancel_rest(job_id)
                        return

                    if broken:
                        _reopen_broken(conns, broken, [source_id, dest_id])
                        src_conn, dst_conn = conns

                    failed += 1
                    save_result(job_id, dict(base, status="error",
                                             message=str(e)[:500]))
                    mark_item_failed(item["id"], str(e)[:500])

                refresh_job_progress(job_id)

        if failed:
            mark_job_failed(job_id,
                            "%s таблиц(ы) не удалось сравнить" % failed)
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
