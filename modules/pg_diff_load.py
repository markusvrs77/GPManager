# -*- coding: utf-8 -*-
"""
Загрузка в приёмник PostgreSQL по итогам сравнения (Postgres Toolkit →
«Сравнение и разница»). Источник только читается, меняется только приёмник.

    load_diff — разница: источник потоком COPY льётся в UNLOGGED-таблицу
                opsentri_sync_stage.stg_<job>_<n> приёмника, затем одной
                транзакцией приёмника: проверка ключа → DELETE (только при
                delete_missing) → UPDATE строк с другим хешем → INSERT новых.
                Без ключа — разность мультимножеств (EXCEPT ALL по хешу
                строки), лишние копии удаляются по ctid через row_number().
                Staging удаляется в finally.
    load_full — TRUNCATE (по желанию) + COPY FROM STDIN одной транзакцией.

Списки колонок всегда явные и в порядке приёмника: порядок колонок
в двух базах может отличаться. Идентификаторы — только sql.Identifier.
"""

import re

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
    refresh_job_progress,
)

import modules.pg_compare as pg_compare
import modules.pg_ranges as pg_ranges
from modules.pg_sync_common import (
    StopWatch,
    open_pg,
    row_hash_sql,
    stream_copy,
    table_column_types,
)
from modules.sync_transport import job_config


STAGE_SCHEMA = "opsentri_sync_stage"

# служебные колонки CTE вставки без ключа
INSERT_HASH = "_pgcmp_h"
INSERT_RN = "_pgcmp_rn"


class DuplicateKeyError(ValueError):
    """Ключ в staging не уникален: таблица не загружается."""


class NotEmptyError(ValueError):
    """Заливка без TRUNCATE в таблицу, где уже есть строки."""


# ------------------------------------------------------------------
# SQL (чистые построители)
# ------------------------------------------------------------------

def _target(schema, table):
    return sql.Identifier(schema, table)


def _stage(stage_name):
    return sql.Identifier(STAGE_SCHEMA, stage_name)


def _cols(alias, columns):
    if alias:
        return sql.SQL(", ").join(sql.Identifier(alias, c) for c in columns)
    return sql.SQL(", ").join(sql.Identifier(c) for c in columns)


def _key_match(key_columns):
    """"t"."k" = "s"."k" AND ... — тип у staging тот же, что у приёмника."""
    return sql.SQL(" AND ").join(
        sql.SQL("{} = {}").format(sql.Identifier("t", k), sql.Identifier("s", k))
        for k in key_columns
    )


def stage_name_for(job_id, n):
    return "stg_%d_%d" % (int(job_id), int(n))


def build_ranges_where(alias, ranges):
    """
    (pred1) OR (pred2) ... — предикаты несовпавших листьев сравнения
    (pg_compare.get_mismatched_ranges) обоих видов: диапазоны и корзины
    md5-префикса; None — без ограничения. Строит общий построитель
    pg_ranges.leaves_predicate; значения — только sql.Literal.
    """
    if ranges is None:
        return None
    if not ranges:
        raise ValueError("Пустой список диапазонов")

    return pg_ranges.leaves_predicate(alias, ranges)


def _batches(ranges):
    """Листья пачками по pg_ranges.LEAF_BATCH; None — одна «пачка» None."""
    if ranges is None:
        return [None]
    return pg_ranges.leaf_batches(ranges)


def _and_where(where):
    return sql.SQL(" AND ({})").format(where) if where is not None         else sql.SQL("")


def _only_where(where):
    return sql.SQL(" WHERE {}").format(where) if where is not None         else sql.SQL("")


def build_full_select_sql(schema, table, columns, where=None):
    """SELECT <колонки по порядку приёмника> FROM schema.table [WHERE ...]."""
    return sql.SQL("SELECT {} FROM {}{}").format(_cols(None, columns),
                                                 _target(schema, table),
                                                 _only_where(where))


def build_source_snapshot_sql():
    """
    Первый запрос транзакции источника: все чтения одной таблицы — по
    одному снимку (строка, переехавшая между листьями, не пропадёт и не
    задвоится между пачками COPY). Источник только читается.
    """
    return sql.SQL("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ "
                   "READ ONLY")


def _open_source_snapshot(src_conn):
    """Новая транзакция источника REPEATABLE READ READ ONLY."""
    src_conn.rollback()
    src_conn.cursor().execute(build_source_snapshot_sql())


def build_stage_schema_sql():
    return sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
        sql.Identifier(STAGE_SCHEMA))


def build_create_stage_sql(schema, table, stage_name, columns):
    """
    Только записываемые колонки с типами приёмника, без NOT NULL, identity
    и выражений generated (CREATE TABLE AS, а не LIKE).
    """
    return sql.SQL("CREATE UNLOGGED TABLE {} AS SELECT {} FROM {} "
                   "WITH NO DATA").format(_stage(stage_name),
                                          _cols(None, columns),
                                          _target(schema, table))


def build_column_flags_sql(server_version=None):
    """
    Имя, attgenerated, attidentity колонок таблицы (параметры: схема,
    таблица). attgenerated есть с PG 12, attidentity — с PG 10.
    """
    modern = server_version is None
    generated = sql.SQL("a.attgenerated::text") \
        if modern or server_version >= 120000 else sql.SQL("''::text")
    identity = sql.SQL("a.attidentity::text") \
        if modern or server_version >= 100000 else sql.SQL("''::text")

    return sql.SQL(
        "SELECT a.attname, {}, {} FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s "
        "AND a.attnum > 0 AND NOT a.attisdropped"
    ).format(generated, identity)


def build_drop_stage_sql(stage_name):
    return sql.SQL("DROP TABLE IF EXISTS {}").format(_stage(stage_name))


def build_stage_duplicate_sql(stage_name, key_columns):
    """Сколько значений ключа повторяется в staging."""
    return sql.SQL(
        "SELECT count(*) FROM (SELECT 1 FROM {} GROUP BY {} "
        "HAVING count(*) > 1) AS x"
    ).format(_stage(stage_name), _cols(None, key_columns))


def build_stage_null_key_sql(stage_name, key_columns):
    """Строки staging с NULL в ключе: по ним строку не сопоставить."""
    return sql.SQL("SELECT count(*) FROM {} WHERE {}").format(
        _stage(stage_name),
        sql.SQL(" OR ").join(sql.SQL("{} IS NULL").format(sql.Identifier(k))
                             for k in key_columns),
    )


def build_key_delete_sql(schema, table, stage_name, key_columns, where=None):
    """
    Строки приёмника с ключом, которого нет в источнике. Строки с NULL
    в ключе не трогаются: NULL = NULL не истинно, и NOT EXISTS счёл бы
    их отсутствующими в источнике. where — предикат по алиасу "t"
    (листья сравнения): строки приёмника вне него не трогаются.
    """
    return sql.SQL(
        "DELETE FROM {} AS {} WHERE {}{} AND NOT EXISTS (SELECT 1 FROM {} "
        "AS {} WHERE {})"
    ).format(_target(schema, table), sql.Identifier("t"),
             sql.SQL(" AND ").join(
                 sql.SQL("{} IS NOT NULL").format(sql.Identifier("t", k))
                 for k in key_columns),
             _and_where(where),
             _stage(stage_name), sql.Identifier("s"), _key_match(key_columns))


def build_analyze_stage_sql(stage_name):
    """Статистика staging для планировщика перед DML."""
    return sql.SQL("ANALYZE {}").format(_stage(stage_name))


def build_not_empty_sql(schema, table):
    return sql.SQL("SELECT 1 FROM {} LIMIT 1").format(_target(schema, table))


def build_lock_for_fill_sql(schema, table):
    """Пока проверяем пустоту и заливаем, никто другой не пишет."""
    return sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(
        _target(schema, table))


def build_table_exists_sql():
    """Есть ли отношение schema.table (параметры: схема, таблица)."""
    return sql.SQL(
        "SELECT 1 FROM pg_class c JOIN pg_namespace n "
        "ON n.oid = c.relnamespace WHERE n.nspname = %s AND c.relname = %s"
    )


def build_serial_sequences_sql():
    """
    Колонка и её последовательность (serial / identity) по приёмнику
    (параметры: схема, таблица).
    """
    return sql.SQL(
        "SELECT a.attname, pg_get_serial_sequence("
        "format('%%I.%%I', n.nspname, c.relname), a.attname) "
        "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s "
        "AND a.attnum > 0 AND NOT a.attisdropped"
    )


def build_setval_sql(schema, table, column):
    """
    setval(seq, max(col)) — только вперёд и только у непустой таблицы
    (параметры: seq три раза). Нетронутая последовательность выдаст
    seqstart, поэтому без last_value сравниваем с seqstart - 1.
    """
    return sql.SQL(
        "SELECT setval(%s::regclass, m.v) FROM (SELECT max({}) AS v FROM {}) "
        "AS m WHERE m.v IS NOT NULL AND m.v > COALESCE("
        "pg_sequence_last_value(%s::regclass), (SELECT p.seqstart - 1 "
        "FROM pg_sequence p WHERE p.seqrelid = %s::regclass))"
    ).format(sql.Identifier(column), _target(schema, table))


def build_key_update_sql(schema, table, stage_name, key_columns, columns,
                        skip=()):
    """
    Ключ совпал, хеш строки другой → неключевые колонки из источника.
    skip — колонки, которые UPDATE менять не может (identity ALWAYS вне
    ключа): их нет ни в SET, ни в хеше.
    """
    columns = [c for c in columns if c in key_columns or c not in skip]
    data_columns = [c for c in columns if c not in key_columns]
    if not data_columns:
        raise ValueError("Все колонки входят в ключ — обновлять нечего")

    return sql.SQL(
        "UPDATE {} AS {} SET {} FROM {} AS {} WHERE {} AND {} <> {}"
    ).format(
        _target(schema, table), sql.Identifier("t"),
        sql.SQL(", ").join(
            sql.SQL("{} = {}").format(sql.Identifier(c), sql.Identifier("s", c))
            for c in data_columns
        ),
        _stage(stage_name), sql.Identifier("s"),
        _key_match(key_columns),
        row_hash_sql("t", columns), row_hash_sql("s", columns),
    )


def _overriding(overriding):
    """identity ALWAYS принимает значения источника только так."""
    return sql.SQL(" OVERRIDING SYSTEM VALUE") if overriding else sql.SQL("")


def build_key_insert_sql(schema, table, stage_name, key_columns, columns,
                        overriding=False):
    """Строки источника, ключа которых нет в приёмнике."""
    return sql.SQL(
        "INSERT INTO {} ({}){} SELECT {} FROM {} AS {} WHERE NOT EXISTS "
        "(SELECT 1 FROM {} AS {} WHERE {})"
    ).format(
        _target(schema, table), _cols(None, columns), _overriding(overriding),
        _cols("s", columns),
        _stage(stage_name), sql.Identifier("s"),
        _target(schema, table), sql.Identifier("t"), _key_match(key_columns),
    )


def _surplus_cte(left, left_alias, right, right_alias, columns,
                 left_where=None, right_where=None):
    """
    "m"(h, c): хеш строки и сколько её копий в left сверх right
    (разность мультимножеств EXCEPT ALL). *_where — один и тот же набор
    диапазонов по алиасу своей стороны.
    """
    return sql.SQL(
        "{m} AS (SELECT {h}, count(*) AS {c} FROM ("
        "SELECT {lh} AS {h} FROM {left} AS {la}{lw} EXCEPT ALL "
        "SELECT {rh} AS {h} FROM {right} AS {ra}{rw}) AS {d} GROUP BY {h})"
    ).format(
        m=sql.Identifier("m"), h=sql.Identifier("h"), c=sql.Identifier("c"),
        d=sql.Identifier("d"),
        lh=row_hash_sql(left_alias, columns), left=left,
        la=sql.Identifier(left_alias), lw=_only_where(left_where),
        rh=row_hash_sql(right_alias, columns), right=right,
        ra=sql.Identifier(right_alias), rw=_only_where(right_where),
    )


def _numbered_cte(source, alias, columns, extra, where=None):
    """
    "x"(<extra>, h, rn): у каждой копии строки свой номер в разрезе хеша,
    чтобы взять ровно нужное число копий.
    """
    hash_expr = row_hash_sql(alias, columns)
    return sql.SQL(
        "{x} AS (SELECT {extra}, {hash} AS {h}, row_number() OVER "
        "(PARTITION BY {hash}) AS {rn} FROM {src} AS {a}{w})"
    ).format(x=sql.Identifier("x"), extra=extra, hash=hash_expr,
             h=sql.Identifier("h"), rn=sql.Identifier("rn"),
             src=source, a=sql.Identifier(alias), w=_only_where(where))


def build_keyless_insert_sql(schema, table, stage_name, columns,
                             overriding=False, ranges=None):
    """
    INSERT недостающих копий строк: staging EXCEPT ALL приёмник.
    ranges — листья сравнения: обе стороны разности берутся в них.
    """
    stage = _stage(stage_name)
    target = _target(schema, table)

    reserved = [c for c in columns if c in (INSERT_HASH, INSERT_RN)]
    if reserved:
        raise ValueError("Колонка с зарезервированным именем: %s"
                         % ", ".join(reserved))

    # CTE "x" отдаёт сами колонки staging, номер копии — под служебными
    # именами, чтобы не столкнуться с колонками таблицы
    return sql.SQL(
        "WITH {m}, {x} INSERT INTO {target} ({cols}){ov} SELECT {x_cols} "
        "FROM {xa} JOIN {ma} ON {m_h} = {x_h} WHERE {x_rn} <= {m_c}"
    ).format(
        m=_surplus_cte(stage, "s", target, "t", columns,
                       build_ranges_where("s", ranges),
                       build_ranges_where("t", ranges)),
        x=sql.SQL(
            "{x} AS (SELECT {s_cols}, {hash} AS {h}, row_number() OVER "
            "(PARTITION BY {hash}) AS {rn} FROM {stage} AS {s}{w})"
        ).format(x=sql.Identifier("x"), s_cols=_cols("s", columns),
                 hash=row_hash_sql("s", columns),
                 h=sql.Identifier(INSERT_HASH), rn=sql.Identifier(INSERT_RN),
                 stage=stage, s=sql.Identifier("s"),
                 w=_only_where(build_ranges_where("s", ranges))),
        target=target, cols=_cols(None, columns), x_cols=_cols("x", columns),
        ov=_overriding(overriding),
        xa=sql.Identifier("x"), ma=sql.Identifier("m"),
        m_h=sql.Identifier("m", "h"), x_h=sql.Identifier("x", INSERT_HASH),
        x_rn=sql.Identifier("x", INSERT_RN), m_c=sql.Identifier("m", "c"),
    )


def build_keyless_delete_sql(schema, table, stage_name, columns,
                             ranges=None):
    """
    DELETE лишних копий строк приёмника: приёмник EXCEPT ALL staging.
    ranges — листья сравнения: строки приёмника вне них не трогаются.
    """
    stage = _stage(stage_name)
    target = _target(schema, table)
    t_where = build_ranges_where("t", ranges)

    return sql.SQL(
        "WITH {m}, {x} DELETE FROM {target} AS {t} USING {xa} JOIN {ma} "
        "ON {m_h} = {x_h} WHERE {x_rn} <= {m_c} AND {t_oid} = {x_oid} "
        "AND {t_tid} = {x_tid}"
    ).format(
        m=_surplus_cte(target, "t", stage, "s", columns, t_where,
                       build_ranges_where("s", ranges)),
        # ctid уникален только внутри партиции — берём и tableoid
        x=_numbered_cte(target, "t", columns, sql.SQL(
            "{} AS {}, {} AS {}").format(
                sql.Identifier("t", "tableoid"), sql.Identifier("toid"),
                sql.Identifier("t", "ctid"), sql.Identifier("tid")),
            t_where),
        target=target, t=sql.Identifier("t"),
        xa=sql.Identifier("x"), ma=sql.Identifier("m"),
        m_h=sql.Identifier("m", "h"), x_h=sql.Identifier("x", "h"),
        x_rn=sql.Identifier("x", "rn"), m_c=sql.Identifier("m", "c"),
        t_oid=sql.Identifier("t", "tableoid"),
        x_oid=sql.Identifier("x", "toid"),
        t_tid=sql.Identifier("t", "ctid"), x_tid=sql.Identifier("x", "tid"),
    )


def build_truncate_sql(schema, table):
    return sql.SQL("TRUNCATE TABLE {}").format(_target(schema, table))


# --- создание отсутствующей таблицы по каталогу PostgreSQL источника ---

def build_source_columns_sql(server_version=None):
    """
    Колонки таблицы источника в порядке attnum (параметры: схема, таблица):
    имя, format_type, attnotnull, attidentity, attgenerated, выражение
    из pg_attrdef, relkind, схема и имя collation (только если она не та,
    что у типа по умолчанию). Только обычные и партиционированные таблицы.
    """
    modern = server_version is None
    identity = sql.SQL("a.attidentity::text") \
        if modern or server_version >= 100000 else sql.SQL("''::text")
    generated = sql.SQL("a.attgenerated::text") \
        if modern or server_version >= 120000 else sql.SQL("''::text")

    return sql.SQL(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod), "
        "a.attnotnull, {}, {}, pg_get_expr(d.adbin, d.adrelid), "
        "c.relkind::text, cn.nspname, co.collname FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_type t ON t.oid = a.atttypid "
        "LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid "
        "AND d.adnum = a.attnum "
        "LEFT JOIN pg_collation co ON co.oid = a.attcollation "
        "AND a.attcollation <> t.typcollation "
        "LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace "
        "WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('r', 'p') "
        "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
    ).format(identity, generated)


def build_source_pk_sql():
    """Колонки первичного ключа источника по порядку (параметры: схема, таблица)."""
    return sql.SQL(
        "SELECT a.attname FROM pg_constraint con "
        "JOIN pg_class c ON c.oid = con.conrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) "
        "JOIN pg_attribute a ON a.attrelid = con.conrelid "
        "AND a.attnum = k.attnum "
        "WHERE con.contype = 'p' AND n.nspname = %s AND c.relname = %s "
        "ORDER BY k.ord"
    )


def build_catalog_search_path_sql():
    return sql.SQL("SET LOCAL search_path = pg_catalog")


def build_schema_exists_sql():
    """Есть ли схема (параметр: схема)."""
    return sql.SQL("SELECT 1 FROM pg_namespace WHERE nspname = %s")


def build_create_schema_sql(schema):
    return sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
        sql.Identifier(schema))


# VIRTUAL generated-колонки — с PostgreSQL 18
VIRTUAL_GENERATED_VERSION = 180000


def build_create_table_sql(schema, table, definition, virtual=False):
    """
    CREATE TABLE по определению из source_table_definition: колонки
    с типом источника и NOT NULL, вычисляемые — GENERATED ... STORED,
    первичный ключ источника. VIRTUAL (attgenerated = 'v') остаётся
    VIRTUAL только при virtual=True (приёмник PG 18+), иначе STORED.
    Ни default-ов, ни identity: значения,
    в том числе serial / identity, приходят из источника. Тип и выражение
    взяты из каталога источника (format_type, pg_get_expr), а не из ввода.
    """
    parts = []

    for column in definition["columns"]:
        piece = [sql.Identifier(column["name"]), sql.SQL(column["type"])]
        if column.get("collation"):
            piece.append(sql.SQL("COLLATE {}").format(
                sql.Identifier(*column["collation"])))
        if column.get("generated"):
            kind = "VIRTUAL" if virtual and column.get("generated_kind") == "v"                 else "STORED"
            piece.append(sql.SQL("GENERATED ALWAYS AS ({}) " + kind).format(
                sql.SQL(column["generated"])))
        if column.get("not_null"):
            piece.append(sql.SQL("NOT NULL"))
        parts.append(sql.SQL(" ").join(piece))

    if definition.get("primary_key"):
        parts.append(sql.SQL("PRIMARY KEY ({})").format(
            _cols(None, definition["primary_key"])))

    return sql.SQL("CREATE TABLE {} ({})").format(
        _target(schema, table), sql.SQL(", ").join(parts))


# ------------------------------------------------------------------
# Применение
# ------------------------------------------------------------------

def _quiet(fn):
    try:
        fn()
    except Exception:
        pass


def _checked_columns(src_conn, dst_conn, schema, table, with_types):
    """
    Колонки в порядке приёмника. Структура могла измениться после
    сравнения — тогда ValueError, данные не трогаются.
    """
    src_types = table_column_types(src_conn, schema, table)
    dst_types = table_column_types(dst_conn, schema, table)

    if not dst_types:
        raise ValueError("Таблицы %s.%s нет в приёмнике" % (schema, table))

    only_src = sorted(set(src_types) - set(dst_types))
    only_dst = sorted(set(dst_types) - set(src_types))
    retyped = []
    if with_types:
        retyped = sorted(c for c in set(src_types) & set(dst_types)
                         if src_types[c] != dst_types[c])

    if only_src or only_dst or retyped:
        parts = []
        if only_src:
            parts.append("только в источнике: %s" % ", ".join(only_src))
        if only_dst:
            parts.append("только в приёмнике: %s" % ", ".join(only_dst))
        if retyped:
            parts.append("тип отличается: %s" % ", ".join(retyped))
        raise ValueError("Структура таблицы отличается (%s). Проверьте DDL "
                         "в режиме «Перенос»." % "; ".join(parts))

    return list(dst_types)


def dest_column_flags(conn, schema, table):
    """
    {колонка: {generated, identity_always}} по приёмнику, одним запросом.
    """
    cur = conn.cursor()
    cur.execute(build_column_flags_sql(getattr(conn, "server_version", None)),
                (schema, table))

    return {row[0]: {"generated": bool(row[1]),
                     "identity_always": row[2] == "a"}
            for row in cur.fetchall()}


def _writable_columns(dst_conn, schema, table, columns):
    """
    (колонки для COPY/INSERT, колонки identity ALWAYS). Вычисляемые
    колонки приёмник считает сам — их не пишем.
    """
    flags = dest_column_flags(dst_conn, schema, table)
    writable = [c for c in columns
                if not flags.get(c, {}).get("generated")]
    always = [c for c in writable
              if flags.get(c, {}).get("identity_always")]
    return writable, always


def build_stage_tables_sql():
    """Таблицы stg_* схемы staging (параметр: схема staging)."""
    return sql.SQL(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n "
        "ON n.oid = c.relnamespace WHERE n.nspname = %s "
        "AND c.relkind = 'r' AND c.relname LIKE 'stg\\_%%'"
    )


STAGE_NAME_RE = re.compile(r"^stg_(\d+)_(\d+)$")


# конечные статусы задачи (job_manager): staging таких задач брошен;
# queued / pending / running / stopping — ещё живые
FINISHED_JOB_STATUSES = ("done", "failed", "cancelled", "interrupted")

STALE_DROP_LOCK_TIMEOUT = "2s"


def _stale_load_jobs(job_ids, current_job_id):
    """Из job_ids — завершённые задачи pg_diff_load, кроме текущей."""
    ids = sorted(set(job_ids) - {int(current_job_id)})
    if not ids:
        return set()

    with sqlite_cursor() as cur:
        cur.execute("SELECT id FROM jobs WHERE job_type = 'pg_diff_load' "
                    "AND status IN (%s) AND id IN (%s)"
                    % (", ".join("?" * len(FINISHED_JOB_STATUSES)),
                       ", ".join("?" * len(ids))),
                    list(FINISHED_JOB_STATUSES) + ids)
        return {int(row[0]) for row in cur.fetchall()}


def build_lock_timeout_sql():
    return sql.SQL("SET LOCAL lock_timeout = {}").format(
        sql.Literal(STALE_DROP_LOCK_TIMEOUT))


def _drop_stale_stage(conn, stage_name):
    """
    DROP с коротким lock_timeout: таблицу держит кто-то ещё — пропускаем
    (без повтора), уборка не ждёт и не падает. -> удалось ли.
    """
    try:
        cur = conn.cursor()
        cur.execute(build_lock_timeout_sql())
        cur.execute(build_drop_stage_sql(stage_name))
        conn.commit()
        return True
    except Exception:
        _quiet(conn.rollback)
        return False


def cleanup_stale_stages(dst_conn, current_job_id):
    """
    Удаляет staging, оставшийся от упавших процессов: stg_<job>_<n> задач
    pg_diff_load в конечном статусе, кроме текущей. Чужие объекты
    схемы (другое имя, другой тип задачи, неизвестная задача) не трогаются.
    -> [удалённые имена]
    """
    cur = dst_conn.cursor()
    try:
        cur.execute(build_stage_tables_sql(), (STAGE_SCHEMA,))
        names = [row[0] for row in cur.fetchall()]
    finally:
        _quiet(dst_conn.rollback)

    owners = {}
    for name in names:
        match = STAGE_NAME_RE.match(name)
        if match:
            owners[name] = int(match.group(1))

    stale = _stale_load_jobs(owners.values(), current_job_id)
    dropped = []

    for name in sorted(owners, key=lambda n: tuple(
            int(g) for g in STAGE_NAME_RE.match(n).groups())):
        if owners[name] in stale and _drop_stale_stage(dst_conn, name):
            dropped.append(name)

    return dropped


def drop_stage(conn, stage_name):
    """
    Удаляет staging с коммитом. Сторож стопа может снять и этот запрос,
    поэтому вторая попытка. -> удалось ли.
    """
    for _attempt in range(2):
        try:
            conn.cursor().execute(build_drop_stage_sql(stage_name))
            conn.commit()
            return True
        except Exception:
            _quiet(conn.rollback)
    return False


def _count(cur, query):
    cur.execute(query)
    row = cur.fetchone()
    return int((row[0] if row else 0) or 0)


def _apply_keyed(cur, schema, table, stage_name, key_columns, columns,
                 always, delete_missing, ranges=None):
    duplicates = _count(cur, build_stage_duplicate_sql(stage_name, key_columns))
    if duplicates:
        raise DuplicateKeyError(
            "Ключ (%s) не уникален в источнике: повторяется значений — %d"
            % (", ".join(key_columns), duplicates))

    nulls = _count(cur, build_stage_null_key_sql(stage_name, key_columns))
    if nulls:
        raise ValueError("В ключе (%s) есть NULL у строк источника: %d"
                         % (", ".join(key_columns), nulls))

    out = {"insert": 0, "update": 0, "delete": 0}

    if delete_missing:
        # DELETE ограничен листьями — по пачке за раз, транзакция одна
        for batch in _batches(ranges):
            cur.execute(build_key_delete_sql(
                schema, table, stage_name, key_columns,
                where=build_ranges_where("t", batch)))
            out["delete"] += max(cur.rowcount, 0)

    # identity ALWAYS вне ключа UPDATE изменить не может
    skip = [c for c in always if c not in key_columns]

    if any(c not in key_columns and c not in skip for c in columns):
        cur.execute(build_key_update_sql(schema, table, stage_name,
                                         key_columns, columns, skip=skip))
        out["update"] = max(cur.rowcount, 0)

    cur.execute(build_key_insert_sql(schema, table, stage_name, key_columns,
                                     columns, overriding=bool(always)))
    out["insert"] = max(cur.rowcount, 0)
    return out


def _apply_keyless(cur, schema, table, stage_name, columns, always,
                   delete_missing, ranges=None):
    out = {"insert": 0, "update": 0, "delete": 0}

    # листья не пересекаются: разность EXCEPT ALL по пачке листьев на
    # обеих сторонах — та же, что по всем сразу
    for batch in _batches(ranges):
        if delete_missing:
            cur.execute(build_keyless_delete_sql(schema, table, stage_name,
                                                 columns, ranges=batch))
            out["delete"] += max(cur.rowcount, 0)

        cur.execute(build_keyless_insert_sql(schema, table, stage_name,
                                             columns,
                                             overriding=bool(always),
                                             ranges=batch))
        out["insert"] += max(cur.rowcount, 0)
    return out


def load_diff(src_conn, dst_conn, schema, table, key_columns, delete_missing,
              stage_name, ranges=None):
    """
    Загрузка разницы. key_columns — [] для таблицы без ключа.
    DELETE — только при delete_missing is True. -> {insert, update, delete}
    ranges — несовпавшие листья сравнения (get_mismatched_ranges): в staging
    только строки источника из них, DELETE и разность без ключа — тоже
    только в них. None — вся таблица.
    """
    key_columns = list(key_columns or [])
    delete_missing = delete_missing is True

    columns = _checked_columns(src_conn, dst_conn, schema, table, True)
    columns, always = _writable_columns(dst_conn, schema, table, columns)

    absent = [k for k in key_columns if k not in columns]
    if absent:
        raise ValueError("Ключевых колонок нет в таблице или они "
                         "вычисляемые: %s" % ", ".join(absent))

    cur = dst_conn.cursor()
    committed = False

    try:
        cur.execute(build_stage_schema_sql())
        cur.execute(build_drop_stage_sql(stage_name))
        cur.execute(build_create_stage_sql(schema, table, stage_name, columns))
        # в staging — строки источника из листьев, по пачке листьев;
        # все COPY таблицы — в одной транзакции источника, одним снимком
        _open_source_snapshot(src_conn)
        for batch in _batches(ranges):
            stream_copy(src_conn, dst_conn,
                        build_full_select_sql(
                            schema, table, columns,
                            build_ranges_where(None, batch)),
                        _stage(stage_name), columns)
        cur.execute(build_analyze_stage_sql(stage_name))
        dst_conn.commit()

        # одна транзакция приёмника на всё применение
        if key_columns:
            out = _apply_keyed(cur, schema, table, stage_name, key_columns,
                               columns, always, delete_missing, ranges)
        else:
            out = _apply_keyless(cur, schema, table, stage_name, columns,
                                 always, delete_missing, ranges)

        dst_conn.commit()
        committed = True
        return out
    finally:
        _quiet(src_conn.rollback)
        if not committed:
            _quiet(dst_conn.rollback)
        drop_stage(dst_conn, stage_name)


def load_full(src_conn, dst_conn, schema, table, truncate,
              require_empty=False):
    """
    Полная загрузка: TRUNCATE (если truncate) и COPY всех строк источника
    в одной транзакции приёмника. При ошибке — откат. -> {rows}
    require_empty — заливка без TRUNCATE только в пустую таблицу
    (проверка под блокировкой, в той же транзакции), иначе NotEmptyError.
    """
    columns = _checked_columns(src_conn, dst_conn, schema, table, False)
    # COPY FROM сам пишет значения identity ALWAYS из потока; вычисляемые
    # колонки приёмник считает сам
    columns, _always = _writable_columns(dst_conn, schema, table, columns)

    try:
        cur = dst_conn.cursor()

        if truncate:
            cur.execute(build_truncate_sql(schema, table))
        elif require_empty:
            cur.execute(build_lock_for_fill_sql(schema, table))
            cur.execute(build_not_empty_sql(schema, table))
            if cur.fetchone():
                raise NotEmptyError(
                    "В таблице %s.%s в приёмнике уже есть строки — «создать "
                    "и залить» их не дописывает. Выберите полную загрузку "
                    "(TRUNCATE + INSERT) или разницу." % (schema, table))

        rows = stream_copy(src_conn, dst_conn,
                           build_full_select_sql(schema, table, columns),
                           _target(schema, table), columns)
        dst_conn.commit()
        return {"rows": rows}
    except Exception:
        _quiet(dst_conn.rollback)
        raise
    finally:
        _quiet(src_conn.rollback)


def source_table_definition(src_conn, schema, table):
    """
    Определение таблицы по каталогу источника (только чтение):
    {columns:[{name, type, collation, not_null, generated}],
    primary_key:[...],
    partitioned}. Таблицы нет — ValueError.
    """
    cur = src_conn.cursor()

    try:
        # format_type / pg_get_expr квалифицируют имена, не видимые через
        # search_path: при pg_catalog — все пользовательские. SET LOCAL
        # живёт до отката транзакции чтения в finally
        cur.execute(build_catalog_search_path_sql())
        cur.execute(build_source_columns_sql(
            getattr(src_conn, "server_version", None)), (schema, table))
        rows = cur.fetchall()
        if not rows:
            raise ValueError("Таблицы %s.%s нет в источнике" % (schema, table))

        cur.execute(build_source_pk_sql(), (schema, table))
        primary_key = [r[0] for r in cur.fetchall()]
    finally:
        _quiet(src_conn.rollback)

    columns = []
    for (name, type_name, not_null, _identity, generated, expr, _kind,
         coll_schema, coll_name) in rows:
        columns.append({
            "name": name,
            "type": type_name,
            "collation": [coll_schema, coll_name] if coll_name else None,
            "not_null": bool(not_null),
            # pg_attrdef у обычной колонки — default (в т.ч. nextval):
            # не переносится
            "generated": expr if generated else None,
            "generated_kind": generated or None,
        })

    return {"columns": columns, "primary_key": primary_key,
            "partitioned": rows[0][6] == "p"}


def create_table_from_source(src_conn, dst_conn, schema, table):
    """
    Создаёт в приёмнике таблицу по каталогу PostgreSQL источника
    (вместо ddl_check.create_missing_objects: тот обращается к функциям
    Greenplum). Схема — только если её нет; схема и таблица — в одной
    транзакции приёмника. Партиционированный родитель создаётся обычной
    таблицей. Ошибка — откат приёмника и исключение.
    -> {partitioned, virtual_as_stored: [колонки VIRTUAL, созданные STORED]}
    """
    definition = source_table_definition(src_conn, schema, table)
    version = getattr(dst_conn, "server_version", None)
    virtual = bool(version) and version >= VIRTUAL_GENERATED_VERSION
    as_stored = [] if virtual else [
        c["name"] for c in definition["columns"]
        if c.get("generated_kind") == "v"]
    cur = dst_conn.cursor()

    try:
        # CREATE SCHEMA IF NOT EXISTS требует права CREATE на базу даже
        # для существующей схемы — поэтому сначала проверка
        cur.execute(build_schema_exists_sql(), (schema,))
        if not cur.fetchall():
            cur.execute(build_create_schema_sql(schema))
        cur.execute(build_create_table_sql(schema, table, definition,
                                           virtual=virtual))
        dst_conn.commit()
    except Exception:
        _quiet(dst_conn.rollback)
        raise

    return {"partitioned": definition["partitioned"],
            "virtual_as_stored": as_stored}


def dest_table_exists(conn, schema, table):
    cur = conn.cursor()
    try:
        cur.execute(build_table_exists_sql(), (schema, table))
        return bool(cur.fetchall())
    finally:
        _quiet(conn.rollback)


def sync_sequences(conn, schema, table):
    """
    После коммита вставки: последовательности serial / identity таблицы
    догоняют max(колонки) — только вперёд, у пустой таблицы не трогаются.
    Не бросает: ошибки — список предупреждений.
    """
    cur = conn.cursor()

    try:
        cur.execute(build_serial_sequences_sql(), (schema, table))
        pairs = [(r[0], r[1]) for r in cur.fetchall() if r[1]]
        _quiet(conn.rollback)
    except Exception as e:
        _quiet(conn.rollback)
        return ["последовательности не прочитаны: %s" % str(e)[:200]]

    warnings = []

    for column, sequence in pairs:
        try:
            cur.execute(build_setval_sql(schema, table, column),
                        (sequence, sequence, sequence))
            conn.commit()
        except Exception as e:
            _quiet(conn.rollback)
            warnings.append("setval %s: %s" % (sequence, str(e)[:200]))

    return warnings


# ------------------------------------------------------------------
# Раннер
# ------------------------------------------------------------------

ACTIONS = ("diff", "full", "create")

MISSING_IN_DEST = ("Таблицы нет в приёмнике — создайте её через сравнение "
                   "(«создать и залить»)")


def _set_item_message(item_id, message):
    """Итог таблицы (фактические числа) — в сообщение строки задачи."""
    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE job_items SET error_message = ? WHERE id = ?",
                    (str(message), item_id))


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
    """Не откатившееся соединение заменяется новым (индекс 0 — источник)."""
    for index in broken:
        _quiet(conns[index].close)
        conns[index] = open_pg(connection_ids[index], readonly=(index == 0))


def _cancel_rest(job_id):
    for rest in get_job_items(job_id):
        if rest["status"] in ("queued", "pending"):
            mark_item_skipped(rest["id"], "остановлено пользователем")

    refresh_job_progress(job_id)
    # таблицу, загрузку которой оборвал стоп, mark_job_cancelled закроет
    mark_job_cancelled(job_id)


PARTITIONED_NOTE = ("пометка: партиционированная таблица источника создана "
                    "в приёмнике обычной таблицей, строки всех партиций "
                    "залиты в неё")


VIRTUAL_AS_STORED_NOTE = ("пометка: приёмник не поддерживает VIRTUAL "
                          "generated-колонки (нужен PostgreSQL 18), созданы "
                          "как STORED: %s")


def _create_and_fill(src_conn, dst_conn, schema, table, note):
    """
    Таблицы нет — создаём по каталогу источника; есть — только если пуста
    (проверка в load_full). Если таблица уже создана, а заливка не прошла,
    это видно в item. -> ({rows}, пометка или None)
    """
    created = False
    remark = None

    if not dest_table_exists(dst_conn, schema, table):
        out = create_table_from_source(src_conn, dst_conn, schema, table)
        created = True
        remarks = []
        if out.get("partitioned"):
            remarks.append(PARTITIONED_NOTE)
        if out.get("virtual_as_stored"):
            remarks.append(VIRTUAL_AS_STORED_NOTE
                           % ", ".join(out["virtual_as_stored"]))
        remark = "; ".join(remarks) or None
        # останется в item, если заливку оборвёт стоп
        note("Таблица %s.%s создана в приёмнике; заливка не завершена"
             % (schema, table))

    try:
        return load_full(src_conn, dst_conn, schema, table, truncate=False,
                         require_empty=True), remark
    except Exception as e:
        if created:
            raise RuntimeError("Таблица %s.%s создана в приёмнике, но "
                               "заливка не прошла: %s" % (schema, table, e))
        raise


# итог сравнения, по которому таблицу можно грузить по листьям
CHUNKED_STATUSES = ("same", "differs")


def diff_ranges(compare_job_id, schema, table):
    """
    Листья сравнения для загрузки разницы:
      None — грузить всю таблицу, как раньше (сравнение не по диапазонам,
             нет сравнения, или листья не сходятся с итогом);
      []   — по диапазонам всё совпало, грузить нечего;
      [Range + column, collate_c] — только эти листья.
    """
    if not compare_job_id:
        return None

    row = None
    for r in pg_compare.get_results(compare_job_id):
        # последняя строка по таблице — актуальная
        if (r["schema"], r["table"]) == (schema, table):
            row = r

    chunked = (row or {}).get("chunked")
    if not chunked or row["status"] not in CHUNKED_STATUSES:
        return None

    if row["status"] == "same":
        return []

    leaves = pg_compare.get_mismatched_ranges(compare_job_id, schema, table)
    # листа не хватает (не записан) — безопаснее вся таблица
    if not leaves or len(leaves) != int(chunked.get("mismatched") or 0):
        return None
    return leaves


def _load_one(src_conn, dst_conn, schema, table, entry, delete_missing,
              stage_name, note, compare_job_id=None):
    """
    -> ("done", сообщение, вставлено строк) или ("skipped", причина, 0).
    """
    action = entry.get("action")

    if action == "diff":
        ranges = diff_ranges(compare_job_id, schema, table)
        if ranges is None:
            out = load_diff(src_conn, dst_conn, schema, table,
                            entry.get("key_columns") or [], delete_missing,
                            stage_name)
        elif ranges:
            out = load_diff(src_conn, dst_conn, schema, table,
                            entry.get("key_columns") or [], delete_missing,
                            stage_name, ranges=ranges)
        else:
            out = {"insert": 0, "update": 0, "delete": 0}

        message = "insert=%d; update=%d; delete=%d" % (
            out["insert"], out["update"], out["delete"])
        if ranges is not None:
            message += "; по диапазонам: %d" % len(ranges)
        return "done", message, out["insert"]

    if action == "full":
        if entry.get("in_dst") is False:
            return "skipped", MISSING_IN_DEST, 0
        out = load_full(src_conn, dst_conn, schema, table, truncate=True)
        return "done", "truncate+insert=%d" % out["rows"], out["rows"]

    if action == "create":
        out, remark = _create_and_fill(src_conn, dst_conn, schema, table,
                                       note)
        message = "create+insert=%d" % out["rows"]
        if remark:
            message += "; " + remark
        return "done", message, out["rows"]

    raise ValueError("Неизвестное действие: %s" % action)


def _with_sequence_warnings(dst_conn, schema, table, message):
    """Таблица уже закоммичена: сбой setval — только предупреждение."""
    try:
        warnings = sync_sequences(dst_conn, schema, table)
    except Exception as e:
        warnings = ["последовательности: %s" % str(e)[:200]]

    if warnings:
        message += "; предупреждение: " + "; ".join(warnings)
    return message


def run_pg_diff_load_job(job_id):
    """
    Раннер job_type='pg_diff_load'. Config: source_connection_id,
    dest_connection_id, delete_missing, compare_job_id, expected,
    tables=[{schema, table, action, key_columns, in_dst}].
    Item на таблицу; done — только после коммита таблицы.
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

        conns.append(open_pg(source_id, readonly=True))
        conns.append(open_pg(dest_id))

        # staging, брошенный упавшими процессами прошлых задач; сбой уборки
        # задачу не валит
        try:
            cleanup_stale_stages(conns[1], job_id)
        except Exception:
            _rollback(conns[1:])

        delete_missing = config.get("delete_missing") is True
        info = {(t.get("schema"), t.get("table")): t
                for t in config.get("tables") or []}
        items = get_job_items(job_id)

        refresh_job_progress(job_id)
        failed = 0
        stopped = False
        open_stage = None

        with StopWatch(job_id, conns) as watch:
            for n, item in enumerate(items, 1):
                if item.get("status") in ("done", "failed", "skipped"):
                    continue

                if watch.check():
                    stopped = True
                    break

                schema, table = item["schema_name"], item["table_name"]
                entry = info.get((schema, table)) or {
                    "action": str(item.get("action") or "").lower()}
                stage_name = stage_name_for(job_id, n)

                mark_item_running(item["id"])
                refresh_job_progress(job_id)

                def note(text, item_id=item["id"]):
                    _set_item_message(item_id, text)

                try:
                    status, message, inserted = _load_one(
                        conns[0], conns[1], schema, table, entry,
                        delete_missing, stage_name, note,
                        compare_job_id=config.get("compare_job_id"))
                except Exception as e:
                    broken = _rollback(conns)

                    # QueryCanceledError от conn.cancel() сторожа — это стоп
                    if watch.stopped or watch.check():
                        stopped = True
                        if entry.get("action") == "diff":
                            open_stage = stage_name
                        break

                    if broken:
                        _reopen_broken(conns, broken, [source_id, dest_id])

                    failed += 1
                    mark_item_failed(item["id"], str(e)[:500])
                else:
                    # таблица закоммичена: отсюда она уже не станет failed
                    if status == "skipped":
                        mark_item_skipped(item["id"], message)
                    else:
                        if inserted:
                            message = _with_sequence_warnings(
                                conns[1], schema, table, message)
                        # итог — до done; сбой записи итога не делает
                        # закоммиченную таблицу failed
                        _quiet(lambda: _set_item_message(item["id"], message))
                        mark_item_done(item["id"])

                refresh_job_progress(job_id)

        if stopped:
            # сторож больше не снимает запросы: добираем staging таблицы,
            # если его удаление в load_diff попало под cancel()
            if open_stage:
                _rollback(conns)
                drop_stage(conns[1], open_stage)
            _cancel_rest(job_id)
            return

        if failed:
            mark_job_failed(job_id,
                            "%s таблиц(ы) не удалось загрузить" % failed)
        else:
            mark_job_done(job_id)

    except Exception as e:
        _rollback(conns)
        mark_job_failed(job_id, str(e)[:500])
    finally:
        for conn in conns:
            _quiet(conn.close)
