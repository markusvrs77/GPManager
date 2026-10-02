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

import modules.ddl_check as ddl_check
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


def build_full_select_sql(schema, table, columns):
    """SELECT <колонки по порядку приёмника> FROM schema.table."""
    return sql.SQL("SELECT {} FROM {}").format(_cols(None, columns),
                                               _target(schema, table))


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


def build_key_delete_sql(schema, table, stage_name, key_columns):
    """
    Строки приёмника с ключом, которого нет в источнике. Строки с NULL
    в ключе не трогаются: NULL = NULL не истинно, и NOT EXISTS счёл бы
    их отсутствующими в источнике.
    """
    return sql.SQL(
        "DELETE FROM {} AS {} WHERE {} AND NOT EXISTS (SELECT 1 FROM {} AS {} "
        "WHERE {})"
    ).format(_target(schema, table), sql.Identifier("t"),
             sql.SQL(" AND ").join(
                 sql.SQL("{} IS NOT NULL").format(sql.Identifier("t", k))
                 for k in key_columns),
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


def _surplus_cte(left, left_alias, right, right_alias, columns):
    """
    "m"(h, c): хеш строки и сколько её копий в left сверх right
    (разность мультимножеств EXCEPT ALL).
    """
    return sql.SQL(
        "{m} AS (SELECT {h}, count(*) AS {c} FROM ("
        "SELECT {lh} AS {h} FROM {left} AS {la} EXCEPT ALL "
        "SELECT {rh} AS {h} FROM {right} AS {ra}) AS {d} GROUP BY {h})"
    ).format(
        m=sql.Identifier("m"), h=sql.Identifier("h"), c=sql.Identifier("c"),
        d=sql.Identifier("d"),
        lh=row_hash_sql(left_alias, columns), left=left,
        la=sql.Identifier(left_alias),
        rh=row_hash_sql(right_alias, columns), right=right,
        ra=sql.Identifier(right_alias),
    )


def _numbered_cte(source, alias, columns, extra):
    """
    "x"(<extra>, h, rn): у каждой копии строки свой номер в разрезе хеша,
    чтобы взять ровно нужное число копий.
    """
    hash_expr = row_hash_sql(alias, columns)
    return sql.SQL(
        "{x} AS (SELECT {extra}, {hash} AS {h}, row_number() OVER "
        "(PARTITION BY {hash}) AS {rn} FROM {src} AS {a})"
    ).format(x=sql.Identifier("x"), extra=extra, hash=hash_expr,
             h=sql.Identifier("h"), rn=sql.Identifier("rn"),
             src=source, a=sql.Identifier(alias))


def build_keyless_insert_sql(schema, table, stage_name, columns,
                             overriding=False):
    """INSERT недостающих копий строк: staging EXCEPT ALL приёмник."""
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
        m=_surplus_cte(stage, "s", target, "t", columns),
        x=sql.SQL(
            "{x} AS (SELECT {s_cols}, {hash} AS {h}, row_number() OVER "
            "(PARTITION BY {hash}) AS {rn} FROM {stage} AS {s})"
        ).format(x=sql.Identifier("x"), s_cols=_cols("s", columns),
                 hash=row_hash_sql("s", columns),
                 h=sql.Identifier(INSERT_HASH), rn=sql.Identifier(INSERT_RN),
                 stage=stage, s=sql.Identifier("s")),
        target=target, cols=_cols(None, columns), x_cols=_cols("x", columns),
        ov=_overriding(overriding),
        xa=sql.Identifier("x"), ma=sql.Identifier("m"),
        m_h=sql.Identifier("m", "h"), x_h=sql.Identifier("x", INSERT_HASH),
        x_rn=sql.Identifier("x", INSERT_RN), m_c=sql.Identifier("m", "c"),
    )


def build_keyless_delete_sql(schema, table, stage_name, columns):
    """DELETE лишних копий строк приёмника: приёмник EXCEPT ALL staging."""
    stage = _stage(stage_name)
    target = _target(schema, table)

    return sql.SQL(
        "WITH {m}, {x} DELETE FROM {target} AS {t} USING {xa} JOIN {ma} "
        "ON {m_h} = {x_h} WHERE {x_rn} <= {m_c} AND {t_oid} = {x_oid} "
        "AND {t_tid} = {x_tid}"
    ).format(
        m=_surplus_cte(target, "t", stage, "s", columns),
        # ctid уникален только внутри партиции — берём и tableoid
        x=_numbered_cte(target, "t", columns, sql.SQL(
            "{} AS {}, {} AS {}").format(
                sql.Identifier("t", "tableoid"), sql.Identifier("toid"),
                sql.Identifier("t", "ctid"), sql.Identifier("tid"))),
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
                 always, delete_missing):
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
        cur.execute(build_key_delete_sql(schema, table, stage_name,
                                         key_columns))
        out["delete"] = max(cur.rowcount, 0)

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
                   delete_missing):
    out = {"insert": 0, "update": 0, "delete": 0}

    if delete_missing:
        cur.execute(build_keyless_delete_sql(schema, table, stage_name,
                                             columns))
        out["delete"] = max(cur.rowcount, 0)

    cur.execute(build_keyless_insert_sql(schema, table, stage_name, columns,
                                         overriding=bool(always)))
    out["insert"] = max(cur.rowcount, 0)
    return out


def load_diff(src_conn, dst_conn, schema, table, key_columns, delete_missing,
              stage_name):
    """
    Загрузка разницы. key_columns — [] для таблицы без ключа.
    DELETE — только при delete_missing is True. -> {insert, update, delete}
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
        stream_copy(src_conn, dst_conn,
                    build_full_select_sql(schema, table, columns),
                    _stage(stage_name), columns)
        cur.execute(build_analyze_stage_sql(stage_name))
        dst_conn.commit()

        # одна транзакция приёмника на всё применение
        if key_columns:
            out = _apply_keyed(cur, schema, table, stage_name, key_columns,
                               columns, always, delete_missing)
        else:
            out = _apply_keyless(cur, schema, table, stage_name, columns,
                                 always, delete_missing)

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


def _create_table(source_id, dest_id, schema, table):
    """Создание по DDL источника; ошибка — исключение с её текстом."""
    rows = ddl_check.create_missing_objects(
        source_id, dest_id, [{"schema": schema, "table": table}])
    row = next((r for r in rows or []
                if (r.get("schema"), r.get("table")) == (schema, table)), None)

    if not row:
        raise RuntimeError("Таблица %s.%s не создана" % (schema, table))

    if not row.get("ok"):
        raise RuntimeError("Не удалось создать таблицу %s.%s: %s"
                           % (schema, table, row.get("error") or "ошибка"))


def _create_and_fill(src_conn, dst_conn, ids, schema, table, note):
    """
    Таблицы нет — создаём; есть — только если пуста (проверка в load_full).
    Если таблица уже создана, а заливка не прошла, это видно в item.
    """
    created = False

    if not dest_table_exists(dst_conn, schema, table):
        _create_table(ids[0], ids[1], schema, table)
        created = True
        # останется в item, если заливку оборвёт стоп
        note("Таблица %s.%s создана в приёмнике; заливка не завершена"
             % (schema, table))

    try:
        return load_full(src_conn, dst_conn, schema, table, truncate=False,
                         require_empty=True)
    except Exception as e:
        if created:
            raise RuntimeError("Таблица %s.%s создана в приёмнике, но "
                               "заливка не прошла: %s" % (schema, table, e))
        raise


def _load_one(src_conn, dst_conn, ids, schema, table, entry, delete_missing,
              stage_name, note):
    """
    -> ("done", сообщение, вставлено строк) или ("skipped", причина, 0).
    """
    action = entry.get("action")

    if action == "diff":
        out = load_diff(src_conn, dst_conn, schema, table,
                        entry.get("key_columns") or [], delete_missing,
                        stage_name)
        return "done", "insert=%d; update=%d; delete=%d" % (
            out["insert"], out["update"], out["delete"]), out["insert"]

    if action == "full":
        if entry.get("in_dst") is False:
            return "skipped", MISSING_IN_DEST, 0
        out = load_full(src_conn, dst_conn, schema, table, truncate=True)
        return "done", "truncate+insert=%d" % out["rows"], out["rows"]

    if action == "create":
        out = _create_and_fill(src_conn, dst_conn, ids, schema, table, note)
        return "done", "create+insert=%d" % out["rows"], out["rows"]

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
                        conns[0], conns[1], (source_id, dest_id), schema,
                        table, entry, delete_missing, stage_name, note)
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
