# -*- coding: utf-8 -*-
"""
Сравнение больших таблиц PostgreSQL по диапазонам (pg_compare).

Таблица режется на диапазоны значений одной колонки; по каждому диапазону
обе стороны считают одинаковую контрольную сумму (count и две суммы
половин md5 строки). Совпавшие диапазоны закрываются сразу, несовпавшие
дробятся до листа, где работает построчное сравнение с предикатом.

Range = {"lo", "hi", "is_null", "depth"}:
  * lo включительно (col >= lo), hi исключительно (col < hi);
  * None — открытая граница; обе None — все значения, кроме NULL;
  * is_null=True — отдельный диапазон col IS NULL.
Первый диапазон уровня открыт снизу, последний — сверху, поэтому строки
приёмника вне [min, max] источника тоже попадают в какой-то диапазон.

Источник только читается. Идентификаторы — sql.Identifier, значения
границ — только sql.Literal / параметры. Транзакцией управляет
вызывающий (здесь нет commit/rollback).
"""

import datetime
import math
from decimal import Decimal

from psycopg2 import sql

from modules.pg_sync_common import row_hash_sql


# порог включения: по статистике источника — размер ИЛИ строки
CHUNK_MIN_BYTES = 2 * 1024 ** 3
CHUNK_MIN_ROWS = 5000000
# диапазонов верхнего уровня
TOP_CHUNKS = 64
# несовпавший диапазон больше этого (по max из сторон) дробится дальше
LEAF_ROWS = 100000
# на сколько частей дробится несовпавший диапазон
SPLIT_FANOUT = 16
# сколько строк читать выборкой TABLESAMPLE для квантилей верхнего уровня
SAMPLE_ROWS = 100000
# страховка от бесконечного дробления
MAX_DEPTH = 24

# типы, где границы считаются арифметически от min/max
ARITHMETIC_KINDS = ("int", "numeric", "date", "timestamp", "timestamptz")
# предпочтение колонки без ключа: меньше — лучше
KIND_RANK = {"date": 1, "timestamp": 1, "timestamptz": 1, "int": 2,
             "numeric": 3, "text": 4, "uuid": 4}


def make_range(lo=None, hi=None, is_null=False, depth=0):
    return {"lo": lo, "hi": hi, "is_null": bool(is_null), "depth": int(depth)}


# ------------------------------------------------------------------
# Порог
# ------------------------------------------------------------------

SIZE_SQL = """
    WITH r AS (
        SELECT c.oid
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
    )
    SELECT coalesce(sum(pg_total_relation_size(x.oid)), 0),
           coalesce(sum(greatest(x.reltuples, 0)), 0)
    FROM pg_class x
    WHERE x.oid IN (SELECT oid FROM r
                    UNION
                    SELECT p.relid FROM r, pg_partition_tree(r.oid) p)
    HAVING count(*) > 0
"""


def table_stats(conn, schema, table):
    """(байт с индексами и TOAST, reltuples) по таблице и её партициям."""
    cur = conn.cursor()
    cur.execute(SIZE_SQL, (schema, table))
    row = cur.fetchone()
    if not row:
        return 0, 0
    return int(row[0] or 0), int(float(row[1] or 0))


def should_chunk(src_conn, schema, table):
    """Включать ли режим диапазонов: по статистике источника."""
    size, rows = table_stats(src_conn, schema, table)
    return size >= CHUNK_MIN_BYTES or rows >= CHUNK_MIN_ROWS


# ------------------------------------------------------------------
# Колонка нарезки
# ------------------------------------------------------------------

def type_kind(type_name):
    """format_type колонки → вид для нарезки или None (не режем)."""
    t = (type_name or "").strip().lower()

    if t in ("smallint", "integer", "bigint"):
        return "int"
    if t == "numeric" or t.startswith("numeric("):
        return "numeric"
    if t == "date":
        return "date"
    if t.startswith("timestamp"):
        return "timestamptz" if t.endswith("with time zone") \
            and "without" not in t else "timestamp"
    if t == "text" or t.startswith("character varying") \
            or t.startswith("character(") or t == "character":
        return "text"
    if t == "uuid":
        return "uuid"
    return None


def choose_chunk_column(src_info, dst_info, key_columns, n_distinct=None):
    """
    Чистый выбор колонки нарезки.
    src_info / dst_info — {колонка: (format_type, collation)}.
    С ключом — первая ключевая колонка поддерживаемого типа (пара строк
    по ключу всегда в одном диапазоне); без ключа — колонка обеих сторон
    с одинаковым типом: дата/время → целое → numeric → text/uuid, среди
    равных — наибольший n_distinct ({колонка: число}).
    -> {"name", "kind", "collate_c"} или None.
    """
    def usable(name):
        if name not in src_info or name not in dst_info:
            return None
        if src_info[name][0] != dst_info[name][0]:
            return None
        return type_kind(src_info[name][0])

    def result(name, kind):
        collate_c = kind == "text" and (
            src_info[name][1] != dst_info[name][1] or not src_info[name][1])
        return {"name": name, "kind": kind, "collate_c": bool(collate_c)}

    if key_columns:
        for name in key_columns:
            kind = usable(name)
            if kind:
                return result(name, kind)
        return None

    distinct = n_distinct or {}
    candidates = []

    for order, name in enumerate(src_info):
        kind = usable(name)
        if kind:
            candidates.append((KIND_RANK[kind],
                               -float(distinct.get(name) or 0), order, name,
                               kind))

    if not candidates:
        return None

    best = min(candidates)
    return result(best[3], best[4])


COLUMNS_SQL = """
    SELECT a.attname,
           format_type(a.atttypid, a.atttypmod),
           CASE
             WHEN a.attcollation = 0 THEN ''
             WHEN co.collname = 'default' THEN (
               SELECT concat_ws('/', 'default', d.datcollate,
                                to_jsonb(d) ->> 'datlocprovider',
                                to_jsonb(d) ->> 'datlocale',
                                to_jsonb(d) ->> 'daticulocale')
               FROM pg_database d WHERE d.datname = current_database())
             ELSE concat_ws('/', co.collname, co.collprovider::text,
                            to_jsonb(co) ->> 'collcollate',
                            to_jsonb(co) ->> 'colllocale',
                            to_jsonb(co) ->> 'colliculocale')
           END
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_collation co ON co.oid = a.attcollation
    WHERE n.nspname = %s AND c.relname = %s
      AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
"""

DISTINCT_SQL = """
    SELECT s.attname,
           max(CASE WHEN s.n_distinct >= 0 THEN s.n_distinct
                    ELSE -s.n_distinct * greatest(c.reltuples, 1) END)
    FROM pg_stats s
    JOIN pg_namespace n ON n.nspname = s.schemaname
    JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = s.tablename
    WHERE s.schemaname = %s AND s.tablename = %s
    GROUP BY s.attname
"""


def _column_info(conn, schema, table):
    cur = conn.cursor()
    cur.execute(COLUMNS_SQL, (schema, table))
    return {r[0]: (r[1], r[2] or "") for r in cur.fetchall()}


def pick_chunk_column(src_conn, dst_conn, schema, table, key_columns):
    """
    Колонка нарезки по каталогам обеих сторон и pg_stats источника.
    -> {"name", "kind", "collate_c"} или None (одна сумма на таблицу).
    """
    src_info = _column_info(src_conn, schema, table)
    dst_info = _column_info(dst_conn, schema, table)
    distinct = None

    if not key_columns:
        cur = src_conn.cursor()
        cur.execute(DISTINCT_SQL, (schema, table))
        distinct = {r[0]: float(r[1] or 0) for r in cur.fetchall()}

    return choose_chunk_column(src_info, dst_info, list(key_columns or []),
                               distinct)


# ------------------------------------------------------------------
# Предикат и контрольная сумма
# ------------------------------------------------------------------

def _col_name(column):
    return column["name"] if isinstance(column, dict) else column


def _col_expr(alias, column, collate_c):
    ref = sql.Identifier(alias, column) if alias else sql.Identifier(column)
    if collate_c:
        return sql.SQL('{} COLLATE "C"').format(ref)
    return ref


def range_predicate(alias, column, rng, collate_c=False):
    """
    Предикат диапазона как Composable: col >= lo AND col < hi; без границ —
    col IS NOT NULL; is_null — col IS NULL. column — имя колонки (или
    словарь из pick_chunk_column). Значения — только sql.Literal.
    """
    name = _col_name(column)
    ref = sql.Identifier(alias, name) if alias else sql.Identifier(name)

    if rng.get("is_null"):
        return sql.SQL("{} IS NULL").format(ref)

    lo, hi = rng.get("lo"), rng.get("hi")

    if lo is None and hi is None:
        return sql.SQL("{} IS NOT NULL").format(ref)

    expr = _col_expr(alias, name, collate_c)
    parts = []
    if lo is not None:
        parts.append(sql.SQL("{} >= {}").format(expr, sql.Literal(lo)))
    if hi is not None:
        parts.append(sql.SQL("{} < {}").format(expr, sql.Literal(hi)))
    return sql.SQL(" AND ").join(parts)


def build_checksum_sql(schema, table, columns, pred):
    """count(*) и две суммы 64-битных половин md5 строки под предикатом."""
    where = sql.SQL(" WHERE {}").format(pred) if pred is not None \
        else sql.SQL("")

    return sql.SQL(
        "SELECT count(*), "
        "coalesce(sum(('x' || substr(s.h, 1, 16))::bit(64)::bigint), 0), "
        "coalesce(sum(('x' || substr(s.h, 17, 16))::bit(64)::bigint), 0) "
        "FROM (SELECT {} AS h FROM {} AS {}{}) AS s"
    ).format(row_hash_sql("t", columns), sql.Identifier(schema, table),
             sql.Identifier("t"), where)


def range_checksum(conn, schema, table, columns, pred):
    """-> (count, s1, s2) — одинаковый запрос на каждой стороне."""
    cur = conn.cursor()
    cur.execute(build_checksum_sql(schema, table, columns, pred))
    row = cur.fetchone() or (0, 0, 0)
    return tuple(int(v or 0) for v in row[:3])


# ------------------------------------------------------------------
# Границы
# ------------------------------------------------------------------

def arithmetic_cuts(kind, lo, hi, parts):
    """
    Точки деления [lo, hi] (lo, hi — min и max источника) на parts равных
    отрезков: строго больше lo и не больше hi, возрастают. [] — делить
    нельзя (одно значение).
    """
    parts = max(2, int(parts))
    if lo is None or hi is None or not hi > lo:
        return []

    if kind == "int":
        step = max(1, -(-(hi - lo) // parts))
        points = [lo + step * i for i in range(1, parts)]
    elif kind == "numeric":
        if lo.is_nan() or hi.is_nan() or lo.is_infinite() \
                or hi.is_infinite():
            return []
        step = (hi - lo) / parts
        points = [lo + step * i for i in range(1, parts)]
    elif kind == "date":
        a, b = lo.toordinal(), hi.toordinal()
        step = max(1, -(-(b - a) // parts))
        points = [datetime.date.fromordinal(a + step * i)
                  for i in range(1, parts) if a + step * i <= b]
    elif kind in ("timestamp", "timestamptz"):
        step = (hi - lo) / parts
        if step <= datetime.timedelta(0):
            step = datetime.timedelta(microseconds=1)
        points = [lo + step * i for i in range(1, parts)]
    else:
        raise ValueError("Арифметическое деление не для типа %s" % kind)

    out = []
    for p in points:
        if lo < p <= hi and (not out or p > out[-1]):
            out.append(p)
    return out


def ranges_from_cuts(lo, hi, cuts, depth):
    """[lo, c1), [c1, c2), ..., [cN, hi) — внешние границы сохраняются."""
    bounds = [lo] + list(cuts) + [hi]
    return [make_range(bounds[i], bounds[i + 1], depth=depth)
            for i in range(len(bounds) - 1)]


def _distinct_sorted(values):
    """Неубывающий список значений → строго возрастающий (по порядку SQL)."""
    out = []
    for v in values:
        if v is not None and (not out or v != out[-1]):
            out.append(v)
    return out


def _min_max(conn, schema, table, column, pred):
    """min и max источника под предикатом (planagg берёт их по индексу)."""
    col = sql.Identifier("t", column["name"])
    cur = conn.cursor()
    cur.execute(sql.SQL(
        "SELECT min({c}), max({c}) FROM {tbl} AS {t} WHERE {pred}"
    ).format(c=col, tbl=sql.Identifier(schema, table),
             t=sql.Identifier("t"), pred=pred))
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def _fractions(parts):
    return [i / float(parts) for i in range(parts)]


def _quantiles(conn, schema, table, column, pred, parts, sample_pct=None):
    """
    percentile_disc(0, 1/parts, ...) по источнику под предикатом в порядке
    колонки (COLLATE "C", если нужно). Первый элемент — минимум.
    """
    order = _col_expr("t", column["name"], column.get("collate_c"))
    sample = sql.SQL("")
    if sample_pct is not None:
        sample = sql.SQL(" TABLESAMPLE SYSTEM ({})").format(
            sql.Literal(float(sample_pct)))

    cur = conn.cursor()
    cur.execute(sql.SQL(
        "SELECT (percentile_disc({f}::float8[]) WITHIN GROUP "
        "(ORDER BY {o}))::text[] FROM {tbl} AS {t}{sample} WHERE {pred}"
    ).format(f=sql.Literal(_fractions(parts)), o=order,
             tbl=sql.Identifier(schema, table), t=sql.Identifier("t"),
             sample=sample, pred=pred))
    row = cur.fetchone()
    values = list(row[0] or []) if row else []
    return _distinct_sorted(values)


def _cuts(src_conn, schema, table, column, rng, parts, sample_pct=None):
    pred = range_predicate("t", column["name"],
                           dict(rng, is_null=False),
                           column.get("collate_c"))

    if column["kind"] in ARITHMETIC_KINDS:
        lo, hi = _min_max(src_conn, schema, table, column, pred)
        return arithmetic_cuts(column["kind"], lo, hi, parts)

    values = _quantiles(src_conn, schema, table, column, pred, parts,
                        sample_pct)
    # values[0] — минимум: точки строго больше него, иначе первый
    # поддиапазон повторил бы родителя
    return values[1:]


def top_ranges(src_conn, schema, table, column):
    """
    Диапазоны верхнего уровня (TOP_CHUNKS) + отдельный col IS NULL.
    column — словарь из pick_chunk_column.
    """
    sample_pct = None
    if column["kind"] not in ARITHMETIC_KINDS:
        _size, rows = table_stats(src_conn, schema, table)
        if rows > SAMPLE_ROWS:
            sample_pct = max(0.0001, min(100.0, 100.0 * SAMPLE_ROWS / rows))

    cuts = _cuts(src_conn, schema, table, column, make_range(), TOP_CHUNKS,
                 sample_pct)
    out = ranges_from_cuts(None, None, cuts, depth=0)
    out.append(make_range(is_null=True, depth=0))
    return out


def split_range(src_conn, schema, table, column, rng):
    """
    SPLIT_FANOUT поддиапазонов rng (глубина +1): числа и даты —
    арифметически от min/max источника внутри rng, прочее —
    percentile_disc источника внутри rng. [] — делить нельзя (лист).
    """
    if rng.get("is_null") or int(rng.get("depth") or 0) >= MAX_DEPTH:
        return []

    cuts = _cuts(src_conn, schema, table, column, rng, SPLIT_FANOUT)
    if not cuts:
        return []

    return ranges_from_cuts(rng.get("lo"), rng.get("hi"), cuts,
                            depth=int(rng.get("depth") or 0) + 1)


# ------------------------------------------------------------------
# Хранение границ
# ------------------------------------------------------------------

def bound_to_json(value):
    """Граница → JSON: int как есть, остальное — текстом (None — null)."""
    if value is None or isinstance(value, bool):
        return None if value is None else value
    if isinstance(value, int):
        return value
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float):
        return repr(value) if math.isfinite(value) else str(value)
    return str(value)
