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
# режим корзин: длина md5-префикса первого уровня, шаг и предел
BUCKET_START = 2
BUCKET_STEP = 2
BUCKET_MAX = 8
# листьев за один COPY в staging / один DELETE при загрузке разницы
LEAF_BATCH = 200

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
    if t == "citext" or t.endswith(".citext"):
        return "text"
    return None


# порядок строк C / POSIX — побайтовый, от версии библиотек не зависит
BYTE_ORDER_LOCALES = ("C", "POSIX")


def same_order(a, b):
    """
    Одинаков ли порядок строк у двух collation. a, b — (имя, провайдер,
    версия) из build_columns_sql. Имя одно ещё не порядок: у glibc и ICU
    разных версий он разный. Хоть что-то неизвестно — порядок разный.
    """
    if not a or not b or tuple(a) != tuple(b):
        return False
    name, provider, version = a
    if not name or not provider:
        return False
    if version:
        return True
    # libc "C" / "POSIX": версии нет, но порядок побайтовый
    locale = str(name).split("/")[-1]
    return provider == "c" and locale in BYTE_ORDER_LOCALES


def choose_chunk_column(src_info, dst_info, key_columns, n_distinct=None):
    """
    Чистый выбор колонки нарезки.
    src_info / dst_info — {колонка: (format_type, (имя, провайдер, версия)
    collation или None)}.
    С ключом — первая ключевая колонка поддерживаемого типа (пара строк
    по ключу всегда в одном диапазоне); без ключа — колонка обеих сторон
    с одинаковым типом: дата/время → целое → numeric → text/uuid, среди
    равных — наибольший n_distinct ({колонка: число}).
    -> {"name", "kind", "collate_c", "mode"} или None. mode "bucket" —
    текстовая колонка, порядок строк сторон разный или неизвестен:
    корзины по md5-префиксу вместо диапазонов. collate_c всегда False
    (COLLATE "C" отключает индекс; поле — для старых листьев).
    """
    def usable(name):
        if name not in src_info or name not in dst_info:
            return None
        if src_info[name][0] != dst_info[name][0]:
            return None
        return type_kind(src_info[name][0])

    def result(name, kind):
        bucket = kind == "text" and not same_order(src_info[name][1],
                                                   dst_info[name][1])
        return {"name": name, "kind": kind, "collate_c": False,
                "mode": "bucket" if bucket else "range"}

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


def build_columns_sql(server_version=0):
    """
    Колонки таблицы: имя, format_type и collation (имя с локалью,
    провайдер, версия). Версия — фактическая библиотеки, где функция есть
    (pg_collation_actual_version — PG10+, pg_database_collation_actual_
    version — PG15+), иначе записанная в каталоге; NULL — неизвестна.
    """
    server_version = int(server_version or 0)
    coll_actual = "pg_collation_actual_version(co.oid)" \
        if server_version >= 100000 else "NULL::text"
    db_actual = "pg_database_collation_actual_version(d.oid)" \
        if server_version >= 150000 else "NULL::text"

    return sql.SQL("""
    SELECT a.attname,
           format_type(a.atttypid, a.atttypmod),
           CASE
             WHEN a.attcollation = 0 THEN NULL
             WHEN co.collname = 'default' THEN
               concat_ws('/', 'default', d.datcollate,
                         to_jsonb(d) ->> 'datlocale',
                         to_jsonb(d) ->> 'daticulocale')
             ELSE concat_ws('/', co.collname, to_jsonb(co) ->> 'collcollate',
                            to_jsonb(co) ->> 'colllocale',
                            to_jsonb(co) ->> 'colliculocale')
           END,
           CASE
             WHEN a.attcollation = 0 THEN NULL
             WHEN co.collname = 'default' THEN
               coalesce(to_jsonb(d) ->> 'datlocprovider', 'c')
             ELSE co.collprovider::text
           END,
           CASE
             WHEN a.attcollation = 0 THEN NULL
             WHEN co.collname = 'default' THEN
               coalesce({db_actual}, to_jsonb(d) ->> 'datcollversion')
             ELSE coalesce({coll_actual}, to_jsonb(co) ->> 'collversion')
           END
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_collation co ON co.oid = a.attcollation
    LEFT JOIN pg_database d ON d.datname = current_database()
    WHERE n.nspname = %s AND c.relname = %s
      AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
""").format(db_actual=sql.SQL(db_actual), coll_actual=sql.SQL(coll_actual))


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
    """{колонка: (format_type, (имя, провайдер, версия) или None)}."""
    cur = conn.cursor()
    cur.execute(build_columns_sql(getattr(conn, "server_version", 0)),
                (schema, table))
    out = {}
    for r in cur.fetchall():
        r = tuple(r) + (None,) * (5 - len(r))
        out[r[0]] = (r[1], (r[2], r[3], r[4]) if r[2] else None)
    return out


def pick_chunk_column(src_conn, dst_conn, schema, table, key_columns,
                      dst=None):
    """
    Колонка нарезки по каталогам обеих сторон и pg_stats источника.
    dst — (схема, таблица) приёмника, если она не одноимённая (targets).
    -> {"name", "kind", "collate_c", "mode"} или None (одна сумма на
    таблицу).
    """
    dst_schema, dst_table = dst or (schema, table)
    src_info = _column_info(src_conn, schema, table)
    dst_info = _column_info(dst_conn, dst_schema, dst_table)
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


# у этих типов бывает ±infinity: арифметика от него бессмысленна
INFINITE_KINDS = ("date", "timestamp", "timestamptz")


def _min_max(conn, schema, table, column, pred):
    """
    min и max источника под предикатом (planagg берёт их по индексу).
    -> (min, max, конечны ли оба): у даты и времени — isfinite, у numeric —
    NaN / ±Infinity.
    """
    col = sql.Identifier("t", column["name"])
    finite = sql.SQL("")
    if column["kind"] in INFINITE_KINDS:
        finite = sql.SQL(", isfinite(min({c})) AND isfinite(max({c}))") \
            .format(c=col)
    cur = conn.cursor()
    cur.execute(sql.SQL(
        "SELECT min({c}), max({c}){f} FROM {tbl} AS {t} WHERE {pred}"
    ).format(c=col, f=finite, tbl=sql.Identifier(schema, table),
             t=sql.Identifier("t"), pred=pred))
    row = cur.fetchone()
    if not row:
        return None, None, True
    lo, hi = row[0], row[1]
    ok = bool(row[2]) if len(row) > 2 and row[2] is not None else True
    if column["kind"] == "numeric" and lo is not None and hi is not None:
        ok = lo.is_finite() and hi.is_finite()
    return lo, hi, ok


def _fractions(parts):
    return [i / float(parts) for i in range(parts)]


def _quantiles(conn, schema, table, column, pred, parts, sample_pct=None):
    """
    percentile_disc(0, 1/parts, ...) по источнику под предикатом в порядке
    колонки (COLLATE "C", если нужно). Первый элемент — минимум. У даты и
    времени — только конечные значения: ±infinity уходят в открытые края.
    """
    order = _col_expr("t", column["name"], column.get("collate_c"))
    if column["kind"] in INFINITE_KINDS:
        pred = sql.SQL("{} AND isfinite({})").format(
            pred, sql.Identifier("t", column["name"]))
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


def _sample_pct(src_conn, schema, table):
    """Процент TABLESAMPLE, чтобы прочитать ~SAMPLE_ROWS строк; None — всё."""
    _size, rows = table_stats(src_conn, schema, table)
    if rows > SAMPLE_ROWS:
        return max(0.0001, min(100.0, 100.0 * SAMPLE_ROWS / rows))
    return None


def _cuts(src_conn, schema, table, column, rng, parts, sample_pct=None,
          top=False):
    pred = range_predicate("t", column["name"],
                           dict(rng, is_null=False),
                           column.get("collate_c"))

    if column["kind"] in ARITHMETIC_KINDS:
        lo, hi, finite = _min_max(src_conn, schema, table, column, pred)
        if finite:
            return arithmetic_cuts(column["kind"], lo, hi, parts)
        # ±infinity / NaN: точки — квантили конечных значений
        if top:
            sample_pct = _sample_pct(src_conn, schema, table)

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
        sample_pct = _sample_pct(src_conn, schema, table)

    cuts = _cuts(src_conn, schema, table, column, make_range(), TOP_CHUNKS,
                 sample_pct, top=True)
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


# ------------------------------------------------------------------
# Режим корзин (порядок строк сторон разный или неизвестен)
# ------------------------------------------------------------------

def _ref(alias, column):
    name = _col_name(column)
    return sql.Identifier(alias, name) if alias else sql.Identifier(name)


def bucket_expr(alias, column, length):
    """substr(md5(col::text), 1, L): от сортировки и версии не зависит."""
    return sql.SQL("substr(md5({}::text), 1, {})").format(
        _ref(alias, column), sql.Literal(int(length)))


def _hex_prefix(value):
    text = str(value or "")
    if not text or len(text) > 32 or \
            any(ch not in "0123456789abcdef" for ch in text):
        raise ValueError("Префикс корзины не hex: %r" % (value,))
    return text


def bucket_predicate(alias, column, prefixes, length):
    """
    substr(md5(col::text), 1, L) IN (SELECT unnest('{...}'::text[])).
    Множество префиксов — одно значение-массив (sql.Literal), а не список
    литералов: планировщик строит хеш-полусоединение (Hash Semi Join или
    hashed SubPlan), а не перебор массива на каждую строку, и текст
    запроса не растёт на десятки тысяч литералов. Префиксы — только hex,
    поэтому литерал массива собирается без экранирования.
    """
    prefixes = [_hex_prefix(p) for p in prefixes]
    if not prefixes:
        raise ValueError("Пустой список корзин")
    return sql.SQL("{} IN (SELECT unnest({}::text[]))").format(
        bucket_expr(alias, column, length),
        sql.Literal("{" + ",".join(prefixes) + "}"))


def build_bucket_sql(schema, table, columns, column, length, parents=None,
                     null_keys=None):
    """
    Один проход: корзина (md5-префикс длины length), count и две суммы
    половин md5 строки по корзинам. parents — несовпавшие корзины прошлого
    уровня (длина length - BUCKET_STEP): читаются только их строки.
    null_keys — ключевые колонки: 4-е поле — строк с NULL в ключе.
    Без временных таблиц: PostgreSQL волен считать параллельно.
    """
    where = sql.SQL("")
    if parents:
        where = sql.SQL(" WHERE {}").format(bucket_predicate(
            "t", column, parents, len(_hex_prefix(parents[0]))))

    nulls = sql.SQL("0")
    if null_keys:
        nulls = sql.SQL("CASE WHEN {} THEN 1 ELSE 0 END").format(
            sql.SQL(" OR ").join(sql.SQL("{} IS NULL").format(
                sql.Identifier("t", k)) for k in null_keys))

    return sql.SQL(
        "SELECT s.b, count(*), "
        "coalesce(sum(('x' || substr(s.h, 1, 16))::bit(64)::bigint), 0), "
        "coalesce(sum(('x' || substr(s.h, 17, 16))::bit(64)::bigint), 0), "
        "coalesce(sum(s.n), 0) "
        "FROM (SELECT {b} AS b, {h} AS h, {n} AS n FROM {tbl} AS {t}{w}) "
        "AS s GROUP BY s.b"
    ).format(b=bucket_expr("t", column, length),
             h=row_hash_sql("t", columns), n=nulls,
             tbl=sql.Identifier(schema, table), t=sql.Identifier("t"),
             w=where)


def bucket_checksums(conn, schema, table, columns, column, length,
                     parents=None, null_keys=None):
    """-> {префикс или None (NULL в колонке): (count, s1, s2, nulls)}."""
    cur = conn.cursor()
    cur.execute(build_bucket_sql(schema, table, columns, column, length,
                                 parents, null_keys))
    return {r[0]: tuple(int(v or 0) for v in r[1:5])
            for r in cur.fetchall()}


def bucket_leaf(column, prefix, src_rows, dst_rows, is_null=False):
    name = _col_name(column)
    return {"column": name, "lo": None if is_null else _hex_prefix(prefix),
            "hi": None, "is_null": bool(is_null),
            "depth": 0 if is_null else len(prefix), "mode": "bucket",
            "src_rows": src_rows, "dst_rows": dst_rows}


def diff_buckets(src, dst, length, column=None):
    """
    Сравнение уровня корзин (карты bucket_checksums сторон).
    -> {"same": {префикс: (src_rows, dst_rows)}, "deeper": [префикс],
        "leaves": [лист]}. Совпавшая корзина закрыта; несовпавшая больше
    LEAF_ROWS — дробится дальше (до BUCKET_MAX), остальные — листья.
    Корзина с NULL в ключе у источника несовпавшая: построчно такие строки
    пары не находят.
    """
    same, deeper, leaves = {}, [], []
    zero = (0, 0, 0, 0)

    for b in sorted(set(src) | set(dst), key=lambda x: (x is None, x or "")):
        s, d = src.get(b, zero), dst.get(b, zero)
        if s[:3] == d[:3] and not s[3]:
            same[b] = (s[0], d[0])
        elif b is None:
            leaves.append(bucket_leaf(column or "", None, s[0], d[0],
                                      is_null=True))
        elif max(s[0], d[0]) > LEAF_ROWS and length < BUCKET_MAX:
            deeper.append(b)
        else:
            leaves.append(bucket_leaf(column or "", b, s[0], d[0]))

    return {"same": same, "deeper": deeper, "leaves": leaves}


# ------------------------------------------------------------------
# Общий предикат листьев (сравнение и загрузка разницы)
# ------------------------------------------------------------------

def leaves_predicate(alias, leaves, column=None):
    """
    (pred1) OR (pred2) ... по листьям обоих видов: диапазоны — range_
    predicate, корзины — substr(md5(col::text), 1, L) IN (...) по длине L,
    NULL-корзина — col IS NULL. Значения — только sql.Literal.
    """
    if not leaves:
        raise ValueError("Пустой список листьев")

    parts, buckets, nulls = [], {}, []
    for leaf in leaves:
        col = leaf.get("column") or column
        if leaf.get("mode") == "bucket":
            if leaf.get("is_null"):
                if col not in nulls:
                    nulls.append(col)
                continue
            prefix = _hex_prefix(leaf.get("lo"))
            buckets.setdefault((col, len(prefix)), []).append(prefix)
            continue
        parts.append(range_predicate(alias, col, leaf,
                                     bool(leaf.get("collate_c"))))

    for (col, length), prefixes in buckets.items():
        parts.append(bucket_predicate(alias, col, prefixes, length))
    for col in nulls:
        parts.append(sql.SQL("{} IS NULL").format(_ref(alias, col)))

    return sql.SQL(" OR ").join(sql.SQL("({})").format(p) for p in parts)


def _is_bucket(leaf):
    return isinstance(leaf, dict) and leaf.get("mode") == "bucket"


def leaf_batches(leaves, size=None):
    """
    Пачки листьев для загрузки. Все корзины — одна пачка: предикат
    корзины индекс не использует, и каждая пачка была бы полным чтением
    таблицы, поэтому корзины читаются одним проходом (множество префиксов
    — хеш-полусоединение). Диапазоны — по size (LEAF_BATCH): их предикат
    идёт по индексу.
    """
    size = int(size or LEAF_BATCH)
    leaves = list(leaves or [])
    buckets = [leaf for leaf in leaves if _is_bucket(leaf)]
    ranges = [leaf for leaf in leaves if not _is_bucket(leaf)]
    out = [buckets] if buckets else []
    return out + [ranges[i:i + size] for i in range(0, len(ranges), size)]
