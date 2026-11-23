"""
Сравнение колонок таблиц источника и приёмника для опции gpcopy
«Только общие колонки».

gpcopy не умеет список колонок: если у цели колонок больше или меньше,
чем у источника, он падает («missing data for column ...»). С опцией
такие таблицы идут через промежуточные таблицы (modules/gpcopy_stage):
gpcopy льёт в таблицу со структурой источника, а в цель переливаются
только общие колонки — лишние колонки цели получают DEFAULT/NULL,
колонки, которых в цели нет, пропускаются.

Источник только читается (каталог), приёмник — тоже.
"""

_COLUMNS_SQL = """
    SELECT n.nspname, c.relname, a.attname, a.attnotnull, a.atthasdef
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE (n.nspname, c.relname) IN %s
      AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY n.nspname, c.relname, a.attnum
"""


def fetch_columns(conn, pairs, chunk=500):
    """
    Колонки таблиц одним запросом на пачку: {(schema, table): [{"name",
    "not_null", "has_default"}]} в порядке attnum. Таблиц, которых нет,
    в ответе нет.
    """
    pairs = list(dict.fromkeys(tuple(p) for p in pairs or []))
    out = {}

    for i in range(0, len(pairs), chunk):
        with conn.cursor() as cur:
            cur.execute(_COLUMNS_SQL, (tuple(pairs[i:i + chunk]),))
            rows = cur.fetchall()

        for schema, table, name, not_null, has_default in rows:
            out.setdefault((schema, table), []).append({
                "name": name,
                "not_null": bool(not_null),
                "has_default": bool(has_default),
            })

    return out


def _names(columns):
    return [c["name"] if isinstance(c, dict) else c for c in columns or []]


def column_diff(src_columns, dst_columns):
    """
    Чистая функция. Колонки — имена или словари fetch_columns.
    -> {"common": [в порядке источника], "src_only", "dst_only",
        "blocking": [колонки только в цели, NOT NULL без DEFAULT]}
    """
    src = _names(src_columns)
    dst = _names(dst_columns)
    src_set, dst_set = set(src), set(dst)

    blocking = [
        c["name"] for c in dst_columns or []
        if isinstance(c, dict) and c["name"] not in src_set
        and c.get("not_null") and not c.get("has_default")
    ]

    return {
        "common": [c for c in src if c in dst_set],
        "src_only": [c for c in src if c not in dst_set],
        "dst_only": [c for c in dst if c not in src_set],
        "blocking": blocking,
    }


def differing_tables(src_conn, dst_conn, pairs):
    """
    pairs — [(src_schema, src_table, dst_schema, dst_table)]. Какие пары
    различаются набором имён колонок: [{"source": (s, t), "dest": (s, t),
    "common", "src_only", "dst_only", "blocking"}]. Пара, у которой нет
    таблицы на одной из сторон, не сравнивается (её судьбу решает gpcopy).
    """
    pairs = list(dict.fromkeys(tuple(p) for p in pairs or []))
    src_cols = fetch_columns(src_conn, [(p[0], p[1]) for p in pairs])
    dst_cols = fetch_columns(dst_conn, [(p[2], p[3]) for p in pairs])
    out = []

    for ss, st, ds, dt in pairs:
        src = src_cols.get((ss, st))
        dst = dst_cols.get((ds, dt))

        if not src or not dst:
            continue

        diff = column_diff(src, dst)

        if diff["src_only"] or diff["dst_only"]:
            out.append(dict(diff, source=(ss, st), dest=(ds, dt)))

    return out


def refuse_reason(diff):
    """
    Почему таблицу нельзя грузить только общими колонками (None — можно).
    Чистая функция.
    """
    dest = "{}.{}".format(*diff["dest"])

    if diff.get("blocking"):
        return (
            "В {} есть колонки, которых нет в источнике, — NOT NULL без "
            "DEFAULT: {}. Загрузка только общих колонок дала бы в них NULL; "
            "таблица не копировалась".format(
                dest, ", ".join(diff["blocking"][:10])))

    if not diff.get("common"):
        return ("У {}.{} и {} нет общих колонок — таблица не "
                "копировалась".format(diff["source"][0], diff["source"][1],
                                      dest))

    return None


def describe(diff):
    """Короткая строка для лога: что отличается. Чистая функция."""
    parts = []

    if diff.get("src_only"):
        parts.append("нет в цели: " + ", ".join(diff["src_only"][:10]))

    if diff.get("dst_only"):
        parts.append("только в цели: " + ", ".join(diff["dst_only"][:10]))

    return "{}.{} -> {}.{}: {}".format(
        diff["source"][0], diff["source"][1], diff["dest"][0],
        diff["dest"][1], "; ".join(parts))
