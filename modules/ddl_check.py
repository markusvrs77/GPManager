# -*- coding: utf-8 -*-
"""
Предпроверка DDL перед gpcopy: сравнение колонок источника и приёмника.

Ловит до запуска три частые причины падений копирования:
- таблицы нет в приёмнике;
- в приёмнике не хватает колонок (extra data after last expected column);
- типы колонок разошлись (value ... is out of range for type integer).

Плюс досоздание недостающих колонок в приёмнике одной кнопкой.
"""

import re

try:
    from modules.gpcopy import open_psycopg2_connection_by_cfg, quote_ident
except ImportError:
    from gpcopy import open_psycopg2_connection_by_cfg, quote_ident

try:
    from modules.connections import get_connection_by_id
except ImportError:
    from connections import get_connection_by_id


# формат format_type(): 'integer', 'character varying(255)', 'numeric(10,2)',
# 'timestamp without time zone', 'text[]' и т.п.
_TYPE_RE = re.compile(r'^[A-Za-z0-9_ (),.\[\]"]+$')

_BATCH = 400


def fetch_columns(conn, tables):
    """
    {(schema, table): [{name, type}]} — колонки в порядке attnum.
    tables: [{schema, table}]
    """
    result = {}
    pairs = [(t["schema"], t["table"]) for t in tables]

    with conn.cursor() as cur:
        for i in range(0, len(pairs), _BATCH):
            chunk = pairs[i:i + _BATCH]
            placeholders = ", ".join(["(%s, %s)"] * len(chunk))
            params = [v for pair in chunk for v in pair]

            cur.execute(
                """
                SELECT n.nspname, c.relname, a.attname,
                       format_type(a.atttypid, a.atttypmod)
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                WHERE a.attnum > 0 AND NOT a.attisdropped
                  AND (n.nspname, c.relname) IN ({})
                ORDER BY n.nspname, c.relname, a.attnum
                """.format(placeholders),
                params,
            )

            for schema, table, column, col_type in cur.fetchall():
                result.setdefault((schema, table), []).append(
                    {"name": column, "type": col_type}
                )

    return result


def compare_ddl(src_cols, dst_cols, tables):
    """
    Чистое сравнение. -> [{schema, table, status, missing_in_dest,
    extra_in_dest, type_diffs}], status: ok | no_dest | no_source | diff.
    """
    out = []

    for t in tables:
        key = (t["schema"], t["table"])
        src = src_cols.get(key)
        dst = dst_cols.get(key)

        row = {
            "schema": t["schema"], "table": t["table"],
            "missing_in_dest": [], "extra_in_dest": [], "type_diffs": [],
        }

        if not src:
            row["status"] = "no_source"
            out.append(row)
            continue

        if not dst:
            row["status"] = "no_dest"
            out.append(row)
            continue

        src_map = {c["name"]: c["type"] for c in src}
        dst_map = {c["name"]: c["type"] for c in dst}

        row["missing_in_dest"] = [
            c for c in src if c["name"] not in dst_map
        ]
        row["extra_in_dest"] = [
            c["name"] for c in dst if c["name"] not in src_map
        ]
        row["type_diffs"] = [
            {"column": c["name"], "src": c["type"], "dst": dst_map[c["name"]]}
            for c in src
            if c["name"] in dst_map and dst_map[c["name"]] != c["type"]
        ]

        row["status"] = "diff" if (
            row["missing_in_dest"] or row["extra_in_dest"] or row["type_diffs"]
        ) else "ok"

        out.append(row)

    return out


def build_add_column_sql(schema, table, columns):
    """
    ALTER TABLE ... ADD COLUMN для недостающих колонок. Чистая функция,
    тип валидируется (формат format_type), имена — через quote_ident.
    """
    statements = []

    for col in columns:
        name = (col.get("name") or "").strip()
        col_type = (col.get("type") or "").strip()

        if not name:
            raise ValueError("Пустое имя колонки")

        if not col_type or not _TYPE_RE.match(col_type):
            raise ValueError("Недопустимый тип колонки: {}".format(col_type))

        statements.append(
            "ALTER TABLE {}.{} ADD COLUMN {} {}".format(
                quote_ident(schema), quote_ident(table),
                quote_ident(name), col_type,
            )
        )

    return statements


def precheck_tables(source_connection_id, dest_connection_id, tables):
    """Полная предпроверка: снять колонки с обеих сторон и сравнить."""
    src_cfg = get_connection_by_id(int(source_connection_id))
    dst_cfg = get_connection_by_id(int(dest_connection_id))

    if not src_cfg or not dst_cfg:
        raise ValueError("Подключение не найдено")

    src_conn = open_psycopg2_connection_by_cfg(src_cfg)

    try:
        src_cols = fetch_columns(src_conn, tables)
    finally:
        try:
            src_conn.close()
        except Exception:
            pass

    dst_conn = open_psycopg2_connection_by_cfg(dst_cfg)

    try:
        dst_cols = fetch_columns(dst_conn, tables)
    finally:
        try:
            dst_conn.close()
        except Exception:
            pass

    results = compare_ddl(src_cols, dst_cols, tables)

    return {
        "results": results,
        "summary": {
            "ok": sum(1 for r in results if r["status"] == "ok"),
            "diff": sum(1 for r in results if r["status"] == "diff"),
            "no_dest": sum(1 for r in results if r["status"] == "no_dest"),
            "no_source": sum(1 for r in results if r["status"] == "no_source"),
        },
    }


def add_missing_columns(dest_connection_id, tables):
    """
    Досоздать колонки в приёмнике. tables: [{schema, table,
    columns: [{name, type}]}]. -> [{schema, table, ok, error, added}]
    """
    dst_cfg = get_connection_by_id(int(dest_connection_id))

    if not dst_cfg:
        raise ValueError("Подключение не найдено")

    conn = open_psycopg2_connection_by_cfg(dst_cfg)
    conn.autocommit = True
    out = []

    try:
        with conn.cursor() as cur:
            for t in tables:
                row = {"schema": t.get("schema"), "table": t.get("table"),
                       "ok": True, "error": "", "added": 0}

                try:
                    statements = build_add_column_sql(
                        t["schema"], t["table"], t.get("columns") or [],
                    )

                    for sql_text in statements:
                        cur.execute(sql_text)
                        row["added"] += 1
                except Exception as e:
                    row["ok"] = False
                    row["error"] = str(e)[:500]

                out.append(row)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return out
