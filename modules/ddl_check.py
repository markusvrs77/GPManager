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


# ------------------------------------------------------------------
# зависимости: функции в DEFAULT'ах и sequences
# ------------------------------------------------------------------

# функции известных расширений: чего не хватает -> какое расширение ставить
EXTENSION_BY_FUNC = {
    "uuid_generate_v1": "uuid-ossp",
    "uuid_generate_v1mc": "uuid-ossp",
    "uuid_generate_v3": "uuid-ossp",
    "uuid_generate_v4": "uuid-ossp",
    "uuid_generate_v5": "uuid-ossp",
    "gen_random_uuid": "pgcrypto",
    "digest": "pgcrypto",
    "hmac": "pgcrypto",
    "crypt": "pgcrypto",
    "gen_salt": "pgcrypto",
}

# встроенные — не считаем зависимостями
_BUILTIN_FUNCS = {
    "nextval", "currval", "setval", "now", "current_timestamp",
    "current_date", "current_time", "localtimestamp", "clock_timestamp",
    "timezone", "coalesce", "nullif", "greatest", "least", "random",
    "md5", "length", "upper", "lower", "substr", "substring", "trim",
    "to_char", "to_date", "to_timestamp", "to_number", "date_trunc",
    "extract", "abs", "round", "floor", "ceil", "ceiling", "concat",
    "replace", "btrim", "char_length", "position", "left", "right",
}

_FUNC_CALL_RE = re.compile(
    r"(?:(?P<schema>[A-Za-z_][\w$]*)\.)?(?P<name>[A-Za-z_][\w$]*)\s*\(")
_SEQ_RE = re.compile(r"nextval\('(?P<seq>[^':]+)'")
_IDENT_PART_RE = re.compile(r"^[A-Za-z0-9_$.\"]+$")


def fetch_defaults(conn, tables):
    """{(schema, table): [выражения DEFAULT]} из pg_attrdef источника."""
    result = {}
    pairs = [(t["schema"], t["table"]) for t in tables]

    with conn.cursor() as cur:
        for i in range(0, len(pairs), _BATCH):
            chunk = pairs[i:i + _BATCH]
            placeholders = ", ".join(["(%s, %s)"] * len(chunk))
            params = [v for pair in chunk for v in pair]

            cur.execute(
                """
                SELECT n.nspname, c.relname,
                       pg_get_expr(d.adbin, d.adrelid)
                FROM pg_attrdef d
                JOIN pg_class c ON c.oid = d.adrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE (n.nspname, c.relname) IN ({})
                """.format(placeholders),
                params,
            )

            for schema, table, expr in cur.fetchall():
                if expr:
                    result.setdefault((schema, table), []).append(expr)

    return result


def collect_dependencies(defaults_map):
    """
    Чистая: из DEFAULT-выражений -> {"functions": {(schema|None, name)},
    "sequences": {"schema.seq"|"seq"}}. Встроенные функции отброшены.
    """
    functions = set()
    sequences = set()

    for exprs in (defaults_map or {}).values():
        for expr in exprs:
            for m in _SEQ_RE.finditer(expr or ""):
                sequences.add(m.group("seq").replace('"', ""))

            for m in _FUNC_CALL_RE.finditer(expr or ""):
                name = m.group("name").lower()

                if name in _BUILTIN_FUNCS:
                    continue

                functions.add((m.group("schema"), name))

    return {"functions": functions, "sequences": sequences}


def find_missing_dependencies(dst_conn, deps):
    """Каких функций/sequences нет в приёмнике."""
    missing_funcs = []
    missing_seqs = []

    with dst_conn.cursor() as cur:
        for schema, name in sorted(deps.get("functions") or set(),
                                   key=lambda p: (p[0] or "", p[1])):
            if schema:
                cur.execute(
                    """
                    SELECT 1 FROM pg_proc p
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE p.proname = %s AND n.nspname = %s LIMIT 1
                    """, (name, schema))
            else:
                cur.execute(
                    "SELECT 1 FROM pg_proc WHERE proname = %s LIMIT 1",
                    (name,))

            if not cur.fetchone():
                missing_funcs.append({"schema": schema, "name": name})

        for seq in sorted(deps.get("sequences") or set()):
            cur.execute("SELECT to_regclass(%s)", (seq,))

            if cur.fetchone()[0] is None:
                missing_seqs.append(seq)

    return missing_funcs, missing_seqs


def fetch_function_defs(src_conn, schema, name):
    """Определения функции (все перегрузки) из источника."""
    with src_conn.cursor() as cur:
        if schema:
            cur.execute(
                """
                SELECT pg_get_functiondef(p.oid) FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE p.proname = %s AND n.nspname = %s
                """, (name, schema))
        else:
            cur.execute(
                """
                SELECT pg_get_functiondef(p.oid) FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE p.proname = %s AND n.nspname NOT IN
                      ('pg_catalog', 'information_schema')
                """, (name,))

        return [r[0] for r in cur.fetchall() if r and r[0]]


def build_fix_plan(missing_funcs, missing_seqs, func_defs):
    """
    Чистая: план досоздания. func_defs: {(schema, name): [definitions]}.
    -> [{kind, name, sql}] (extension'ы дедуплицированы).
    """
    plan = []
    seen_ext = set()

    for f in missing_funcs:
        ext = EXTENSION_BY_FUNC.get(f["name"])

        if ext:
            if ext not in seen_ext:
                seen_ext.add(ext)
                plan.append({
                    "kind": "extension", "name": ext,
                    "sql": 'CREATE EXTENSION IF NOT EXISTS "{}"'.format(ext),
                })
            continue

        for definition in func_defs.get((f.get("schema"), f["name"]), []):
            plan.append({
                "kind": "function",
                "name": (f.get("schema") + "." if f.get("schema") else "") +
                        f["name"],
                "sql": definition,
            })

    for seq in missing_seqs:
        if not _IDENT_PART_RE.match(seq):
            continue

        parts = seq.split(".")
        quoted = ".".join(quote_ident(p) for p in parts)
        plan.append({
            "kind": "sequence", "name": seq,
            "sql": "CREATE SEQUENCE IF NOT EXISTS {}".format(quoted),
        })

    return plan


def analyze_dependencies(src_conn, dst_conn, tables):
    """Отсутствующие в приёмнике зависимости + план их досоздания."""
    defaults = fetch_defaults(src_conn, tables)
    deps = collect_dependencies(defaults)
    missing_funcs, missing_seqs = find_missing_dependencies(dst_conn, deps)

    func_defs = {}

    for f in missing_funcs:
        if f["name"] in EXTENSION_BY_FUNC:
            continue

        try:
            func_defs[(f.get("schema"), f["name"])] = fetch_function_defs(
                src_conn, f.get("schema"), f["name"])
        except Exception:
            func_defs[(f.get("schema"), f["name"])] = []

    return build_fix_plan(missing_funcs, missing_seqs, func_defs)


def apply_dependency_fixes(source_connection_id, dest_connection_id, tables):
    """Пересобрать план зависимостей и применить его в приёмнике."""
    src_cfg = get_connection_by_id(int(source_connection_id))
    dst_cfg = get_connection_by_id(int(dest_connection_id))

    if not src_cfg or not dst_cfg:
        raise ValueError("Подключение не найдено")

    src_conn = open_psycopg2_connection_by_cfg(src_cfg)
    dst_conn = open_psycopg2_connection_by_cfg(dst_cfg)
    dst_conn.autocommit = True
    results = []

    try:
        plan = analyze_dependencies(src_conn, dst_conn, tables)

        with dst_conn.cursor() as cur:
            for step in plan:
                row = dict(step)
                row.pop("sql", None)

                try:
                    cur.execute(step["sql"])
                    row["ok"] = True
                    row["error"] = ""
                except Exception as e:
                    row["ok"] = False
                    row["error"] = str(e)[:400]

                results.append(row)
    finally:
        for c in (src_conn, dst_conn):
            try:
                c.close()
            except Exception:
                pass

    return results


def precheck_tables(source_connection_id, dest_connection_id, tables):
    """
    Полная предпроверка: сравнение колонок + отсутствующие в приёмнике
    зависимости (функции из DEFAULT'ов, sequences).
    """
    src_cfg = get_connection_by_id(int(source_connection_id))
    dst_cfg = get_connection_by_id(int(dest_connection_id))

    if not src_cfg or not dst_cfg:
        raise ValueError("Подключение не найдено")

    src_conn = open_psycopg2_connection_by_cfg(src_cfg)
    dst_conn = open_psycopg2_connection_by_cfg(dst_cfg)

    try:
        src_cols = fetch_columns(src_conn, tables)
        dst_cols = fetch_columns(dst_conn, tables)

        try:
            deps = analyze_dependencies(src_conn, dst_conn, tables)
        except Exception:
            deps = []
    finally:
        for c in (src_conn, dst_conn):
            try:
                c.close()
            except Exception:
                pass

    results = compare_ddl(src_cols, dst_cols, tables)

    return {
        "results": results,
        "deps": [
            {"kind": d["kind"], "name": d["name"]} for d in deps
        ],
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
