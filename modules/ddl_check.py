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

try:
    from modules.sync_targets import is_mapped, target_of
except ImportError:
    from sync_targets import is_mapped, target_of


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


# латинские двойники кириллицы: имена ТСП и TCП выглядят одинаково,
# но это разные колонки — при сверке считаем их одной и той же
_HOMOGLYPHS = {
    "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н", "K": "К", "M": "М",
    "O": "О", "P": "Р", "T": "Т", "X": "Х", "a": "а", "c": "с", "e": "е",
    "o": "о", "p": "р", "x": "х", "y": "у",
}


def normalize_column_name(name):
    """
    Имя колонки без кавычек, регистра, лишних пробелов и латинских
    двойников — чтобы поймать «ТСП» против «TCП» и «"ГРУППИРОВКА"»
    против «ГРУППИРОВКА». Чистая функция.
    """
    text = (name or "").strip().strip('"').strip()
    text = re.sub(r"\s+", " ", text)
    text = "".join(_HOMOGLYPHS.get(ch, ch) for ch in text)

    return text.casefold()


def match_renames(missing, extra):
    """
    Пары «колонка в приёмнике -> как она называется в источнике»: одно и
    то же поле, записанное иначе. Такое чинится переименованием, а не
    добавлением колонки. Чистая функция.

    missing: [{name, type}] из источника, extra: [имя] из приёмника
    -> ([{from, to, type}], оставшиеся missing, оставшиеся extra)
    """
    by_norm = {}

    for name in extra:
        by_norm.setdefault(normalize_column_name(name), []).append(name)

    renames = []
    left_missing = []
    used = set()

    for col in missing:
        key = normalize_column_name(col["name"])
        candidates = [n for n in by_norm.get(key, []) if n not in used]

        if candidates and candidates[0] != col["name"]:
            used.add(candidates[0])
            renames.append({"from": candidates[0], "to": col["name"],
                            "type": col.get("type")})
        else:
            left_missing.append(col)

    left_extra = [n for n in extra if n not in used]

    return renames, left_missing, left_extra


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

        # одно и то же поле, записанное иначе -> переименование
        renames, row["missing_in_dest"], row["extra_in_dest"] = match_renames(
            row["missing_in_dest"], row["extra_in_dest"])
        row["renames"] = renames

        row["status"] = "diff" if (
            row["missing_in_dest"] or row["extra_in_dest"]
            or row["type_diffs"] or row["renames"]
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
# создание недостающих объектов в приёмнике
# ------------------------------------------------------------------

def _column_def(col):
    """Кусок «имя тип [DEFAULT ...] [NOT NULL]» для CREATE TABLE."""
    name = (col.get("name") or "").strip()
    col_type = (col.get("type") or "").strip()

    if not name:
        raise ValueError("Пустое имя колонки")

    if not col_type or not _TYPE_RE.match(col_type):
        raise ValueError("Недопустимый тип колонки: {}".format(col_type))

    part = "{} {}".format(quote_ident(name), col_type)

    if col.get("default"):
        part += " DEFAULT {}".format(col["default"])

    if col.get("not_null"):
        part += " NOT NULL"

    return part


def _access_method(amname):
    """
    Способ хранения для USING, или None для обычной heap-таблицы.

    В Greenplum 7 append-optimized — это метод доступа (pg_class.relam), а
    в reloptions остаются только compresstype и прочие его параметры. Без
    USING таблица создавалась как heap, и heap отвергал compresstype:
    unrecognized parameter "compresstype". В GP6 relam у таблиц нулевой,
    и признак append-optimized лежит в самих reloptions — там USING не
    нужен.
    """
    name = (amname or "").strip()

    return None if name in ("", "heap") else name


def build_create_table_sql(schema, table, columns, options=None,
                           partition_by=None, distributed_by=None,
                           access_method=None):
    """
    CREATE TABLE по описанию из каталога источника. Чистая функция.

    Порядок частей — как в грамматике Greenplum 7:
    (колонки) PARTITION BY ... USING ... WITH (...) DISTRIBUTED BY (...).
    """
    if not columns:
        raise ValueError("Нет колонок для {}.{}".format(schema, table))

    sql = "CREATE TABLE IF NOT EXISTS {}.{} (\n    {}\n)".format(
        quote_ident(schema), quote_ident(table),
        ",\n    ".join(_column_def(c) for c in columns),
    )

    if partition_by:
        sql += "\n" + partition_by

    if _access_method(access_method):
        sql += "\nUSING {}".format(quote_ident(_access_method(access_method)))

    if options:
        sql += "\nWITH ({})".format(", ".join(options))

    if distributed_by:
        sql += "\n" + distributed_by

    return sql


def build_create_partition_sql(schema, table, parent_schema, parent_table,
                               bound, options=None, access_method=None):
    """CREATE TABLE ... PARTITION OF ... FOR VALUES ... Чистая функция.

    Способ хранения указывается у каждой партиции свой: Greenplum
    допускает, что старые партиции лежат в ao_column, а свежие — в heap.
    """
    if not bound:
        raise ValueError("Нет границ партиции {}.{}".format(schema, table))

    sql = "CREATE TABLE IF NOT EXISTS {}.{} PARTITION OF {}.{}\n{}".format(
        quote_ident(schema), quote_ident(table),
        quote_ident(parent_schema), quote_ident(parent_table), bound,
    )

    if _access_method(access_method):
        sql += "\nUSING {}".format(quote_ident(_access_method(access_method)))

    if options:
        sql += "\nWITH ({})".format(", ".join(options))

    return sql


def build_create_view_sql(schema, table, definition, materialized=False):
    """CREATE VIEW / MATERIALIZED VIEW по определению источника."""
    body = (definition or "").strip().rstrip(";")

    if not body:
        raise ValueError("Пустое определение вьюхи {}.{}".format(schema, table))

    return "CREATE {}VIEW {}.{} AS\n{}".format(
        "MATERIALIZED " if materialized else "",
        quote_ident(schema), quote_ident(table), body,
    )


def _partition_clause(partkeydef):
    """
    pg_get_partkeydef() отдаёт «RANGE (d)» без префикса — без «PARTITION BY»
    такой DDL не выполнится. Чистая функция.
    """
    text = (partkeydef or "").strip()

    if not text:
        return None

    if text.upper().startswith("PARTITION BY"):
        return text

    return "PARTITION BY " + text


def _table_meta(cur, schema, table):
    """relkind / reloptions / определение вьюхи / partition key / политика."""
    cur.execute(
        """
        SELECT c.oid, c.relkind, c.reloptions, am.amname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_am am ON am.oid = c.relam
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, table),
    )
    row = cur.fetchone()

    if not row:
        return None

    oid, relkind, reloptions = row[0], row[1], row[2]
    meta = {"oid": oid, "relkind": relkind,
            "options": list(reloptions or []), "partition_by": None,
            "distributed_by": None, "definition": None,
            "access_method": _access_method(row[3])}

    if relkind in ("v", "m"):
        cur.execute("SELECT pg_get_viewdef(%s, true)", (oid,))
        meta["definition"] = cur.fetchone()[0]
        return meta

    if relkind == "p":
        try:
            cur.execute("SELECT pg_get_partkeydef(%s)", (oid,))
            meta["partition_by"] = _partition_clause(cur.fetchone()[0])
        except Exception:
            meta["partition_by"] = None

    # у обычного Postgres такой функции нет — это нормально
    try:
        cur.execute("SELECT pg_get_table_distributedby(%s)", (oid,))
        meta["distributed_by"] = (cur.fetchone()[0] or "").strip() or None
    except Exception:
        meta["distributed_by"] = None

    return meta


def _table_columns(cur, oid):
    cur.execute(
        """
        SELECT a.attname,
               format_type(a.atttypid, a.atttypmod),
               a.attnotnull,
               pg_get_expr(d.adbin, d.adrelid)
        FROM pg_attribute a
        LEFT JOIN pg_attrdef d
               ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (oid,),
    )

    return [{"name": r[0], "type": r[1], "not_null": r[2], "default": r[3]}
            for r in cur.fetchall()]


def _partition_children(cur, oid):
    """Дочерние партиции с границами — чтобы дерево доехало целиком."""
    cur.execute(
        """
        SELECT n.nspname, c.relname,
               pg_get_expr(c.relpartbound, c.oid),
               c.reloptions, am.amname, c.relkind
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_am am ON am.oid = c.relam
        WHERE i.inhparent = %s
        ORDER BY n.nspname, c.relname
        """,
        (oid,),
    )

    return [{"schema": r[0], "table": r[1], "bound": r[2],
             "options": list(r[3] or []),
             "access_method": _access_method(r[4]),
             "relkind": r[5]} for r in cur.fetchall()]


def _partition_parent(cur, oid):
    """
    Родитель и границы, если таблица — партиция; иначе None.

    Партицию нельзя создавать отдельной таблицей с её именем: gpcopy лил
    бы потом в самостоятельную таблицу, никак не связанную с родителем.
    Наследник без границ — обычное наследование Postgres, не партиция.
    """
    # relpartbound есть только с Postgres 10 и в Greenplum 7. Без проверки
    # на GP6 и старом Postgres падал бы запрос для любой таблицы, а
    # декларативных партиций там всё равно нет
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_attribute
            WHERE attrelid = 'pg_catalog.pg_class'::regclass
              AND attname = 'relpartbound' AND NOT attisdropped
        )
        """
    )

    if not cur.fetchone()[0]:
        return None

    cur.execute(
        """
        SELECT pn.nspname, pc.relname, pg_get_expr(c.relpartbound, c.oid)
        FROM pg_inherits i
        JOIN pg_class pc ON pc.oid = i.inhparent
        JOIN pg_namespace pn ON pn.oid = pc.relnamespace
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhrelid = %s
        """,
        (oid,),
    )
    row = cur.fetchone()

    if not row or not row[2]:
        return None

    return {"schema": row[0], "table": row[1], "bound": row[2]}


def fetch_object_ddl(src_conn, schema, table, with_partitions=True):
    """
    DDL объекта из каталогов источника: сама таблица (или вьюха) и, если
    она секционированная, все её партиции. -> {"kind", "statements": [...]}
    """
    with src_conn.cursor() as cur:
        meta = _table_meta(cur, schema, table)

        if not meta:
            return None

        if meta["relkind"] in ("v", "m"):
            return {
                "kind": "view" if meta["relkind"] == "v" else "matview",
                "statements": [build_create_view_sql(
                    schema, table, meta["definition"],
                    materialized=meta["relkind"] == "m")],
            }

        parent = _partition_parent(cur, meta["oid"])

        if parent:
            return {
                "kind": "partition",
                "parent": (parent["schema"], parent["table"]),
                "statements": [build_create_partition_sql(
                    schema, table, parent["schema"], parent["table"],
                    parent["bound"], options=meta["options"],
                    access_method=meta["access_method"],
                )],
            }

        columns = _table_columns(cur, meta["oid"])
        statements = [build_create_table_sql(
            schema, table, columns,
            options=meta["options"],
            partition_by=meta["partition_by"],
            distributed_by=meta["distributed_by"],
            access_method=meta["access_method"],
        )]

        kind = "partitioned" if meta["relkind"] == "p" else "table"

        if kind == "partitioned" and with_partitions:
            for child in _partition_children(cur, meta["oid"]):
                statements.append(build_create_partition_sql(
                    child["schema"], child["table"], schema, table,
                    child["bound"], options=child["options"],
                    access_method=child["access_method"],
                ))

    return {"kind": kind, "statements": statements}


# ------------------------------------------------------------------
# цель под другим именем (карта targets, modules/sync_targets.py)
# ------------------------------------------------------------------

def target_leaf_name(src_root, src_leaf, dst_root):
    """
    Имя партиции цели — такое, какое ждёт gpcopy.

    gpcopy, копируя секционированную таблицу под другим именем, льёт
    каждую партицию источника в партицию цели, у которой префикс корня
    заменён: dm_stock_lot_prt_20260614 -> dm_stock_lot_new_prt_20260614
    (так в его логе). Партиция с другим именем для него не существует.
    """
    if src_leaf.startswith(src_root):
        name = dst_root + src_leaf[len(src_root):]
    else:
        name = "{}_{}".format(dst_root, src_leaf)

    if len(name) > 63:
        raise ValueError(
            "Имя партиции цели {} длиннее 63 символов — Postgres его "
            "обрежет, и gpcopy не найдёт партицию. Выберите имя цели "
            "короче".format(name))

    return name


def fetch_target_ddl(src_conn, schema, table, dst_schema, dst_table):
    """
    CREATE TABLE для цели с другим именем — по структуре источника:
    колонки, типы, NOT NULL, способ хранения и распределение
    (DISTRIBUTED BY / RANDOMLY).

    Секционированный источник даёт секционированную цель с тем же ключом
    и теми же границами; партиции названы по правилу gpcopy
    (target_leaf_name). Без партиций gpcopy падал на каждой из них:
    «relation ..._new_prt_... does not exist».

    DEFAULT'ы не переносятся: последовательности в них принадлежат таблице
    источника. Источник только читается.
    -> {"kind": "table" | "partitioned", "statements": [...]} или None,
    если в источнике нет.
    """
    with src_conn.cursor() as cur:
        meta = _table_meta(cur, schema, table)

        if not meta:
            return None

        columns = [dict(c, default=None)
                   for c in _table_columns(cur, meta["oid"])]

        partitioned = meta["relkind"] == "p" and meta["partition_by"]
        children = _partition_children(cur, meta["oid"]) if partitioned else []

    statements = [build_create_table_sql(
        dst_schema, dst_table, columns,
        options=meta["options"] if meta["relkind"] not in ("v", "m") else None,
        partition_by=meta["partition_by"] if partitioned else None,
        distributed_by=meta["distributed_by"],
        access_method=meta["access_method"],
    )]

    for child in children:
        statements.append(build_create_partition_sql(
            dst_schema, target_leaf_name(table, child["table"], dst_table),
            dst_schema, dst_table, child["bound"],
            options=child["options"], access_method=child["access_method"],
        ))

    return {"kind": "partitioned" if partitioned else "table",
            "statements": statements}


def relation_exists(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def _ensure_schema(cur, schema):
    """
    Схема в приёмнике. Сначала проверка: CREATE SCHEMA IF NOT EXISTS
    требует права CREATE на базу даже тогда, когда схема уже есть.
    """
    cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,))

    if cur.fetchone() is None:
        cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(quote_ident(schema)))


def create_target_table(src_conn, dst_conn, schema, table,
                        dst_schema, dst_table):
    """
    Создаёт цель dst_schema.dst_table по структуре schema.table источника.
    DDL выполняется только на dst_conn; commit — на вызывающем, если
    соединение не в autocommit. -> сколько операторов DDL таблицы
    выполнено (создание схемы не считается — как в create_missing_objects).
    """
    ddl = fetch_target_ddl(src_conn, schema, table, dst_schema, dst_table)

    if not ddl:
        raise ValueError("{}.{} нет в источнике".format(schema, table))

    done = 0

    with dst_conn.cursor() as cur:
        _ensure_schema(cur, dst_schema)

        for sql_text in ddl["statements"]:
            cur.execute(sql_text)
            done += 1

    return done


def _relkind(conn, schema, table):
    """relkind таблицы ('r', 'p', ...) или None, если её нет."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            """,
            (schema, table),
        )
        row = cur.fetchone()

    return row[0] if row else None


def match_target_leaves(src_conn, dst_conn, schema, table,
                        dst_schema, dst_table):
    """
    Партиции источника и цели, сопоставленные по границам, а не по имени.

    Имя партиции цели может быть любым: после ALTER TABLE ... RENAME
    партиции переименованной таблицы сохраняют старые имена
    (dm_stock_lot_new -> dm_stock_lot_prt_20230101). Граница же у
    партиции одна, и одинаковые границы — одна и та же партиция.

    -> (pairs, missing): pairs — [(src_schema, src_leaf, dst_schema,
    dst_leaf)], missing — партиции источника, которым в цели нет пары.
    (None, []) — источник не секционирован. ValueError — цель не
    секционирована или партиции многоуровневые.
    """
    with src_conn.cursor() as cur:
        meta = _table_meta(cur, schema, table)

        if not meta or meta["relkind"] != "p":
            return None, []

        src_children = _partition_children(cur, meta["oid"])

    if any(c.get("relkind") == "p" for c in src_children):
        raise ValueError(
            "{}.{}: многоуровневые партиции при загрузке в другую таблицу "
            "не поддерживаются".format(schema, table))

    with dst_conn.cursor() as cur:
        dst_meta = _table_meta(cur, dst_schema, dst_table)

        if not dst_meta or dst_meta["relkind"] != "p":
            raise ValueError(
                "{}.{} в источнике секционирована, а цель {}.{} — нет: "
                "gpcopy льёт по партициям и не найдёт их. Удалите цель или "
                "пересоздайте её («Подготовить приёмник» → «Пересоздать») — "
                "она создастся с партициями".format(
                    schema, table, dst_schema, dst_table))

        dst_by_bound = {}
        for child in _partition_children(cur, dst_meta["oid"]):
            dst_by_bound.setdefault((child["bound"] or "").strip(), child)

    pairs, missing = [], []

    for child in src_children:
        hit = dst_by_bound.get((child["bound"] or "").strip())

        if hit:
            pairs.append((child["schema"], child["table"],
                          hit["schema"], hit["table"]))
        else:
            missing.append(child)

    return pairs, missing


def ensure_target_table(src_conn, dst_conn, schema, table,
                        dst_schema, dst_table):
    """
    Цель есть — False; не было и создана — True; не вышло — исключение.

    У секционированного источника цель тоже должна быть секционированной.
    Партиции сопоставляются по границам (match_target_leaves): в цели
    досоздаются только те, чьих границ там нет, — партиция с другим
    именем, но теми же границами, уже та самая. Обычная таблица на месте
    секционированной цели — ошибка до запуска gpcopy.
    """
    if _relkind(dst_conn, dst_schema, dst_table) is None:
        create_target_table(src_conn, dst_conn, schema, table,
                            dst_schema, dst_table)
        return True

    pairs, missing = match_target_leaves(src_conn, dst_conn, schema, table,
                                         dst_schema, dst_table)

    for child in missing:
        name = target_leaf_name(table, child["table"], dst_table)

        # IF NOT EXISTS молча пропустил бы занятое имя, и партиции с
        # нужными границами так и не появилось бы
        if _relkind(dst_conn, dst_schema, name) is not None:
            raise ValueError(
                "В цели {}.{} нет партиции с границами {} источника {}, а "
                "имя {} уже занято".format(dst_schema, dst_table,
                                           child["bound"], child["table"],
                                           name))

        with dst_conn.cursor() as cur:
            cur.execute(build_create_partition_sql(
                dst_schema, name, dst_schema, dst_table, child["bound"],
                options=child["options"],
                access_method=child["access_method"],
            ))

    return False


def ensure_mapped_targets(source_connection_id, dest_connection_id,
                          targets, pairs):
    """
    Перед загрузкой: для каждой таблицы из pairs, у которой есть карта,
    цель в приёмнике должна существовать — нет, создаём по источнику.
    Таблицы без карты не трогаем.

    -> {(schema, table): текст ошибки} — по тем, чью цель не удалось
    проверить или создать. Пустой словарь — всё на месте.
    """
    mapped = []

    for schema, table in pairs or []:
        if is_mapped(targets, schema, table) and (schema, table) not in mapped:
            mapped.append((schema, table))

    if not mapped:
        return {}

    errors = {}
    src_conn = dst_conn = None

    try:
        src_cfg = get_connection_by_id(int(source_connection_id))
        dst_cfg = get_connection_by_id(int(dest_connection_id))

        if not src_cfg or not dst_cfg:
            raise ValueError("Подключение не найдено")

        src_conn = open_psycopg2_connection_by_cfg(src_cfg)

        try:
            src_conn.set_session(readonly=True)
        except Exception:
            pass

        dst_conn = open_psycopg2_connection_by_cfg(dst_cfg)
        dst_conn.autocommit = True
    except Exception as e:
        for pair in mapped:
            errors[pair] = "Цель {}.{}: не удалось проверить — {}".format(
                *(target_of(targets, *pair) + (str(e)[:300],)))

        for c in (src_conn, dst_conn):
            try:
                if c is not None:
                    c.close()
            except Exception:
                pass

        return errors

    try:
        for schema, table in mapped:
            dst_schema, dst_table = target_of(targets, schema, table)

            try:
                if ensure_target_table(src_conn, dst_conn, schema, table,
                                       dst_schema, dst_table):
                    print("[targets] создана цель {}.{} по {}.{}".format(
                        dst_schema, dst_table, schema, table))
            except Exception as e:
                errors[(schema, table)] = (
                    "Цель {}.{}: подготовить по {}.{} не удалось: "
                    "{}".format(dst_schema, dst_table, schema, table,
                                str(e)[:400]))
    finally:
        for c in (src_conn, dst_conn):
            try:
                c.close()
            except Exception:
                pass

    return errors


def mapped_partition_leaves(source_connection_id, dest_connection_id,
                            targets, pairs):
    """
    Для полной замены: партиции секционированных таблиц с картой.

    gpcopy, получив корень под другим именем, сам выводит имена партиций
    цели (замена префикса) и не находит их, если партиции названы иначе.
    Поэтому такие таблицы уходят в gpcopy попартиционно: каждая партиция
    источника — в партицию цели с теми же границами.

    -> (leaves, errors): leaves — {(schema, table): [(src_schema, src_leaf,
    dst_schema, dst_leaf)]} только для секционированных; errors —
    {(schema, table): текст}. Только чтение каталогов обеих сторон.
    """
    mapped = [p for p in dict.fromkeys(pairs or [])
              if is_mapped(targets, *p)]

    if not mapped:
        return {}, {}

    leaves, errors = {}, {}
    src_conn = dst_conn = None

    try:
        src_cfg = get_connection_by_id(int(source_connection_id))
        dst_cfg = get_connection_by_id(int(dest_connection_id))

        if not src_cfg or not dst_cfg:
            raise ValueError("Подключение не найдено")

        src_conn = open_psycopg2_connection_by_cfg(src_cfg)
        dst_conn = open_psycopg2_connection_by_cfg(dst_cfg)

        for conn in (src_conn, dst_conn):
            try:
                conn.set_session(readonly=True, autocommit=True)
            except Exception:
                pass

        for schema, table in mapped:
            dst_schema, dst_table = target_of(targets, schema, table)

            try:
                found, missing = match_target_leaves(
                    src_conn, dst_conn, schema, table, dst_schema, dst_table)
            except Exception as e:
                errors[(schema, table)] = "Цель {}.{}: {}".format(
                    dst_schema, dst_table, str(e)[:400])
                continue

            if found is None:
                continue

            if missing:
                errors[(schema, table)] = (
                    "Цель {}.{}: нет партиций с границами {}".format(
                        dst_schema, dst_table,
                        ", ".join(c["bound"] for c in missing[:3])
                        + (" и ещё {}".format(len(missing) - 3)
                           if len(missing) > 3 else "")))
                continue

            leaves[(schema, table)] = found
    except Exception as e:
        for pair in mapped:
            errors.setdefault(pair, "Партиции цели проверить не удалось: "
                                    "{}".format(str(e)[:300]))
    finally:
        for conn in (src_conn, dst_conn):
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    return leaves, errors


def truncate_targets(dest_connection_id, names):
    """
    TRUNCATE целей с картой перед попартиционной полной заменой: партиции
    цели, которых нет в источнике, иначе сохранили бы старые строки.
    Только приёмник и только цели, выбранные пользователем с truncate.
    """
    if not names:
        return

    dst_cfg = get_connection_by_id(int(dest_connection_id))

    if not dst_cfg:
        raise ValueError("Подключение не найдено")

    conn = open_psycopg2_connection_by_cfg(dst_cfg)

    try:
        with conn.cursor() as cur:
            for schema, table in names:
                cur.execute("TRUNCATE TABLE {}.{}".format(
                    quote_ident(schema), quote_ident(table)))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _target_label(targets, schema, table):
    """«schema.table» цели для ответа API, None — грузится в одноимённую."""
    if not is_mapped(targets, schema, table):
        return None

    return "{}.{}".format(*target_of(targets, schema, table))


def create_missing_objects(source_connection_id, dest_connection_id, tables,
                           targets=None):
    """
    Создать в приёмнике объекты, которых там нет: схему, таблицу (со всеми
    партициями) или вьюху — по DDL источника. Данные не трогаем, только
    структура. -> [{schema, table, kind, ok, error, statements}]

    targets — карта «источник -> цель»: для таблицы с картой создаётся
    цель под её именем (fetch_target_ddl), а не одноимённая таблица.
    """
    src_cfg = get_connection_by_id(int(source_connection_id))
    dst_cfg = get_connection_by_id(int(dest_connection_id))

    if not src_cfg or not dst_cfg:
        raise ValueError("Подключение не найдено")

    src_conn = open_psycopg2_connection_by_cfg(src_cfg)
    dst_conn = open_psycopg2_connection_by_cfg(dst_cfg)
    dst_conn.autocommit = True
    out = []
    made_schemas = set()

    # Маска dwh_bi.* приносит и корень, и его партиции. Корень создаётся
    # со всеми партициями сразу, поэтому партиции, чей предок тоже в
    # запросе, отдельно не создаём — иначе, окажись они в списке раньше
    # корня, корень потом не смог бы их к себе прикрепить.
    from modules.table_catalog import drop_covered_partitions

    keys = [(t.get("schema"), t.get("table")) for t in tables]

    try:
        from modules.table_catalog import fetch_partition_pairs

        child_parent = fetch_partition_pairs(int(source_connection_id))
    except Exception:
        # без иерархии не страшно: партиция создаётся как PARTITION OF
        # и отдельной таблицей всё равно не станет
        child_parent = {}

    kept, covered = drop_covered_partitions(keys, child_parent)

    for key in keys:
        if key in covered:
            out.append({
                "schema": key[0], "table": key[1], "kind": "partition",
                "ok": True, "skipped": True, "statements": 0,
                "error": "",
                "note": "создаётся вместе с {}.{}".format(*covered[key]),
            })

    try:
        for schema, table in kept:
            row = {"schema": schema, "table": table, "kind": "table",
                   "ok": True, "error": "", "statements": 0}

            if is_mapped(targets, schema, table):
                dst_schema, dst_table = target_of(targets, schema, table)
                row["target"] = _target_label(targets, schema, table)

                try:
                    row["statements"] = create_target_table(
                        src_conn, dst_conn, schema, table,
                        dst_schema, dst_table)
                except Exception as e:
                    row["ok"] = False
                    row["error"] = str(e)[:500]

                out.append(row)
                continue

            try:
                ddl = fetch_object_ddl(src_conn, schema, table)

                if not ddl:
                    row["ok"] = False
                    row["error"] = "нет в источнике"
                    out.append(row)
                    continue

                row["kind"] = ddl["kind"]

                with dst_conn.cursor() as cur:
                    if schema not in made_schemas:
                        cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(
                            quote_ident(schema)))
                        made_schemas.add(schema)

                    for sql_text in ddl["statements"]:
                        cur.execute(sql_text)
                        row["statements"] += 1
            except Exception as e:
                row["ok"] = False
                row["error"] = str(e)[:500]

            out.append(row)
    finally:
        for c in (src_conn, dst_conn):
            try:
                c.close()
            except Exception:
                pass

    return out


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


def build_rename_column_sql(schema, table, old_name, new_name):
    """ALTER TABLE ... RENAME COLUMN. Чистая функция."""
    if not old_name or not new_name:
        raise ValueError("Пустое имя колонки")

    return "ALTER TABLE {}.{} RENAME COLUMN {} TO {}".format(
        quote_ident(schema), quote_ident(table),
        quote_ident(old_name), quote_ident(new_name),
    )


def apply_column_renames(dest_connection_id, tables, targets=None):
    """
    Привести имена колонок приёмника к именам источника.
    tables: [{schema, table, renames: [{from, to}]}] — имена источника;
    с картой targets переименование идёт в цели.
    -> [{schema, table, ok, error, renamed}]
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
                       "ok": True, "error": "", "renamed": 0}

                try:
                    dst_schema, dst_table = target_of(
                        targets, t["schema"], t["table"])

                    for r in t.get("renames") or []:
                        cur.execute(build_rename_column_sql(
                            dst_schema, dst_table, r["from"], r["to"]))
                        row["renamed"] += 1
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


def recreate_tables(source_connection_id, dest_connection_id, tables,
                    targets=None):
    """
    Пересоздать таблицы в приёмнике по DDL источника: DROP + CREATE.
    Единственный способ починить разошедшиеся типы и лишние колонки.
    ДАННЫЕ В ПРИЁМНИКЕ ТЕРЯЮТСЯ — вызывается только по явной команде.
    С картой targets пересоздаётся цель, а не одноимённая таблица.
    """
    dst_cfg = get_connection_by_id(int(dest_connection_id))

    if not dst_cfg:
        raise ValueError("Подключение не найдено")

    conn = open_psycopg2_connection_by_cfg(dst_cfg)
    conn.autocommit = True
    dropped = []

    try:
        with conn.cursor() as cur:
            for t in tables:
                try:
                    dst_schema, dst_table = target_of(
                        targets, t["schema"], t["table"])
                    cur.execute("DROP TABLE IF EXISTS {}.{} CASCADE".format(
                        quote_ident(dst_schema), quote_ident(dst_table)))
                    dropped.append(dict(t))
                except Exception as e:
                    dropped.append(dict(t, drop_error=str(e)[:500]))
    finally:
        try:
            conn.close()
        except Exception:
            pass

    results = create_missing_objects(
        source_connection_id, dest_connection_id,
        [t for t in dropped if not t.get("drop_error")],
        targets=targets,
    )

    for t in dropped:
        if t.get("drop_error"):
            results.append({"schema": t.get("schema"), "table": t.get("table"),
                            "kind": "table", "ok": False,
                            "error": t["drop_error"], "statements": 0})

    return results


def dest_columns_by_source(dst_conn, tables, targets=None):
    """
    Колонки приёмника под ключами источника: для таблицы с картой
    читается цель, но в результате она лежит под (schema, table)
    источника — так её и сравнивает compare_ddl.
    """
    pairs = [(t["schema"], t["table"]) for t in tables]
    dst_pairs = [target_of(targets, s, t) for s, t in pairs]

    raw = fetch_columns(
        dst_conn, [{"schema": s, "table": t} for s, t in dst_pairs])

    out = {}

    for src_key, dst_key in zip(pairs, dst_pairs):
        if dst_key in raw:
            out[src_key] = raw[dst_key]

    return out


def precheck_tables(source_connection_id, dest_connection_id, tables,
                    targets=None):
    """
    Полная предпроверка: сравнение колонок + отсутствующие в приёмнике
    зависимости (функции из DEFAULT'ов, sequences).

    targets — карта «источник -> цель»: источник сравнивается с целью.
    В строках результата имя источника и поле target («schema.table»
    цели или None).
    """
    src_cfg = get_connection_by_id(int(source_connection_id))
    dst_cfg = get_connection_by_id(int(dest_connection_id))

    if not src_cfg or not dst_cfg:
        raise ValueError("Подключение не найдено")

    src_conn = open_psycopg2_connection_by_cfg(src_cfg)
    dst_conn = open_psycopg2_connection_by_cfg(dst_cfg)

    try:
        src_cols = fetch_columns(src_conn, tables)
        dst_cols = dest_columns_by_source(dst_conn, tables, targets)

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

    for row in results:
        row["target"] = _target_label(targets, row["schema"], row["table"])

    return {
        "results": results,
        "deps": [
            {"kind": d["kind"], "name": d["name"]} for d in deps
        ],
        "summary": {
            "renames": sum(len(r.get("renames") or []) for r in results),
            "ok": sum(1 for r in results if r["status"] == "ok"),
            "diff": sum(1 for r in results if r["status"] == "diff"),
            "no_dest": sum(1 for r in results if r["status"] == "no_dest"),
            "no_source": sum(1 for r in results if r["status"] == "no_source"),
        },
    }


def add_missing_columns(dest_connection_id, tables, targets=None):
    """
    Досоздать колонки в приёмнике. tables: [{schema, table,
    columns: [{name, type}]}] — имена источника; с картой targets колонки
    добавляются в цель. -> [{schema, table, ok, error, added}]
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
                    dst_schema, dst_table = target_of(
                        targets, t["schema"], t["table"])
                    statements = build_add_column_sql(
                        dst_schema, dst_table, t.get("columns") or [],
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
