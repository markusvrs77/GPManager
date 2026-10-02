# -*- coding: utf-8 -*-
"""
«Создать и залить» для PG↔PG: DDL таблицы по каталогу PostgreSQL
источника (без ddl_check), её создание в приёмнике и уборка висящего
staging прошлых задач.
"""

import modules.pg_diff_load as pdl
from tests.pg_fakes import FakeConn, render

# Каталог источника: (attname, format_type, attnotnull, attidentity,
# attgenerated, pg_get_expr(adbin), relkind, схема и имя collation —
# только если она не та, что у типа по умолчанию) в порядке attnum
ORDERS_COLUMNS = [
    ("id", "bigint", True, "a", "", None, "r", None, None),
    ("code", "integer", True, "", "", "nextval('app.orders_code_seq'::regclass)", "r", None, None),
    ("title", "character varying(200)", True, "", "", "'new'::character varying", "r", "pg_catalog", "C"),
    ("payload", "jsonb", False, "", "", None, "r", None, None),
    ("qty", "integer", False, "", "", None, "r", None, None),
    ("price", "numeric(12,2)", False, "", "", None, "r", None, None),
    ("total", "numeric", False, "", "s", "((qty)::numeric * price)", "r", None, None),
]


def _source(columns=ORDERS_COLUMNS, pk=(("id",),)):
    return FakeConn(responses=[("format_type", columns),
                               ("contype", list(pk))])


def test_create_table_ddl_from_source_catalog():
    src = _source()

    definition = pdl.source_table_definition(src, "app", "orders")
    text = render(pdl.build_create_table_sql("app", "orders", definition))

    # identity и default на последовательность не переносятся: значения
    # приходят из источника; вычисляемая колонка — по выражению источника
    assert text == (
        'CREATE TABLE "app"."orders" ('
        '"id" bigint NOT NULL, '
        '"code" integer NOT NULL, '
        '"title" character varying(200) COLLATE "pg_catalog"."C" NOT NULL, '
        '"payload" jsonb, '
        '"qty" integer, '
        '"price" numeric(12,2), '
        '"total" numeric GENERATED ALWAYS AS (((qty)::numeric * price)) STORED, '
        'PRIMARY KEY ("id"))')
    assert definition["partitioned"] is False


def _dest(schema_exists, error=None):
    dst = FakeConn(responses=[("pg_namespace", [(1,)] if schema_exists else [])])
    real_execute = dst.respond

    def respond(text, params):
        if error and text.startswith("CREATE TABLE"):
            raise error
        return real_execute(text, params)

    dst.respond = respond
    dst.commit = lambda: dst.executed.append(("COMMIT", None))
    dst.rollback = lambda: dst.executed.append(("ROLLBACK", None))
    return dst


def _writes(conn):
    return [t for t, _ in conn.executed
            if t.split(" ", 1)[0] in ("CREATE", "COMMIT", "ROLLBACK")]


def test_create_makes_missing_schema_and_table_in_one_dest_transaction():
    src, dst = _source(), _dest(schema_exists=False)

    out = pdl.create_table_from_source(src, dst, "app", "orders")

    assert _writes(dst) == [
        'CREATE SCHEMA IF NOT EXISTS "app"',
        render(pdl.build_create_table_sql(
            "app", "orders", pdl.source_table_definition(_source(), "app",
                                                         "orders"))),
        "COMMIT"]
    assert out == {"partitioned": False, "virtual_as_stored": []}
    # источник только читается
    assert all(t.startswith(("SELECT", "SET LOCAL search_path"))
               for t, _ in src.executed)


def test_create_skips_schema_when_it_exists():
    dst = _dest(schema_exists=True)

    pdl.create_table_from_source(_source(), dst, "app", "orders")

    assert [w.split(" (")[0] for w in _writes(dst)] == [
        'CREATE TABLE "app"."orders"', "COMMIT"]


def test_create_error_rolls_back_dest_and_raises():
    dst = _dest(schema_exists=False,
                error=RuntimeError('type "app.mood" does not exist'))

    try:
        pdl.create_table_from_source(_source(), dst, "app", "orders")
    except RuntimeError as e:
        assert "mood" in str(e)
    else:
        raise AssertionError("ошибка создания не дошла до вызывающего")

    assert _writes(dst)[-1] == "ROLLBACK" and "COMMIT" not in _writes(dst)


def test_partitioned_source_parent_is_created_as_a_plain_table():
    rows = [r[:6] + ("p",) + r[7:] for r in ORDERS_COLUMNS]
    dst = _dest(schema_exists=True)

    out = pdl.create_table_from_source(_source(columns=rows), dst, "app",
                                       "orders")

    assert out == {"partitioned": True, "virtual_as_stored": []}
    assert "PARTITION" not in render_all(dst)


def render_all(conn):
    return "\n".join(t for t, _ in conn.executed)


def test_source_catalog_is_read_with_pg_catalog_search_path_and_reverted():
    src = _source()
    src.rollback = lambda: src.executed.append(("ROLLBACK", None))

    pdl.source_table_definition(src, "app", "orders")

    texts = [t for t, _ in src.executed]
    # SET LOCAL живёт до конца транзакции чтения: откат возвращает search_path
    assert texts[0] == "SET LOCAL search_path = pg_catalog"
    assert texts[-1] == "ROLLBACK"
    assert all(t.startswith("SELECT") for t in texts[1:-1])


VIRTUAL_COLUMNS = [
    ("id", "integer", True, "", "", None, "r", None, None),
    ("twice", "integer", False, "", "v", "(id * 2)", "r", None, None),
]


def test_virtual_generated_column_is_virtual_only_where_dest_supports_it():
    definition = pdl.source_table_definition(
        _source(columns=VIRTUAL_COLUMNS), "app", "nums")

    on_pg18 = render(pdl.build_create_table_sql("app", "nums", definition,
                                                virtual=True))
    older = render(pdl.build_create_table_sql("app", "nums", definition,
                                              virtual=False))

    assert on_pg18 == ('CREATE TABLE "app"."nums" ("id" integer NOT NULL, '
                       '"twice" integer GENERATED ALWAYS AS ((id * 2)) VIRTUAL, '
                       'PRIMARY KEY ("id"))')
    assert older == ('CREATE TABLE "app"."nums" ("id" integer NOT NULL, '
                     '"twice" integer GENERATED ALWAYS AS ((id * 2)) STORED, '
                     'PRIMARY KEY ("id"))')


def test_create_reports_virtual_columns_made_stored_on_older_dest():
    dst = _dest(schema_exists=True)
    dst.server_version = 170004

    out = pdl.create_table_from_source(_source(columns=VIRTUAL_COLUMNS), dst,
                                       "app", "nums")

    assert out == {"partitioned": False, "virtual_as_stored": ["twice"]}
    assert "STORED" in [t for t, _ in dst.executed
                        if t.startswith("CREATE TABLE")][0]
