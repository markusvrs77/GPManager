# -*- coding: utf-8 -*-
"""Создание недостающих объектов в приёмнике: генерация DDL."""

import pytest

from modules.ddl_check import (
    _partition_clause,
    build_create_partition_sql,
    build_create_table_sql,
    build_create_view_sql,
)


def test_create_table_with_storage_and_distribution():
    sql = build_create_table_sql(
        "dwh_fin_pbi", "allocation_prd",
        [
            {"name": "id", "type": "integer", "not_null": True},
            {"name": "created", "type": "timestamp without time zone",
             "default": "now()"},
            {"name": "note", "type": "character varying(255)"},
        ],
        options=["appendonly=true", "compresstype=zstd"],
        distributed_by="DISTRIBUTED BY (id)",
    )

    assert sql.startswith(
        'CREATE TABLE IF NOT EXISTS "dwh_fin_pbi"."allocation_prd" (')
    assert '"id" integer NOT NULL' in sql
    assert '"created" timestamp without time zone DEFAULT now()' in sql
    assert '"note" character varying(255)' in sql
    # порядок частей важен для грамматики Greenplum 7
    assert (sql.index("WITH (appendonly=true, compresstype=zstd)")
            < sql.index("DISTRIBUTED BY (id)"))


def test_create_partitioned_parent_and_child():
    parent = build_create_table_sql(
        "dwh", "events", [{"name": "d", "type": "date", "not_null": True}],
        partition_by="PARTITION BY RANGE (d)",
        distributed_by="DISTRIBUTED RANDOMLY",
    )

    assert (parent.index("PARTITION BY RANGE (d)")
            < parent.index("DISTRIBUTED RANDOMLY"))

    child = build_create_partition_sql(
        "dwh", "events_2026", "dwh", "events",
        "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
        options=["appendonly=true"],
    )

    assert 'PARTITION OF "dwh"."events"' in child
    assert "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')" in child
    assert child.rstrip().endswith("WITH (appendonly=true)")


def test_create_view_and_matview():
    view = build_create_view_sql("dwh", "v_orders",
                                 " SELECT * FROM dwh.orders; ")

    assert view == ('CREATE VIEW "dwh"."v_orders" AS\n'
                    "SELECT * FROM dwh.orders")

    mat = build_create_view_sql("dwh", "mv", "SELECT 1", materialized=True)

    assert mat.startswith('CREATE MATERIALIZED VIEW "dwh"."mv" AS')


def test_bad_input_is_rejected():
    # тип из каталога не проходит валидацию — не подставляем его в DDL
    with pytest.raises(ValueError):
        build_create_table_sql("s", "t", [{"name": "a", "type": "int; DROP"}])

    with pytest.raises(ValueError):
        build_create_table_sql("s", "t", [])

    with pytest.raises(ValueError):
        build_create_partition_sql("s", "t_1", "s", "t", "")

    with pytest.raises(ValueError):
        build_create_view_sql("s", "v", "   ")


def test_partition_clause_gets_prefix():
    """GP7 отдаёт ключ секционирования без «PARTITION BY» — дописываем."""
    assert (_partition_clause("RANGE (report_date)")
            == "PARTITION BY RANGE (report_date)")
    assert (_partition_clause("PARTITION BY LIST (x)")
            == "PARTITION BY LIST (x)")
    assert _partition_clause("  ") is None
    assert _partition_clause(None) is None


# ------------------------------------------------------------------
# Greenplum 7: способ хранения и партиции
# ------------------------------------------------------------------
#
# В GP7 append-optimized — метод доступа (pg_class.relam), а в reloptions
# остаются только compresstype и прочие его параметры. Без USING таблица
# создавалась как heap и падала с unrecognized parameter "compresstype".

from modules import ddl_check  # noqa: E402

COLS = [{"name": "id", "type": "integer", "not_null": True}]


def test_ao_table_gets_its_access_method():
    sql = build_create_table_sql(
        "dwh_bi", "osv", COLS,
        options=["compresstype=zstd", "compresslevel=1"],
        partition_by="PARTITION BY RANGE (id)",
        distributed_by="DISTRIBUTED BY (id)",
        access_method="ao_column",
    )

    # порядок частей по грамматике GP7
    assert (sql.index("PARTITION BY RANGE (id)")
            < sql.index('USING "ao_column"')
            < sql.index("WITH (compresstype=zstd, compresslevel=1)")
            < sql.index("DISTRIBUTED BY (id)"))


@pytest.mark.parametrize("method", [None, "", "heap"])
def test_heap_table_has_no_using(method):
    sql = build_create_table_sql("dwh_bi", "t", COLS, access_method=method)

    assert "USING" not in sql


def test_ao_partition_gets_its_access_method():
    sql = build_create_partition_sql(
        "dwh_bi", "osv_1_prt_p20260915", "dwh_bi", "osv",
        "FOR VALUES FROM ('2026-09-15') TO ('2026-09-16')",
        options=["compresstype=zstd"], access_method="ao_row",
    )

    assert (sql.index("FOR VALUES")
            < sql.index('USING "ao_row"')
            < sql.index("WITH (compresstype=zstd)"))


class CatalogCursor(object):
    """Каталог источника: отвечает по тексту запроса."""

    def __init__(self, tables, parents=None, columns=None, declarative=True):
        self.declarative = declarative  # есть ли relpartbound (GP7, PG 10+)
        self.tables = tables            # (schema, table) -> meta-строка
        self.parents = parents or {}    # oid -> (p_schema, p_table, bound)
        self.columns = columns or COLS
        self.sql = ""
        self.params = ()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=()):
        self.sql, self.params = sql, params

    def fetchone(self):
        if "attname = 'relpartbound'" in self.sql:
            return (self.declarative,)
        if "LEFT JOIN pg_am" in self.sql and "relname = %s" in self.sql:
            return self.tables.get(tuple(self.params))
        if "i.inhrelid = %s" in self.sql:
            return self.parents.get(self.params[0])
        if "pg_get_table_distributedby" in self.sql:
            return ("DISTRIBUTED BY (id)",)
        if "pg_get_partkeydef" in self.sql:
            return ("RANGE (id)",)
        return None

    def fetchall(self):
        if "FROM pg_attribute" in self.sql:
            return [(c["name"], c["type"], c["not_null"], None)
                    for c in self.columns]
        return []


class CatalogConn(object):
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_ao_table_ddl_from_catalog_uses_its_method():
    cur = CatalogCursor({("dwh_bi", "inv_1"): (
        101, "r", ["compresstype=zstd", "compresslevel=1"], "ao_column")})

    ddl = ddl_check.fetch_object_ddl(CatalogConn(cur), "dwh_bi", "inv_1")

    assert ddl["kind"] == "table"
    assert 'USING "ao_column"' in ddl["statements"][0]


def test_leaf_partition_is_created_as_a_partition_not_a_table():
    """Иначе gpcopy лил бы в самостоятельную таблицу с именем партиции."""
    bound = "FOR VALUES FROM ('2026-09-15') TO ('2026-09-16')"
    cur = CatalogCursor(
        {("dwh_bi", "osv_1_prt_p20260915"): (
            202, "r", ["compresstype=zstd"], "ao_column")},
        parents={202: ("dwh_bi", "osv", bound)},
    )

    ddl = ddl_check.fetch_object_ddl(
        CatalogConn(cur), "dwh_bi", "osv_1_prt_p20260915")

    assert ddl["kind"] == "partition"
    assert 'PARTITION OF "dwh_bi"."osv"' in ddl["statements"][0]
    assert bound in ddl["statements"][0]
    assert 'USING "ao_column"' in ddl["statements"][0]


def test_inheritance_child_without_bounds_stays_a_table():
    """Наследник без границ — обычное наследование Postgres."""
    cur = CatalogCursor(
        {("public", "child"): (303, "r", [], "heap")},
        parents={303: ("public", "parent", None)},
    )

    ddl = ddl_check.fetch_object_ddl(CatalogConn(cur), "public", "child")

    assert ddl["kind"] == "table"


def test_leaves_are_not_created_separately_when_their_root_is_asked(
        monkeypatch):
    import modules.table_catalog as catalog

    executed = []

    class DestCursor(object):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            executed.append(sql)

    class DestConn(object):
        autocommit = False

        def cursor(self):
            return DestCursor()

        def close(self):
            pass

    root = ("dwh_bi", "osv")
    leaf = ("dwh_bi", "osv_1_prt_p20260915")
    asked = []

    monkeypatch.setattr(ddl_check, "get_connection_by_id", lambda cid: {"id": cid})
    monkeypatch.setattr(ddl_check, "open_psycopg2_connection_by_cfg",
                        lambda cfg: DestConn())
    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: {leaf: root})

    def fake_ddl(conn, schema, table, with_partitions=True):
        asked.append((schema, table))
        return {"kind": "partitioned", "statements": ["CREATE root"]}

    monkeypatch.setattr(ddl_check, "fetch_object_ddl", fake_ddl)

    results = ddl_check.create_missing_objects(1, 2, [
        {"schema": leaf[0], "table": leaf[1]},      # партиция раньше корня
        {"schema": root[0], "table": root[1]},
    ])

    by_table = {r["table"]: r for r in results}

    assert asked == [root]
    assert by_table["osv_1_prt_p20260915"]["skipped"] is True
    assert "dwh_bi.osv" in by_table["osv_1_prt_p20260915"]["note"]
    assert by_table["osv"]["ok"] is True


def test_old_catalog_without_declarative_partitions_still_works():
    """На GP6 и Postgres до 10 relpartbound нет — таблица создаётся как раньше."""
    cur = CatalogCursor(
        {("dwh", "legacy"): (404, "r", ["appendonly=true"], None)},
        declarative=False,
    )

    ddl = ddl_check.fetch_object_ddl(CatalogConn(cur), "dwh", "legacy")

    assert ddl["kind"] == "table"
    assert "WITH (appendonly=true)" in ddl["statements"][0]
    assert "USING" not in ddl["statements"][0]
