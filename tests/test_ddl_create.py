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
