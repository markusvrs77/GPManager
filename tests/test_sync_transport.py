# -*- coding: utf-8 -*-
"""Выбор транспорта по паре типов СУБД — без подключения к БД."""

import pytest

from modules.sync_transport import (
    build_create_table_sql,
    normalize_db_type,
    pick_transport,
    qident,
)


def test_normalize_db_type():
    assert normalize_db_type("Greenplum") == "greenplum"
    assert normalize_db_type(" postgres ") == "postgres"
    assert normalize_db_type(None) == "greenplum"       # легаси-строки без типа
    assert normalize_db_type("unknown") == "greenplum"


def test_pick_transport_matrix():
    assert pick_transport("greenplum", "greenplum") == "gpcopy"
    assert pick_transport("postgres", "postgres") == "copy_pipe"
    assert pick_transport("postgres", "greenplum") == "copy_pipe"
    assert pick_transport("greenplum", "postgres") == "copy_pipe"
    # легаси: None считается greenplum
    assert pick_transport(None, None) == "gpcopy"


def test_pick_transport_unsupported():
    with pytest.raises(ValueError) as e:
        pick_transport("mysql", "greenplum")
    assert "MySQL" in str(e.value)

    with pytest.raises(ValueError):
        pick_transport("greenplum", "oracle")


def test_qident_escapes_quotes():
    assert qident("plain") == '"plain"'
    assert qident('we"ird') == '"we""ird"'


def test_build_create_table_sql():
    cols = [
        {"name": "id", "type": "bigint"},
        {"name": "name", "type": "character varying(100)"},
    ]
    sql = build_create_table_sql("dwh", "orders", cols)
    assert '"dwh"."orders"' in sql
    assert '"id" bigint' in sql
    assert '"name" character varying(100)' in sql
    assert "DISTRIBUTED" not in sql

    gp_sql = build_create_table_sql("dwh", "orders", cols, distributed_randomly=True)
    assert gp_sql.endswith("DISTRIBUTED RANDOMLY")

    with pytest.raises(ValueError):
        build_create_table_sql("dwh", "empty", [])
