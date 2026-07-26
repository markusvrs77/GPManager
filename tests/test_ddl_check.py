# -*- coding: utf-8 -*-
"""Предпроверка DDL — чистое сравнение и генерация ALTER, без БД."""

import pytest

from modules.ddl_check import build_add_column_sql, compare_ddl


def test_compare_ddl_statuses():
    tables = [
        {"schema": "s", "table": "same"},
        {"schema": "s", "table": "missing_col"},
        {"schema": "s", "table": "type_diff"},
        {"schema": "s", "table": "no_dest"},
    ]
    src = {
        ("s", "same"): [{"name": "id", "type": "bigint"}],
        ("s", "missing_col"): [{"name": "id", "type": "bigint"},
                               {"name": "extra", "type": "text"}],
        ("s", "type_diff"): [{"name": "id", "type": "bigint"}],
        ("s", "no_dest"): [{"name": "id", "type": "bigint"}],
    }
    dst = {
        ("s", "same"): [{"name": "id", "type": "bigint"}],
        ("s", "missing_col"): [{"name": "id", "type": "bigint"}],
        ("s", "type_diff"): [{"name": "id", "type": "integer"}],
    }

    by_table = {r["table"]: r for r in compare_ddl(src, dst, tables)}

    assert by_table["same"]["status"] == "ok"

    assert by_table["missing_col"]["status"] == "diff"
    assert by_table["missing_col"]["missing_in_dest"] == [
        {"name": "extra", "type": "text"}]

    assert by_table["type_diff"]["status"] == "diff"
    assert by_table["type_diff"]["type_diffs"] == [
        {"column": "id", "src": "bigint", "dst": "integer"}]

    assert by_table["no_dest"]["status"] == "no_dest"


def test_build_add_column_sql():
    stmts = build_add_column_sql("s", "t", [
        {"name": "col1", "type": "character varying(255)"},
        {"name": "col2", "type": "numeric(10,2)"},
    ])

    assert stmts == [
        'ALTER TABLE "s"."t" ADD COLUMN "col1" character varying(255)',
        'ALTER TABLE "s"."t" ADD COLUMN "col2" numeric(10,2)',
    ]

    # инъекция в типе — отклоняется
    with pytest.raises(ValueError):
        build_add_column_sql("s", "t", [
            {"name": "x", "type": "text; DROP TABLE users"}])

    with pytest.raises(ValueError):
        build_add_column_sql("s", "t", [{"name": "", "type": "text"}])
