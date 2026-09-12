# -*- coding: utf-8 -*-
"""Колонка есть, но названа иначе — это переименование, а не добавление."""

import pytest

from modules.ddl_check import (
    build_rename_column_sql,
    compare_ddl,
    match_renames,
    normalize_column_name,
)


def test_normalize_strips_quotes_case_and_homoglyphs():
    # реальный случай: в источнике имя записано вместе с кавычками
    assert (normalize_column_name('"ГРУППИРОВКА"')
            == normalize_column_name("ГРУППИРОВКА"))
    # латинские двойники кириллицы: TC (лат.) против ТС (кир.)
    assert normalize_column_name("TCП") == normalize_column_name("ТСП")
    assert normalize_column_name("  Дата  Добавления ") == "дата добавления"


def test_match_renames_pairs_columns():
    renames, missing, extra = match_renames(
        [{"name": '"ГРУППИРОВКА"', "type": "character varying(256)"},
         {"name": "новая", "type": "text"}],
        ["ГРУППИРОВКА", "report_date"],
    )

    assert renames == [{"from": "ГРУППИРОВКА", "to": '"ГРУППИРОВКА"',
                        "type": "character varying(256)"}]
    # пара ушла из обоих списков — иначе ADD COLUMN плодит дубли
    assert missing == [{"name": "новая", "type": "text"}]
    assert extra == ["report_date"]


def test_match_renames_keeps_identical_names_out():
    renames, missing, extra = match_renames(
        [{"name": "id", "type": "bigint"}], ["id"])

    assert renames == []
    assert missing == [{"name": "id", "type": "bigint"}]
    assert extra == ["id"]


def test_compare_ddl_reports_renames():
    tables = [{"schema": "s", "table": "t"}]
    src = {("s", "t"): [{"name": "id", "type": "bigint"},
                        {"name": "ТСП", "type": "character varying(50)"}]}
    dst = {("s", "t"): [{"name": "id", "type": "bigint"},
                        {"name": "TCП", "type": "character varying(50)"}]}

    row = compare_ddl(src, dst, tables)[0]

    assert row["status"] == "diff"
    assert row["renames"] == [{"from": "TCП", "to": "ТСП",
                               "type": "character varying(50)"}]
    assert row["missing_in_dest"] == []
    assert row["extra_in_dest"] == []


def test_rename_sql_quotes_identifiers():
    sql = build_rename_column_sql("dwh", "t", "ГРУППИРОВКА", '"ГРУППИРОВКА"')

    assert sql == ('ALTER TABLE "dwh"."t" RENAME COLUMN "ГРУППИРОВКА" '
                   'TO """ГРУППИРОВКА"""')

    with pytest.raises(ValueError):
        build_rename_column_sql("dwh", "t", "", "x")
