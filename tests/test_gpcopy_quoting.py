# -*- coding: utf-8 -*-
"""Имена объектов для gpcopy: без кавычек он приводит их к нижнему регистру."""

import os

from modules.gpcopy import (
    gpcopy_full_name,
    make_include_table_file,
    quote_ident_if_needed,
    quote_table_for_include,
)


def test_plain_names_stay_bare():
    assert quote_ident_if_needed("accounts") == "accounts"
    assert quote_ident_if_needed("x_dwh_3255_1") == "x_dwh_3255_1"
    assert (gpcopy_full_name("adb", "dwh_fin_pbi", "accounts")
            == "adb.dwh_fin_pbi.accounts")


def test_tricky_names_get_quoted():
    # именно на таком имени падал запуск #160
    assert (gpcopy_full_name("adb", "dwh_fin_pbi",
                             "Alloc_KBK_product matrix_xx")
            == 'adb.dwh_fin_pbi."Alloc_KBK_product matrix_xx"')
    # заглавные буквы, дефис, цифра в начале, точка внутри имени
    assert (quote_ident_if_needed("Payroll-Product_Matrix")
            == '"Payroll-Product_Matrix"')
    assert quote_ident_if_needed("01") == '"01"'
    assert quote_ident_if_needed("a.b") == '"a.b"'
    # кавычка внутри имени удваивается
    assert quote_ident_if_needed('weird"name') == '"weird""name"'


def test_two_part_name_for_include():
    assert quote_table_for_include("dwh", "Orders") == 'dwh."Orders"'
    assert quote_table_for_include("dwh", "orders") == "dwh.orders"


def test_include_file_quotes_only_where_needed():
    path = make_include_table_file(
        [
            {"schema_name": "dwh_fin_pbi",
             "table_name": "Alloc_KBK_product matrix_xx"},
            {"schema_name": "dwh_fin_pbi", "table_name": "accounts"},
            {"schema": "dwh_fin_pbi", "table": "Payroll-Product_Matrix"},
        ],
        dbname="adb",
    )

    try:
        with open(path, encoding="utf-8") as fh:
            lines = [line.strip() for line in fh if line.strip()]
    finally:
        os.remove(path)

    assert lines == [
        'adb.dwh_fin_pbi."Alloc_KBK_product matrix_xx"',
        "adb.dwh_fin_pbi.accounts",
        'adb.dwh_fin_pbi."Payroll-Product_Matrix"',
    ]
