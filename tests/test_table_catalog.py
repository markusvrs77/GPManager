import pytest

from modules.table_catalog import (
    match_mask,
    parse_table_list,
    pick_columns,
    create_table_set,
    get_table_set,
    list_table_sets,
    delete_table_set,
)
from db import init_db, sqlite_cursor


CATALOG = [
    ("dwh", "fact_sales"),
    ("dwh", "fact_orders"),
    ("dwh", "dim_client"),
    ("stg", "fact_sales"),
]


def test_match_mask_wildcard():
    assert match_mask(CATALOG, "dwh.fact_*") == [
        ("dwh", "fact_sales"), ("dwh", "fact_orders"),
    ]


def test_match_mask_schema_only():
    assert len(match_mask(CATALOG, "stg.*")) == 1


def test_match_mask_no_wildcard_is_exact():
    assert match_mask(CATALOG, "dwh.dim_client") == [("dwh", "dim_client")]


def test_parse_table_list_valid_and_invalid():
    text = "dwh.fact_sales\n  stg.fact_sales  \nnope\ndwh.missing\n\n"
    valid, invalid = parse_table_list(text, CATALOG)
    assert valid == [("dwh", "fact_sales"), ("stg", "fact_sales")]
    assert invalid == ["nope", "dwh.missing"]


def test_pick_columns_priority_and_missing():
    columns_by_table = {
        ("dwh", "fact_sales"): ["id", "updated_at", "date_change$"],
        ("dwh", "fact_orders"): ["id", "created_at"],
        ("dwh", "dim_client"): ["id"],
    }
    priority = ["date_change$", "updated_at", "created_at"]

    resolved, missing = pick_columns(columns_by_table, priority)

    assert resolved[("dwh", "fact_sales")] == "date_change$"
    assert resolved[("dwh", "fact_orders")] == "created_at"
    assert missing == [("dwh", "dim_client")]


def test_table_set_crud():
    init_db()

    set_id = create_table_set({
        "name": "unit-test-set",
        "connection_id": 1,
        "tables": [{"schema": "dwh", "table": "fact_sales"}],
        "rules": {"date_priority": ["date_change$"]},
    })

    ts = get_table_set(set_id)
    assert ts["name"] == "unit-test-set"
    assert ts["tables"][0]["table"] == "fact_sales"
    assert ts["rules"]["date_priority"] == ["date_change$"]

    assert any(s["id"] == set_id for s in list_table_sets())

    delete_table_set(set_id)
    assert get_table_set(set_id) is None
