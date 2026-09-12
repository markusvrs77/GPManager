import pytest

from modules.gpcopy_increment import build_increment_items


TABLES = [{"schema": "dwh", "table": "orders", "watermark_column": "id"}]


def test_no_watermark_copies_everything():
    items = build_increment_items(TABLES, {}, "prod", "test")
    assert len(items) == 1
    assert items[0]["source"] == "prod.dwh.orders"
    assert items[0]["dest"] == "test.dwh.orders"
    assert items[0]["sql"] == 'SELECT * FROM "dwh"."orders"'


def test_watermark_builds_where_greater_than():
    wm = {("dwh", "orders"): 1000}
    items = build_increment_items(TABLES, wm, "prod", "test")
    assert items[0]["sql"] == 'SELECT * FROM "dwh"."orders" WHERE "id" > 1000'


def test_string_watermark_is_escaped():
    wm = {("dwh", "orders"): "2026-07-20 10:00:00"}
    items = build_increment_items(TABLES, wm, "prod", "test")
    assert items[0]["sql"] == (
        'SELECT * FROM "dwh"."orders" WHERE "id" > \'2026-07-20 10:00:00\''
    )


def test_sql_injection_in_watermark_is_neutralised():
    wm = {("dwh", "orders"): "x'; DROP TABLE users;--"}
    items = build_increment_items(TABLES, wm, "prod", "test")
    assert "''" in items[0]["sql"]  # single quote doubled
    assert items[0]["sql"].count("'") % 2 == 0


def test_missing_watermark_column_raises():
    bad = [{"schema": "dwh", "table": "orders"}]
    with pytest.raises(ValueError):
        build_increment_items(bad, {("dwh", "orders"): 5}, "prod", "test")


def test_empty_tables_raises():
    with pytest.raises(ValueError):
        build_increment_items([], {}, "prod", "test")
