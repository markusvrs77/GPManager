# -*- coding: utf-8 -*-
"""
Сравнение двух баз PostgreSQL: раскрытие выбора и подсчёт разницы.
Живой базы нет: фейковые соединения отвечают по подстроке SQL.
"""

import pytest

import modules.pg_compare as cmp
from tests.pg_fakes import FakeConn


def _catalog(namespaces, relations, inherits=()):
    """Фейк каталога одной стороны."""
    return FakeConn(responses=[
        ("pg_inherits", list(inherits)),
        ("c.relkind IN", list(relations)),
        ("FROM pg_namespace", [(n,) for n in namespaces]),
    ])


def _pairs(result):
    return [(r["schema"], r["table"], r["in_src"], r["in_dst"])
            for r in result]


# ------------------------------------------------------------ expand_selection

def test_schema_expands_on_both_sides_and_drops_partition_leaves():
    src = _catalog(
        ["sales"],
        [("sales", "orders"), ("sales", "items"),
         ("sales", "part"), ("sales", "part_2024")],
        [("sales", "part_2024", "sales", "part")],
    )
    dst = _catalog(["sales"], [("sales", "orders"), ("sales", "legacy"),
                               ("sales", "part")])

    result = cmp.expand_selection(src, dst, ["sales"], [])

    assert sorted(_pairs(result)) == sorted([
        ("sales", "orders", True, True),
        ("sales", "items", True, False),
        ("sales", "part", True, True),
        ("sales", "legacy", False, True),
    ])


def test_single_table_missing_in_dest_is_marked():
    src = _catalog(["hr"], [("hr", "staff")])
    dst = _catalog([], [])

    result = cmp.expand_selection(src, dst, [], [{"schema": "hr",
                                                  "table": "staff"}])

    assert _pairs(result) == [("hr", "staff", True, False)]


def test_schema_and_table_from_it_collapse_into_one():
    src = _catalog(["sales"], [("sales", "orders")])
    dst = _catalog(["sales"], [("sales", "orders")])

    result = cmp.expand_selection(
        src, dst, ["sales", "sales"],
        [{"schema": "sales", "table": "orders"},
         {"schema": "sales", "table": "orders"}],
    )

    assert _pairs(result) == [("sales", "orders", True, True)]


def test_leaf_of_a_selected_parent_is_dropped():
    src = _catalog(["sales"], [("sales", "part"), ("sales", "part_2024")],
                   [("sales", "part_2024", "sales", "part")])
    dst = _catalog(["sales"], [("sales", "part"), ("sales", "part_2024")])

    result = cmp.expand_selection(
        src, dst, [],
        [{"schema": "sales", "table": "part"},
         {"schema": "sales", "table": "part_2024"}],
    )

    assert _pairs(result) == [("sales", "part", True, True)]


def test_unknown_table_is_rejected():
    src = _catalog(["sales"], [("sales", "orders")])
    dst = _catalog(["sales"], [("sales", "orders")])

    with pytest.raises(ValueError) as err:
        cmp.expand_selection(src, dst, [], [{"schema": "sales",
                                             "table": "nope"}])

    assert "sales.nope" in str(err.value)


def test_unknown_schema_is_rejected():
    src = _catalog(["sales"], [("sales", "orders")])
    dst = _catalog(["sales"], [])

    with pytest.raises(ValueError) as err:
        cmp.expand_selection(src, dst, ["ghost"], [])

    assert "ghost" in str(err.value)


# ------------------------------------------------------------ compare_table

COLS = [("id", "integer"), ("name", "text"), ("amount", "numeric")]


def _sides(src_cols=COLS, dst_cols=COLS, dup=0, counts=(3, 0, 0, 0),
           copy_out=b"1\ta\n2\tb\n3\tc\n", dst_dup=0):
    src = FakeConn(responses=[("a.attname", list(src_cols))],
                   copy_out=copy_out)
    dst = FakeConn(responses=[
        ("a.attname", list(dst_cols)),
        ('FROM "pgcmp_src" GROUP BY', [(dup,)]),
        ("HAVING count(*) > 1", [(dst_dup,)]),
        ("AS to_insert", [tuple(counts)]),
    ])
    return src, dst


def test_keyed_compare_streams_key_and_hash_into_a_temp_table():
    src, dst = _sides(dst_cols=list(reversed(COLS)), counts=(4, 1, 2, 2))

    result = cmp.compare_table(src, dst, "sales", "orders", ["id"])

    assert result["status"] == "differs"
    assert (result["src_rows"], result["dst_rows"]) == (3, 4)
    assert (result["to_insert"], result["to_update"],
            result["to_delete"]) == (1, 2, 2)

    out = src.copies[0]
    assert out.startswith("COPY (SELECT ")
    assert '"t"."id"::text' in out
    assert 'md5(ROW("t"."amount", "t"."id", "t"."name")::text)' in out
    assert 'FROM "sales"."orders" AS "t"' in out
    assert out.endswith(") TO STDOUT")

    dst_sql = dst.sql_text()
    assert 'CREATE TEMP TABLE "pgcmp_src" ("k0" text, "h" text)' in dst_sql
    assert dst.copies == ['COPY "pgcmp_src" ("k0", "h") FROM STDIN']
    assert '"s"."k0" = "d"."k0"' in dst_sql
    assert '"s"."h" <> "d"."h"' in dst_sql
    # временная таблица уходит вместе с откатом; источник тоже закрыт
    assert dst.rollbacks >= 1 and src.rollbacks >= 1
    assert dst.commits == 0


def test_identical_tables_are_same():
    src, dst = _sides(counts=(3, 0, 0, 0))

    assert cmp.compare_table(src, dst, "s", "t", ["id"])["status"] == "same"


def test_duplicate_source_keys_are_reported():
    src, dst = _sides(dup=2)

    result = cmp.compare_table(src, dst, "s", "t", ["id"])

    assert result["status"] == "duplicate_keys"
    assert "2" in result["message"]


def test_keyless_compare_uses_except_all_on_hashes():
    src, dst = _sides(counts=(3, 1, 0, 1))

    result = cmp.compare_table(src, dst, "s", "t", [])

    assert result["status"] == "differs"
    assert (result["to_insert"], result["to_update"],
            result["to_delete"]) == (1, 0, 1)
    assert 'CREATE TEMP TABLE "pgcmp_src" ("h" text)' in dst.sql_text()
    assert "EXCEPT ALL" in dst.sql_text()
    assert "HAVING" not in dst.sql_text()


def test_different_column_sets_are_structure_diff_without_reading_data():
    src, dst = _sides(dst_cols=[("id", "integer"), ("name", "text"),
                                ("note", "text")])

    result = cmp.compare_table(src, dst, "s", "t", ["id"])

    assert result["status"] == "structure_diff"
    assert "amount" in result["message"] and "note" in result["message"]
    assert src.copies == [] and dst.copies == []


def test_key_absent_in_a_table_is_refused():
    src, dst = _sides()

    with pytest.raises(ValueError):
        cmp.compare_table(src, dst, "s", "t", ["code"])


def test_duplicate_dest_keys_are_reported_with_the_side():
    src, dst = _sides(dst_dup=3)

    result = cmp.compare_table(src, dst, "s", "t", ["id"])

    assert result["status"] == "duplicate_keys"
    assert "приёмник" in result["message"] and "3" in result["message"]
    assert 'FROM "s"."t" AS "t" GROUP BY "t"."id"' in dst.sql_text()


def test_type_mismatch_of_one_column_is_structure_diff():
    src, dst = _sides(dst_cols=[("id", "integer"), ("name", "text"),
                                ("amount", "numeric(10,2)")])

    result = cmp.compare_table(src, dst, "s", "t", ["id"])

    assert result["status"] == "structure_diff"
    assert "amount" in result["message"]
    assert "numeric(10,2)" in result["message"]
    assert src.copies == [] and dst.copies == []


def test_temp_table_is_recreated_safely():
    src, dst = _sides()

    cmp.compare_table(src, dst, "s", "t", ["id"])

    text = dst.sql_text()
    assert 'DROP TABLE IF EXISTS "pg_temp"."pgcmp_src"' in text
    assert text.index("DROP TABLE IF EXISTS") < text.index("CREATE TEMP")
    assert "ON COMMIT DROP" in text
