# -*- coding: utf-8 -*-
"""
Сравнение двух баз PostgreSQL: раскрытие выбора и подсчёт разницы.
Живой базы нет: фейковые соединения отвечают по подстроке SQL.
"""

import pytest

import modules.pg_compare as cmp
from tests.pg_fakes import FakeConn, render


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
           copy_out=b"1\ta\n2\tb\n3\tc\n", dst_dup=0,
           dst_unique=None):
    src = FakeConn(responses=[("a.attname", list(src_cols))],
                   copy_out=copy_out)
    dst = FakeConn(responses=[
        # каталог приёмника: есть ли PK / уникальный NOT NULL индекс на ключе
        ("indisunique", [] if dst_unique is None else [(dst_unique,)]),
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


def test_keyless_compare_counts_hash_multiplicities_in_one_pass():
    src, dst = _sides(counts=(3, 1, 0, 1))

    result = cmp.compare_table(src, dst, "s", "t", [])

    assert result["status"] == "differs"
    assert (result["to_insert"], result["to_update"],
            result["to_delete"]) == (1, 0, 1)
    assert 'CREATE TEMP TABLE "pgcmp_src" ("h" text)' in dst.sql_text()
    assert "EXCEPT ALL" not in dst.sql_text()
    assert "HAVING" not in dst.sql_text()


HASH = 'md5(ROW("t"."amount", "t"."id", "t"."name")::text)'


def test_keyed_count_is_one_full_outer_join_with_filtered_counts():
    text = render(cmp.build_count_sql("s", "t", ["id", "name"],
                                      ["id", "name", "amount"]))

    # строка без пары в d — вставка, без пары в s — удаление,
    # пара с другим хешем — изменение; d.h (md5) не бывает NULL
    assert text == (
        'SELECT count("d"."h") AS dst_rows, '
        'count(*) FILTER (WHERE "d"."h" IS NULL) AS to_insert, '
        'count(*) FILTER (WHERE "s"."h" <> "d"."h") AS to_update, '
        'count(*) FILTER (WHERE "s"."h" IS NULL) AS to_delete '
        'FROM "pgcmp_src" AS "s" FULL OUTER JOIN '
        '(SELECT "t"."id"::text AS "k0", "t"."name"::text AS "k1", '
        + HASH + ' AS "h" FROM "s"."t" AS "t") AS "d" '
        'ON "s"."k0" = "d"."k0" AND "s"."k1" = "d"."k1"'
    )


def test_keyless_count_aggregates_both_sides_by_hash_once():
    text = render(cmp.build_count_sql("s", "t", [],
                                      ["id", "name", "amount"]))

    # лишние копии хеша в источнике — вставка, в приёмнике — удаление
    assert text == (
        'SELECT sum("dc") AS dst_rows, '
        'sum(greatest("sc" - "dc", 0)) AS to_insert, 0 AS to_update, '
        'sum(greatest("dc" - "sc", 0)) AS to_delete '
        'FROM (SELECT "h", '
        'count(*) FILTER (WHERE "side" = 1) AS "sc", '
        'count(*) FILTER (WHERE "side" = 2) AS "dc" '
        'FROM (SELECT "h", 1 AS "side" FROM "pgcmp_src" '
        'UNION ALL SELECT ' + HASH + ', 2 FROM "s"."t" AS "t") AS "u" '
        'GROUP BY "h") AS "x"'
    )


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


def test_temp_table_is_analyzed_before_counting():
    src, dst = _sides()

    cmp.compare_table(src, dst, "s", "t", ["id"])

    text = dst.sql_text()
    assert 'ANALYZE "pg_temp"."pgcmp_src"' in text
    assert (text.index("CREATE TEMP") < text.index("ANALYZE")
            < text.index("AS to_insert"))


def test_work_mem_is_raised_only_for_the_dest_transaction():
    src, dst = _sides()

    cmp.compare_table(src, dst, "s", "t", [])

    text = dst.sql_text()
    assert cmp.WORK_MEM == "256MB"
    assert "SET LOCAL work_mem = '256MB'" in text
    assert text.index("SET LOCAL work_mem") < text.index("AS to_insert")
    # SET LOCAL уходит вместе с откатом; источник не трогаем
    assert dst.commits == 0 and dst.rollbacks >= 1
    assert "work_mem" not in src.sql_text()


SRC_DUP_SQL = 'FROM "pgcmp_src" GROUP BY'
DST_DUP_SQL = 'FROM "s"."t" AS "t" GROUP BY'


@pytest.mark.parametrize("key_source", ["pk", "unique_index"])
def test_source_duplicate_check_is_skipped_for_a_source_unique_key(key_source):
    # ответ «2 дубля» был бы, если бы проверку запустили
    src, dst = _sides(dup=2, counts=(3, 0, 0, 0))

    result = cmp.compare_table(src, dst, "s", "t", ["id"],
                               key_source=key_source)

    assert result["status"] == "same"
    assert SRC_DUP_SQL not in dst.sql_text()


@pytest.mark.parametrize("key_source", ["sync_keys", None])
def test_source_duplicate_check_stays_for_other_keys(key_source):
    src, dst = _sides(dup=2)

    result = cmp.compare_table(src, dst, "s", "t", ["id"],
                               key_source=key_source)

    assert result["status"] == "duplicate_keys"
    assert "источник" in result["message"]


def test_dest_duplicate_check_is_skipped_when_dest_key_is_unique():
    src, dst = _sides(dst_dup=3, dst_unique=True, counts=(3, 0, 0, 0))

    result = cmp.compare_table(src, dst, "s", "t", ["id"],
                               key_source="sync_keys")

    assert result["status"] == "same"
    assert DST_DUP_SQL not in dst.sql_text()
    params = [p for text, p in dst.executed if "indisunique" in text]
    assert len(params) == 1
    assert "s" in params[0] and "t" in params[0] and ["id"] in params[0]


def test_dest_duplicate_check_stays_when_dest_key_is_not_unique():
    src, dst = _sides(dst_dup=3, dst_unique=False)

    result = cmp.compare_table(src, dst, "s", "t", ["id"], key_source="pk")

    assert result["status"] == "duplicate_keys"
    assert "приёмник" in result["message"]
    assert SRC_DUP_SQL not in dst.sql_text()
