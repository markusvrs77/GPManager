# -*- coding: utf-8 -*-
"""
Загрузка разницы по несовпавшим диапазонам сравнения (п.9 спеки):
staging, DELETE и разность без ключа ограничены предикатами листьев.
"""

import pytest

import modules.pg_diff_load as pdl
from tests.pg_fakes import FakeConn

STAGE = "stg_7_1"
STAGE_REF = '"opsentri_sync_stage"."stg_7_1"'

# листья, как их отдаёт pg_compare.get_mismatched_ranges
LEAVES = [
    {"column": "id", "lo": 100, "hi": 200, "is_null": False, "depth": 1,
     "collate_c": False},
    {"column": "id", "lo": None, "hi": 5, "is_null": False, "depth": 0,
     "collate_c": False},
]
LEAF_PREDS = ('("id" >= 100 AND "id" < 200) OR ("id" < 5)')


@pytest.fixture
def columns(monkeypatch):
    types = {"id": "integer", "name": "text"}
    monkeypatch.setattr(pdl, "table_column_types",
                        lambda conn, schema, table: dict(types))


def _pair(dst_responses=()):
    src = FakeConn(copy_out=b"101\ta\n")
    dst = FakeConn(responses=list(dst_responses))
    dst.commit = lambda: dst.executed.append(("COMMIT", None))
    return src, dst


def _texts(conn):
    return [text for text, _ in conn.executed]


KEYED = [("GROUP BY", []), ("IS NULL", [(0,)]),
         ("DELETE FROM", [(1,)]), ("UPDATE", [(1,)] * 2),
         ("INSERT INTO", [(1,)] * 3)]


def test_staging_copies_only_source_rows_of_the_leaves(columns):
    src, dst = _pair(KEYED)

    pdl.load_diff(src, dst, "s", "t", ["id"], False, STAGE, ranges=LEAVES)

    assert src.copies == ['COPY (SELECT "id", "name" FROM "s"."t" WHERE '
                          + LEAF_PREDS + ') TO STDOUT']


def _one(texts, needle):
    found = [t for t in texts if t.startswith(needle)]
    assert len(found) == 1, found
    return found[0]


def test_keyed_delete_touches_only_dest_rows_inside_the_leaves(columns):
    src, dst = _pair(KEYED)

    result = pdl.load_diff(src, dst, "s", "t", ["id"], True, STAGE,
                           ranges=LEAVES)

    assert result == {"insert": 3, "update": 2, "delete": 1}
    delete = _one(_texts(dst), "DELETE FROM")
    assert delete == (
        'DELETE FROM "s"."t" AS "t" WHERE "t"."id" IS NOT NULL AND '
        '(("t"."id" >= 100 AND "t"."id" < 200) OR ("t"."id" < 5)) AND NOT '
        'EXISTS (SELECT 1 FROM ' + STAGE_REF + ' AS "s" WHERE '
        '"t"."id" = "s"."id")')
    # DELETE, UPDATE, INSERT — одна транзакция приёмника
    texts = _texts(dst)
    first = texts.index(delete)
    last = next(i for i, t in enumerate(texts) if t.startswith("INSERT INTO"))
    assert "COMMIT" not in texts[first:last]


def test_without_delete_missing_there_is_no_delete_in_leaves_either(columns):
    src, dst = _pair(KEYED)

    result = pdl.load_diff(src, dst, "s", "t", ["id"], False, STAGE,
                           ranges=LEAVES)

    assert result["delete"] == 0
    assert not any("DELETE" in t for t in _texts(dst))


TEXT_LEAF = [{"column": "code", "lo": "A", "hi": "b", "is_null": False,
              "depth": 2, "collate_c": True},
             {"column": "code", "lo": None, "hi": None, "is_null": True,
              "depth": 0, "collate_c": True}]


def test_keyless_insert_and_delete_compare_the_same_leaves_on_both_sides(
        columns):
    src, dst = _pair([("DELETE FROM", [(1,)]), ("INSERT INTO", [(1,)] * 2)])

    result = pdl.load_diff(src, dst, "s", "t", [], True, STAGE,
                           ranges=TEXT_LEAF)

    assert result == {"insert": 2, "update": 0, "delete": 1}
    pred = ('("{a}"."code" COLLATE "C" >= \'A\' AND "{a}"."code" COLLATE "C"'
            ' < \'b\') OR ("{a}"."code" IS NULL)')
    on_t, on_s = pred.format(a="t"), pred.format(a="s")
    assert src.copies == ['COPY (SELECT "id", "name" FROM "s"."t" WHERE '
                          + pred.replace('"{a}".', "") + ') TO STDOUT']

    delete, insert = [t for t in _texts(dst) if t.startswith("WITH")]
    assert "DELETE FROM" in delete and "INSERT INTO" in insert
    for text in (delete, insert):
        # EXCEPT ALL: приёмник и staging — в одних и тех же листьях
        assert 'AS "t" WHERE ' + on_t in text
        assert 'AS "s" WHERE ' + on_s in text
    # нумерация копий для удаления — только строки приёмника в листьях
    assert delete.count('AS "t" WHERE ' + on_t) == 2


# ------------------------------------------------------------------
# Раннер: листья берутся из сравнения compare_job_id
# ------------------------------------------------------------------

import modules.pg_compare as cmp  # noqa: E402
import modules.pg_sync_common as common  # noqa: E402
from job_manager import create_job, get_job_items  # noqa: E402


@pytest.fixture
def runner(monkeypatch):
    state = {"diff": [], "copied": []}

    def fake_diff(src, dst, schema, table, key_columns, delete_missing,
                  stage_name, ranges=None):
        state["diff"].append((table, ranges))
        return {"insert": 1, "update": 2, "delete": 3}

    monkeypatch.setattr(pdl, "open_pg",
                        lambda cid, readonly=False: FakeConn())
    monkeypatch.setattr(pdl, "load_diff", fake_diff)
    monkeypatch.setattr(pdl, "sync_sequences", lambda conn, s, t: [])
    monkeypatch.setattr(common, "is_stop_requested", lambda job_id: False)
    return state


def _compared(rows, leaves=()):
    """Сравнение: строки результата и листья differs (как пишет pg_compare)."""
    job_id = create_job("pg_compare", 11, {
        "source_connection_id": 11, "dest_connection_id": 12, "tables": []})
    for row in rows:
        cmp.save_result(job_id, dict({"schema": "s", "key_columns": ["id"],
                                      "key_source": "pk"}, **row))
    for table, rng in leaves:
        run = {"schema": "s", "table": table,
               "column": {"name": "id", "collate_c": False}}
        cmp.save_range(job_id, run, rng, {
            "status": "differs", "src_rows": 10, "dst_rows": 9,
            "to_insert": 1, "to_update": 0, "to_delete": 0})
    return job_id


def _load(compare_job_id, tables):
    job_id = create_job("pg_diff_load", 11, {
        "source_connection_id": 11, "dest_connection_id": 12,
        "compare_job_id": compare_job_id, "delete_missing": True,
        "tables": [{"schema": "s", "table": t, "action": "diff",
                    "key_columns": ["id"], "in_dst": True} for t in tables],
        "expected": []})
    pdl.run_pg_diff_load_job(job_id)
    return {i["table_name"]: (i["status"], i["error_message"])
            for i in get_job_items(job_id)}


CHUNKED = {"checked": 70, "total": 70, "mismatched": 2}


def test_runner_loads_a_chunked_table_only_by_its_leaves(runner):
    compared = _compared(
        [{"table": "big", "status": "differs", "to_insert": 2,
          "chunked": CHUNKED}],
        [("big", {"lo": 100, "hi": 200, "depth": 1}),
         ("big", {"lo": None, "hi": None, "is_null": True, "depth": 0})])

    items = _load(compared, ["big"])

    assert items == {"big": ("done", "insert=1; update=2; delete=3; "
                                     "по диапазонам: 2")}
    (table, ranges), = runner["diff"]
    assert [(r["column"], r["lo"], r["hi"], r["is_null"], r["collate_c"])
            for r in ranges] == [("id", 100, 200, False, False),
                                 ("id", None, None, True, False)]


def test_runner_skips_a_big_table_whose_ranges_all_matched(runner):
    compared = _compared([{"table": "big", "status": "same", "to_insert": 0,
                           "chunked": dict(CHUNKED, mismatched=0)}])

    items = _load(compared, ["big"])

    assert items == {"big": ("done", "insert=0; update=0; delete=0; "
                                     "по диапазонам: 0")}
    assert runner["diff"] == []


def test_runner_loads_a_small_table_as_before(runner):
    compared = _compared([{"table": "small", "status": "differs",
                           "to_insert": 1}])

    items = _load(compared, ["small"])

    assert items == {"small": ("done", "insert=1; update=2; delete=3")}
    assert runner["diff"] == [("small", None)]
