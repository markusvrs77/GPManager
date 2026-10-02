# -*- coding: utf-8 -*-
"""
Загрузка разницы и полная загрузка PG↔PG на фейковых соединениях:
порядок DML, одна транзакция приёмника, staging, явный список колонок.
"""

import pytest

import modules.pg_diff_load as pdl
from tests.pg_fakes import FakeConn, render

STAGE = "stg_7_1"
STAGE_REF = '"opsentri_sync_stage"."stg_7_1"'
WRITES = ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "CREATE", "DROP", "ALTER")


def _rows(n):
    return [(1,)] * n


@pytest.fixture
def columns(monkeypatch):
    """Типы колонок по сторонам: порядок в приёмнике другой."""
    state = {"src": {"id": "integer", "name": "text", "amount": "numeric"},
             "dst": {"name": "text", "amount": "numeric", "id": "integer"}}

    def fake_types(conn, schema, table):
        return dict(state[conn.side])

    monkeypatch.setattr(pdl, "table_column_types", fake_types)
    return state


def _pair(dst_responses=(), copy_out=b"1\ta\t2\n2\tb\t3\n", copy_error=None):
    src = FakeConn(copy_out=copy_out)
    src.side = "src"
    dst = FakeConn(responses=list(dst_responses), copy_error=copy_error)
    dst.side = "dst"
    # коммит — маркер в ленте запросов, чтобы видеть границы транзакций
    dst.commit = lambda: dst.executed.append(("COMMIT", None))
    return src, dst


def _texts(conn):
    return [text for text, _ in conn.executed]


def _writes(conn):
    """Лента приёмника без чтения метаданных колонок."""
    return [t for t in _texts(conn) if "attgenerated" not in t]


def _index(texts, needle):
    return next(i for i, t in enumerate(texts) if t.startswith(needle))


KEYED = [("GROUP BY", []), ("IS NULL", [(0,)]),
         ("DELETE FROM", _rows(1)), ("UPDATE", _rows(2)),
         ("INSERT INTO", _rows(3))]


def test_keyed_diff_without_delete_missing_never_deletes(columns):
    src, dst = _pair(KEYED)

    result = pdl.load_diff(src, dst, "s", "t", ["id"], False, STAGE)

    assert result == {"insert": 3, "update": 2, "delete": 0}
    assert not any("DELETE" in t for t in _texts(dst))


def test_keyed_diff_runs_delete_update_insert_in_one_transaction(columns):
    src, dst = _pair(KEYED)

    result = pdl.load_diff(src, dst, "s", "t", ["id"], True, STAGE)

    assert result == {"insert": 3, "update": 2, "delete": 1}
    texts = _texts(dst)
    order = [_index(texts, w) for w in ("DELETE FROM", "UPDATE", "INSERT INTO")]
    assert order == sorted(order)
    commits = [i for i, t in enumerate(texts) if t == "COMMIT"]
    # между DELETE и INSERT нет коммита, а сразу после INSERT — есть
    assert not [c for c in commits if order[0] < c < order[-1]]
    assert any(c > order[-1] for c in commits)
    # staging удалён после применения
    assert texts[-2].startswith("DROP TABLE IF EXISTS " + STAGE_REF)


def test_staging_gets_source_rows_with_explicit_columns(columns):
    src, dst = _pair(KEYED)

    pdl.load_diff(src, dst, "s", "t", ["id"], False, STAGE)

    assert any(t.startswith("CREATE UNLOGGED TABLE " + STAGE_REF)
               for t in _texts(dst))
    # порядок колонок — по приёмнику, в обоих концах COPY он один
    assert src.copies == ['COPY (SELECT "name", "amount", "id" FROM "s"."t") '
                          'TO STDOUT']
    assert dst.copies == ['COPY %s ("name", "amount", "id") FROM STDIN'
                          % STAGE_REF]
    assert not any(t.startswith(WRITES) for t in _texts(src))


def test_update_touches_only_rows_whose_hash_differs():
    text = render(pdl.build_key_update_sql("s", "t", STAGE, ["id"],
                                           ["id", "name", "amount"]))

    assert text.startswith('UPDATE "s"."t" AS "t" SET "name" = "s"."name", '
                           '"amount" = "s"."amount" FROM ' + STAGE_REF)
    assert ('md5(ROW("t"."amount", "t"."id", "t"."name")::text) <> '
            'md5(ROW("s"."amount", "s"."id", "s"."name")::text)') in text
    assert '"t"."id" = "s"."id"' in text


def test_insert_adds_only_new_keys():
    text = render(pdl.build_key_insert_sql("s", "t", STAGE, ["id", "day"],
                                           ["id", "day", "v"]))

    assert text.startswith('INSERT INTO "s"."t" ("id", "day", "v") SELECT '
                           '"s"."id", "s"."day", "s"."v" FROM ' + STAGE_REF)
    assert ('WHERE NOT EXISTS (SELECT 1 FROM "s"."t" AS "t" WHERE '
            '"t"."id" = "s"."id" AND "t"."day" = "s"."day")') in text


def test_delete_removes_only_keys_missing_in_source():
    text = render(pdl.build_key_delete_sql("s", "t", STAGE, ["id"]))

    assert text == ('DELETE FROM "s"."t" AS "t" WHERE "t"."id" IS NOT NULL '
                    'AND NOT EXISTS (SELECT 1 FROM %s AS "s" WHERE '
                    '"t"."id" = "s"."id")' % STAGE_REF)


def test_delete_never_removes_dest_rows_with_null_in_the_key():
    text = render(pdl.build_key_delete_sql("s", "t", STAGE, ["id", "day"]))

    # NULL = NULL не истинно: без этой проверки NOT EXISTS удалил бы строку
    assert text == ('DELETE FROM "s"."t" AS "t" WHERE "t"."id" IS NOT NULL '
                    'AND "t"."day" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM '
                    '%s AS "s" WHERE "t"."id" = "s"."id" AND "t"."day" = '
                    '"s"."day")' % STAGE_REF)


def test_staging_is_analyzed_after_copy_and_before_apply(columns):
    src, dst = _pair(KEYED)

    pdl.load_diff(src, dst, "s", "t", ["id"], True, STAGE)

    texts = _texts(dst)
    analyze = texts.index("ANALYZE " + STAGE_REF)
    assert _index(texts, "CREATE UNLOGGED TABLE") < analyze
    assert analyze < _index(texts, "SELECT count(*) FROM (SELECT 1 FROM "
                                   + STAGE_REF)
    assert analyze < _index(texts, "DELETE FROM")


def test_keyless_insert_takes_columns_from_the_cte_not_by_ctid():
    text = render(pdl.build_keyless_insert_sql("s", "t", STAGE, ["a", "b"]))

    assert "ctid" not in text
    assert ('INSERT INTO "s"."t" ("a", "b") SELECT "x"."a", "x"."b" FROM "x" '
            'JOIN "m" ON "m"."h" = "x"."_pgcmp_h" WHERE "x"."_pgcmp_rn" <= '
            '"m"."c"') in text


def test_full_load_without_truncate_refuses_a_non_empty_table(columns):
    src, dst = _pair([("LIMIT 1", [(1,)])])

    with pytest.raises(pdl.NotEmptyError) as err:
        pdl.load_full(src, dst, "s", "t", truncate=False, require_empty=True)

    assert "уже есть строки" in str(err.value)
    assert dst.copies == [] and "COMMIT" not in _texts(dst)
    assert dst.rollbacks >= 1


def test_full_load_into_an_empty_table_passes_the_check(columns):
    src, dst = _pair()

    pdl.load_full(src, dst, "s", "t", truncate=False, require_empty=True)

    assert len(dst.copies) == 1 and "COMMIT" in _texts(dst)


def test_sequences_move_forward_only_from_the_table_max():
    dst = FakeConn(responses=[("pg_get_serial_sequence",
                               [("id", "s.t_id_seq")])])

    warnings = pdl.sync_sequences(dst, "s", "t")

    assert warnings == []
    text, params = next((t, p) for t, p in dst.executed if "setval" in t)
    assert 'max("id")' in text and 'FROM "s"."t"' in text
    # пустая таблица (max IS NULL) и движение назад — без setval
    assert "IS NOT NULL" in text
    assert "> COALESCE(pg_sequence_last_value(" in text
    assert params == ("s.t_id_seq", "s.t_id_seq", "s.t_id_seq")
    assert dst.commits == 1


def test_sequence_error_is_only_a_warning():
    def denied(_params):
        raise RuntimeError("permission denied for sequence t_id_seq")

    dst = FakeConn(responses=[("pg_get_serial_sequence",
                               [("id", "s.t_id_seq")]), ("setval", denied)])

    warnings = pdl.sync_sequences(dst, "s", "t")

    assert len(warnings) == 1 and "t_id_seq" in warnings[0]
    assert dst.rollbacks >= 1


def test_duplicate_keys_in_staging_fail_and_leave_dest_untouched(columns):
    src, dst = _pair([("GROUP BY", [(4,)])] + KEYED[1:])

    with pytest.raises(pdl.DuplicateKeyError) as err:
        pdl.load_diff(src, dst, "s", "t", ["id"], True, STAGE)

    assert "не уникален" in str(err.value)
    texts = _texts(dst)
    assert not any(t.startswith(("DELETE", "UPDATE", "INSERT")) for t in texts)
    assert dst.rollbacks >= 1
    assert texts[-2].startswith("DROP TABLE IF EXISTS " + STAGE_REF)


def test_null_key_values_fail_the_table(columns):
    src, dst = _pair([("GROUP BY", []), ("IS NULL", [(1,)])] + KEYED[2:])

    with pytest.raises(ValueError):
        pdl.load_diff(src, dst, "s", "t", ["id"], False, STAGE)

    assert not any(t.startswith(("UPDATE", "INSERT")) for t in _texts(dst))


def test_staging_is_dropped_when_the_copy_fails(columns):
    src, dst = _pair(KEYED, copy_error=RuntimeError("disk full"))

    with pytest.raises(RuntimeError):
        pdl.load_diff(src, dst, "s", "t", ["id"], False, STAGE)

    texts = _texts(dst)
    assert dst.rollbacks >= 1
    assert texts[-2].startswith("DROP TABLE IF EXISTS " + STAGE_REF)
    assert texts[-1] == "COMMIT"


def test_structure_change_since_compare_is_refused(columns):
    columns["dst"]["extra"] = "text"
    src, dst = _pair(KEYED)

    with pytest.raises(ValueError) as err:
        pdl.load_diff(src, dst, "s", "t", ["id"], False, STAGE)

    assert "extra" in str(err.value)
    assert dst.copies == []


def test_delete_missing_must_be_a_real_true(columns):
    src, dst = _pair(KEYED)

    pdl.load_diff(src, dst, "s", "t", ["id"], "true", STAGE)

    assert not any("DELETE" in t for t in _texts(dst))


# ---------------------------------------------------------------- без ключа

def test_keyless_insert_is_a_multiset_difference():
    text = render(pdl.build_keyless_insert_sql("s", "t", STAGE, ["a", "b"]))

    assert "EXCEPT ALL" in text
    assert "DELETE" not in text
    # копий вставляется ровно столько, сколько не хватает
    assert "row_number() OVER (PARTITION BY" in text
    assert text.index("EXCEPT ALL") < text.index('INSERT INTO "s"."t" ("a", "b")')


def test_keyless_delete_removes_only_surplus_copies_by_ctid():
    text = render(pdl.build_keyless_delete_sql("s", "t", STAGE, ["a", "b"]))

    assert text.count("EXCEPT ALL") == 1
    assert "row_number() OVER (PARTITION BY" in text
    # ctid уникален только внутри партиции — сверяем и tableoid
    assert '"t"."ctid" = "x"."tid"' in text
    assert '"t"."tableoid" = "x"."toid"' in text
    assert '"x"."rn" <= "m"."c"' in text


def test_keyless_diff_without_delete_missing_only_inserts(columns):
    src, dst = _pair([("INSERT INTO", _rows(2)), ("DELETE", _rows(9))])

    result = pdl.load_diff(src, dst, "s", "t", [], False, STAGE)

    assert result == {"insert": 2, "update": 0, "delete": 0}
    assert not any("DELETE" in t for t in _texts(dst))


def test_keyless_diff_with_delete_missing_deletes_then_inserts(columns):
    src, dst = _pair([("DELETE", _rows(1)), ("INSERT INTO", _rows(2))])

    result = pdl.load_diff(src, dst, "s", "t", [], True, STAGE)

    assert result == {"insert": 2, "update": 0, "delete": 1}
    texts = _texts(dst)
    assert _index(texts, "WITH") < max(i for i, t in enumerate(texts)
                                       if "INSERT INTO" in t)


# ---------------------------------------------------------------- полная

def test_full_load_truncates_and_copies_in_one_transaction(columns):
    src, dst = _pair()

    result = pdl.load_full(src, dst, "s", "t", truncate=True)

    assert result == {"rows": 2}
    assert _writes(dst) == ['TRUNCATE TABLE "s"."t"', "COMMIT"]
    assert dst.copies == ['COPY "s"."t" ("name", "amount", "id") FROM STDIN']
    assert src.copies == ['COPY (SELECT "name", "amount", "id" FROM "s"."t") '
                          'TO STDOUT']


def test_full_load_rolls_back_on_copy_error(columns):
    src, dst = _pair(copy_error=RuntimeError("bad row"))

    with pytest.raises(RuntimeError):
        pdl.load_full(src, dst, "s", "t", truncate=True)

    assert "COMMIT" not in _texts(dst)
    assert dst.rollbacks >= 1


def test_full_load_without_truncate_only_copies(columns):
    src, dst = _pair()

    pdl.load_full(src, dst, "s", "t", truncate=False)

    assert _writes(dst) == ["COMMIT"]
    assert len(dst.copies) == 1


# ------------------------------------------------- identity и вычисляемые

# total — GENERATED ALWAYS AS (...) STORED, id — GENERATED ALWAYS AS IDENTITY
FLAGS = [("attgenerated", [("name", "", ""), ("amount", "", ""),
                           ("id", "", "a"), ("total", "s", "")])]


@pytest.fixture
def generated(columns):
    for side in ("src", "dst"):
        columns[side]["total"] = "numeric"
    return columns


def test_stage_is_a_plain_copy_of_the_writable_columns():
    text = render(pdl.build_create_stage_sql("s", "t", STAGE, ["id", "v"]))

    # без LIKE: ни NOT NULL, ни identity, ни выражений generated
    assert text == ('CREATE UNLOGGED TABLE %s AS SELECT "id", "v" FROM '
                    '"s"."t" WITH NO DATA' % STAGE_REF)


def test_insert_into_identity_always_overrides_system_value():
    text = render(pdl.build_key_insert_sql("s", "t", STAGE, ["id"],
                                           ["id", "v"], overriding=True))

    assert text.startswith('INSERT INTO "s"."t" ("id", "v") OVERRIDING SYSTEM '
                           'VALUE SELECT "s"."id", "s"."v" FROM ' + STAGE_REF)


def test_keyless_insert_into_identity_always_overrides_system_value():
    text = render(pdl.build_keyless_insert_sql("s", "t", STAGE, ["a", "b"],
                                               overriding=True))

    assert ('INSERT INTO "s"."t" ("a", "b") OVERRIDING SYSTEM VALUE SELECT '
            '"x"."a", "x"."b"') in text


def test_update_never_sets_identity_always_outside_the_key():
    text = render(pdl.build_key_update_sql("s", "t", STAGE, ["code"],
                                           ["id", "code", "v"], skip=["id"]))

    assert text == (
        'UPDATE "s"."t" AS "t" SET "v" = "s"."v" FROM %s AS "s" WHERE '
        '"t"."code" = "s"."code" AND md5(ROW("t"."code", "t"."v")::text) <> '
        'md5(ROW("s"."code", "s"."v")::text)' % STAGE_REF)


def test_diff_skips_generated_columns_and_overrides_identity(generated):
    src, dst = _pair(FLAGS + KEYED)

    pdl.load_diff(src, dst, "s", "t", ["name"], False, STAGE)

    assert src.copies == ['COPY (SELECT "name", "amount", "id" FROM "s"."t") '
                          'TO STDOUT']
    assert dst.copies == ['COPY %s ("name", "amount", "id") FROM STDIN'
                          % STAGE_REF]
    texts = _texts(dst)
    update = texts[_index(texts, "UPDATE")]
    insert = texts[_index(texts, "INSERT INTO")]
    assert update.startswith('UPDATE "s"."t" AS "t" SET "amount" = "s"."amount"'
                             ' FROM ')
    assert insert.startswith('INSERT INTO "s"."t" ("name", "amount", "id") '
                             'OVERRIDING SYSTEM VALUE SELECT')
    assert not any('"total"' in t for t in texts if "attgenerated" not in t)


def test_full_load_skips_generated_and_keeps_identity_values(generated):
    src, dst = _pair(FLAGS)

    pdl.load_full(src, dst, "s", "t", truncate=True)

    # COPY FROM пишет значения identity ALWAYS как есть
    assert dst.copies == ['COPY "s"."t" ("name", "amount", "id") FROM STDIN']
    assert src.copies == ['COPY (SELECT "name", "amount", "id" FROM "s"."t") '
                          'TO STDOUT']


def test_column_flags_are_read_from_dest_in_one_query():
    dst = FakeConn(responses=FLAGS)

    flags = pdl.dest_column_flags(dst, "s", "t")

    assert flags["total"] == {"generated": True, "identity_always": False}
    assert flags["id"] == {"generated": False, "identity_always": True}
    assert len(dst.executed) == 1
    assert dst.executed[0][1] == ("s", "t")
