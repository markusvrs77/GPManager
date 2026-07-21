from modules.table_catalog import (
    resolve_keys_hierarchy,
    choose_candidate_columns,
    pick_columns_with_fallback,
)


def test_keys_pk_wins_over_unique():
    pk = {("s", "a"): ["id"]}
    uniq = {("s", "a"): [["code"], ["guid", "dt"]], ("s", "b"): [["guid", "dt"], ["code"]]}

    resolved, unresolved = resolve_keys_hierarchy(
        [("s", "a"), ("s", "b"), ("s", "c")], pk, uniq
    )

    assert resolved[("s", "a")] == {"columns": ["id"], "source": "pk"}
    # без PK — кратчайший уникальный индекс
    assert resolved[("s", "b")] == {"columns": ["code"], "source": "unique_index"}
    assert unresolved == [("s", "c")]


def test_candidate_columns_ordered_by_heuristics():
    columns = [
        ("amount", "numeric"), ("client_id", "bigint"), ("id", "bigint"),
        ("note", "text"), ("guid", "uuid"), ("created_at", "timestamp"),
    ]
    out = choose_candidate_columns(columns, limit=3)

    # id-подобные и uuid раньше произвольного текста; numeric-метрики в конце
    assert out[0] == "id"
    assert "guid" in out[:3]
    assert "amount" not in out


def test_pick_columns_with_fallback_any_date():
    columns_by_table = {
        ("s", "a"): ["date_change$"],
        ("s", "b"): [],          # приоритет не нашёл
        ("s", "c"): [],
    }
    date_cols = {("s", "b"): ["dt_load", "some_ts"]}

    resolved, missing = pick_columns_with_fallback(
        columns_by_table, ["date_change$"], date_cols
    )

    assert resolved[("s", "a")] == {"column": "date_change$", "via": "priority"}
    assert resolved[("s", "b")] == {"column": "dt_load", "via": "fallback_date"}
    assert missing == [("s", "c")]
