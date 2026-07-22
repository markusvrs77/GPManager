from modules.table_catalog import (
    resolve_keys_hierarchy,
    choose_candidate_columns,
    pick_columns_with_fallback,
    filter_candidates_by_stats,
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


def test_filter_candidates_by_stats():
    candidates = ["id", "code", "region_id", "note", "fresh_col"]
    stats = {
        "id": {"n_distinct": -1.0, "null_frac": 0.0},        # уникальна по статистике
        "code": {"n_distinct": -1.0, "null_frac": 0.02},      # есть NULL — мимо
        "region_id": {"n_distinct": 40.0, "null_frac": 0.0},  # 40 значений на 1М строк — мимо
        "note": {"n_distinct": -0.3, "null_frac": 0.0},       # 30% уникальных — мимо
        # fresh_col: статистики нет (не ANALYZEd) — оставляем на проверку
    }

    keep, rejected = filter_candidates_by_stats(candidates, stats, reltuples=1000000)

    assert keep == ["id", "fresh_col"]  # stats-уникальная первой, без статистики — после
    assert {r["column"] for r in rejected} == {"code", "region_id", "note"}
    reasons = {r["column"]: r["reason"] for r in rejected}
    assert reasons["code"] == "nulls"
    assert reasons["region_id"] == "low_cardinality"
