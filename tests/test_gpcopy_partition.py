from modules.gpcopy_partition import (
    classify_partition_diff,
    partitions_to_copy,
    build_batched_count_sql,
    classify_stats_maps,
)


def test_missing_in_dest_is_copy_missing():
    rows = classify_partition_diff({"p1": 100}, {})
    assert rows[0]["action"] == "copy_missing"
    assert rows[0]["dest_count"] is None


def test_different_count_is_copy_changed():
    rows = classify_partition_diff({"p1": 100}, {"p1": 90})
    assert rows[0]["action"] == "copy_changed"
    assert rows[0]["src_count"] == 100
    assert rows[0]["dest_count"] == 90


def test_equal_count_is_skip():
    rows = classify_partition_diff({"p1": 100}, {"p1": 100})
    assert rows[0]["action"] == "skip"


def test_partitions_to_copy_filters_skip():
    rows = classify_partition_diff(
        {"p1": 100, "p2": 50, "p3": 7},
        {"p1": 100, "p2": 40},  # p1 equal, p2 changed, p3 missing
    )
    to_copy = partitions_to_copy(rows)
    assert set(to_copy) == {"p2", "p3"}


def test_empty_diff_returns_nothing_to_copy():
    rows = classify_partition_diff({"p1": 5}, {"p1": 5})
    assert partitions_to_copy(rows) == []


def test_batched_count_sql_chunks_and_quotes():
    leaves = [("s", "p{}".format(i)) for i in range(5)]
    chunks = build_batched_count_sql(leaves, chunk=2)

    assert len(chunks) == 3  # 2 + 2 + 1
    assert 'FROM "s"."p0"' in chunks[0]
    assert "UNION ALL" in chunks[0]
    assert "'p4'" in chunks[2]


def test_classify_stats_maps_per_root():
    src = {
        ("s", "fact"): {"fp1": {"schema": "s", "table": "fp1", "rows": 10},
                        "fp2": {"schema": "s", "table": "fp2", "rows": 20}},
        ("s", "dim"): {"dp1": {"schema": "s", "table": "dp1", "rows": 5}},
    }
    dest = {
        ("s", "fact"): {"fp1": {"schema": "s", "table": "fp1", "rows": 10}},
        # dim вообще нет в dest
    }

    out = classify_stats_maps(src, dest)

    fact = {r["partition"]: r["action"] for r in out[("s", "fact")]}
    assert fact == {"fp1": "skip", "fp2": "copy_missing"}
    dim = {r["partition"]: r["action"] for r in out[("s", "dim")]}
    assert dim == {"dp1": "copy_missing"}
