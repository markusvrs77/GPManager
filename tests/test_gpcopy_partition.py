from modules.gpcopy_partition import classify_partition_diff, partitions_to_copy


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
