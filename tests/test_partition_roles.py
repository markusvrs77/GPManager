from modules.table_catalog import classify_partition_roles


def test_classify_regular_parent_partition():
    tables = [
        ("s", "plain"),
        ("s", "fact"),            # родитель
        ("s", "fact_1_prt_1"),    # партиция
        ("s", "fact_1_prt_2"),
    ]
    child_parent = {
        ("s", "fact_1_prt_1"): ("s", "fact"),
        ("s", "fact_1_prt_2"): ("s", "fact"),
    }

    roles = classify_partition_roles(tables, child_parent)

    assert roles[("s", "plain")] == {"kind": "regular", "root": None, "partitions": 0}
    assert roles[("s", "fact")]["kind"] == "parent"
    assert roles[("s", "fact")]["partitions"] == 2
    assert roles[("s", "fact_1_prt_1")] == {
        "kind": "partition", "root": ("s", "fact"), "partitions": 0,
    }


def test_classify_subpartitions_roll_up_to_root():
    tables = [("s", "fact"), ("s", "fact_m1"), ("s", "fact_m1_d1")]
    child_parent = {
        ("s", "fact_m1"): ("s", "fact"),
        ("s", "fact_m1_d1"): ("s", "fact_m1"),
    }

    roles = classify_partition_roles(tables, child_parent)

    # промежуточный уровень — тоже партиция, корень у всех один
    assert roles[("s", "fact_m1")]["kind"] == "partition"
    assert roles[("s", "fact_m1")]["root"] == ("s", "fact")
    assert roles[("s", "fact_m1_d1")]["root"] == ("s", "fact")
    assert roles[("s", "fact")]["partitions"] == 2
