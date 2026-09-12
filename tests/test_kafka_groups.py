# -*- coding: utf-8 -*-
"""Расчёт лага консьюмер-групп и подготовка сброса оффсетов."""

import pytest

from modules import kafka_groups
from modules.kafka_clusters import create_cluster, delete_cluster
from modules.kafka_groups import (
    GroupBusy,
    assert_group_is_idle,
    build_groups,
    build_reset_specs,
    collect_groups,
    empty_groups,
    find_group,
    load_snapshot,
    parse_moment,
    save_snapshot,
)

# имена полей сверены со схемами kafka-python 3.x
GROUPS_META = [
    {
        "group_id": "etl-loader",
        "group_state": "Stable",
        "protocol_data": "range",
        "members": [
            {"member_id": "m-1", "client_id": "c-1",
             "client_host": "10.0.0.7",
             "member_assignment": {"assigned_partitions": [
                 {"topic": "orders", "partitions": [0, 1]}]}},
        ],
    },
    {
        "group_id": "idle-group",
        "group_state": "Empty",
        "protocol_data": "",
        "members": [],
    },
]

COMMITTED = {
    ("etl-loader", "orders", 0): 4100,
    ("etl-loader", "orders", 1): None,     # коммита не было
    ("idle-group", "orders", 0): 9000,
}

END = {("orders", 0): 9000, ("orders", 1): 700}


def test_lag_is_end_minus_committed():
    data = build_groups(GROUPS_META, COMMITTED, END)
    group = find_group(data, "etl-loader")
    topic = group["topics"][0]

    assert topic["name"] == "orders"
    assert topic["parts"][0]["lag"] == 4900
    assert group["lag"] == 4900
    assert group["members"] == 1
    assert group["state"] == "Stable"


def test_partition_without_commit_has_no_lag():
    data = build_groups(GROUPS_META, COMMITTED, END)
    part = find_group(data, "etl-loader")["topics"][0]["parts"][1]

    # «не читали» — это не «отставания нет»
    assert part["committed"] is None
    assert part["lag"] is None


def test_group_without_lag_is_zero_not_none():
    data = build_groups(GROUPS_META, COMMITTED, END)
    group = find_group(data, "idle-group")

    assert group["lag"] == 0
    assert group["state"] == "Empty"


def test_group_with_no_commits_at_all_has_null_lag():
    data = build_groups(GROUPS_META, {("etl-loader", "orders", 0): None}, END)

    assert find_group(data, "etl-loader")["lag"] is None


def test_owner_comes_from_assignment():
    data = build_groups(GROUPS_META, COMMITTED, END)
    part = find_group(data, "etl-loader")["topics"][0]["parts"][0]

    assert part["client"] == "c-1"
    assert part["host"] == "10.0.0.7"


def test_groups_sorted_by_lag_desc():
    data = build_groups(GROUPS_META, COMMITTED, END)

    assert [g["id"] for g in data["groups"]] == ["etl-loader", "idle-group"]


def test_assert_group_is_idle():
    data = build_groups(GROUPS_META, COMMITTED, END)

    with pytest.raises(GroupBusy) as err:
        assert_group_is_idle(find_group(data, "etl-loader"))

    assert "1" in str(err.value)

    assert_group_is_idle(find_group(data, "idle-group"))   # не бросает


def test_build_reset_specs_modes():
    parts = [("orders", 0), ("orders", 1)]

    assert build_reset_specs("earliest", None, parts) == {
        ("orders", 0): ("earliest", None),
        ("orders", 1): ("earliest", None),
    }
    assert build_reset_specs("latest", None, parts)[("orders", 0)] == (
        "latest", None)

    stamped = build_reset_specs("timestamp", "2026-08-30 12:00", parts)

    assert stamped[("orders", 0)][0] == "timestamp"
    assert stamped[("orders", 0)][1] == parse_moment("2026-08-30 12:00")


def test_build_reset_specs_rejects_bad_input():
    with pytest.raises(ValueError):
        build_reset_specs("earliest", None, [])

    with pytest.raises(ValueError):
        build_reset_specs("nonsense", None, [("orders", 0)])

    with pytest.raises(ValueError):
        build_reset_specs("timestamp", "не дата", [("orders", 0)])


def test_snapshot_roundtrip():
    cluster_id = create_cluster({
        "name": "Groups", "bootstrap_servers": "kfk1:9092"})

    assert load_snapshot(cluster_id) is None

    save_snapshot(cluster_id, build_groups(GROUPS_META, COMMITTED, END))
    loaded = load_snapshot(cluster_id)

    assert loaded["empty"] is False
    assert loaded["taken_at"]
    assert len(loaded["groups"]) == 2

    delete_cluster(cluster_id)

    assert load_snapshot(cluster_id) is None


def test_empty_groups_shape():
    data = empty_groups(7)

    assert data["empty"] is True
    assert data["cluster_id"] == 7
    assert data["groups"] == []
    assert data["taken_at"] is None


def test_collect_without_force_does_not_touch_cluster(monkeypatch):
    cluster_id = create_cluster({
        "name": "Cached", "bootstrap_servers": "kfk1:9092"})

    def never(*args, **kwargs):
        raise AssertionError("без force кластер трогать нельзя")

    monkeypatch.setattr(kafka_groups, "fetch_groups", never)
    save_snapshot(cluster_id, build_groups(GROUPS_META, COMMITTED, END))

    assert len(collect_groups(cluster_id)["groups"]) == 2

    delete_cluster(cluster_id)


def test_collect_with_force_asks_cluster(monkeypatch):
    cluster_id = create_cluster({
        "name": "Live", "bootstrap_servers": "kfk1:9092"})

    seen = {}

    def fake_end_offsets(cluster, pairs):
        seen["pairs"] = sorted(pairs)
        return {}, END

    monkeypatch.setattr(kafka_groups, "fetch_groups",
                        lambda c: GROUPS_META)
    monkeypatch.setattr(kafka_groups, "fetch_group_offsets",
                        lambda c, ids: COMMITTED)
    monkeypatch.setattr(kafka_groups, "fetch_offsets", fake_end_offsets)

    data = collect_groups(cluster_id, force=True)

    # концы спрашиваем только по партициям, которые кто-то читает
    assert seen["pairs"] == [("orders", 0), ("orders", 1)]
    assert find_group(data, "etl-loader")["lag"] == 4900
    assert load_snapshot(cluster_id) is not None

    delete_cluster(cluster_id)
