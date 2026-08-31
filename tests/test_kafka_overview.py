# -*- coding: utf-8 -*-
"""Сборка среза кластера и его хранение в SQLite."""

from modules import kafka_overview
from modules.kafka_clusters import create_cluster, delete_cluster
from modules.kafka_overview import (
    build_overview,
    collect_overview,
    empty_overview,
    load_snapshot,
    save_snapshot,
)

CLUSTER_META = {
    "cluster_id": "MkU3OEVBNTcwNTJENDM2Qk",
    "controller_id": 1,
    "brokers": [
        {"node_id": 1, "host": "kfk1", "port": 9092, "rack": None},
        {"node_id": 2, "host": "kfk2", "port": 9092, "rack": "b"},
    ],
}

TOPICS_META = [
    {
        "topic": "orders",
        "is_internal": False,
        "partitions": [
            {"partition": 0, "leader": 1, "replicas": [1, 2], "isr": [1, 2]},
            {"partition": 1, "leader": 2, "replicas": [1, 2], "isr": [2]},
        ],
    },
    {
        "topic": "__consumer_offsets",
        "is_internal": True,
        "partitions": [
            {"partition": 0, "leader": 1, "replicas": [1], "isr": [1]},
        ],
    },
]

BEGIN = {("orders", 0): 0, ("orders", 1): 100, ("__consumer_offsets", 0): 0}
END = {("orders", 0): 500, ("orders", 1): 700, ("__consumer_offsets", 0): 3}

# kafka-python 3.x переименовала поля ответа; имена ниже сверены со схемами
# самой библиотеки (kafka/protocol/metadata и kafka/protocol/admin/cluster)
CLUSTER_META_V3 = {
    "cluster_id": "MkU3OEVBNTcwNTJENDM2Qk",
    "controller_id": 1,
    "brokers": [
        {"broker_id": 1, "host": "kfk1", "port": 9092, "rack": None},
        {"broker_id": 2, "host": "kfk2", "port": 9092, "rack": "b"},
    ],
}

TOPICS_META_V3 = [
    {
        "name": "orders",
        "is_internal": False,
        "partitions": [
            {"partition_index": 0, "leader_id": 1,
             "replica_nodes": [1, 2], "isr_nodes": [1, 2]},
            {"partition_index": 1, "leader_id": 2,
             "replica_nodes": [1, 2], "isr_nodes": [2]},
        ],
    },
]


def test_build_overview_counts_messages():
    data = build_overview(CLUSTER_META, TOPICS_META, BEGIN, END)

    orders = [t for t in data["topics"] if t["name"] == "orders"][0]

    assert data["controller_id"] == 1
    assert len(data["brokers"]) == 2
    assert data["brokers"][0]["host"] == "kfk1"
    assert orders["partitions"] == 2
    assert orders["replication"] == 2
    # (500 - 0) + (700 - 100)
    assert orders["messages"] == 1100


def test_build_overview_reads_kafka_python_3_field_names():
    data = build_overview(CLUSTER_META_V3, TOPICS_META_V3, BEGIN, END)

    assert [b["id"] for b in data["brokers"]] == [1, 2]
    assert data["brokers"][1]["rack"] == "b"

    orders = data["topics"][0]

    assert orders["name"] == "orders"
    assert orders["partitions"] == 2
    assert orders["replication"] == 2
    assert orders["messages"] == 1100
    assert orders["under_replicated"] is True
    assert orders["parts"][0]["leader"] == 1
    assert orders["parts"][0]["replicas"] == [1, 2]
    assert orders["parts"][1]["isr"] == [2]


def test_build_overview_marks_under_replicated():
    data = build_overview(CLUSTER_META, TOPICS_META, BEGIN, END)

    orders = [t for t in data["topics"] if t["name"] == "orders"][0]

    # у партиции 1 в ISR только одна реплика из двух
    assert orders["under_replicated"] is True


def test_build_overview_marks_internal_and_sorts():
    data = build_overview(CLUSTER_META, TOPICS_META, BEGIN, END)

    internal = [t for t in data["topics"]
                if t["name"] == "__consumer_offsets"][0]

    assert internal["internal"] is True
    # сортировка по имени: подчёркивания идут раньше букв
    assert data["topics"][0]["name"] == "__consumer_offsets"


def test_build_overview_survives_missing_offsets():
    data = build_overview(CLUSTER_META, TOPICS_META, {}, {})

    orders = [t for t in data["topics"] if t["name"] == "orders"][0]

    assert orders["messages"] == 0


def test_snapshot_roundtrip():
    cluster_id = create_cluster({
        "name": "Snap", "bootstrap_servers": "kfk1:9092"})

    assert load_snapshot(cluster_id) is None

    data = build_overview(CLUSTER_META, TOPICS_META, BEGIN, END)
    save_snapshot(cluster_id, data)

    loaded = load_snapshot(cluster_id)

    assert loaded["empty"] is False
    assert loaded["taken_at"]
    assert len(loaded["topics"]) == 2

    delete_cluster(cluster_id)

    assert load_snapshot(cluster_id) is None


def test_empty_overview_shape():
    data = empty_overview(42)

    assert data["empty"] is True
    assert data["cluster_id"] == 42
    assert data["topics"] == []
    assert data["taken_at"] is None


def test_collect_reads_snapshot_without_force(monkeypatch):
    cluster_id = create_cluster({
        "name": "Cached", "bootstrap_servers": "kfk1:9092"})

    def never(*args, **kwargs):
        raise AssertionError("без force кластер трогать нельзя")

    monkeypatch.setattr(kafka_overview, "fetch_cluster_meta", never)

    save_snapshot(cluster_id, build_overview(
        CLUSTER_META, TOPICS_META, BEGIN, END))

    data = collect_overview(cluster_id)

    assert data["empty"] is False
    assert len(data["topics"]) == 2

    delete_cluster(cluster_id)


def test_collect_with_force_asks_cluster(monkeypatch):
    cluster_id = create_cluster({
        "name": "Live", "bootstrap_servers": "kfk1:9092"})

    monkeypatch.setattr(kafka_overview, "fetch_cluster_meta",
                        lambda c: (CLUSTER_META, TOPICS_META))
    monkeypatch.setattr(kafka_overview, "fetch_offsets",
                        lambda c, pairs: (BEGIN, END))

    data = collect_overview(cluster_id, force=True)

    assert data["empty"] is False
    assert data["taken_at"]
    # и сохранился, чтобы следующий заход был без опроса
    assert load_snapshot(cluster_id) is not None

    delete_cluster(cluster_id)
