# -*- coding: utf-8 -*-
"""Журнал опасных действий над Kafka."""

from modules.kafka_audit import recent, write
from modules.kafka_clusters import create_cluster, delete_cluster


def test_write_and_read_back():
    cluster_id = create_cluster({
        "name": "Audit", "bootstrap_servers": "kfk1:9092"})

    first = write(cluster_id, "reset_offsets", "etl-loader",
                  {"mode": "earliest", "partitions": 6}, "ok")
    second = write(cluster_id, "delete_group", "old-group", None, "ok")

    assert first and second and second > first

    rows = recent(cluster_id)

    # новые записи первыми
    assert [r["action"] for r in rows] == ["delete_group", "reset_offsets"]
    assert rows[1]["details"]["mode"] == "earliest"
    assert rows[0]["details"] is None
    assert rows[0]["created_at"]

    delete_cluster(cluster_id)


def test_audit_survives_cluster_removal():
    cluster_id = create_cluster({
        "name": "Gone", "bootstrap_servers": "kfk1:9092"})

    write(cluster_id, "reset_offsets", "g1", None, "ok")
    delete_cluster(cluster_id)

    # журнал должен пережить объект, к которому относится
    assert len(recent(cluster_id)) == 1


def test_limit_is_applied():
    cluster_id = create_cluster({
        "name": "Many", "bootstrap_servers": "kfk1:9092"})

    for i in range(5):
        write(cluster_id, "reset_offsets", "g{}".format(i), None, "ok")

    assert len(recent(cluster_id, limit=3)) == 3

    delete_cluster(cluster_id)
