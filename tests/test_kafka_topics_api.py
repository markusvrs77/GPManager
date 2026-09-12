# -*- coding: utf-8 -*-
"""API управления топиками."""

import kafka_routes
from modules.kafka_audit import recent
from modules.kafka_client import KafkaUnavailable
from modules.kafka_clusters import create_cluster, delete_cluster

OVERVIEW = {
    "cluster_id": 1, "empty": False, "taken_at": "2026-08-31 20:00:00",
    "brokers": [{"id": 1, "host": "kfk1", "port": 9092, "rack": None}],
    "topics": [{"name": "orders", "internal": False, "partitions": 6,
                "replication": 3, "messages": 100,
                "under_replicated": False, "parts": []}],
}

DESCRIBED = {
    "topic": {
        "orders": {
            "retention.ms": {"value": "604800000", "read_only": False,
                             "is_default": False, "is_sensitive": False,
                             "config_source": "DYNAMIC_TOPIC_CONFIG"},
        }
    }
}


def _cluster():
    return create_cluster({"name": "T", "bootstrap_servers": "kfk1:9092"})


def test_create_topic_writes_audit(client, monkeypatch):
    cluster_id = _cluster()
    seen = {}

    monkeypatch.setattr(kafka_routes, "collect_overview",
                        lambda cid, force=False: OVERVIEW)
    monkeypatch.setattr(kafka_routes, "create_topic",
                        lambda cluster, spec: seen.update(spec=spec))

    response = client.post(
        "/api/kafka/clusters/{}/topics".format(cluster_id),
        json={"name": "payments", "partitions": 3, "replication": 2,
              "retention_hours": 24})

    assert response.status_code == 200
    assert seen["spec"]["configs"]["retention.ms"] == "86400000"

    row = recent(cluster_id)[0]

    assert row["action"] == "create_topic"
    assert row["target"] == "payments"

    delete_cluster(cluster_id)


def test_create_topic_rejects_bad_name(client):
    cluster_id = _cluster()

    response = client.post(
        "/api/kafka/clusters/{}/topics".format(cluster_id),
        json={"name": "плохое имя"})

    assert response.status_code == 400

    delete_cluster(cluster_id)


def test_delete_topic_writes_audit(client, monkeypatch):
    cluster_id = _cluster()
    seen = {}

    monkeypatch.setattr(kafka_routes, "collect_overview",
                        lambda cid, force=False: OVERVIEW)
    monkeypatch.setattr(kafka_routes, "delete_topic",
                        lambda cluster, name: seen.update(name=name))

    response = client.delete(
        "/api/kafka/clusters/{}/topics/orders".format(cluster_id))

    assert response.status_code == 200
    assert seen["name"] == "orders"
    assert recent(cluster_id)[0]["action"] == "delete_topic"

    delete_cluster(cluster_id)


def test_delete_topic_reports_disabled(client, monkeypatch):
    cluster_id = _cluster()

    def boom(cluster, name):
        raise KafkaUnavailable(
            "Брокер запрещает удаление топиков: включите delete.topic.enable")

    monkeypatch.setattr(kafka_routes, "collect_overview",
                        lambda cid, force=False: OVERVIEW)
    monkeypatch.setattr(kafka_routes, "delete_topic", boom)

    response = client.delete(
        "/api/kafka/clusters/{}/topics/orders".format(cluster_id))

    assert response.status_code == 502
    assert "delete.topic.enable" in response.get_json()["message"]
    assert recent(cluster_id)[0]["result"] == "error"

    delete_cluster(cluster_id)


def test_partitions_refuse_shrink(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "collect_overview",
                        lambda cid, force=False: OVERVIEW)

    response = client.post(
        "/api/kafka/clusters/{}/topics/orders/partitions".format(cluster_id),
        json={"total": 6})

    assert response.status_code == 409
    assert "6" in response.get_json()["message"]

    delete_cluster(cluster_id)


def test_partitions_grow(client, monkeypatch):
    cluster_id = _cluster()
    seen = {}

    monkeypatch.setattr(kafka_routes, "collect_overview",
                        lambda cid, force=False: OVERVIEW)
    monkeypatch.setattr(
        kafka_routes, "add_partitions",
        lambda cluster, name, total: seen.update(name=name, total=total))

    response = client.post(
        "/api/kafka/clusters/{}/topics/orders/partitions".format(cluster_id),
        json={"total": 12})

    assert response.status_code == 200
    assert seen == {"name": "orders", "total": 12}
    assert recent(cluster_id)[0]["action"] == "add_partitions"

    delete_cluster(cluster_id)


def test_get_configs(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "fetch_topic_configs",
                        lambda cluster, name: DESCRIBED)

    body = client.get(
        "/api/kafka/clusters/{}/topics/orders/configs".format(cluster_id)
    ).get_json()

    assert body["ok"] is True
    assert body["configs"][0]["key"] == "retention.ms"

    delete_cluster(cluster_id)


def test_put_configs_sends_only_changes(client, monkeypatch):
    cluster_id = _cluster()
    seen = {}

    monkeypatch.setattr(kafka_routes, "fetch_topic_configs",
                        lambda cluster, name: DESCRIBED)
    monkeypatch.setattr(
        kafka_routes, "alter_topic_configs",
        lambda cluster, name, changes: seen.update(changes=changes))

    body = client.put(
        "/api/kafka/clusters/{}/topics/orders/configs".format(cluster_id),
        json={"configs": {"retention.ms": "3600000"}}).get_json()

    assert body["ok"] is True
    assert body["changed"] == 1
    assert seen["changes"] == {"retention.ms": "3600000"}
    assert recent(cluster_id)[0]["action"] == "alter_configs"

    delete_cluster(cluster_id)


def test_put_configs_without_changes_does_nothing(client, monkeypatch):
    cluster_id = _cluster()

    def never(*args, **kwargs):
        raise AssertionError("без изменений брокер трогать не нужно")

    monkeypatch.setattr(kafka_routes, "fetch_topic_configs",
                        lambda cluster, name: DESCRIBED)
    monkeypatch.setattr(kafka_routes, "alter_topic_configs", never)

    body = client.put(
        "/api/kafka/clusters/{}/topics/orders/configs".format(cluster_id),
        json={"configs": {"retention.ms": "604800000"}}).get_json()

    assert body["ok"] is True
    assert body["changed"] == 0

    delete_cluster(cluster_id)
