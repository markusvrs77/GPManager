# -*- coding: utf-8 -*-
"""API вкладки Kafka."""

import kafka_routes
from modules.kafka_client import KafkaUnavailable
from modules.kafka_clusters import create_cluster, delete_cluster

OVERVIEW = {
    "cluster_id": 1,
    "empty": False,
    "taken_at": "2026-08-31 19:04:00",
    "kafka_cluster_id": "MkU3",
    "controller_id": 1,
    "brokers": [{"id": 1, "host": "kfk1", "port": 9092, "rack": None}],
    "topics": [{"name": "orders", "internal": False, "partitions": 2,
                "replication": 2, "messages": 1100,
                "under_replicated": False, "parts": []}],
}


def test_page_opens(client):
    response = client.get("/kafka")

    assert response.status_code == 200


def test_connections_page_opens(client):
    response = client.get("/kafka/connections")

    assert response.status_code == 200
    assert "Кластеры Kafka" in response.get_data(as_text=True)


def test_clusters_crud(client):
    created = client.post("/api/kafka/clusters", json={
        "name": "Kafka TEST", "bootstrap_servers": "kfk1"})

    assert created.status_code == 200
    cluster_id = created.get_json()["id"]

    listed = client.get("/api/kafka/clusters").get_json()
    row = [c for c in listed["clusters"] if c["id"] == cluster_id][0]

    assert row["bootstrap_servers"] == "kfk1:9092"
    assert "sasl_password" not in row

    changed = client.put(
        "/api/kafka/clusters/{}".format(cluster_id),
        json={"name": "Kafka PROD"})

    assert changed.get_json()["ok"] is True

    removed = client.delete("/api/kafka/clusters/{}".format(cluster_id))

    assert removed.get_json()["ok"] is True


def test_create_rejects_empty_name(client):
    response = client.post("/api/kafka/clusters", json={
        "name": "", "bootstrap_servers": "kfk1"})

    assert response.status_code == 400
    assert "имя" in response.get_json()["message"].lower()


def test_overview_reads_snapshot(client, monkeypatch):
    cluster_id = create_cluster({
        "name": "Snap", "bootstrap_servers": "kfk1:9092"})

    seen = {}

    def fake_collect(cid, force=False):
        seen["force"] = force
        return OVERVIEW

    monkeypatch.setattr(kafka_routes, "collect_overview", fake_collect)

    response = client.get(
        "/api/kafka/clusters/{}/overview".format(cluster_id))

    assert response.status_code == 200
    assert seen["force"] is False
    assert response.get_json()["overview"]["topics"][0]["name"] == "orders"

    delete_cluster(cluster_id)


def test_refresh_forces_collect(client, monkeypatch):
    cluster_id = create_cluster({
        "name": "Live", "bootstrap_servers": "kfk1:9092"})

    seen = {}

    def fake_collect(cid, force=False):
        seen["force"] = force
        return OVERVIEW

    monkeypatch.setattr(kafka_routes, "collect_overview", fake_collect)

    response = client.post(
        "/api/kafka/clusters/{}/overview/refresh".format(cluster_id))

    assert response.status_code == 200
    assert seen["force"] is True

    delete_cluster(cluster_id)


def test_refresh_reports_unavailable_cluster(client, monkeypatch):
    cluster_id = create_cluster({
        "name": "Dead", "bootstrap_servers": "kfk1:9092"})

    def boom(cid, force=False):
        raise KafkaUnavailable("Кластер недоступен: kfk1:9092")

    monkeypatch.setattr(kafka_routes, "collect_overview", boom)

    response = client.post(
        "/api/kafka/clusters/{}/overview/refresh".format(cluster_id))

    assert response.status_code == 502
    assert "kfk1:9092" in response.get_json()["message"]

    delete_cluster(cluster_id)


def test_overview_unknown_cluster_is_404(client):
    response = client.get("/api/kafka/clusters/999999/overview")

    assert response.status_code == 404


def test_ping_route(client, monkeypatch):
    cluster_id = create_cluster({
        "name": "Ping", "bootstrap_servers": "kfk1:9092"})

    monkeypatch.setattr(
        kafka_routes, "ping",
        lambda cluster: {"ok": True, "message": "Связь есть, брокеров: 2",
                         "brokers": 2})

    response = client.post(
        "/api/kafka/clusters/{}/ping".format(cluster_id))

    assert response.get_json()["brokers"] == 2

    delete_cluster(cluster_id)
