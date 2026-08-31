# -*- coding: utf-8 -*-
"""API вкладки консьюмер-групп."""

import kafka_routes
from modules.kafka_audit import recent
from modules.kafka_client import KafkaUnavailable
from modules.kafka_clusters import create_cluster, delete_cluster

SNAPSHOT = {
    "cluster_id": 1,
    "empty": False,
    "taken_at": "2026-08-31 19:04:00",
    "groups": [
        {"id": "etl-loader", "state": "Stable", "protocol": "range",
         "members": 2, "partitions": 2, "lag": 4900,
         "topics": [{"name": "orders", "lag": 4900, "parts": [
             {"p": 0, "committed": 4100, "end": 9000, "lag": 4900,
              "client": "c-1", "host": "10.0.0.7"}]}]},
        {"id": "idle-group", "state": "Empty", "protocol": "",
         "members": 0, "partitions": 1, "lag": 0,
         "topics": [{"name": "orders", "lag": 0, "parts": [
             {"p": 0, "committed": 9000, "end": 9000, "lag": 0,
              "client": None, "host": None}]}]},
    ],
}


def _cluster():
    return create_cluster({"name": "G", "bootstrap_servers": "kfk1:9092"})


def test_groups_page_opens(client):
    assert client.get("/kafka/groups").status_code == 200


def test_groups_snapshot(client, monkeypatch):
    cluster_id = _cluster()
    seen = {}

    def fake_collect(cid, force=False):
        seen["force"] = force
        return SNAPSHOT

    monkeypatch.setattr(kafka_routes, "collect_groups", fake_collect)

    response = client.get(
        "/api/kafka/clusters/{}/groups".format(cluster_id))

    assert response.status_code == 200
    assert seen["force"] is False
    assert response.get_json()["groups"]["groups"][0]["id"] == "etl-loader"

    delete_cluster(cluster_id)


def test_groups_refresh_forces(client, monkeypatch):
    cluster_id = _cluster()
    seen = {}

    def fake_collect(cid, force=False):
        seen["force"] = force
        return SNAPSHOT

    monkeypatch.setattr(kafka_routes, "collect_groups", fake_collect)

    response = client.post(
        "/api/kafka/clusters/{}/groups/refresh".format(cluster_id))

    assert response.status_code == 200
    assert seen["force"] is True

    delete_cluster(cluster_id)


def test_reset_refuses_busy_group(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "collect_groups",
                        lambda cid, force=False: SNAPSHOT)

    response = client.post(
        "/api/kafka/clusters/{}/groups/etl-loader/reset".format(cluster_id),
        json={"mode": "earliest"})

    assert response.status_code == 409
    assert "участник" in response.get_json()["message"]

    delete_cluster(cluster_id)


def test_reset_runs_and_writes_audit(client, monkeypatch):
    cluster_id = _cluster()
    seen = {}

    def fake_reset(cluster, group_id, specs):
        seen["group"] = group_id
        seen["specs"] = specs
        return {("orders", 0): None}

    monkeypatch.setattr(kafka_routes, "collect_groups",
                        lambda cid, force=False: SNAPSHOT)
    monkeypatch.setattr(kafka_routes, "reset_offsets", fake_reset)

    response = client.post(
        "/api/kafka/clusters/{}/groups/idle-group/reset".format(cluster_id),
        json={"mode": "latest"})
    body = response.get_json()

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["done"] == 1 and body["failed"] == []
    assert seen["specs"] == {("orders", 0): ("latest", None)}

    row = recent(cluster_id)[0]

    assert row["action"] == "reset_offsets"
    assert row["target"] == "idle-group"
    assert row["result"] == "ok"

    delete_cluster(cluster_id)


def test_reset_reports_partial_failure(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "collect_groups",
                        lambda cid, force=False: SNAPSHOT)
    monkeypatch.setattr(
        kafka_routes, "reset_offsets",
        lambda cluster, gid, specs: {("orders", 0): "UNKNOWN_MEMBER_ID"})

    body = client.post(
        "/api/kafka/clusters/{}/groups/idle-group/reset".format(cluster_id),
        json={"mode": "earliest"}).get_json()

    assert body["ok"] is True
    assert body["done"] == 0
    assert body["failed"] == [{"topic": "orders", "partition": 0,
                               "error": "UNKNOWN_MEMBER_ID"}]
    assert recent(cluster_id)[0]["result"] == "failed"

    delete_cluster(cluster_id)


def test_reset_unknown_group_is_404(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "collect_groups",
                        lambda cid, force=False: SNAPSHOT)

    response = client.post(
        "/api/kafka/clusters/{}/groups/nope/reset".format(cluster_id),
        json={"mode": "earliest"})

    assert response.status_code == 404

    delete_cluster(cluster_id)


def test_reset_bad_mode_is_400(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "collect_groups",
                        lambda cid, force=False: SNAPSHOT)

    response = client.post(
        "/api/kafka/clusters/{}/groups/idle-group/reset".format(cluster_id),
        json={"mode": "nonsense"})

    assert response.status_code == 400

    delete_cluster(cluster_id)


def test_delete_group_requires_idle(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "collect_groups",
                        lambda cid, force=False: SNAPSHOT)
    monkeypatch.setattr(kafka_routes, "delete_group",
                        lambda cluster, gid: None)

    busy = client.delete(
        "/api/kafka/clusters/{}/groups/etl-loader".format(cluster_id))

    assert busy.status_code == 409

    ok = client.delete(
        "/api/kafka/clusters/{}/groups/idle-group".format(cluster_id))

    assert ok.status_code == 200
    assert recent(cluster_id)[0]["action"] == "delete_group"

    delete_cluster(cluster_id)


def test_groups_refresh_reports_unavailable(client, monkeypatch):
    cluster_id = _cluster()

    def boom(cid, force=False):
        raise KafkaUnavailable("Кластер недоступен: kfk1:9092")

    monkeypatch.setattr(kafka_routes, "collect_groups", boom)

    response = client.post(
        "/api/kafka/clusters/{}/groups/refresh".format(cluster_id))

    assert response.status_code == 502

    delete_cluster(cluster_id)


def test_audit_route(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "collect_groups",
                        lambda cid, force=False: SNAPSHOT)
    monkeypatch.setattr(kafka_routes, "delete_group",
                        lambda cluster, gid: None)

    client.delete(
        "/api/kafka/clusters/{}/groups/idle-group".format(cluster_id))

    body = client.get(
        "/api/kafka/clusters/{}/audit".format(cluster_id)).get_json()

    assert body["ok"] is True
    assert body["records"][0]["action"] == "delete_group"

    delete_cluster(cluster_id)
