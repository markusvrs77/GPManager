# -*- coding: utf-8 -*-
"""Правила доступа: транспорт и роуты."""

import pytest

import kafka_routes
from modules import kafka_client
from modules.kafka_audit import recent
from modules.kafka_client import AclsDisabled, KafkaUnavailable
from modules.kafka_clusters import create_cluster, delete_cluster

CLUSTER = {"bootstrap_servers": "kfk1:9092", "security_protocol": "PLAINTEXT",
           "request_timeout_ms": 2000}

FILTER = {"principal": None, "host": None, "resource_type": "ANY",
          "resource_name": None, "pattern_type": "ANY", "operation": "ANY",
          "permission": "ANY"}

SPEC = {"principal": "User:svc_etl", "host": "*", "resource_type": "TOPIC",
        "resource_name": "orders", "pattern_type": "LITERAL",
        "operations": ["READ", "DESCRIBE"], "permission": "ALLOW"}


class NoError(object):
    errno = 0


class FakeAcl(object):
    def __init__(self):
        self.principal = "User:svc_etl"
        self.host = "*"
        self.operation = type("E", (), {"name": "READ"})()
        self.permission_type = type("E", (), {"name": "ALLOW"})()
        self.resource_pattern = type("P", (), {
            "resource_type": type("E", (), {"name": "TOPIC"})(),
            "resource_name": "orders",
            "pattern_type": type("E", (), {"name": "LITERAL"})(),
        })()


def test_fetch_acls_unpacks_tuple(monkeypatch):
    class FakeAdmin(object):
        def describe_acls(self, acl_filter):
            # библиотека отдаёт КОРТЕЖ (правила, ошибка)
            return [FakeAcl()], NoError()

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    rows = kafka_client.fetch_acls(CLUSTER, FILTER)

    assert len(rows) == 1
    assert rows[0].principal == "User:svc_etl"


def test_fetch_acls_reports_disabled_authorizer(monkeypatch):
    class FakeAdmin(object):
        def describe_acls(self, acl_filter):
            raise RuntimeError("SecurityDisabledError: no authorizer")

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    with pytest.raises(AclsDisabled) as err:
        kafka_client.fetch_acls(CLUSTER, FILTER)

    assert "authorizer.class.name" in str(err.value)


def test_create_acls_makes_rule_per_operation(monkeypatch):
    seen = {}

    class FakeAdmin(object):
        def create_acls(self, acls):
            seen["acls"] = acls

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    count = kafka_client.create_acls(CLUSTER, [SPEC])

    # две операции — два правила
    assert count == 2
    assert len(seen["acls"]) == 2
    assert {a.principal for a in seen["acls"]} == {"User:svc_etl"}


def test_delete_acls_counts_removed(monkeypatch):
    class FakeAdmin(object):
        def delete_acls(self, filters):
            # список 3-кортежей (фильтр, правила, ошибка)
            return [(filters[0], [FakeAcl(), FakeAcl()], NoError())]

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    assert kafka_client.delete_acls(CLUSTER, FILTER) == 2


def _cluster():
    return create_cluster({"name": "A", "bootstrap_servers": "kfk1:9092"})


def test_acl_page_opens(client):
    assert client.get("/kafka/acl").status_code == 200


def test_list_returns_formatted(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "fetch_acls",
                        lambda cluster, spec: [FakeAcl()])

    body = client.post(
        "/api/kafka/clusters/{}/acls/list".format(cluster_id),
        json={}).get_json()

    assert body["ok"] is True
    assert body["acls"][0]["operation"] == "READ"
    assert body["acls"][0]["resource_name"] == "orders"
    # подключение PLAINTEXT — предупреждаем про неразличимых клиентов
    assert body["anonymous"] is True

    delete_cluster(cluster_id)


def test_list_reports_disabled(client, monkeypatch):
    cluster_id = _cluster()

    def boom(cluster, spec):
        raise AclsDisabled("На брокерах не включена авторизация")

    monkeypatch.setattr(kafka_routes, "fetch_acls", boom)

    response = client.post(
        "/api/kafka/clusters/{}/acls/list".format(cluster_id), json={})

    assert response.status_code == 409
    assert response.get_json()["disabled"] is True

    delete_cluster(cluster_id)


def test_list_reports_unavailable(client, monkeypatch):
    cluster_id = _cluster()

    def boom(cluster, spec):
        raise KafkaUnavailable("Кластер недоступен: kfk1:9092")

    monkeypatch.setattr(kafka_routes, "fetch_acls", boom)

    response = client.post(
        "/api/kafka/clusters/{}/acls/list".format(cluster_id), json={})

    assert response.status_code == 502

    delete_cluster(cluster_id)


def test_grant_writes_audit(client, monkeypatch):
    cluster_id = _cluster()
    seen = {}

    monkeypatch.setattr(
        kafka_routes, "create_acls",
        lambda cluster, specs: seen.update(specs=specs) or len(specs) * 2)

    body = client.post(
        "/api/kafka/clusters/{}/acls".format(cluster_id),
        json={"principal": "svc_etl", "resource_name": "orders",
              "operations": ["READ", "DESCRIBE"]}).get_json()

    assert body["ok"] is True
    assert body["created"] == 2
    assert seen["specs"][0]["principal"] == "User:svc_etl"

    row = recent(cluster_id)[0]

    assert row["action"] == "grant_acl"
    assert row["target"] == "User:svc_etl"

    delete_cluster(cluster_id)


def test_grant_by_preset(client, monkeypatch):
    cluster_id = _cluster()
    seen = {}

    monkeypatch.setattr(
        kafka_routes, "create_acls",
        lambda cluster, specs: seen.update(specs=specs) or 3)

    body = client.post(
        "/api/kafka/clusters/{}/acls".format(cluster_id),
        json={"preset": "reader", "principal": "svc_etl",
              "topic": "orders", "group": "etl"}).get_json()

    assert body["ok"] is True

    kinds = {(s["resource_type"], s["resource_name"])
             for s in seen["specs"]}

    assert kinds == {("TOPIC", "orders"), ("GROUP", "etl")}

    delete_cluster(cluster_id)


def test_grant_rejects_empty_principal(client):
    cluster_id = _cluster()

    response = client.post(
        "/api/kafka/clusters/{}/acls".format(cluster_id),
        json={"resource_name": "orders", "operations": ["READ"]})

    assert response.status_code == 400

    delete_cluster(cluster_id)


def test_revoke_does_nothing_when_filter_matches_none(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "fetch_acls",
                        lambda cluster, spec: [])

    def never(cluster, spec):
        raise AssertionError("нечего удалять — брокер трогать не нужно")

    monkeypatch.setattr(kafka_routes, "delete_acls", never)

    body = client.post(
        "/api/kafka/clusters/{}/acls/delete".format(cluster_id),
        json={"principal": "svc_etl"}).get_json()

    assert body["ok"] is True
    assert body["removed"] == 0
    assert "ничего не попало" in body["message"]

    delete_cluster(cluster_id)


def test_revoke_deletes_and_writes_audit(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(kafka_routes, "fetch_acls",
                        lambda cluster, spec: [FakeAcl(), FakeAcl()])
    monkeypatch.setattr(kafka_routes, "delete_acls",
                        lambda cluster, spec: 2)

    body = client.post(
        "/api/kafka/clusters/{}/acls/delete".format(cluster_id),
        json={"principal": "svc_etl"}).get_json()

    assert body["removed"] == 2

    row = recent(cluster_id)[0]

    assert row["action"] == "revoke_acl"
    assert row["details"]["matched"] == 2

    delete_cluster(cluster_id)
