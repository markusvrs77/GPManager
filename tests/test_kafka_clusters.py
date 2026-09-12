# -*- coding: utf-8 -*-
"""Подключения к Kafka: нормализация адресов и CRUD."""

import pytest

from modules.kafka_clusters import (
    create_cluster,
    delete_cluster,
    get_cluster,
    list_clusters,
    normalize_bootstrap,
    update_cluster,
)


def test_normalize_bootstrap_adds_default_port():
    assert normalize_bootstrap("kfk1") == "kfk1:9092"
    assert normalize_bootstrap("kfk1:9093") == "kfk1:9093"


def test_normalize_bootstrap_cleans_list():
    value = " kfk1:9092 , kfk2 ,, kfk1:9092 "

    assert normalize_bootstrap(value) == "kfk1:9092,kfk2:9092"


def test_normalize_bootstrap_accepts_list():
    assert normalize_bootstrap(["kfk1", "kfk2:9093"]) == "kfk1:9092,kfk2:9093"


def test_normalize_bootstrap_rejects_empty():
    with pytest.raises(ValueError):
        normalize_bootstrap("   ")


def test_crud_roundtrip():
    cluster_id = create_cluster({
        "name": "Kafka TEST",
        "bootstrap_servers": "kfk1, kfk2:9093",
    })

    saved = get_cluster(cluster_id)

    assert saved["name"] == "Kafka TEST"
    assert saved["bootstrap_servers"] == "kfk1:9092,kfk2:9093"
    assert saved["security_protocol"] == "PLAINTEXT"
    assert saved["request_timeout_ms"] == 15000

    assert update_cluster(cluster_id, {
        "name": "Kafka PROD",
        "bootstrap_servers": "kfk9:9092",
        "request_timeout_ms": 30000,
    }) is True

    saved = get_cluster(cluster_id)

    assert saved["name"] == "Kafka PROD"
    assert saved["bootstrap_servers"] == "kfk9:9092"
    assert saved["request_timeout_ms"] == 30000

    assert delete_cluster(cluster_id) is True
    assert get_cluster(cluster_id) is None


def test_list_hides_password():
    cluster_id = create_cluster({
        "name": "SASL",
        "bootstrap_servers": "kfk1:9092",
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_username": "svc_opsentri",
        "sasl_password": "secret",
    })

    row = [c for c in list_clusters() if c["id"] == cluster_id][0]

    assert "sasl_password" not in row
    assert row["has_password"] is True

    # а внутреннему коду пароль нужен целиком
    assert get_cluster(cluster_id)["sasl_password"] == "secret"

    delete_cluster(cluster_id)


def test_empty_name_rejected():
    with pytest.raises(ValueError):
        create_cluster({"name": "  ", "bootstrap_servers": "kfk1:9092"})
