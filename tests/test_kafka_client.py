# -*- coding: utf-8 -*-
"""Слой транспорта: аргументы клиента и человеческие ошибки."""

import pytest

from modules import kafka_client
from modules.kafka_client import KafkaUnavailable, client_kwargs, ping

PLAIN = {
    "id": 1,
    "name": "Kafka TEST",
    "bootstrap_servers": "kfk1:9092,kfk2:9092",
    "security_protocol": "PLAINTEXT",
    "request_timeout_ms": 15000,
}

SASL = dict(PLAIN, security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="svc_opsentri", sasl_password="secret")


def test_kwargs_plaintext_has_no_sasl():
    kwargs = client_kwargs(PLAIN)

    assert kwargs["bootstrap_servers"] == ["kfk1:9092", "kfk2:9092"]
    assert kwargs["security_protocol"] == "PLAINTEXT"
    assert kwargs["request_timeout_ms"] == 15000
    assert "sasl_mechanism" not in kwargs
    assert "sasl_plain_username" not in kwargs


def test_kwargs_sasl_carries_credentials():
    kwargs = client_kwargs(SASL)

    assert kwargs["sasl_mechanism"] == "SCRAM-SHA-512"
    assert kwargs["sasl_plain_username"] == "svc_opsentri"
    assert kwargs["sasl_plain_password"] == "secret"


def test_kwargs_default_timeout():
    kwargs = client_kwargs({"bootstrap_servers": "kfk1:9092"})

    assert kwargs["request_timeout_ms"] == 15000


def test_kwargs_limit_bootstrap_wait():
    # у клиента свой таймаут ожидания брокеров: без него он ждёт 30 с
    # независимо от request_timeout_ms, и вкладка висит вдвое дольше
    kwargs = client_kwargs(dict(PLAIN, request_timeout_ms=5000))

    assert kwargs["bootstrap_timeout_ms"] == 5000


def test_kwargs_are_accepted_by_the_library():
    """Все ключи должны существовать в библиотеке, иначе она бросит
    KafkaConfigurationError уже на создании клиента."""
    kafka = pytest.importorskip("kafka")

    admin = set(kafka.KafkaAdminClient.DEFAULT_CONFIG)
    consumer = set(kafka.KafkaConsumer.DEFAULT_CONFIG)

    for key in client_kwargs(SASL):
        assert key in admin, key
        assert key in consumer, key


def test_ping_reports_broker_count(monkeypatch):
    class FakeAdmin(object):
        def describe_cluster(self):
            return {"brokers": [{"node_id": 1}, {"node_id": 2}],
                    "controller_id": 1, "cluster_id": "test"}

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    result = ping(PLAIN)

    assert result["ok"] is True
    assert result["brokers"] == 2


def test_ping_turns_failure_into_message(monkeypatch):
    def boom(cluster):
        raise KafkaUnavailable("Кластер недоступен: kfk1:9092")

    monkeypatch.setattr(kafka_client, "open_admin", boom)

    result = ping(PLAIN)

    assert result["ok"] is False
    assert "kfk1:9092" in result["message"]
    assert result["brokers"] == 0


def test_fetch_cluster_meta_never_asks_for_all_topics(monkeypatch):
    """Запрос без списка топиков брокер отвергает: All topics must not
    be None. Имена берём у консьюмера и передаём явно."""
    seen = {}

    class FakeAdmin(object):
        def describe_cluster(self):
            return {"brokers": [{"broker_id": 1, "host": "kfk1",
                                 "port": 9092, "rack": None}],
                    "controller_id": 1, "cluster_id": "MkU3"}

        def describe_topics(self, topics=None):
            seen["topics"] = topics
            # 3.x отдаёт весь ответ метаданных, а не список топиков
            return {"topics": [{"name": "orders", "partitions": []}],
                    "brokers": []}

        def close(self):
            seen["admin_closed"] = True

    class FakeConsumer(object):
        def topics(self):
            return {"payments", "orders"}

        def close(self):
            seen["consumer_closed"] = True

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())
    monkeypatch.setattr(kafka_client, "open_consumer",
                        lambda c: FakeConsumer())

    cluster_meta, topics_meta = kafka_client.fetch_cluster_meta(PLAIN)

    assert seen["topics"] == ["orders", "payments"]
    assert topics_meta == [{"name": "orders", "partitions": []}]
    assert cluster_meta["controller_id"] == 1
    assert seen["admin_closed"] and seen["consumer_closed"]


def test_request_error_is_not_reported_as_timeout(monkeypatch):
    class FakeAdmin(object):
        def describe_cluster(self):
            raise ValueError("All topics must not be None")

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    with pytest.raises(KafkaUnavailable) as err:
        kafka_client.fetch_cluster_meta(PLAIN)

    text = str(err.value)

    assert "ответил ошибкой" in text
    assert "не ответил за" not in text


def test_missing_library_is_explained(monkeypatch):
    def no_library():
        raise ImportError("no kafka")

    monkeypatch.setattr(kafka_client, "_import_kafka", no_library)

    with pytest.raises(KafkaUnavailable) as err:
        kafka_client.open_admin(PLAIN)

    assert "kafka-python" in str(err.value)
