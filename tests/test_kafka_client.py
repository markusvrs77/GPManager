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


def test_fetch_groups_flattens_describe(monkeypatch):
    class FakeAdmin(object):
        def list_groups(self):
            return [{"group_id": "etl-loader", "protocol_type": "consumer",
                     "group_state": "Stable"},
                    {"group_id": "old", "protocol_type": "consumer",
                     "group_state": "Empty"}]

        def describe_groups(self, group_ids, **kwargs):
            assert sorted(group_ids) == ["etl-loader", "old"]
            return {
                "etl-loader": {"group_id": "etl-loader",
                               "group_state": "Stable",
                               "protocol_data": "range",
                               "members": [{"client_id": "c-1",
                                            "client_host": "10.0.0.7"}]},
                "old": {"group_id": "old", "group_state": "Empty",
                        "protocol_data": "", "members": []},
            }

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    groups = kafka_client.fetch_groups(PLAIN)

    assert [g["group_id"] for g in groups] == ["etl-loader", "old"]
    assert groups[0]["members"][0]["client_id"] == "c-1"


def test_fetch_group_offsets_uses_plain_keys(monkeypatch):
    import kafka as real_kafka

    tp = real_kafka.TopicPartition("orders", 0)
    missing = real_kafka.TopicPartition("orders", 1)

    class Meta(object):
        def __init__(self, offset):
            self.offset = offset

    class FakeAdmin(object):
        def list_group_offsets(self, specs):
            assert specs == {"etl-loader": None}
            return {"etl-loader": {tp: Meta(4100), missing: Meta(-1)}}

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    offsets = kafka_client.fetch_group_offsets(PLAIN, ["etl-loader"])

    assert offsets[("etl-loader", "orders", 0)] == 4100
    # -1 у Kafka значит «коммита не было», а не нулевой оффсет
    assert offsets[("etl-loader", "orders", 1)] is None


def test_reset_offsets_translates_modes(monkeypatch):
    seen = {}

    class FakeAdmin(object):
        def reset_group_offsets(self, group_id, specs, **kwargs):
            seen["group"] = group_id
            seen["specs"] = specs
            return {list(specs)[0]: {"error": None}}

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    result = kafka_client.reset_offsets(
        PLAIN, "etl-loader", {("orders", 0): ("earliest", None)})

    key = list(seen["specs"])[0]

    assert seen["group"] == "etl-loader"
    assert (key.topic, key.partition) == ("orders", 0)
    assert int(seen["specs"][key]) == -2
    assert result == {("orders", 0): None}


def test_add_partitions_sends_total_count(monkeypatch):
    seen = {}

    class FakeAdmin(object):
        def create_partitions(self, topic_partitions, **kwargs):
            seen["arg"] = topic_partitions

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    kafka_client.add_partitions(PLAIN, "orders", 12)

    spec = seen["arg"]["orders"]

    # NewPartitions принимает ИТОГОВОЕ число, а не приращение
    assert spec.total_count == 12


def test_create_topic_passes_configs(monkeypatch):
    seen = {}

    class FakeAdmin(object):
        def create_topics(self, new_topics, **kwargs):
            seen["topics"] = new_topics

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    kafka_client.create_topic(PLAIN, {
        "name": "orders", "partitions": 6, "replication": 3,
        "configs": {"retention.ms": "86400000"}})

    topic = seen["topics"][0]

    assert topic.name == "orders"
    assert topic.num_partitions == 6
    assert topic.replication_factor == 3
    assert topic.topic_configs == {"retention.ms": "86400000"}


def test_existing_topic_error_is_explained(monkeypatch):
    class FakeAdmin(object):
        def create_topics(self, new_topics, **kwargs):
            raise RuntimeError("TopicAlreadyExistsError: orders")

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    with pytest.raises(KafkaUnavailable) as err:
        kafka_client.create_topic(PLAIN, {
            "name": "orders", "partitions": 1, "replication": 1,
            "configs": {}})

    assert "уже есть" in str(err.value)


def test_delete_disabled_error_is_explained(monkeypatch):
    class FakeAdmin(object):
        def delete_topics(self, topics, **kwargs):
            raise RuntimeError("TopicDeletionDisabledError")

        def close(self):
            pass

    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    with pytest.raises(KafkaUnavailable) as err:
        kafka_client.delete_topic(PLAIN, "orders")

    assert "delete.topic.enable" in str(err.value)


def test_missing_library_is_explained(monkeypatch):
    def no_library():
        raise ImportError("no kafka")

    monkeypatch.setattr(kafka_client, "_import_kafka", no_library)

    with pytest.raises(KafkaUnavailable) as err:
        kafka_client.open_admin(PLAIN)

    assert "kafka-python" in str(err.value)
