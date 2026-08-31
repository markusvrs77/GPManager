# -*- coding: utf-8 -*-
"""Чтение и отправка сообщений: транспорт и роуты."""

import kafka_routes
from modules import kafka_client
from modules.kafka_audit import recent
from modules.kafka_client import KafkaUnavailable
from modules.kafka_clusters import create_cluster, delete_cluster

CLUSTER = {"bootstrap_servers": "kfk1:9092", "security_protocol": "PLAINTEXT",
           "request_timeout_ms": 2000}


class FakeRecord(object):
    def __init__(self, partition, offset, value):
        self.topic = "orders"
        self.partition = partition
        self.offset = offset
        self.timestamp = 1788073200000
        self.key = b"k"
        self.value = value
        self.headers = []


class FakeConsumer(object):
    """Ведёт себя как консьюмер без группы: assign + seek + poll."""

    def __init__(self, seen):
        self.seen = seen
        self._served = False

    def partitions_for_topic(self, topic):
        return {0, 1}

    def assign(self, partitions):
        self.seen["assigned"] = sorted(
            (tp.topic, tp.partition) for tp in partitions)

    def seek(self, partition, offset):
        self.seen.setdefault("seeks", []).append(
            (partition.partition, offset))

    def beginning_offsets(self, partitions):
        return {tp: 0 for tp in partitions}

    def end_offsets(self, partitions):
        return {tp: 1000 for tp in partitions}

    def offsets_for_times(self, timestamps):
        self.seen["times"] = {tp.partition: ms
                              for tp, ms in timestamps.items()}

        class Found(object):
            offset = 777

        return {tp: Found() for tp in timestamps}

    def poll(self, timeout_ms=0, max_records=None):
        if self._served:
            return {}

        self._served = True

        import kafka as real_kafka

        tp = real_kafka.TopicPartition("orders", 0)

        return {tp: [FakeRecord(0, 10, b"one"), FakeRecord(0, 11, b"two")]}

    def close(self):
        self.seen["closed"] = True


def test_read_latest_seeks_from_end(monkeypatch):
    seen = {}

    monkeypatch.setattr(kafka_client, "open_consumer",
                        lambda c: FakeConsumer(seen))

    rows = kafka_client.read_messages(CLUSTER, "orders", {
        "mode": "latest", "limit": 50, "partition": None})

    assert seen["assigned"] == [("orders", 0), ("orders", 1)]
    # 50 записей на две партиции — по 25 с конца каждой
    assert sorted(seen["seeks"]) == [(0, 975), (1, 975)]
    assert [r["offset"] for r in rows] == [10, 11]
    assert seen["closed"] is True


def test_read_offset_mode(monkeypatch):
    seen = {}

    monkeypatch.setattr(kafka_client, "open_consumer",
                        lambda c: FakeConsumer(seen))

    kafka_client.read_messages(CLUSTER, "orders", {
        "mode": "offset", "limit": 10, "offset": 100, "partition": 1})

    assert seen["assigned"] == [("orders", 1)]
    assert seen["seeks"] == [(1, 100)]


def test_read_timestamp_mode(monkeypatch):
    seen = {}

    monkeypatch.setattr(kafka_client, "open_consumer",
                        lambda c: FakeConsumer(seen))

    kafka_client.read_messages(CLUSTER, "orders", {
        "mode": "timestamp", "limit": 10, "timestamp_ms": 1788073200000,
        "partition": None})

    assert seen["times"] == {0: 1788073200000, 1: 1788073200000}
    assert sorted(seen["seeks"]) == [(0, 777), (1, 777)]


def test_send_message_returns_offset(monkeypatch):
    seen = {}

    class Meta(object):
        partition = 2
        offset = 555

    class FakeFuture(object):
        def get(self, timeout=None):
            return Meta()

    class FakeProducer(object):
        def __init__(self, **kwargs):
            seen["kwargs"] = kwargs

        def send(self, topic, value=None, key=None, partition=None):
            seen["sent"] = (topic, value, key, partition)
            return FakeFuture()

        def close(self, timeout=None):
            seen["closed"] = True

    class FakeKafka(object):
        KafkaProducer = FakeProducer

    monkeypatch.setattr(kafka_client, "_import_kafka", lambda: FakeKafka())

    result = kafka_client.send_message(
        CLUSTER, "orders", "client-42", '{"sum": 10}')

    assert result == {"partition": 2, "offset": 555}
    assert seen["sent"][0] == "orders"
    assert seen["sent"][1] == b'{"sum": 10}'
    assert seen["sent"][2] == b"client-42"
    assert seen["closed"] is True


def _cluster():
    return create_cluster({"name": "M", "bootstrap_servers": "kfk1:9092"})


def test_messages_page_opens(client):
    assert client.get("/kafka/messages").status_code == 200


def test_read_returns_formatted(client, monkeypatch):
    cluster_id = _cluster()
    seen = {}

    def fake_read(cluster, topic, plan):
        seen["plan"] = plan
        return [{"topic": topic, "partition": 0, "offset": 5,
                 "timestamp": 1788073200000, "key": b"client-42",
                 "value": b'{"sum": 10}', "headers": []}]

    monkeypatch.setattr(kafka_routes, "read_messages", fake_read)

    body = client.post(
        "/api/kafka/clusters/{}/messages/read".format(cluster_id),
        json={"topic": "orders", "limit": 10}).get_json()

    assert body["ok"] is True
    assert seen["plan"]["limit"] == 10
    assert body["records"][0]["value"]["kind"] == "json"
    assert body["records"][0]["key"]["text"] == "client-42"

    delete_cluster(cluster_id)


def test_read_requires_topic(client):
    cluster_id = _cluster()

    response = client.post(
        "/api/kafka/clusters/{}/messages/read".format(cluster_id),
        json={"topic": ""})

    assert response.status_code == 400

    delete_cluster(cluster_id)


def test_read_reports_unavailable(client, monkeypatch):
    cluster_id = _cluster()

    def boom(cluster, topic, plan):
        raise KafkaUnavailable("Кластер недоступен: kfk1:9092")

    monkeypatch.setattr(kafka_routes, "read_messages", boom)

    response = client.post(
        "/api/kafka/clusters/{}/messages/read".format(cluster_id),
        json={"topic": "orders"})

    assert response.status_code == 502

    delete_cluster(cluster_id)


def test_send_writes_audit(client, monkeypatch):
    cluster_id = _cluster()

    monkeypatch.setattr(
        kafka_routes, "send_message",
        lambda cluster, topic, key, value, partition=None: {
            "partition": 1, "offset": 99})

    body = client.post(
        "/api/kafka/clusters/{}/messages".format(cluster_id),
        json={"topic": "orders", "key": "client-42",
              "value": '{"sum": 10}'}).get_json()

    assert body["ok"] is True
    assert body["offset"] == 99

    row = recent(cluster_id)[0]

    assert row["action"] == "send_message"
    assert row["target"] == "orders"
    assert row["details"]["offset"] == 99
    # тело целиком в журнал не пишем
    assert len(row["details"]["preview"]) <= 120

    delete_cluster(cluster_id)


def test_send_requires_topic(client):
    cluster_id = _cluster()

    response = client.post(
        "/api/kafka/clusters/{}/messages".format(cluster_id),
        json={"value": "x"})

    assert response.status_code == 400

    delete_cluster(cluster_id)
