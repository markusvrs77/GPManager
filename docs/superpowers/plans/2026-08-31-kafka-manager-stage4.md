# Kafka Manager, этап 4 — план реализации

> **Для исполнителя:** задача за задачей, каждая заканчивается зелёными
> тестами и коммитом.

**Спека:** `docs/superpowers/specs/2026-08-31-kafka-manager-stage4-design.md`

**Цель:** вкладка «Сообщения» — чтение записей из топика и ручная
отправка.

**Архитектура:** прежняя. `import kafka` только в
`modules/kafka_client.py`; `modules/kafka_messages.py` — чистые функции;
роуты в существующий Blueprint.

## Общие требования

- Тексты и комментарии по-русски.
- `import kafka` только в `modules/kafka_client.py`.
- Чтение **без `group_id` и без коммитов**: просмотр не должен двигать
  чужие оффсеты.
- Тесты без брокера, клиенты подменяются через `monkeypatch`.
- `python -m flake8 --select=E9,F63,F7,F82 .` чистый.
- Сообщения нигде не кэшируются.

## Сверенные факты о kafka-python 3.0.11

- `consumer.assign(partitions)`, `consumer.seek(partition, offset)`,
  `consumer.poll(timeout_ms=0, max_records=None)` →
  `{TopicPartition: [ConsumerRecord]}`.
- `consumer.offsets_for_times({tp: ms})` → `{tp: значение с .offset или None}`.
- `ConsumerRecord`: `topic`, `partition`, `offset`, `timestamp`,
  `timestamp_type`, `key`, `value`, `headers`, `serialized_value_size`.
- `producer.send(topic, value=None, key=None, headers=None,
  partition=None, timestamp_ms=None)`; `future.get(timeout=сек)` →
  метаданные с `partition` и `offset`.
- `KafkaProducer.DEFAULT_CONFIG` принимает те же ключи, что отдаёт
  `client_kwargs` (включая `bootstrap_timeout_ms`), но **не** принимает
  `enable_auto_commit` и `consumer_timeout_ms` — их добавляет только
  `open_consumer`, поэтому `KafkaProducer(**client_kwargs(cluster))`
  безопасен.

## Карта файлов

| Файл | Ответственность |
|------|-----------------|
| `modules/kafka_messages.py` | декодирование, план чтения, форматирование |
| `modules/kafka_client.py` | `read_messages`, `send_message` |
| `kafka_routes.py` | страница и два API-роута |
| `templates/kafka_messages.html` | разметка |
| `static/js/kafka_messages.js` | поведение |
| `templates/base.html` | четвёртый пункт меню |
| `tests/test_kafka_messages.py` | чистые функции |
| `tests/test_kafka_messages_api.py` | транспорт и роуты |

---

### Задача 1: чистые функции сообщений

**Файлы:**
- Создать: `modules/kafka_messages.py`
- Тест: `tests/test_kafka_messages.py`

**Интерфейсы:**
- Даёт наружу: `decode_payload(raw) -> dict`,
  `trim_text(text, limit=8192) -> (str, bool)`,
  `parse_moment(text) -> int`, `build_read_plan(data) -> dict`,
  `format_record(record) -> dict`.

- [ ] **Шаг 1: написать падающий тест**

Создать `tests/test_kafka_messages.py`:

```python
# -*- coding: utf-8 -*-
"""Сообщения Kafka: декодирование и план чтения."""

import pytest

from modules.kafka_messages import (
    build_read_plan,
    decode_payload,
    format_record,
    trim_text,
)


def test_decode_empty():
    for raw in (None, b""):
        row = decode_payload(raw)

        assert row["kind"] == "empty"
        assert row["size"] == 0
        assert row["text"] is None


def test_decode_text():
    row = decode_payload("привет".encode("utf-8"))

    assert row["kind"] == "text"
    assert row["text"] == "привет"
    assert row["size"] == 12


def test_decode_json_is_formatted():
    row = decode_payload(b'{"id":1,"name":"orders"}')

    assert row["kind"] == "json"
    # разложен по строкам, значит читаемый
    assert "\n" in row["text"]
    assert '"id"' in row["text"]


def test_decode_binary_shows_size_and_hex():
    raw = bytes([0x00, 0xFF, 0xFE, 0x01]) * 4
    row = decode_payload(raw)

    assert row["kind"] == "binary"
    assert row["text"] is None
    assert row["size"] == 16
    assert row["hex"].startswith("00 ff fe 01")


def test_decode_trims_long_text():
    row = decode_payload(("a" * 20000).encode("utf-8"))

    assert row["kind"] == "text"
    assert row["truncated"] is True
    assert len(row["text"]) <= 8192


def test_trim_text():
    assert trim_text("abc") == ("abc", False)

    cut, flag = trim_text("a" * 100, limit=10)

    assert flag is True
    assert len(cut) == 10


def test_read_plan_latest_defaults():
    plan = build_read_plan({"topic": "orders"})

    assert plan == {"topic": "orders", "partition": None, "mode": "latest",
                    "limit": 50, "offset": None, "timestamp_ms": None}


def test_read_plan_clamps_limit():
    assert build_read_plan({"topic": "t", "limit": 5000})["limit"] == 500
    assert build_read_plan({"topic": "t", "limit": 0})["limit"] == 1


def test_read_plan_offset_mode():
    plan = build_read_plan({"topic": "t", "mode": "offset", "offset": 100,
                            "partition": 2})

    assert plan["mode"] == "offset"
    assert plan["offset"] == 100
    assert plan["partition"] == 2


def test_read_plan_timestamp_mode():
    plan = build_read_plan({"topic": "t", "mode": "timestamp",
                            "timestamp": "2026-08-30 12:00"})

    assert plan["mode"] == "timestamp"
    assert plan["timestamp_ms"] > 0


def test_read_plan_rejects_bad_input():
    with pytest.raises(ValueError):
        build_read_plan({"topic": ""})

    with pytest.raises(ValueError):
        build_read_plan({"topic": "t", "mode": "offset", "offset": -1})

    with pytest.raises(ValueError):
        build_read_plan({"topic": "t", "mode": "timestamp",
                         "timestamp": "вчера"})

    with pytest.raises(ValueError):
        build_read_plan({"topic": "t", "mode": "нечто"})


def test_format_record():
    row = format_record({
        "topic": "orders", "partition": 1, "offset": 42,
        "timestamp": 1788073200000,
        "key": b"client-42", "value": b'{"sum": 10}',
        "headers": [("source", b"etl"), ("try", None)],
    })

    assert row["partition"] == 1
    assert row["offset"] == 42
    assert row["timestamp"].startswith("2026-")
    assert row["key"]["text"] == "client-42"
    assert row["value"]["kind"] == "json"
    assert row["headers"] == [["source", "etl"], ["try", ""]]


def test_format_record_without_key():
    row = format_record({"topic": "t", "partition": 0, "offset": 1,
                         "timestamp": None, "key": None, "value": b"x",
                         "headers": None})

    assert row["key"]["kind"] == "empty"
    assert row["timestamp"] is None
    assert row["headers"] == []
```

- [ ] **Шаг 2: тест падает**

`python -m pytest tests/test_kafka_messages.py -q` →
`ModuleNotFoundError: No module named 'modules.kafka_messages'`

- [ ] **Шаг 3: написать `modules/kafka_messages.py`**

```python
# -*- coding: utf-8 -*-
"""
Сообщения Kafka: декодирование тела, план чтения и форматирование.

Чистые функции: ни сети, ни базы. Ключ и значение приходят байтами —
угадывать Avro или Protobuf мы не беремся, честное «двоичные данные»
полезнее мусора на экране.
"""

import json
from datetime import datetime

SHOW_LIMIT = 8192
HEX_PREVIEW_BYTES = 64
MAX_LIMIT = 500
DEFAULT_LIMIT = 50
MOMENT_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M")


def trim_text(text, limit=SHOW_LIMIT):
    """(обрезанный текст, признак обрезки)."""
    value = text or ""

    if len(value) <= limit:
        return value, False

    return value[:limit], True


def _hex_preview(raw):
    head = raw[:HEX_PREVIEW_BYTES]
    return " ".join("{:02x}".format(b) for b in head)


def decode_payload(raw):
    """Байты → описание для экрана."""
    if raw is None or raw == b"":
        return {"kind": "empty", "text": None, "size": 0, "hex": None,
                "truncated": False}

    if isinstance(raw, str):
        raw = raw.encode("utf-8")

    size = len(raw)

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {"kind": "binary", "text": None, "size": size,
                "hex": _hex_preview(raw),
                "truncated": size > HEX_PREVIEW_BYTES}

    kind = "text"

    try:
        parsed = json.loads(text)
    except ValueError:
        parsed = None

    if isinstance(parsed, (dict, list)):
        kind = "json"
        text = json.dumps(parsed, ensure_ascii=False, indent=2)

    shown, truncated = trim_text(text)

    return {"kind": kind, "text": shown, "size": size, "hex": None,
            "truncated": truncated}


def parse_moment(text):
    """'2026-08-30 12:00' → миллисекунды эпохи."""
    raw = str(text or "").strip()

    for fmt in MOMENT_FORMATS:
        try:
            moment = datetime.strptime(raw, fmt)
        except ValueError:
            continue

        return int(moment.timestamp() * 1000)

    raise ValueError("Не разобрал дату и время: {}".format(text))


def build_read_plan(data):
    """Форма → план чтения."""
    data = data or {}
    topic = str(data.get("topic") or "").strip()

    if not topic:
        raise ValueError("Выберите топик")

    mode = str(data.get("mode") or "latest").strip().lower()

    if mode not in ("latest", "offset", "timestamp"):
        raise ValueError("Неизвестный режим чтения: {}".format(mode))

    try:
        limit = int(data.get("limit") or DEFAULT_LIMIT)
    except (TypeError, ValueError):
        raise ValueError("Лимит должен быть числом")

    limit = max(1, min(limit, MAX_LIMIT))

    partition = data.get("partition")

    if partition in (None, "", "all"):
        partition = None
    else:
        try:
            partition = int(partition)
        except (TypeError, ValueError):
            raise ValueError("Номер партиции должен быть числом")

    offset = None

    if mode == "offset":
        try:
            offset = int(data.get("offset"))
        except (TypeError, ValueError):
            raise ValueError("Оффсет должен быть числом")

        if offset < 0:
            raise ValueError("Оффсет не может быть отрицательным")

    timestamp_ms = None

    if mode == "timestamp":
        timestamp_ms = parse_moment(data.get("timestamp"))

    return {"topic": topic, "partition": partition, "mode": mode,
            "limit": limit, "offset": offset, "timestamp_ms": timestamp_ms}


def format_record(record):
    """Сырая запись с байтами → JSON-безопасная структура."""
    record = record or {}
    stamp = record.get("timestamp")
    when = None

    if stamp:
        try:
            when = datetime.fromtimestamp(int(stamp) / 1000.0).strftime(
                "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError, OSError):
            when = None

    headers = []

    for name, value in record.get("headers") or []:
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except UnicodeDecodeError:
                value = "<{} байт>".format(len(value))

        headers.append([str(name), str(value or "")])

    return {
        "topic": record.get("topic"),
        "partition": record.get("partition"),
        "offset": record.get("offset"),
        "timestamp": when,
        "key": decode_payload(record.get("key")),
        "value": decode_payload(record.get("value")),
        "headers": headers,
    }
```

- [ ] **Шаг 4: тесты зелёные** — `13 passed`
- [ ] **Шаг 5: коммит**

```bash
git add modules/kafka_messages.py tests/test_kafka_messages.py
git commit -m "feat(kafka): декодирование сообщений и план чтения"
```

---

### Задача 2: чтение и отправка в транспорте

**Файлы:**
- Изменить: `modules/kafka_client.py`
- Тест: `tests/test_kafka_messages_api.py` (первая часть)

**Интерфейсы:**
- Даёт наружу: `read_messages(cluster, topic, plan) -> list[dict]`
  (записи с байтами внутри), `send_message(cluster, topic, key, value,
  partition=None) -> {"partition": int, "offset": int}`.

- [ ] **Шаг 1: написать падающий тест**

Создать `tests/test_kafka_messages_api.py`:

```python
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
```

- [ ] **Шаг 2: тест падает** —
`AttributeError: module 'modules.kafka_client' has no attribute 'read_messages'`

- [ ] **Шаг 3: дописать `modules/kafka_client.py`**

Добавить в конец:

```python
def _as_bytes(value):
    if value is None or value == "":
        return None

    if isinstance(value, bytes):
        return value

    return str(value).encode("utf-8")


def _plan_seek(consumer, tps, plan):
    """Ставит каждую партицию на нужную позицию согласно режиму."""
    mode = plan.get("mode") or "latest"

    if mode == "offset":
        for tp in tps:
            consumer.seek(tp, int(plan.get("offset") or 0))
        return

    if mode == "timestamp":
        stamp = int(plan.get("timestamp_ms") or 0)
        found = consumer.offsets_for_times({tp: stamp for tp in tps}) or {}
        ends = consumer.end_offsets(tps)

        for tp in tps:
            row = found.get(tp)
            # записей позже указанного времени нет — встаём в конец
            consumer.seek(tp, getattr(row, "offset", None) if row
                          else int(ends.get(tp, 0)))
        return

    # latest: делим лимит между партициями и отступаем от конца
    limit = int(plan.get("limit") or 50)
    per = max(1, limit // max(1, len(tps)))
    begins = consumer.beginning_offsets(tps)
    ends = consumer.end_offsets(tps)

    for tp in tps:
        start = max(int(begins.get(tp, 0)), int(ends.get(tp, 0)) - per)
        consumer.seek(tp, start)


def read_messages(cluster, topic, plan):
    """
    Записи топика без консьюмер-группы.

    assign + seek, никаких коммитов: просмотр не должен двигать оффсеты
    боевых потребителей.
    """
    try:
        kafka = _import_kafka()
    except ImportError:
        raise KafkaUnavailable(INSTALL_HINT)

    consumer = open_consumer(cluster)

    try:
        numbers = sorted(consumer.partitions_for_topic(topic) or [])
        wanted = plan.get("partition")

        if wanted is not None:
            numbers = [n for n in numbers if n == int(wanted)]

        if not numbers:
            return []

        tps = [kafka.TopicPartition(topic, n) for n in numbers]
        consumer.assign(tps)
        _plan_seek(consumer, tps, plan)

        limit = int(plan.get("limit") or 50)
        rows = []
        empty_rounds = 0

        # три пустых опроса подряд — в топике больше нечего читать
        while len(rows) < limit and empty_rounds < 3:
            batch = consumer.poll(timeout_ms=1000,
                                  max_records=limit - len(rows))

            if not batch:
                empty_rounds += 1
                continue

            empty_rounds = 0

            for _tp, batch_rows in batch.items():
                for record in batch_rows:
                    rows.append({
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                        "timestamp": record.timestamp,
                        "key": record.key,
                        "value": record.value,
                        "headers": list(record.headers or []),
                    })

        rows.sort(key=lambda r: (r["partition"], r["offset"]))

        return rows[:limit]
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(consumer)


def send_message(cluster, topic, key, value, partition=None):
    """Отправка одной записи. Возвращает партицию и оффсет от брокера."""
    try:
        kafka = _import_kafka()
    except ImportError:
        raise KafkaUnavailable(INSTALL_HINT)

    timeout = int(cluster.get("request_timeout_ms") or DEFAULT_TIMEOUT_MS)

    # KafkaProducer принимает те же ключи, что client_kwargs;
    # enable_auto_commit и consumer_timeout_ms добавляет только консьюмер
    producer = kafka.KafkaProducer(**client_kwargs(cluster))

    try:
        future = producer.send(
            topic,
            value=_as_bytes(value),
            key=_as_bytes(key),
            partition=int(partition) if partition not in (None, "")
            else None,
        )
        meta = future.get(timeout=timeout / 1000.0)

        return {"partition": getattr(meta, "partition", None),
                "offset": getattr(meta, "offset", None)}
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        try:
            producer.close(timeout=1)
        except Exception:
            pass
```

- [ ] **Шаг 4: тесты зелёные** — `4 passed`
- [ ] **Шаг 5: коммит**

```bash
git add modules/kafka_client.py tests/test_kafka_messages_api.py
git commit -m "feat(kafka): чтение и отправка сообщений в транспорте"
```

---

### Задача 3: роуты сообщений

**Файлы:**
- Изменить: `kafka_routes.py`
- Создать: `templates/kafka_messages.html` (заглушка)
- Тест: `tests/test_kafka_messages_api.py` (дополнить)

- [ ] **Шаг 1: заглушка шаблона**

```html
{% extends "base.html" %}{% block content %}<div id="kmRoot"></div>{% endblock %}
```

- [ ] **Шаг 2: дописать тест**

В конец `tests/test_kafka_messages_api.py`:

```python
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
```

- [ ] **Шаг 3: дописать `kafka_routes.py`**

К импортам `modules.kafka_client` добавить `read_messages`,
`send_message`. Отдельным блоком:

```python
from modules.kafka_messages import build_read_plan, format_record
```

В конец файла:

```python
# ---------------- сообщения ----------------

@kafka_bp.route("/kafka/messages")
def kafka_messages_page():
    return render_template(
        "kafka_messages.html",
        clusters=list_clusters(),
        library_ready=library_available(),
    )


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/messages/read",
                methods=["POST"])
def api_kafka_messages_read(cluster_id):
    try:
        cluster = _cluster_or_404(cluster_id)
        plan = build_read_plan(request.get_json(silent=True) or {})
    except LookupError as error:
        return _fail(error, 404)
    except ValueError as error:
        return _fail(error)

    try:
        raw = read_messages(cluster, plan["topic"], plan)
    except KafkaUnavailable as error:
        return _fail(error, 502)

    return jsonify({"ok": True,
                    "records": [format_record(r) for r in raw]})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/messages",
                methods=["POST"])
def api_kafka_message_send(cluster_id):
    body = request.get_json(silent=True) or {}
    topic = str(body.get("topic") or "").strip()

    try:
        cluster = _cluster_or_404(cluster_id)

        if not topic:
            raise ValueError("Выберите топик")
    except LookupError as error:
        return _fail(error, 404)
    except ValueError as error:
        return _fail(error)

    value = body.get("value")
    # в журнал идёт только начало значения: оно может быть большим
    # и содержать данные клиентов
    intent = {"key": str(body.get("key") or "")[:120],
              "size": len(str(value or "")),
              "preview": str(value or "")[:120]}

    try:
        meta = send_message(cluster, topic, body.get("key"), value,
                            body.get("partition"))
    except KafkaUnavailable as error:
        audit_write(cluster_id, "send_message", topic, intent, "error")
        return _fail(error, 502)

    intent["partition"] = meta.get("partition")
    intent["offset"] = meta.get("offset")
    audit_write(cluster_id, "send_message", topic, intent, "ok")

    return jsonify({"ok": True, "partition": meta.get("partition"),
                    "offset": meta.get("offset")})
```

- [ ] **Шаг 4: тесты зелёные** — `10 passed` в файле
- [ ] **Шаг 5: коммит**

```bash
git add kafka_routes.py templates/kafka_messages.html tests/test_kafka_messages_api.py
git commit -m "feat(kafka): роуты чтения и отправки сообщений"
```

---

### Задача 4: экран сообщений

**Файлы:**
- Изменить: `templates/kafka_messages.html` (заменить заглушку)
- Создать: `static/js/kafka_messages.js`
- Изменить: `templates/base.html` — четвёртый пункт и заголовок страницы

- [ ] **Шаг 1: разметка** — полный текст `templates/kafka_messages.html`:

```html
{% extends "base.html" %}

{% block title %}Сообщения · Opsentri{% endblock %}

{% block content %}

<style>
.km { max-width: 1560px; display: flex; flex-direction: column; gap: 14px; }
.km-bar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  background: var(--surface); border: 1px solid var(--hairline);
  border-radius: var(--radius); padding: 10px 12px; }
.km-bar select, .km-bar input { flex: 0 1 200px; }
.km-bar .sp { flex: 1 1 auto; }
.km-note { border-radius: var(--radius); padding: 10px 14px; font-size: 13px;
  border: 1px solid var(--hairline); }
.km-note.warn { color: var(--warn);
  background: color-mix(in srgb, var(--warn) 12%, transparent); }
.km-note.err { color: var(--crit);
  background: color-mix(in srgb, var(--crit) 12%, transparent); }
.km-panel { background: var(--surface); border: 1px solid var(--hairline);
  border-radius: var(--radius); overflow: hidden; }
.km-panel > h2 { font-size: 12px; font-weight: 800; letter-spacing: .08em;
  text-transform: uppercase; color: var(--text-muted); margin: 0;
  padding: 11px 16px; border-bottom: 1px solid var(--hairline);
  display: flex; align-items: center; gap: 10px; }
.km-panel > h2 .sp { flex: 1; }
.km-tools { display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  padding: 10px 16px; border-bottom: 1px solid var(--hairline); }
.km-hint { font-size: 11.5px; color: var(--text-muted); }
.km-list { max-height: 520px; overflow: auto; padding: 6px; }
.km-row { display: flex; justify-content: space-between; gap: 12px;
  padding: 7px 10px; border-radius: var(--radius-sm); cursor: pointer;
  font-variant-numeric: tabular-nums; }
.km-row:hover, .km-row.open { background: var(--surface-2); }
.km-row .meta { font-size: 12px; color: var(--text-muted);
  white-space: nowrap; }
.km-row .peek { font-size: 12px; overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap; flex: 1 1 auto; }
.km-body { padding: 4px 12px 12px 22px; }
.km-body pre { background: var(--surface-2); border-radius: 8px;
  padding: 10px 12px; font-size: 12px; max-height: 320px; overflow: auto;
  margin: 6px 0; white-space: pre-wrap; word-break: break-word; }
.km-kv { font-size: 12px; color: var(--text-muted); }
.km-form { padding: 12px 16px; display: none; }
.km-form.on { display: block; }
.km-form textarea { width: 100%; min-height: 110px; font-size: 12px;
  font-family: var(--mono, monospace); }
.km-form .line { display: flex; gap: 10px; align-items: flex-end;
  flex-wrap: wrap; margin-bottom: 10px; }
.km-form .line > div { flex: 0 1 220px; }
.km-form .form-label { font-size: 11.5px; font-weight: 700;
  text-transform: uppercase; letter-spacing: .04em;
  color: var(--text-muted); margin-bottom: 4px; }
.km-empty { padding: 18px 16px; font-size: 13px; color: var(--text-muted); }
</style>

<div class="km" id="kmRoot">

  {% if not library_ready %}
  <div class="km-note warn">
    Библиотека <code>kafka-python</code> не установлена. На сервере выполните
    <code>pip install -r requirements.txt</code> и перезапустите app.py.
  </div>
  {% endif %}

  <div class="km-bar">
    <select id="kmCluster" class="form-select form-select-sm"
            {% if not clusters %}hidden{% endif %}>
      {% for c in clusters %}
      <option value="{{ c.id }}">{{ c.name }} — {{ c.bootstrap_servers }}</option>
      {% endfor %}
    </select>
    {% if not clusters %}
    <span class="km-hint">Кластеры ещё не заведены</span>
    {% endif %}
    <select id="kmTopic" class="form-select form-select-sm"></select>
    <select id="kmMode" class="form-select form-select-sm">
      <option value="latest">последние</option>
      <option value="offset">с оффсета</option>
      <option value="timestamp">с даты</option>
    </select>
    <input type="number" id="kmOffset" class="form-control form-control-sm"
           placeholder="оффсет" min="0" hidden>
    <input type="datetime-local" id="kmAt"
           class="form-control form-control-sm" hidden>
    <input type="number" id="kmLimit" class="form-control form-control-sm"
           value="50" min="1" max="500">
    <input type="number" id="kmPart" class="form-control form-control-sm"
           placeholder="партиция: все" min="0">
    <span class="sp"></span>
    <button class="btn btn-sm btn-outline-primary" id="kmRead" type="button"
            {% if not library_ready or not clusters %}disabled{% endif %}>
      Прочитать</button>
  </div>

  <div class="km-note err" id="kmError" style="display: none;"></div>

  <div class="km-panel">
    <h2>Записи<span class="sp"></span>
      <button class="btn btn-sm btn-secondary" id="kmSendToggle"
              type="button">Отправить сообщение</button></h2>

    <div class="km-form" id="kmForm">
      <div class="line">
        <div>
          <label class="form-label" for="kmKey">Ключ</label>
          <input type="text" id="kmKey" class="form-control form-control-sm"
                 placeholder="необязательно">
        </div>
        <div>
          <label class="form-label" for="kmSendPart">Партиция</label>
          <input type="number" id="kmSendPart"
                 class="form-control form-control-sm"
                 placeholder="выберет Kafka" min="0">
        </div>
      </div>
      <label class="form-label" for="kmValue">Значение</label>
      <textarea id="kmValue" class="form-control"
                placeholder='{"example": true}'></textarea>
      <div class="line" style="margin-top: 10px;">
        <button class="btn btn-sm btn-primary" id="kmSend" type="button">
          Отправить</button>
        <button class="btn btn-sm btn-outline-primary" id="kmSendCancel"
                type="button">Отмена</button>
      </div>
    </div>

    <div class="km-tools">
      <input type="search" id="kmFilter" class="form-control form-control-sm"
             placeholder="фильтр по загруженным записям">
      <span class="km-hint">Kafka не умеет искать по содержимому —
        фильтр работает только по тому, что уже прочитано.</span>
      <span class="km-hint" id="kmCount" style="margin-left: auto;"></span>
    </div>

    <div id="kmList"></div>
  </div>

</div>

<script src="{{ url_for('static', filename='js/kafka_messages.js') }}"></script>
{% endblock %}
```

- [ ] **Шаг 2: `static/js/kafka_messages.js`**

```javascript
/* Вкладка «Сообщения»: чтение записей и ручная отправка. */
(function () {
    "use strict";

    var records = [];
    var openIndex = null;

    function $(id) { return document.getElementById(id); }

    function esc(s) {
        return String(s === null || s === undefined ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }

    function fmtN(n) { return Number(n || 0).toLocaleString("ru-RU"); }

    function toast(message, kind) {
        if (window.gpToast) { window.gpToast(message, kind); }
    }

    function clusterId() {
        var sel = $("kmCluster");
        return sel && sel.value ? Number(sel.value) : null;
    }

    function api(url, options) {
        return fetch(url, options || {}).then(function (r) {
            return r.json().then(function (data) {
                return { status: r.status, data: data };
            });
        });
    }

    function send(url, body) {
        return api(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body || {}),
        });
    }

    function showError(message) {
        var box = $("kmError");
        if (!message) { box.style.display = "none"; return; }
        box.textContent = message;
        box.style.display = "";
    }

    function payloadPeek(payload) {
        if (!payload) { return ""; }
        if (payload.kind === "empty") { return "—"; }
        if (payload.kind === "binary") {
            return "двоичные данные, " + fmtN(payload.size) + " байт";
        }
        return (payload.text || "").replace(/\s+/g, " ").slice(0, 200);
    }

    function payloadBlock(title, payload) {
        if (!payload) { return ""; }

        if (payload.kind === "empty") {
            return '<div class="km-kv">' + title + ": —</div>";
        }

        if (payload.kind === "binary") {
            return '<div class="km-kv">' + title + ": двоичные данные, " +
                fmtN(payload.size) + " байт</div><pre>" +
                esc(payload.hex) + "</pre>";
        }

        return '<div class="km-kv">' + title + " · " + fmtN(payload.size) +
            " байт" + (payload.truncated ? " · показано начало" : "") +
            "</div><pre>" + esc(payload.text) + "</pre>";
    }

    function visible() {
        var needle = ($("kmFilter").value || "").trim().toLowerCase();

        if (!needle) { return records; }

        return records.filter(function (r) {
            var hay = (payloadPeek(r.key) + " " + payloadPeek(r.value))
                .toLowerCase();
            return hay.indexOf(needle) >= 0;
        });
    }

    function paint() {
        var rows = visible();

        $("kmCount").textContent = records.length
            ? "показано " + fmtN(rows.length) + " из " + fmtN(records.length)
            : "";

        if (!rows.length) {
            $("kmList").innerHTML = '<div class="km-empty">' +
                (records.length
                    ? "Ничего не найдено."
                    : "Записей нет — выберите топик и нажмите «Прочитать».") +
                "</div>";
            return;
        }

        $("kmList").innerHTML = '<div class="km-list">' +
            rows.map(function (r, i) {
                var open = openIndex === i;
                var body = open
                    ? '<div class="km-body">' +
                      payloadBlock("Ключ", r.key) +
                      payloadBlock("Значение", r.value) +
                      (r.headers && r.headers.length
                          ? '<div class="km-kv">Заголовки: ' +
                            esc(r.headers.map(function (h) {
                                return h[0] + "=" + h[1];
                            }).join(", ")) + "</div>"
                          : "") +
                      "</div>"
                    : "";

                return '<div class="km-row' + (open ? " open" : "") +
                    '" data-i="' + i + '"><span class="meta">п.' +
                    esc(r.partition) + " · оф. " + fmtN(r.offset) + " · " +
                    esc(r.timestamp || "—") + '</span><span class="peek">' +
                    esc(payloadPeek(r.value)) + "</span></div>" + body;
            }).join("") + "</div>";

        Array.prototype.forEach.call(
            $("kmList").querySelectorAll("[data-i]"),
            function (row) {
                row.onclick = function () {
                    var i = Number(row.getAttribute("data-i"));
                    openIndex = openIndex === i ? null : i;
                    repaint();
                };
            }
        );
    }

    function repaint() {
        if (window.gpKeepScroll) {
            window.gpKeepScroll($("kmList"), paint);
        } else {
            paint();
        }
    }

    function loadTopics() {
        var id = clusterId();

        if (!id) { return; }

        api("/api/kafka/clusters/" + id + "/overview").then(function (r) {
            var topics = ((r.data || {}).overview || {}).topics || [];

            $("kmTopic").innerHTML = topics.filter(function (t) {
                return !t.internal;
            }).map(function (t) {
                return '<option value="' + esc(t.name) + '">' +
                    esc(t.name) + "</option>";
            }).join("");

            if (!topics.length) {
                showError("В срезе обзора нет топиков — обновите срез на " +
                    "вкладке «Обзор кластера».");
            }
        });
    }

    function read() {
        var id = clusterId();

        if (!id) { return; }

        var body = {
            topic: $("kmTopic").value,
            mode: $("kmMode").value,
            limit: $("kmLimit").value,
            partition: $("kmPart").value,
        };

        if (body.mode === "offset") { body.offset = $("kmOffset").value; }
        if (body.mode === "timestamp") {
            body.timestamp = ($("kmAt").value || "").replace("T", " ");
        }

        $("kmRead").disabled = true;

        send("/api/kafka/clusters/" + id + "/messages/read", body)
            .then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    showError(r.data.message || "Не удалось прочитать");
                    return;
                }

                showError("");
                records = r.data.records || [];
                openIndex = null;
                paint();

                if (!records.length) {
                    toast("Записей нет: возможно, оффсет или дата за " +
                        "концом партиции", "warning");
                }
            })
            .catch(function (e) { showError(String(e)); })
            .then(function () { $("kmRead").disabled = false; });
    }

    function sendMessage() {
        var id = clusterId();
        var topic = $("kmTopic").value;

        if (!id || !topic) { return; }

        var value = $("kmValue").value;
        var question = "Отправить сообщение в топик «" + topic + "»?\n" +
            value.slice(0, 200);

        var doIt = function () {
            send("/api/kafka/clusters/" + id + "/messages", {
                topic: topic,
                key: $("kmKey").value,
                value: value,
                partition: $("kmSendPart").value,
            }).then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    toast(r.data.message || "Не удалось отправить", "danger");
                    return;
                }

                toast("Отправлено: партиция " + r.data.partition +
                    ", оффсет " + r.data.offset, "success");
                $("kmForm").classList.remove("on");
                $("kmValue").value = "";
                read();
            });
        };

        if (window.gpConfirm) {
            window.gpConfirm(question).then(function (yes) {
                if (yes) { doIt(); }
            });
        } else if (window.confirm(question)) {
            doIt();
        }
    }

    function wire() {
        if (!$("kmRoot") || !$("kmCluster")) { return; }

        $("kmCluster").onchange = function () {
            records = [];
            paint();
            loadTopics();
        };

        $("kmMode").onchange = function () {
            $("kmOffset").hidden = $("kmMode").value !== "offset";
            $("kmAt").hidden = $("kmMode").value !== "timestamp";
        };

        $("kmRead").onclick = read;
        $("kmFilter").oninput = repaint;

        $("kmSendToggle").onclick = function () {
            $("kmForm").classList.toggle("on");
        };
        $("kmSendCancel").onclick = function () {
            $("kmForm").classList.remove("on");
        };
        $("kmSend").onclick = sendMessage;

        paint();
        loadTopics();
    }

    document.addEventListener("DOMContentLoaded", wire);
}());
```

- [ ] **Шаг 3: меню**

В `templates/base.html`, в `<nav ... id="secBody-kfk">` после
«Консьюмер-группы»:

```html
                <a href="/kafka/messages" class="{% if p.startswith('/kafka/messages') %}active{% endif %}">
                    <span class="ic"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16v12H7l-3 3z"/></svg></span>
                    <span class="lbl">Сообщения</span></a>
```

И в блоке заголовков **перед** веткой `p.startswith('/kafka')`:

```html
{% elif p.startswith('/kafka/messages') %}
    {% set page_title, page_crumb = 'Сообщения', 'чтение топика и отправка' %}
```

- [ ] **Шаг 4: проверить**

```bash
node --check static/js/kafka_messages.js
python -m pytest tests -q
python -m flake8 --select=E9,F63,F7,F82 .
```

Ожидается: 282 прежних + 13 (`test_kafka_messages`) + 10
(`test_kafka_messages_api`) = `305 passed`, flake8 без вывода.

- [ ] **Шаг 5: коммит**

```bash
git add templates/kafka_messages.html templates/base.html static/js/kafka_messages.js
git commit -m "feat(kafka): экран сообщений"
```

---

### Задача 5: проверка в браузере

- [ ] `preview_stop` + `preview_start`, открыть `/kafka/messages`.
- [ ] Подменить `window.fetch`: обзор отдаёт топики, чтение — три записи
  (JSON, текст, двоичная). Проверить: список отрисован, раскрытие
  показывает `<pre>`, у двоичной вместо текста размер и hex.
- [ ] Проверить фильтр по загруженным записям и подпись под ним.
- [ ] Подменить чтение на 502 — красный баннер, записи на экране
  остаются.
- [ ] Снять скриншот; правки, если нашлись, закоммитить.

---

## Чем заканчивается Kafka Manager

Четыре этапа закрывают то, ради чего инструмент затевался: подключения и
обзор, лаг консьюмер-групп, управление топиками, чтение и отправка
сообщений.

За рамками осознанно остались: переназначение реплик, ACL и квоты,
Schema Registry, Kafka Connect. Каждое — отдельный инструмент со своей
моделью и своей ценой ошибки.
