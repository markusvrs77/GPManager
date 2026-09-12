# Kafka Manager, этап 3 — план реализации

> **Для исполнителя:** план выполняется задача за задачей. Каждая задача
> заканчивается зелёными тестами и коммитом.

**Спека:** `docs/superpowers/specs/2026-08-31-kafka-manager-stage3-design.md`

**Цель:** создание, удаление, изменение конфигурации и увеличение числа
партиций топиков — на вкладке «Обзор кластера».

**Архитектура:** прежняя. `import kafka` только в
`modules/kafka_client.py`; `modules/kafka_topics.py` — чистые функции;
роуты в существующий Blueprint; экран — правки в готовых файлах.

## Общие требования

- Комментарии и тексты интерфейса — по-русски.
- SQLite только через `db.sqlite_cursor()`; новых таблиц нет.
- `import kafka` разрешён только в `modules/kafka_client.py`.
- Тесты без брокера, клиент подменяется через `monkeypatch`.
- `python -m flake8 --select=E9,F63,F7,F82 .` обязан быть чистым.

## Сверенные факты о kafka-python 3.0.11

- `NewTopic(name, num_partitions=-1, replication_factor=-1,
  replica_assignments=None, topic_configs=None)`.
- `NewPartitions(total_count)` — **итоговое** число партиций.
- `create_partitions({topic: NewPartitions(n)})`.
- `describe_configs([ConfigResource(ConfigResourceType.TOPIC, name)],
  config_filter='dynamic')` → `{"topic": {name: {config_key: {...}}}}`.
- В словаре конфига **нет** ключа `name` — он стал ключом словаря.
  Внутри: `value`, `read_only`, `is_default`, `is_sensitive`,
  `documentation`, `config_source` (строка вида `DEFAULT_CONFIG`,
  `DYNAMIC_TOPIC_CONFIG`).
- `alter_configs([ConfigResource(ConfigResourceType.TOPIC, name,
  configs={key: value})])`.
- `ConfigResource`, `ConfigResourceType`, `NewTopic`, `NewPartitions`
  импортируются из `kafka.admin`.

## Карта файлов

| Файл | Ответственность |
|------|-----------------|
| `modules/kafka_topics.py` | валидация имени, сборка спецификаций, разбор конфигов |
| `modules/kafka_client.py` | пять операций над топиками |
| `kafka_routes.py` | пять роутов |
| `templates/kafka.html` | форма создания и панели в развороте |
| `static/js/kafka.js` | поведение действий |
| `tests/test_kafka_topics.py` | чистые функции |
| `tests/test_kafka_topics_api.py` | роуты |

---

### Задача 1: чистые функции топиков

**Файлы:**
- Создать: `modules/kafka_topics.py`
- Тест: `tests/test_kafka_topics.py`

**Интерфейсы:**
- Использует: только стандартную библиотеку.
- Даёт наружу: `validate_topic_name(name) -> str`,
  `build_topic_spec(data) -> dict`,
  `parse_configs(described, topic) -> list[dict]`,
  `build_config_changes(current, wanted) -> dict`,
  `assert_can_grow(current_count, target_count) -> int`.

- [ ] **Шаг 1: написать падающий тест**

Создать `tests/test_kafka_topics.py`:

```python
# -*- coding: utf-8 -*-
"""Топики Kafka: валидация имени, спецификации и конфиги."""

import pytest

from modules.kafka_topics import (
    assert_can_grow,
    build_config_changes,
    build_topic_spec,
    parse_configs,
    validate_topic_name,
)

# ответ describe_configs: имя ключа вынесено в ключ словаря,
# config_source — строка; сверено со схемами kafka-python 3.x
DESCRIBED = {
    "topic": {
        "orders": {
            "retention.ms": {"value": "604800000", "read_only": False,
                             "is_default": False, "is_sensitive": False,
                             "config_source": "DYNAMIC_TOPIC_CONFIG"},
            "cleanup.policy": {"value": "delete", "read_only": False,
                               "is_default": True, "is_sensitive": False,
                               "config_source": "DEFAULT_CONFIG"},
            "some.secret": {"value": "hidden", "read_only": False,
                            "is_default": False, "is_sensitive": True,
                            "config_source": "DYNAMIC_TOPIC_CONFIG"},
        }
    }
}


def test_valid_names_pass():
    assert validate_topic_name(" orders ") == "orders"
    assert validate_topic_name("dwh.orders_v2-1") == "dwh.orders_v2-1"


@pytest.mark.parametrize("bad", [
    "", "   ", ".", "..", "заказы", "orders topic", "orders:1",
    "a" * 250,
])
def test_bad_names_rejected(bad):
    with pytest.raises(ValueError):
        validate_topic_name(bad)


def test_build_topic_spec_defaults():
    spec = build_topic_spec({"name": "orders"})

    assert spec == {"name": "orders", "partitions": 1, "replication": 1,
                    "configs": {}}


def test_build_topic_spec_converts_retention_hours():
    spec = build_topic_spec({
        "name": "orders", "partitions": 6, "replication": 3,
        "retention_hours": 24, "cleanup_policy": "compact"})

    assert spec["partitions"] == 6
    assert spec["replication"] == 3
    # 24 часа в миллисекундах
    assert spec["configs"]["retention.ms"] == "86400000"
    assert spec["configs"]["cleanup.policy"] == "compact"


def test_build_topic_spec_drops_empty_values():
    spec = build_topic_spec({
        "name": "orders", "retention_hours": "", "cleanup_policy": "",
        "configs": {"segment.ms": "", "max.message.bytes": "1048576"}})

    assert spec["configs"] == {"max.message.bytes": "1048576"}


def test_build_topic_spec_rejects_bad_numbers():
    with pytest.raises(ValueError):
        build_topic_spec({"name": "orders", "partitions": 0})

    with pytest.raises(ValueError):
        build_topic_spec({"name": "orders", "retention_hours": "сутки"})


def test_parse_configs_flattens_and_sorts():
    rows = parse_configs(DESCRIBED, "orders")

    assert [r["key"] for r in rows] == [
        "cleanup.policy", "retention.ms", "some.secret"]

    by_key = {r["key"]: r for r in rows}

    assert by_key["retention.ms"]["value"] == "604800000"
    assert by_key["retention.ms"]["default"] is False
    assert by_key["cleanup.policy"]["default"] is True
    # значение секретного ключа наружу не отдаём
    assert by_key["some.secret"]["sensitive"] is True
    assert by_key["some.secret"]["value"] is None


def test_parse_configs_of_unknown_topic_is_empty():
    assert parse_configs(DESCRIBED, "payments") == []


def test_build_config_changes_keeps_only_changed():
    current = parse_configs(DESCRIBED, "orders")

    assert build_config_changes(current, {"retention.ms": "604800000"}) == {}
    assert build_config_changes(current, {"retention.ms": "3600000"}) == {
        "retention.ms": "3600000"}
    # ключ, которого не было, тоже изменение
    assert build_config_changes(current, {"segment.ms": "60000"}) == {
        "segment.ms": "60000"}


def test_build_config_changes_ignores_sensitive_echo():
    current = parse_configs(DESCRIBED, "orders")

    # у секретного ключа значения на экране не было, вернуть пустую
    # строку как «изменение» браузер не должен
    assert build_config_changes(current, {"some.secret": ""}) == {}


def test_assert_can_grow():
    assert assert_can_grow(3, 6) == 6

    with pytest.raises(ValueError) as err:
        assert_can_grow(6, 6)

    assert "6" in str(err.value)

    with pytest.raises(ValueError):
        assert_can_grow(6, 3)
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_topics.py -q`
Ожидается: `ModuleNotFoundError: No module named 'modules.kafka_topics'`

- [ ] **Шаг 3: написать `modules/kafka_topics.py`**

```python
# -*- coding: utf-8 -*-
"""
Топики Kafka: проверка имени, сборка спецификаций и разбор конфигов.

Всё здесь — чистые функции: ни сети, ни базы. Библиотека валидирует имя
сама, но бросает TypeError без объяснений, поэтому проверяем заранее и
своими словами.
"""

import re

TOPIC_NAME_RE = re.compile(r"^[a-zA-Z0-9._-]+$")
TOPIC_MAX_LENGTH = 249
CLEANUP_POLICIES = ("delete", "compact", "compact,delete", "delete,compact")


def validate_topic_name(name):
    """Очищенное имя либо ValueError с человеческим текстом."""
    clean = str(name or "").strip()

    if not clean:
        raise ValueError("Укажите имя топика")

    if len(clean) > TOPIC_MAX_LENGTH:
        raise ValueError(
            "Имя топика длиннее {} символов".format(TOPIC_MAX_LENGTH))

    if clean in (".", ".."):
        raise ValueError('Имя топика не может быть "." или ".."')

    if not TOPIC_NAME_RE.match(clean):
        raise ValueError(
            "В имени топика можно использовать только латинские буквы, "
            "цифры, точку, дефис и подчёркивание"
        )

    return clean


def _positive_int(value, default, label):
    if value in (None, ""):
        return default

    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError("{} должно быть числом".format(label))

    if number < 1:
        raise ValueError("{} должно быть больше нуля".format(label))

    return number


def build_topic_spec(data):
    """Форма создания → {name, partitions, replication, configs}."""
    spec = {
        "name": validate_topic_name((data or {}).get("name")),
        "partitions": _positive_int(
            (data or {}).get("partitions"), 1, "Число партиций"),
        "replication": _positive_int(
            (data or {}).get("replication"), 1, "Фактор репликации"),
        "configs": {},
    }

    policy = str((data or {}).get("cleanup_policy") or "").strip()

    if policy:
        if policy not in CLEANUP_POLICIES:
            raise ValueError(
                "cleanup.policy может быть delete, compact или "
                "compact,delete")

        spec["configs"]["cleanup.policy"] = policy

    hours = (data or {}).get("retention_hours")

    if hours not in (None, ""):
        try:
            value = float(hours)
        except (TypeError, ValueError):
            raise ValueError("Retention должен быть числом часов")

        if value <= 0:
            raise ValueError("Retention должен быть больше нуля")

        spec["configs"]["retention.ms"] = str(int(value * 3600 * 1000))

    for key, value in ((data or {}).get("configs") or {}).items():
        key = str(key or "").strip()

        if not key or value in (None, ""):
            continue

        spec["configs"][key] = str(value)

    return spec


def parse_configs(described, topic):
    """
    Ответ describe_configs → плоский список, отсортированный по ключу.

    Библиотека кладёт имя ключа в ключ словаря и заменяет config_source
    на строку вида DEFAULT_CONFIG.
    """
    entries = ((described or {}).get("topic") or {}).get(topic) or {}
    rows = []

    for key in sorted(entries):
        row = entries[key] or {}
        source = str(row.get("config_source") or "")
        sensitive = bool(row.get("is_sensitive"))

        rows.append({
            "key": key,
            # значение секретного ключа наружу не отдаём
            "value": None if sensitive else row.get("value"),
            "source": source,
            "default": bool(row.get("is_default")) or
            source.startswith("DEFAULT"),
            "sensitive": sensitive,
            "read_only": bool(row.get("read_only")),
        })

    return rows


def build_config_changes(current, wanted):
    """Только реально изменившиеся ключи."""
    have = {row["key"]: row for row in current or []}
    changes = {}

    for key, value in (wanted or {}).items():
        key = str(key or "").strip()

        if not key or value is None:
            continue

        row = have.get(key)

        # у секретного ключа значения на экране не было — менять нечего
        if row and row.get("sensitive") and value == "":
            continue

        if row is not None and str(row.get("value") or "") == str(value):
            continue

        changes[key] = str(value)

    return changes


def assert_can_grow(current_count, target_count):
    """Kafka умеет только увеличивать число партиций."""
    current = int(current_count or 0)

    try:
        target = int(target_count)
    except (TypeError, ValueError):
        raise ValueError("Число партиций должно быть числом")

    if target <= current:
        raise ValueError(
            "У топика уже {} партиций — уменьшить нельзя, "
            "только увеличить".format(current)
        )

    return target
```

- [ ] **Шаг 4: тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_topics.py -q`
Ожидается: `18 passed`

- [ ] **Шаг 5: коммит**

```bash
git add modules/kafka_topics.py tests/test_kafka_topics.py
git commit -m "feat(kafka): проверка имени и конфигов топиков"
```

---

### Задача 2: операции над топиками в транспорте

**Файлы:**
- Изменить: `modules/kafka_client.py`
- Тест: `tests/test_kafka_client.py` (дополнить)

**Интерфейсы:**
- Даёт наружу: `create_topic(cluster, spec)`, `delete_topic(cluster, name)`,
  `add_partitions(cluster, name, total_count)`,
  `fetch_topic_configs(cluster, name) -> dict`,
  `alter_topic_configs(cluster, name, changes)`.

- [ ] **Шаг 1: написать падающий тест**

Дописать в конец `tests/test_kafka_client.py`:

```python
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
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_client.py -q`
Ожидается: `AttributeError: module 'modules.kafka_client' has no attribute
'add_partitions'`

- [ ] **Шаг 3: дописать `modules/kafka_client.py`**

Добавить в конец файла:

```python
def _topic_error(cluster, name, error):
    """Частые ошибки брокера — человеческим языком."""
    text = str(error)

    if "AlreadyExists" in text or "already exists" in text:
        return KafkaUnavailable(
            "Топик {} уже есть в кластере".format(name))

    if "DeletionDisabled" in text or "delete.topic.enable" in text:
        return KafkaUnavailable(
            "Брокер запрещает удаление топиков: включите "
            "delete.topic.enable"
        )

    if "InvalidReplicationFactor" in text:
        return KafkaUnavailable(
            "Фактор репликации больше числа брокеров в кластере")

    return _request_failed(cluster, error)


def create_topic(cluster, spec):
    from kafka.admin import NewTopic

    admin = open_admin(cluster)

    try:
        admin.create_topics([NewTopic(
            name=spec["name"],
            num_partitions=int(spec.get("partitions") or 1),
            replication_factor=int(spec.get("replication") or 1),
            topic_configs=spec.get("configs") or None,
        )])
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _topic_error(cluster, spec.get("name"), error)
    finally:
        _close(admin)


def delete_topic(cluster, name):
    admin = open_admin(cluster)

    try:
        admin.delete_topics([name])
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _topic_error(cluster, name, error)
    finally:
        _close(admin)


def add_partitions(cluster, name, total_count):
    """total_count — ИТОГОВОЕ число партиций, а не приращение."""
    from kafka.admin import NewPartitions

    admin = open_admin(cluster)

    try:
        admin.create_partitions({name: NewPartitions(int(total_count))})
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _topic_error(cluster, name, error)
    finally:
        _close(admin)


def fetch_topic_configs(cluster, name):
    """
    Сырой ответ describe_configs по одному топику.

    Фильтр dynamic, а не умолчательный modified: на экране нужны все
    изменяемые ключи, включая те, что сидят на значениях по умолчанию.
    """
    from kafka.admin import ConfigResource, ConfigResourceType

    admin = open_admin(cluster)

    try:
        return admin.describe_configs(
            [ConfigResource(ConfigResourceType.TOPIC, name)],
            config_filter="dynamic",
        )
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _topic_error(cluster, name, error)
    finally:
        _close(admin)


def alter_topic_configs(cluster, name, changes):
    from kafka.admin import ConfigResource, ConfigResourceType

    if not changes:
        return

    admin = open_admin(cluster)

    try:
        admin.alter_configs([ConfigResource(
            ConfigResourceType.TOPIC, name, configs=dict(changes))])
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _topic_error(cluster, name, error)
    finally:
        _close(admin)
```

- [ ] **Шаг 4: тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_client.py -q`
Ожидается: `17 passed`

- [ ] **Шаг 5: коммит**

```bash
git add modules/kafka_client.py tests/test_kafka_client.py
git commit -m "feat(kafka): операции над топиками в транспорте"
```

---

### Задача 3: роуты топиков

**Файлы:**
- Изменить: `kafka_routes.py`
- Тест: `tests/test_kafka_topics_api.py`

**Интерфейсы:**
- Использует: `modules.kafka_topics` (все функции),
  `modules.kafka_client.create_topic`, `delete_topic`, `add_partitions`,
  `fetch_topic_configs`, `alter_topic_configs`,
  `modules.kafka_overview.collect_overview`, `modules.kafka_audit.write`.

- [ ] **Шаг 1: написать падающий тест**

Создать `tests/test_kafka_topics_api.py`:

```python
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
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_topics_api.py -q`
Ожидается: `AttributeError: module 'kafka_routes' has no attribute
'create_topic'`

- [ ] **Шаг 3: дописать `kafka_routes.py`**

В существующий блок `from modules.kafka_client import (...)` добавить
имена `add_partitions`, `alter_topic_configs`, `create_topic`,
`delete_topic`, `fetch_topic_configs`. Отдельным блоком добавить:

```python
from modules.kafka_topics import (
    assert_can_grow,
    build_config_changes,
    build_topic_spec,
    parse_configs,
    validate_topic_name,
)
```

И в конец файла:

```python
# ---------------- управление топиками ----------------

def _topic_in_overview(cluster_id, name):
    """Топик из среза обзора: оттуда берём текущее число партиций."""
    data = collect_overview(cluster_id)

    for topic in data.get("topics") or []:
        if topic.get("name") == name:
            return topic

    return None


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/topics",
                methods=["POST"])
def api_kafka_topic_create(cluster_id):
    try:
        cluster = _cluster_or_404(cluster_id)
        spec = build_topic_spec(request.get_json(silent=True) or {})
    except LookupError as error:
        return _fail(error, 404)
    except ValueError as error:
        return _fail(error)

    try:
        create_topic(cluster, spec)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "create_topic", spec["name"], spec, "error")
        code = 409 if "уже есть" in str(error) else 502
        return _fail(error, code)

    audit_write(cluster_id, "create_topic", spec["name"], spec, "ok")
    collect_overview(cluster_id, force=True)

    return jsonify({"ok": True, "name": spec["name"]})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/topics/<name>",
                methods=["DELETE"])
def api_kafka_topic_delete(cluster_id, name):
    try:
        cluster = _cluster_or_404(cluster_id)
        topic = validate_topic_name(name)
    except LookupError as error:
        return _fail(error, 404)
    except ValueError as error:
        return _fail(error)

    try:
        delete_topic(cluster, topic)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "delete_topic", topic, None, "error")
        return _fail(error, 502)

    audit_write(cluster_id, "delete_topic", topic, None, "ok")
    collect_overview(cluster_id, force=True)

    return jsonify({"ok": True})


@kafka_bp.route(
    "/api/kafka/clusters/<int:cluster_id>/topics/<name>/partitions",
    methods=["POST"])
def api_kafka_topic_partitions(cluster_id, name):
    body = request.get_json(silent=True) or {}
    known = None

    try:
        cluster = _cluster_or_404(cluster_id)
        topic = validate_topic_name(name)
        known = _topic_in_overview(cluster_id, topic)

        if not known:
            raise LookupError(
                "Топик не найден: {} — обновите срез".format(topic))

        total = assert_can_grow(known.get("partitions"), body.get("total"))
    except LookupError as error:
        return _fail(error, 404)
    except ValueError as error:
        # уменьшение партиций — конфликт состояния, не ошибка ввода
        code = 409 if "уменьшить" in str(error) else 400
        return _fail(error, code)

    intent = {"from": known.get("partitions"), "to": total}

    try:
        add_partitions(cluster, topic, total)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "add_partitions", topic, intent, "error")
        return _fail(error, 502)

    audit_write(cluster_id, "add_partitions", topic, intent, "ok")
    collect_overview(cluster_id, force=True)

    return jsonify({"ok": True, "total": total})


@kafka_bp.route(
    "/api/kafka/clusters/<int:cluster_id>/topics/<name>/configs",
    methods=["GET"])
def api_kafka_topic_configs(cluster_id, name):
    try:
        cluster = _cluster_or_404(cluster_id)
        topic = validate_topic_name(name)
        described = fetch_topic_configs(cluster, topic)
    except LookupError as error:
        return _fail(error, 404)
    except KafkaUnavailable as error:
        return _fail(error, 502)
    except ValueError as error:
        return _fail(error)

    return jsonify({"ok": True, "configs": parse_configs(described, topic)})


@kafka_bp.route(
    "/api/kafka/clusters/<int:cluster_id>/topics/<name>/configs",
    methods=["PUT"])
def api_kafka_topic_configs_update(cluster_id, name):
    body = request.get_json(silent=True) or {}

    try:
        cluster = _cluster_or_404(cluster_id)
        topic = validate_topic_name(name)
        current = parse_configs(fetch_topic_configs(cluster, topic), topic)
        changes = build_config_changes(current, body.get("configs") or {})
    except LookupError as error:
        return _fail(error, 404)
    except KafkaUnavailable as error:
        return _fail(error, 502)
    except ValueError as error:
        return _fail(error)

    if not changes:
        return jsonify({"ok": True, "changed": 0,
                        "message": "Изменений нет"})

    try:
        alter_topic_configs(cluster, topic, changes)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "alter_configs", topic, changes, "error")
        return _fail(error, 502)

    audit_write(cluster_id, "alter_configs", topic, changes, "ok")

    return jsonify({"ok": True, "changed": len(changes)})
```

- [ ] **Шаг 4: тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_topics_api.py -q`
Ожидается: `9 passed`

- [ ] **Шаг 5: коммит**

```bash
git add kafka_routes.py tests/test_kafka_topics_api.py
git commit -m "feat(kafka): роуты управления топиками"
```

---

### Задача 4: экран управления топиками

**Файлы:**
- Изменить: `templates/kafka.html`
- Изменить: `static/js/kafka.js`

- [ ] **Шаг 1: стили и разметка**

В `templates/kafka.html` в блок `<style>` добавить перед `.kf-empty`:

```css
.kf-head-btn { margin-left: auto; }
.kf-form { padding: 12px 16px; border-bottom: 1px solid var(--hairline);
  display: none; }
.kf-form.on { display: block; }
.kf-form .fgrid { display: grid; gap: 12px;
  grid-template-columns: repeat(4, minmax(0, 1fr)); }
@media (max-width: 900px) {
  .kf-form .fgrid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
.kf-form .form-label { font-size: 11.5px; font-weight: 700;
  letter-spacing: .04em; text-transform: uppercase;
  color: var(--text-muted); margin-bottom: 4px; }
.kf-form .buttons { display: flex; gap: 8px; margin-top: 12px; }
.kf-acts { display: flex; gap: 8px; margin: 10px 0 2px; flex-wrap: wrap;
  align-items: center; }
.kf-cfg { margin-top: 8px; }
.kf-cfg-row { display: flex; align-items: center; gap: 10px; padding: 3px 0;
  font-size: 12px; }
.kf-cfg-row .k { flex: 0 0 260px; color: var(--text-muted);
  font-family: var(--mono, monospace); }
.kf-cfg-row input { flex: 0 1 220px; }
.kf-cfg-row .d { font-size: 11px; color: var(--text-muted); }
```

Заголовок карточки топиков (`<h2>Топики</h2>`) заменить на:

```html
    <h2>Топики
      <button class="btn btn-sm btn-outline-primary kf-head-btn"
              id="kfTopicAdd" type="button">+ Топик</button></h2>

    <div class="kf-form" id="kfTopicForm">
      <div class="fgrid">
        <div>
          <label class="form-label" for="kfTopicName">Имя</label>
          <input type="text" id="kfTopicName" class="form-control"
                 placeholder="dwh.orders">
        </div>
        <div>
          <label class="form-label" for="kfTopicParts">Партиций</label>
          <input type="number" id="kfTopicParts" class="form-control"
                 value="1" min="1">
        </div>
        <div>
          <label class="form-label" for="kfTopicRf">Репликация</label>
          <input type="number" id="kfTopicRf" class="form-control"
                 value="1" min="1">
        </div>
        <div>
          <label class="form-label" for="kfTopicRet">Retention, часов</label>
          <input type="number" id="kfTopicRet" class="form-control"
                 placeholder="по умолчанию" min="1">
        </div>
        <div>
          <label class="form-label" for="kfTopicPolicy">cleanup.policy</label>
          <select id="kfTopicPolicy" class="form-select">
            <option value="">по умолчанию</option>
            <option value="delete">delete</option>
            <option value="compact">compact</option>
            <option value="compact,delete">compact,delete</option>
          </select>
        </div>
      </div>
      <div class="buttons">
        <button class="btn btn-sm btn-primary" id="kfTopicSave"
                type="button">Создать</button>
        <button class="btn btn-sm btn-outline-primary" id="kfTopicCancel"
                type="button">Отмена</button>
      </div>
    </div>
```

- [ ] **Шаг 2: поведение**

В `static/js/kafka.js` добавить перед `function wire()`:

```javascript
    /* ---------------- управление топиками ---------------- */

    var configs = {};   // конфиги топиков, загруженные по требованию

    function toast(message, kind) {
        if (window.gpToast) { window.gpToast(message, kind); }
    }

    function send(url, method, body) {
        return api(url, {
            method: method,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body || {}),
        });
    }

    function ask(question) {
        if (window.gpConfirm) { return window.gpConfirm(question); }
        return Promise.resolve(window.confirm(question));
    }

    function actionsHtml(topic) {
        var rows = configs[topic.name];
        var html = '<div class="kf-acts">' +
            '<button class="btn btn-sm btn-secondary" data-cfg="' +
            esc(topic.name) + '">Конфигурация</button>' +
            '<button class="btn btn-sm btn-secondary" data-parts="' +
            esc(topic.name) + '">Добавить партиции</button>' +
            '<button class="btn btn-sm btn-outline-primary" data-drop="' +
            esc(topic.name) + '">Удалить</button></div>';

        if (rows) {
            html += '<div class="kf-cfg">' + rows.map(function (c) {
                var value = c.value === null ? "" : c.value;
                return '<div class="kf-cfg-row"><span class="k">' +
                    esc(c.key) + "</span>" +
                    '<input class="form-control form-control-sm" ' +
                    'data-cfg-key="' + esc(c.key) + '" value="' +
                    esc(value) + '"' + (c.read_only ? " disabled" : "") +
                    '><span class="d">' +
                    (c.default ? "по умолчанию" : "задано") +
                    (c.sensitive ? " · скрыто" : "") + "</span></div>";
            }).join("") +
                '<div class="kf-acts"><button class="btn btn-sm btn-primary"' +
                ' data-cfg-save="' + esc(topic.name) +
                '">Сохранить</button></div></div>';
        }

        return html;
    }

    function wireTopicActions() {
        var bind = function (attr, handler) {
            Array.prototype.forEach.call(
                $("kfTopics").querySelectorAll("[" + attr + "]"),
                function (b) {
                    b.onclick = function (event) {
                        event.stopPropagation();
                        handler(b.getAttribute(attr));
                    };
                }
            );
        };

        bind("data-cfg", loadConfigs);
        bind("data-parts", growPartitions);
        bind("data-drop", dropTopic);
        bind("data-cfg-save", saveConfigs);
    }

    function loadConfigs(name) {
        api("/api/kafka/clusters/" + clusterId() + "/topics/" +
            encodeURIComponent(name) + "/configs").then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                toast(r.data.message || "Не удалось получить конфигурацию",
                    "danger");
                return;
            }

            configs[name] = r.data.configs;
            repaintTopics();
        });
    }

    function saveConfigs(name) {
        var wanted = {};

        Array.prototype.forEach.call(
            $("kfTopics").querySelectorAll("[data-cfg-key]"),
            function (input) {
                wanted[input.getAttribute("data-cfg-key")] = input.value;
            }
        );

        send("/api/kafka/clusters/" + clusterId() + "/topics/" +
            encodeURIComponent(name) + "/configs", "PUT", { configs: wanted })
            .then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    toast(r.data.message || "Не удалось сохранить", "danger");
                    return;
                }

                toast(r.data.changed
                    ? "Изменено ключей: " + r.data.changed
                    : "Изменений нет", "success");
                loadConfigs(name);
            });
    }

    function growPartitions(name) {
        var topic = ((overview && overview.topics) || []).filter(
            function (t) { return t.name === name; })[0];

        if (!topic) { return; }

        var target = window.prompt(
            "Сколько партиций должно стать у «" + name + "»?\n" +
            "Сейчас " + topic.partitions + ". Уменьшать Kafka не умеет.\n" +
            "После увеличения записи с тем же ключом пойдут в другую " +
            "партицию — порядок по ключу сломается.",
            String(topic.partitions + 1));

        if (!target) { return; }

        send("/api/kafka/clusters/" + clusterId() + "/topics/" +
            encodeURIComponent(name) + "/partitions", "POST",
            { total: Number(target) }).then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                toast(r.data.message || "Не удалось изменить", "danger");
                return;
            }

            toast("Партиций стало " + r.data.total, "success");
            loadOverview(false);
        });
    }

    function dropTopic(name) {
        ask("Удалить топик «" + name + "»? Все его данные будут потеряны.")
            .then(function (yes) {
                if (!yes) { return; }

                api("/api/kafka/clusters/" + clusterId() + "/topics/" +
                    encodeURIComponent(name), { method: "DELETE" })
                    .then(function (r) {
                        if (r.status !== 200 || !r.data.ok) {
                            toast(r.data.message || "Не удалось удалить",
                                "danger");
                            return;
                        }

                        toast("Топик удалён", "success");
                        delete configs[name];
                        openTopic = null;
                        loadOverview(false);
                    });
            });
    }

    function createTopic() {
        send("/api/kafka/clusters/" + clusterId() + "/topics", "POST", {
            name: $("kfTopicName").value,
            partitions: $("kfTopicParts").value,
            replication: $("kfTopicRf").value,
            retention_hours: $("kfTopicRet").value,
            cleanup_policy: $("kfTopicPolicy").value,
        }).then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                toast(r.data.message || "Не удалось создать", "danger");
                return;
            }

            toast("Топик «" + r.data.name + "» создан", "success");
            $("kfTopicForm").classList.remove("on");
            $("kfTopicName").value = "";
            loadOverview(false);
        });
    }
```

В `partsHtml` заменить завершающее `.join("") + "</div>"` на
`.join("") + "</div>" + actionsHtml(topic)` — кнопки появляются в
развороте топика.

В конце `paintTopics()`, после существующего цикла по `[data-topic]`,
добавить строку `wireTopicActions();`.

В `wire()` добавить:

```javascript
        $("kfTopicAdd").onclick = function () {
            $("kfTopicForm").classList.toggle("on");
        };
        $("kfTopicCancel").onclick = function () {
            $("kfTopicForm").classList.remove("on");
        };
        $("kfTopicSave").onclick = createTopic;
```

- [ ] **Шаг 3: проверить и прогнать всё**

```bash
node --check static/js/kafka.js
python -m pytest tests -q
python -m flake8 --select=E9,F63,F7,F82 .
```

Ожидается: `node --check` без вывода, `282 passed` (251 прежний + 18 в
test_kafka_topics + 4 в test_kafka_client + 9 в test_kafka_topics_api),
flake8 без вывода.

- [ ] **Шаг 4: коммит**

```bash
git add templates/kafka.html static/js/kafka.js
git commit -m "feat(kafka): создание и настройка топиков на экране"
```

---

### Задача 5: проверка в браузере

- [ ] **Шаг 1:** `preview_stop` + `preview_start`, открыть `/kafka`.
- [ ] **Шаг 2:** подменить `window.fetch` так, чтобы обзор вернул два
  топика; развернуть топик и убедиться, что появились три кнопки.
- [ ] **Шаг 3:** нажать «Конфигурация» с подменённым ответом конфигов;
  проверить, что ключи отрисовались, «по умолчанию» помечено, а поле с
  `read_only` заблокировано.
- [ ] **Шаг 4:** проверить отказы — 409 при уменьшении партиций и 502 при
  запрете удаления: оба должны показать тост с текстом сервера, а список
  топиков остаться на экране.
- [ ] **Шаг 5:** снять скриншот; правки, если нашлись, закоммитить.

---

## Что остаётся за рамками этапа 3

Просмотр и отправка сообщений — этап 4.

Переназначение реплик (`alter_partition_reassignments`) не входит: это
операция уровня балансировки кластера, у неё своя механика отслеживания
прогресса и своя цена ошибки.
