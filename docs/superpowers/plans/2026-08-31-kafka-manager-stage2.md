# Kafka Manager, этап 2 — план реализации

> **Для исполнителя:** план выполняется задача за задачей. Шаги отмечаются
> чекбоксами (`- [ ]`). Каждая задача заканчивается зелёными тестами и
> коммитом.

**Спека:** `docs/superpowers/specs/2026-08-31-kafka-manager-stage2-design.md`

**Цель:** вкладка «Консьюмер-группы»: состояние групп, лаг по партициям,
сброс оффсетов и удаление пустой группы, с журналом действий.

**Архитектура:** та же слоёная схема, что на этапе 1. Весь контакт с
библиотекой остаётся в `modules/kafka_client.py`; `modules/kafka_groups.py`
считает лаг чистыми функциями и хранит срез; `modules/kafka_audit.py` пишет
журнал; роуты добавляются в существующий Blueprint `kafka_routes.py`.

**Стек:** Python 3, Flask, SQLite, `kafka-python` 3.x, ванильный JS.

## Общие требования

- Комментарии и тексты интерфейса — по-русски.
- Даты — строки `YYYY-MM-DD HH:MM:SS`.
- SQLite только через `db.sqlite_cursor()`.
- `import kafka` разрешён **только** в `modules/kafka_client.py`.
- Тесты не требуют брокера: клиенты подменяются через `monkeypatch`.
- `python -m flake8 --select=E9,F63,F7,F82 .` обязан быть чистым.
- Автообновления нет: кластер опрашивается только по кнопке.
- Имена полей ответов библиотеки — только сверенные (см. ниже), не по памяти.

## Сверенные факты о kafka-python 3.0.11

Проверено по исходникам перед написанием плана:

- `list_groups()` → список dict с `group_id`, `protocol_type`, `group_state`.
- `describe_groups(ids)` → `{group_id: {...}}`; у группы `group_id`,
  `group_state`, `protocol_type`, `protocol_data`, `members[]`.
- Участник: `member_id`, `group_instance_id`, `client_id`, `client_host`,
  `member_metadata`, `member_assignment`. Assignment после `to_dict()`
  содержит `assigned_partitions` — список `{"topic": str,
  "partitions": [int]}`.
- `list_group_offsets(specs)` → `{group_id: {TopicPartition:
  OffsetAndMetadata}}`; у `OffsetAndMetadata` поля `offset`, `metadata`,
  `leader_epoch`. **Отсутствующий коммит приходит как `offset == -1`.**
- `reset_group_offsets(group_id, {TopicPartition: value})`, где value —
  `OffsetSpec` (`EARLIEST = -2`, `LATEST = -1`), `OffsetTimestamp` (мс
  эпохи) или обычный int (явный оффсет). Требует группу без активных
  участников. Возвращает `{TopicPartition: {'error': ...}}`.
- `kafka.TopicPartition` — namedtuple `(topic, partition)`.
- `OffsetSpec` и `OffsetTimestamp` импортируются из `kafka.admin`.

## Карта файлов

| Файл | Ответственность |
|------|-----------------|
| `db.py` | таблица `kafka_group_snapshots` |
| `modules/kafka_audit.py` | запись и чтение журнала действий |
| `modules/kafka_clusters.py` | `delete_cluster` чистит и групповой срез |
| `modules/kafka_client.py` | четыре новые операции по группам |
| `modules/kafka_groups.py` | расчёт лага, срез, спецификации сброса |
| `kafka_routes.py` | страница и API групп |
| `templates/kafka_groups.html` | разметка |
| `static/js/kafka_groups.js` | поведение |
| `templates/base.html` | третий пункт меню |
| `tests/test_kafka_audit.py` | журнал |
| `tests/test_kafka_groups.py` | лаг, спецификации, срез |
| `tests/test_kafka_groups_api.py` | роуты |

---

### Задача 1: таблица среза и журнал действий

**Файлы:**
- Изменить: `db.py` — в `init_db()`, сразу после блока
  `CREATE TABLE IF NOT EXISTS kafka_audit`
- Создать: `modules/kafka_audit.py`
- Изменить: `modules/kafka_clusters.py` — функция `delete_cluster`
- Тест: `tests/test_kafka_audit.py`

**Интерфейсы:**
- Использует: `db.sqlite_cursor`.
- Даёт наружу: `write(cluster_id, action, target, details=None,
  result="ok") -> int` (id записи), `recent(cluster_id=None, limit=50)
  -> list[dict]` с ключами `id`, `cluster_id`, `action`, `target`,
  `details`, `result`, `created_at`.

- [ ] **Шаг 1: написать падающий тест**

Создать `tests/test_kafka_audit.py`:

```python
# -*- coding: utf-8 -*-
"""Журнал опасных действий над Kafka."""

from modules.kafka_audit import recent, write
from modules.kafka_clusters import create_cluster, delete_cluster


def test_write_and_read_back():
    cluster_id = create_cluster({
        "name": "Audit", "bootstrap_servers": "kfk1:9092"})

    first = write(cluster_id, "reset_offsets", "etl-loader",
                  {"mode": "earliest", "partitions": 6}, "ok")
    second = write(cluster_id, "delete_group", "old-group", None, "ok")

    assert first and second and second > first

    rows = recent(cluster_id)

    # новые записи первыми
    assert [r["action"] for r in rows] == ["delete_group", "reset_offsets"]
    assert rows[1]["details"]["mode"] == "earliest"
    assert rows[0]["details"] is None
    assert rows[0]["created_at"]

    delete_cluster(cluster_id)


def test_audit_survives_cluster_removal():
    cluster_id = create_cluster({
        "name": "Gone", "bootstrap_servers": "kfk1:9092"})

    write(cluster_id, "reset_offsets", "g1", None, "ok")
    delete_cluster(cluster_id)

    # журнал должен пережить объект, к которому относится
    assert len(recent(cluster_id)) == 1


def test_limit_is_applied():
    cluster_id = create_cluster({
        "name": "Many", "bootstrap_servers": "kfk1:9092"})

    for i in range(5):
        write(cluster_id, "reset_offsets", "g{}".format(i), None, "ok")

    assert len(recent(cluster_id, limit=3)) == 3

    delete_cluster(cluster_id)
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_audit.py -q`
Ожидается: `ModuleNotFoundError: No module named 'modules.kafka_audit'`

- [ ] **Шаг 3: добавить таблицу в `db.py`**

Вставить в `init_db()` сразу после блока
`CREATE TABLE IF NOT EXISTS kafka_audit`:

```python
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS kafka_group_snapshots (
                cluster_id INTEGER PRIMARY KEY,
                taken_at TEXT NOT NULL,
                payload BLOB NOT NULL,
                groups_total INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_kafka_audit_cluster
            ON kafka_audit(cluster_id, id DESC)
            """
        )
```

- [ ] **Шаг 4: написать `modules/kafka_audit.py`**

```python
# -*- coding: utf-8 -*-
"""
Журнал опасных действий над Kafka.

Сброс оффсетов и удаление группы необратимы, поэтому каждое такое
действие оставляет след: что, когда и чем кончилось. Журнал переживает
удаление кластера — иначе следы можно было бы замести, удалив
подключение.
"""

import json
from datetime import datetime

from db import sqlite_cursor


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def write(cluster_id, action, target, details=None, result="ok"):
    """Возвращает id записи."""
    payload = None

    if details is not None:
        payload = json.dumps(details, ensure_ascii=False)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO kafka_audit (
                cluster_id, action, target, details_json, result, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                int(cluster_id) if cluster_id is not None else None,
                str(action),
                str(target) if target is not None else None,
                payload,
                str(result),
                _now(),
            ),
        )
        return int(cur.lastrowid)


def recent(cluster_id=None, limit=50):
    """Последние записи, новые первыми."""
    limit = max(1, min(int(limit or 50), 500))

    sql = """
        SELECT id, cluster_id, action, target, details_json, result,
               created_at
        FROM kafka_audit
    """
    params = []

    if cluster_id is not None:
        sql += " WHERE cluster_id = ?"
        params.append(int(cluster_id))

    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with sqlite_cursor() as cur:
        cur.execute(sql, params)
        rows = []

        for row in cur.fetchall():
            item = dict(row)
            raw = item.pop("details_json", None)

            try:
                item["details"] = json.loads(raw) if raw else None
            except ValueError:
                item["details"] = None

            rows.append(item)

        return rows
```

- [ ] **Шаг 5: научить `delete_cluster` чистить групповой срез**

В `modules/kafka_clusters.py` заменить тело `delete_cluster`:

```python
def delete_cluster(cluster_id):
    """
    Вместе с кластером уходят его срезы — они больше ни к чему.
    Записи kafka_audit остаются: журнал должен переживать объект.
    """
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "DELETE FROM kafka_snapshots WHERE cluster_id = ?",
            (int(cluster_id),),
        )
        cur.execute(
            "DELETE FROM kafka_group_snapshots WHERE cluster_id = ?",
            (int(cluster_id),),
        )
        cur.execute(
            "DELETE FROM kafka_clusters WHERE id = ?", (int(cluster_id),)
        )
        return cur.rowcount > 0
```

- [ ] **Шаг 6: убедиться, что тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_audit.py tests/test_kafka_clusters.py -q`
Ожидается: `10 passed`

- [ ] **Шаг 7: коммит**

```bash
git add db.py modules/kafka_audit.py modules/kafka_clusters.py tests/test_kafka_audit.py
git commit -m "feat(kafka): журнал действий и таблица среза групп"
```

---

### Задача 2: операции по группам в транспорте

**Файлы:**
- Изменить: `modules/kafka_client.py`
- Тест: `tests/test_kafka_client.py` (дополнить)

**Интерфейсы:**
- Использует: `open_admin`, `_request_failed`, `_close`, `_import_kafka`,
  `INSTALL_HINT`, `KafkaUnavailable` — всё уже есть.
- Даёт наружу:
  `fetch_groups(cluster) -> list[dict]` — описания групп: `group_id`,
  `group_state`, `protocol_data`, `members`;
  `fetch_group_offsets(cluster, group_ids) -> {(group, topic, partition): int | None}`;
  `reset_offsets(cluster, group_id, specs) -> {(topic, partition): str | None}`,
  где значение — текст ошибки или `None` при успехе, а `specs` — это
  `{(topic, partition): (mode, value)}` из `build_reset_specs`;
  `delete_group(cluster, group_id) -> None`.

- [ ] **Шаг 1: написать падающий тест**

Дописать в конец `tests/test_kafka_client.py`:

```python
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
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_client.py -q`
Ожидается: `AttributeError: module 'modules.kafka_client' has no attribute 'fetch_groups'`

- [ ] **Шаг 3: дописать `modules/kafka_client.py`**

Добавить в конец файла:

```python
def fetch_groups(cluster):
    """
    Описания всех консьюмер-групп кластера.

    list_groups даёт только имена и состояние; участники и протокол
    приходят из describe_groups.
    """
    admin = open_admin(cluster)

    try:
        listed = admin.list_groups() or []
        ids = [g.get("group_id") for g in listed if g.get("group_id")]

        if not ids:
            return []

        described = admin.describe_groups(ids) or {}
        groups = []

        for group_id in ids:
            row = described.get(group_id)

            if row:
                groups.append(dict(row))
                continue

            # брокер не описал группу — показываем хотя бы состояние
            base = [g for g in listed if g.get("group_id") == group_id][0]
            groups.append({
                "group_id": group_id,
                "group_state": base.get("group_state"),
                "protocol_data": base.get("protocol_type"),
                "members": [],
            })

        return groups
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(admin)


def fetch_group_offsets(cluster, group_ids):
    """
    {(группа, топик, партиция): оффсет или None}.

    Kafka отдаёт -1, когда коммита по партиции не было; наружу это
    уходит как None, иначе «не читали» превратится в «нулевой оффсет»
    и лаг посчитается неверно.
    """
    if not group_ids:
        return {}

    admin = open_admin(cluster)

    try:
        specs = {}

        for group_id in group_ids:
            specs[group_id] = None

        answer = admin.list_group_offsets(specs) or {}
        out = {}

        for group_id, partitions in answer.items():
            for tp, meta in (partitions or {}).items():
                offset = getattr(meta, "offset", meta)
                offset = int(offset) if offset is not None else None

                if offset is not None and offset < 0:
                    offset = None

                out[(group_id, tp.topic, tp.partition)] = offset

        return out
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(admin)


def _offset_value(mode, value):
    """(режим, значение) -> то, что понимает reset_group_offsets."""
    from kafka.admin import OffsetSpec, OffsetTimestamp

    if mode == "earliest":
        return OffsetSpec.EARLIEST

    if mode == "latest":
        return OffsetSpec.LATEST

    if mode == "timestamp":
        return OffsetTimestamp(int(value))

    raise KafkaUnavailable("Неизвестный режим сброса: {}".format(mode))


def reset_offsets(cluster, group_id, specs):
    """
    specs — {(топик, партиция): (режим, значение)} из build_reset_specs.
    Возвращает {(топик, партиция): текст ошибки или None}.
    """
    if not specs:
        return {}

    try:
        kafka = _import_kafka()
    except ImportError:
        raise KafkaUnavailable(INSTALL_HINT)

    admin = open_admin(cluster)

    try:
        request = {}

        for (topic, part), (mode, value) in specs.items():
            request[kafka.TopicPartition(topic, part)] = _offset_value(
                mode, value)

        answer = admin.reset_group_offsets(group_id, request) or {}
        out = {}

        for tp, row in answer.items():
            error = (row or {}).get("error")
            out[(tp.topic, tp.partition)] = str(error) if error else None

        return out
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(admin)


def delete_group(cluster, group_id):
    admin = open_admin(cluster)

    try:
        admin.delete_groups([group_id])
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(admin)
```

- [ ] **Шаг 4: убедиться, что тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_client.py -q`
Ожидается: `11 passed`

- [ ] **Шаг 5: коммит**

```bash
git add modules/kafka_client.py tests/test_kafka_client.py
git commit -m "feat(kafka): операции по группам в транспорте"
```

---

### Задача 3: расчёт лага и срез групп

**Файлы:**
- Создать: `modules/kafka_groups.py`
- Тест: `tests/test_kafka_groups.py`

**Интерфейсы:**
- Использует: `modules.kafka_clusters.get_cluster`,
  `modules.kafka_client.fetch_groups`, `fetch_group_offsets`,
  `fetch_offsets`.
- Даёт наружу:
  `GroupBusy(Exception)`;
  `build_groups(groups_meta, committed, end_offsets) -> dict`;
  `find_group(data, group_id) -> dict | None`;
  `assert_group_is_idle(group) -> None`;
  `parse_moment(text) -> int` (мс эпохи);
  `build_reset_specs(mode, target, partitions) -> {(topic, partition): (mode, value)}`;
  `empty_groups(cluster_id) -> dict`;
  `load_snapshot(cluster_id) -> dict | None`;
  `save_snapshot(cluster_id, data) -> None`;
  `collect_groups(cluster_id, force=False) -> dict`.

- [ ] **Шаг 1: написать падающий тест**

Создать `tests/test_kafka_groups.py`:

```python
# -*- coding: utf-8 -*-
"""Расчёт лага консьюмер-групп и подготовка сброса оффсетов."""

import pytest

from modules import kafka_groups
from modules.kafka_clusters import create_cluster, delete_cluster
from modules.kafka_groups import (
    GroupBusy,
    assert_group_is_idle,
    build_groups,
    build_reset_specs,
    collect_groups,
    empty_groups,
    find_group,
    load_snapshot,
    parse_moment,
    save_snapshot,
)

# имена полей сверены со схемами kafka-python 3.x
GROUPS_META = [
    {
        "group_id": "etl-loader",
        "group_state": "Stable",
        "protocol_data": "range",
        "members": [
            {"member_id": "m-1", "client_id": "c-1",
             "client_host": "10.0.0.7",
             "member_assignment": {"assigned_partitions": [
                 {"topic": "orders", "partitions": [0, 1]}]}},
        ],
    },
    {
        "group_id": "idle-group",
        "group_state": "Empty",
        "protocol_data": "",
        "members": [],
    },
]

COMMITTED = {
    ("etl-loader", "orders", 0): 4100,
    ("etl-loader", "orders", 1): None,     # коммита не было
    ("idle-group", "orders", 0): 9000,
}

END = {("orders", 0): 9000, ("orders", 1): 700}


def test_lag_is_end_minus_committed():
    data = build_groups(GROUPS_META, COMMITTED, END)
    group = find_group(data, "etl-loader")
    topic = group["topics"][0]

    assert topic["name"] == "orders"
    assert topic["parts"][0]["lag"] == 4900
    assert group["lag"] == 4900
    assert group["members"] == 1
    assert group["state"] == "Stable"


def test_partition_without_commit_has_no_lag():
    data = build_groups(GROUPS_META, COMMITTED, END)
    part = find_group(data, "etl-loader")["topics"][0]["parts"][1]

    # «не читали» — это не «отставания нет»
    assert part["committed"] is None
    assert part["lag"] is None


def test_group_without_lag_is_zero_not_none():
    data = build_groups(GROUPS_META, COMMITTED, END)
    group = find_group(data, "idle-group")

    assert group["lag"] == 0
    assert group["state"] == "Empty"


def test_group_with_no_commits_at_all_has_null_lag():
    data = build_groups(GROUPS_META, {("etl-loader", "orders", 0): None}, END)

    assert find_group(data, "etl-loader")["lag"] is None


def test_owner_comes_from_assignment():
    data = build_groups(GROUPS_META, COMMITTED, END)
    part = find_group(data, "etl-loader")["topics"][0]["parts"][0]

    assert part["client"] == "c-1"
    assert part["host"] == "10.0.0.7"


def test_groups_sorted_by_lag_desc():
    data = build_groups(GROUPS_META, COMMITTED, END)

    assert [g["id"] for g in data["groups"]] == ["etl-loader", "idle-group"]


def test_assert_group_is_idle():
    data = build_groups(GROUPS_META, COMMITTED, END)

    with pytest.raises(GroupBusy) as err:
        assert_group_is_idle(find_group(data, "etl-loader"))

    assert "1" in str(err.value)

    assert_group_is_idle(find_group(data, "idle-group"))   # не бросает


def test_build_reset_specs_modes():
    parts = [("orders", 0), ("orders", 1)]

    assert build_reset_specs("earliest", None, parts) == {
        ("orders", 0): ("earliest", None),
        ("orders", 1): ("earliest", None),
    }
    assert build_reset_specs("latest", None, parts)[("orders", 0)] == (
        "latest", None)

    stamped = build_reset_specs("timestamp", "2026-08-30 12:00", parts)

    assert stamped[("orders", 0)][0] == "timestamp"
    assert stamped[("orders", 0)][1] == parse_moment("2026-08-30 12:00")


def test_build_reset_specs_rejects_bad_input():
    with pytest.raises(ValueError):
        build_reset_specs("earliest", None, [])

    with pytest.raises(ValueError):
        build_reset_specs("nonsense", None, [("orders", 0)])

    with pytest.raises(ValueError):
        build_reset_specs("timestamp", "не дата", [("orders", 0)])


def test_snapshot_roundtrip():
    cluster_id = create_cluster({
        "name": "Groups", "bootstrap_servers": "kfk1:9092"})

    assert load_snapshot(cluster_id) is None

    save_snapshot(cluster_id, build_groups(GROUPS_META, COMMITTED, END))
    loaded = load_snapshot(cluster_id)

    assert loaded["empty"] is False
    assert loaded["taken_at"]
    assert len(loaded["groups"]) == 2

    delete_cluster(cluster_id)

    assert load_snapshot(cluster_id) is None


def test_empty_groups_shape():
    data = empty_groups(7)

    assert data["empty"] is True
    assert data["cluster_id"] == 7
    assert data["groups"] == []
    assert data["taken_at"] is None


def test_collect_without_force_does_not_touch_cluster(monkeypatch):
    cluster_id = create_cluster({
        "name": "Cached", "bootstrap_servers": "kfk1:9092"})

    def never(*args, **kwargs):
        raise AssertionError("без force кластер трогать нельзя")

    monkeypatch.setattr(kafka_groups, "fetch_groups", never)
    save_snapshot(cluster_id, build_groups(GROUPS_META, COMMITTED, END))

    assert len(collect_groups(cluster_id)["groups"]) == 2

    delete_cluster(cluster_id)


def test_collect_with_force_asks_cluster(monkeypatch):
    cluster_id = create_cluster({
        "name": "Live", "bootstrap_servers": "kfk1:9092"})

    seen = {}

    def fake_end_offsets(cluster, pairs):
        seen["pairs"] = sorted(pairs)
        return {}, END

    monkeypatch.setattr(kafka_groups, "fetch_groups",
                        lambda c: GROUPS_META)
    monkeypatch.setattr(kafka_groups, "fetch_group_offsets",
                        lambda c, ids: COMMITTED)
    monkeypatch.setattr(kafka_groups, "fetch_offsets", fake_end_offsets)

    data = collect_groups(cluster_id, force=True)

    # концы спрашиваем только по партициям, которые кто-то читает
    assert seen["pairs"] == [("orders", 0), ("orders", 1)]
    assert find_group(data, "etl-loader")["lag"] == 4900
    assert load_snapshot(cluster_id) is not None

    delete_cluster(cluster_id)
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_groups.py -q`
Ожидается: `ModuleNotFoundError: No module named 'modules.kafka_groups'`

- [ ] **Шаг 3: написать `modules/kafka_groups.py`**

```python
# -*- coding: utf-8 -*-
"""
Консьюмер-группы: расчёт лага, срез в SQLite и подготовка сброса.

Лаг = конец партиции минус закоммиченный оффсет. Обе половины приходят
снаружи, поэтому основная работа — чистая функция без сети и базы.
"""

import json
import zlib
from datetime import datetime

from db import sqlite_cursor
from modules.kafka_client import (
    fetch_group_offsets,
    fetch_groups,
    fetch_offsets,
)
from modules.kafka_clusters import get_cluster

MOMENT_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M")


class GroupBusy(Exception):
    """В группе есть активные участники — менять оффсеты нельзя."""


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _pack(payload):
    return zlib.compress(
        json.dumps(payload, ensure_ascii=False).encode("utf-8"), 6)


def _unpack(blob):
    if blob is None:
        return None

    if isinstance(blob, (bytes, bytearray)):
        try:
            blob = zlib.decompress(bytes(blob)).decode("utf-8")
        except zlib.error:
            blob = bytes(blob).decode("utf-8", "replace")

    try:
        return json.loads(blob)
    except Exception:
        return None


def _owners(group):
    """{(топик, партиция): (клиент, хост)} из назначений участников."""
    out = {}

    for member in group.get("members") or []:
        assignment = member.get("member_assignment")

        if not isinstance(assignment, dict):
            continue

        for row in assignment.get("assigned_partitions") or []:
            topic = row.get("topic")

            for number in row.get("partitions") or []:
                out[(topic, number)] = (
                    member.get("client_id"), member.get("client_host")
                )

    return out


def build_groups(groups_meta, committed, end_offsets):
    """
    Описания групп + коммиты + концы партиций → структура среза.

    Чистая функция: ни сети, ни базы. committed приходит ключами
    (группа, топик, партиция), end_offsets — (топик, партиция).
    """
    groups = []

    for meta in groups_meta or []:
        group_id = meta.get("group_id")
        owners = _owners(meta)

        # партиции группы: и те, по которым есть коммит, и назначенные
        keys = set(
            (topic, number) for (gid, topic, number) in committed or {}
            if gid == group_id
        )
        keys |= set(owners)

        by_topic = {}
        group_lag = 0
        known = 0

        for topic, number in sorted(keys):
            offset = (committed or {}).get((group_id, topic, number))
            end = (end_offsets or {}).get((topic, number))
            lag = None

            if offset is not None and end is not None:
                lag = max(0, int(end) - int(offset))
                group_lag += lag
                known += 1

            client, host = owners.get((topic, number), (None, None))

            by_topic.setdefault(topic, []).append({
                "p": number,
                "committed": offset,
                "end": end,
                "lag": lag,
                "client": client,
                "host": host,
            })

        topics = []

        for name in sorted(by_topic):
            parts = by_topic[name]
            lags = [p["lag"] for p in parts if p["lag"] is not None]

            topics.append({
                "name": name,
                "lag": sum(lags) if lags else None,
                "parts": parts,
            })

        groups.append({
            "id": group_id,
            "state": meta.get("group_state"),
            "protocol": meta.get("protocol_data") or "",
            "members": len(meta.get("members") or []),
            "partitions": len(keys),
            # ни одного известного лага — честный null, а не ноль
            "lag": group_lag if known else None,
            "topics": topics,
        })

    groups.sort(key=lambda g: (g["lag"] is None, -(g["lag"] or 0), g["id"]))

    return {"groups": groups}


def find_group(data, group_id):
    for group in (data or {}).get("groups") or []:
        if group.get("id") == group_id:
            return group

    return None


def assert_group_is_idle(group):
    """Kafka не даёт менять оффсеты у группы с активными участниками."""
    members = int((group or {}).get("members") or 0)

    if members:
        raise GroupBusy(
            "В группе {} активных участника(ов) — остановите потребителей "
            "и повторите".format(members)
        )


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


def build_reset_specs(mode, target, partitions):
    """
    {(топик, партиция): (режим, значение)} для reset_offsets.

    Значение нужно только режиму timestamp; остальные разрешает брокер.
    """
    mode = str(mode or "").strip().lower()

    if mode not in ("earliest", "latest", "timestamp"):
        raise ValueError("Неизвестный режим сброса: {}".format(mode))

    if not partitions:
        raise ValueError("Не выбрано ни одной партиции")

    value = parse_moment(target) if mode == "timestamp" else None

    return {(topic, number): (mode, value) for topic, number in partitions}


def empty_groups(cluster_id):
    return {
        "cluster_id": int(cluster_id),
        "empty": True,
        "taken_at": None,
        "groups": [],
    }


def load_snapshot(cluster_id):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT taken_at, payload
            FROM kafka_group_snapshots
            WHERE cluster_id = ?
            """,
            (int(cluster_id),),
        )
        row = cur.fetchone()

    if not row:
        return None

    data = _unpack(row["payload"])

    if data is None:
        return None

    return {
        "cluster_id": int(cluster_id),
        "empty": False,
        "taken_at": row["taken_at"],
        "groups": data.get("groups") or [],
    }


def save_snapshot(cluster_id, data):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO kafka_group_snapshots (
                cluster_id, taken_at, payload, groups_total
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(cluster_id) DO UPDATE SET
                taken_at = excluded.taken_at,
                payload = excluded.payload,
                groups_total = excluded.groups_total
            """,
            (
                int(cluster_id),
                _now(),
                _pack(data),
                len(data.get("groups") or []),
            ),
        )


def collect_groups(cluster_id, force=False):
    """Без force — срез из базы, с force — опрос кластера и сохранение."""
    cluster = get_cluster(cluster_id)

    if not cluster:
        raise ValueError("Кластер не найден: {}".format(cluster_id))

    if not force:
        return load_snapshot(cluster_id) or empty_groups(cluster_id)

    groups_meta = fetch_groups(cluster)
    ids = [g.get("group_id") for g in groups_meta if g.get("group_id")]
    committed = fetch_group_offsets(cluster, ids)

    # концы нужны только по тем партициям, которые кто-то читает
    pairs = sorted(set(
        (topic, number) for (_gid, topic, number) in committed
    ))

    _begin, end = fetch_offsets(cluster, pairs)
    data = build_groups(groups_meta, committed, end)

    try:
        save_snapshot(cluster_id, data)
    except Exception:
        result = dict(data)
        result.update({
            "cluster_id": int(cluster_id),
            "empty": False,
            "taken_at": _now(),
            "saved": False,
        })
        return result

    return load_snapshot(cluster_id) or empty_groups(cluster_id)
```

- [ ] **Шаг 4: убедиться, что тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_groups.py -q`
Ожидается: `13 passed`

- [ ] **Шаг 5: коммит**

```bash
git add modules/kafka_groups.py tests/test_kafka_groups.py
git commit -m "feat(kafka): расчёт лага и срез консьюмер-групп"
```

---

### Задача 4: роуты групп

**Файлы:**
- Изменить: `kafka_routes.py`
- Создать: `templates/kafka_groups.html` — заглушка (разметка в задаче 5)
- Тест: `tests/test_kafka_groups_api.py`

**Интерфейсы:**
- Использует: `modules.kafka_groups` (весь публичный интерфейс),
  `modules.kafka_client.reset_offsets`, `delete_group`,
  `modules.kafka_audit.write`, `recent`.
- Даёт наружу: пять новых роутов в существующем `kafka_bp`.

Коды ответов: 200 — успех; 400 — плохой ввод; 404 — кластер или группа не
найдены; 409 — в группе есть участники; 502 — кластер недоступен.

- [ ] **Шаг 1: создать заглушку шаблона**

Создать `templates/kafka_groups.html`:

```html
{% extends "base.html" %}{% block content %}<div id="kgRoot"></div>{% endblock %}
```

- [ ] **Шаг 2: написать падающий тест**

Создать `tests/test_kafka_groups_api.py`:

```python
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
```

- [ ] **Шаг 3: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_groups_api.py -q`
Ожидается: `AttributeError: module 'kafka_routes' has no attribute 'collect_groups'`

- [ ] **Шаг 4: дописать `kafka_routes.py`**

Добавить импорты рядом с существующими:

```python
from modules.kafka_audit import recent as audit_recent
from modules.kafka_audit import write as audit_write
from modules.kafka_client import delete_group, reset_offsets
from modules.kafka_groups import (
    GroupBusy,
    assert_group_is_idle,
    build_reset_specs,
    collect_groups,
    find_group,
)
```

И в конец файла:

```python
@kafka_bp.route("/kafka/groups")
def kafka_groups_page():
    return render_template(
        "kafka_groups.html",
        clusters=list_clusters(),
        library_ready=library_available(),
    )


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/groups",
                methods=["GET"])
def api_kafka_groups(cluster_id):
    return _groups(cluster_id, force=False)


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/groups/refresh",
                methods=["POST"])
def api_kafka_groups_refresh(cluster_id):
    return _groups(cluster_id, force=True)


def _groups(cluster_id, force):
    try:
        _cluster_or_404(cluster_id)
        data = collect_groups(cluster_id, force=force)
    except LookupError as error:
        return _fail(error, 404)
    except KafkaUnavailable as error:
        return _fail(error, 502)
    except ValueError as error:
        return _fail(error)

    return jsonify({"ok": True, "groups": data})


def _idle_group_or_error(cluster_id, group_id):
    """Группа из среза, уже проверенная на активных участников."""
    group = find_group(collect_groups(cluster_id), group_id)

    if not group:
        raise LookupError(
            "Группа не найдена: {} — обновите срез".format(group_id))

    assert_group_is_idle(group)

    return group


def _group_partitions(group, topics):
    """Партиции группы, при желании суженные до выбранных топиков."""
    wanted = set(topics or [])
    pairs = []

    for topic in group.get("topics") or []:
        if wanted and topic.get("name") not in wanted:
            continue

        for part in topic.get("parts") or []:
            pairs.append((topic.get("name"), part.get("p")))

    return pairs


@kafka_bp.route(
    "/api/kafka/clusters/<int:cluster_id>/groups/<group_id>/reset",
    methods=["POST"])
def api_kafka_group_reset(cluster_id, group_id):
    body = request.get_json(silent=True) or {}

    try:
        cluster = _cluster_or_404(cluster_id)
        group = _idle_group_or_error(cluster_id, group_id)
        specs = build_reset_specs(
            body.get("mode"),
            body.get("timestamp"),
            _group_partitions(group, body.get("topics")),
        )
    except LookupError as error:
        return _fail(error, 404)
    except GroupBusy as error:
        return _fail(error, 409)
    except ValueError as error:
        return _fail(error)

    intent = {
        "mode": body.get("mode"),
        "timestamp": body.get("timestamp"),
        "partitions": len(specs),
    }

    try:
        answer = reset_offsets(cluster, group_id, specs)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "reset_offsets", group_id, intent, "error")
        return _fail(error, 502)

    failed = [
        {"topic": topic, "partition": part, "error": text}
        for (topic, part), text in sorted(answer.items()) if text
    ]
    done = len(answer) - len(failed)
    result = "ok" if not failed else ("partial" if done else "failed")

    intent["done"] = done
    intent["failed"] = failed
    audit_write(cluster_id, "reset_offsets", group_id, intent, result)

    return jsonify({"ok": True, "done": done, "failed": failed,
                    "result": result})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/groups/<group_id>",
                methods=["DELETE"])
def api_kafka_group_delete(cluster_id, group_id):
    try:
        cluster = _cluster_or_404(cluster_id)
        _idle_group_or_error(cluster_id, group_id)
    except LookupError as error:
        return _fail(error, 404)
    except GroupBusy as error:
        return _fail(error, 409)

    try:
        delete_group(cluster, group_id)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "delete_group", group_id, None, "error")
        return _fail(error, 502)

    audit_write(cluster_id, "delete_group", group_id, None, "ok")

    return jsonify({"ok": True})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/audit",
                methods=["GET"])
def api_kafka_audit(cluster_id):
    return jsonify({"ok": True, "records": audit_recent(cluster_id)})
```

- [ ] **Шаг 5: убедиться, что тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_groups_api.py -q`
Ожидается: `11 passed`

- [ ] **Шаг 6: коммит**

```bash
git add kafka_routes.py templates/kafka_groups.html tests/test_kafka_groups_api.py
git commit -m "feat(kafka): роуты консьюмер-групп и журнал"
```

---

### Задача 5: экран групп

**Файлы:**
- Изменить: `templates/kafka_groups.html` (заменить заглушку)
- Создать: `static/js/kafka_groups.js`
- Изменить: `templates/base.html` — третий пункт в разделе Kafka и ветка
  `page_title` для `/kafka/groups`

**Интерфейсы:**
- Использует API задачи 4 и глобальные помощники `window.gpToast`,
  `window.gpConfirm`, `window.gpKeepScroll`.

- [ ] **Шаг 1: заменить `templates/kafka_groups.html`**

```html
{% extends "base.html" %}

{% block title %}Консьюмер-группы · Opsentri{% endblock %}

{% block content %}

<style>
/* ============ Kafka: консьюмер-группы ============ */
.kg { max-width: 1560px; display: flex; flex-direction: column; gap: 14px; }

.kg-bar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  background: var(--surface); border: 1px solid var(--hairline);
  border-radius: var(--radius); padding: 10px 12px; }
.kg-bar select { flex: 0 1 420px; min-width: 220px; }
.kg-bar .sp { flex: 1 1 auto; }
.kg-snap { font-size: 12px; color: var(--text-muted); white-space: nowrap; }

.kg-note { border-radius: var(--radius); padding: 10px 14px; font-size: 13px;
  border: 1px solid var(--hairline); }
.kg-note.warn { color: var(--warn);
  background: color-mix(in srgb, var(--warn) 12%, transparent); }
.kg-note.err { color: var(--crit);
  background: color-mix(in srgb, var(--crit) 12%, transparent); }

.kg-panel { background: var(--surface); border: 1px solid var(--hairline);
  border-radius: var(--radius); overflow: hidden; }
.kg-panel > h2 { font-size: 12px; font-weight: 800; letter-spacing: .08em;
  text-transform: uppercase; color: var(--text-muted); margin: 0;
  padding: 11px 16px; border-bottom: 1px solid var(--hairline); }

.kg-tools { display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
  padding: 10px 16px; border-bottom: 1px solid var(--hairline); }
.kg-tools input[type="search"] { flex: 0 1 280px; min-width: 180px; }
.kg-check { display: flex; align-items: center; gap: 7px; font-size: 13px;
  color: var(--text-muted); white-space: nowrap; cursor: pointer; }
.kg-count { font-size: 12px; color: var(--text-muted); margin-left: auto; }

.kg-list { max-height: 460px; overflow: auto; padding: 6px; }
.kg-row { display: flex; justify-content: space-between; align-items: center;
  gap: 12px; padding: 8px 10px; border-radius: var(--radius-sm);
  cursor: pointer; }
.kg-row:hover, .kg-row.open { background: var(--surface-2); }
.kg-row .r { font-size: 12px; color: var(--text-muted); white-space: nowrap;
  font-variant-numeric: tabular-nums; }

.kg-tag { font-size: 11px; padding: 1px 8px; border-radius: var(--radius-pill);
  border: 1px solid var(--hairline); color: var(--text-muted);
  margin-left: 6px; white-space: nowrap; }
.kg-tag.busy { color: var(--warn);
  border-color: color-mix(in srgb, var(--warn) 45%, transparent); }
.kg-lag { font-variant-numeric: tabular-nums; font-weight: 700; }
.kg-lag.hot { color: var(--crit); }

.kg-body { padding: 4px 10px 12px 22px; }
.kg-topic { font-size: 12px; color: var(--text-muted); margin: 6px 0 2px;
  font-weight: 700; }
.kg-part { font-size: 12px; color: var(--text-muted); padding: 2px 0;
  font-variant-numeric: tabular-nums; }
.kg-acts { display: flex; gap: 8px; margin-top: 10px; flex-wrap: wrap;
  align-items: center; }

.kg-reset { padding: 12px 16px; border-top: 1px solid var(--hairline);
  display: none; }
.kg-reset.on { display: block; }
.kg-reset .line { display: flex; gap: 10px; align-items: center;
  flex-wrap: wrap; margin-bottom: 10px; }
.kg-reset select, .kg-reset input { max-width: 230px; }

.kg-empty { padding: 18px 16px; font-size: 13px; color: var(--text-muted); }
.kg-audit { font-size: 12px; color: var(--text-muted); padding: 8px 16px;
  border-bottom: 1px solid var(--hairline);
  font-variant-numeric: tabular-nums; }
.kg-audit:last-child { border-bottom: none; }
.kg-audit.bad { color: var(--crit); }
</style>

<div class="kg" id="kgRoot">

  {% if not library_ready %}
  <div class="kg-note warn">
    Библиотека <code>kafka-python</code> не установлена. На сервере выполните
    <code>pip install -r requirements.txt</code> и перезапустите app.py.
  </div>
  {% endif %}

  <div class="kg-bar">
    <select id="kgCluster" class="form-select form-select-sm"
            {% if not clusters %}hidden{% endif %}>
      {% for c in clusters %}
      <option value="{{ c.id }}">{{ c.name }} — {{ c.bootstrap_servers }}</option>
      {% endfor %}
    </select>
    {% if not clusters %}
    <span class="kg-snap">Кластеры ещё не заведены</span>
    {% endif %}
    <a class="btn btn-sm btn-secondary" href="/kafka/connections">
      Подключения</a>
    <span class="sp"></span>
    <span class="kg-snap" id="kgSnap">среза нет</span>
    <button class="btn btn-sm btn-outline-primary" id="kgRefresh" type="button"
            {% if not library_ready or not clusters %}disabled{% endif %}>
      Обновить срез</button>
  </div>

  <div class="kg-note err" id="kgError" style="display: none;"></div>

  <div class="kg-panel">
    <h2>Консьюмер-группы</h2>
    <div class="kg-tools">
      <input type="search" id="kgFilter" class="form-control form-control-sm"
             placeholder="поиск по имени группы">
      <label class="kg-check">
        <input type="checkbox" id="kgOnlyLag"> только с лагом
      </label>
      <span class="kg-count" id="kgCount"></span>
    </div>
    <div id="kgList"></div>

    <div class="kg-reset" id="kgReset">
      <div class="line">
        <b id="kgResetTitle"></b>
        <select id="kgResetMode" class="form-select form-select-sm">
          <option value="earliest">к началу</option>
          <option value="latest">к концу</option>
          <option value="timestamp">на дату и время</option>
        </select>
        <input type="datetime-local" id="kgResetAt"
               class="form-control form-control-sm" hidden>
        <button class="btn btn-sm btn-primary" id="kgResetGo" type="button">
          Сбросить оффсеты</button>
        <button class="btn btn-sm btn-outline-primary" id="kgResetCancel"
                type="button">Отмена</button>
      </div>
      <div class="kg-count" id="kgResetHint"></div>
    </div>
  </div>

  <div class="kg-panel">
    <h2>Журнал действий</h2>
    <div id="kgAudit"></div>
  </div>

</div>

<script src="{{ url_for('static', filename='js/kafka_groups.js') }}"></script>
{% endblock %}
```

- [ ] **Шаг 2: написать `static/js/kafka_groups.js`**

```javascript
/* Вкладка «Консьюмер-группы»: лаг и сброс оффсетов. */
(function () {
    "use strict";

    var snapshot = null;    // последний показанный срез
    var openGroup = null;   // какая группа развёрнута
    var resetFor = null;    // для какой группы открыта панель сброса

    function $(id) { return document.getElementById(id); }

    function esc(s) {
        return String(s === null || s === undefined ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }

    function fmtN(n) {
        return Number(n || 0).toLocaleString("ru-RU");
    }

    function lagText(lag) {
        return lag === null || lag === undefined ? "—" : fmtN(lag);
    }

    function clusterId() {
        var sel = $("kgCluster");
        return sel && sel.value ? Number(sel.value) : null;
    }

    function api(url, options) {
        return fetch(url, options || {}).then(function (r) {
            return r.json().then(function (data) {
                return { status: r.status, data: data };
            });
        });
    }

    function toast(message, kind) {
        if (window.gpToast) { window.gpToast(message, kind); }
    }

    function showError(message) {
        var box = $("kgError");
        if (!message) { box.style.display = "none"; return; }
        box.textContent = message;
        box.style.display = "";
    }

    function groups() {
        return (snapshot && snapshot.groups) || [];
    }

    function byId(id) {
        return groups().filter(function (g) { return g.id === id; })[0];
    }

    function visible() {
        var needle = ($("kgFilter").value || "").trim().toLowerCase();
        var onlyLag = $("kgOnlyLag").checked;

        return groups().filter(function (g) {
            if (onlyLag && !g.lag) { return false; }
            if (needle && String(g.id).toLowerCase().indexOf(needle) < 0) {
                return false;
            }
            return true;
        });
    }

    function bodyHtml(group) {
        if (openGroup !== group.id) { return ""; }

        var html = '<div class="kg-body">';

        (group.topics || []).forEach(function (t) {
            html += '<div class="kg-topic">' + esc(t.name) + " · лаг " +
                lagText(t.lag) + "</div>";

            (t.parts || []).forEach(function (p) {
                var who = p.client
                    ? " · " + esc(p.client) + " (" + esc(p.host) + ")" : "";
                html += '<div class="kg-part">п. <b>' + esc(p.p) +
                    "</b> · закоммичено " +
                    (p.committed === null ? "—" : fmtN(p.committed)) +
                    " · конец " + fmtN(p.end) +
                    " · лаг " + lagText(p.lag) + who + "</div>";
            });
        });

        html += '<div class="kg-acts">' +
            '<button class="btn btn-sm btn-secondary" data-reset="' +
            esc(group.id) + '">Сбросить оффсеты</button>' +
            '<button class="btn btn-sm btn-outline-primary" data-drop="' +
            esc(group.id) + '">Удалить группу</button>';

        if (group.members) {
            html += '<span class="kg-count">Пока есть участники, действия ' +
                "недоступны</span>";
        }

        return html + "</div></div>";
    }

    function paintList() {
        var rows = visible();

        $("kgCount").textContent = groups().length
            ? "показано " + fmtN(rows.length) + " из " + fmtN(groups().length)
            : "";

        if (!rows.length) {
            $("kgList").innerHTML = '<div class="kg-empty">' +
                (groups().length
                    ? "Ничего не найдено."
                    : "Среза ещё нет — нажмите «Обновить срез».") +
                "</div>";
            return;
        }

        $("kgList").innerHTML = '<div class="kg-list">' +
            rows.map(function (g) {
                var open = openGroup === g.id;
                var busy = g.members
                    ? '<span class="kg-tag busy">' + fmtN(g.members) +
                      " участн.</span>" : "";
                var hot = g.lag ? " hot" : "";

                return '<div class="kg-row' + (open ? " open" : "") +
                    '" data-group="' + esc(g.id) + '"><span><b>' +
                    esc(g.id) + '</b><span class="kg-tag">' +
                    esc(g.state) + "</span>" + busy +
                    '</span><span class="r">' + fmtN(g.partitions) +
                    ' парт. · лаг <span class="kg-lag' + hot + '">' +
                    lagText(g.lag) + "</span></span></div>" + bodyHtml(g);
            }).join("") + "</div>";

        wireRows();
    }

    function repaint() {
        if (window.gpKeepScroll) {
            window.gpKeepScroll($("kgList"), paintList);
        } else {
            paintList();
        }
    }

    function wireRows() {
        Array.prototype.forEach.call(
            $("kgList").querySelectorAll("[data-group]"),
            function (row) {
                row.onclick = function () {
                    var id = row.getAttribute("data-group");
                    openGroup = openGroup === id ? null : id;
                    closeReset();
                    repaint();
                };
            }
        );

        Array.prototype.forEach.call(
            $("kgList").querySelectorAll("[data-reset]"),
            function (b) {
                b.onclick = function (event) {
                    event.stopPropagation();
                    openReset(b.getAttribute("data-reset"));
                };
            }
        );

        Array.prototype.forEach.call(
            $("kgList").querySelectorAll("[data-drop]"),
            function (b) {
                b.onclick = function (event) {
                    event.stopPropagation();
                    dropGroup(b.getAttribute("data-drop"));
                };
            }
        );
    }

    function openReset(id) {
        var group = byId(id);

        if (!group) { return; }

        resetFor = id;
        $("kgResetTitle").textContent = "Группа " + id;
        $("kgReset").classList.add("on");
        $("kgResetHint").textContent = "Затронет партиций: " +
            fmtN(group.partitions);
    }

    function closeReset() {
        resetFor = null;
        $("kgReset").classList.remove("on");
    }

    function runReset() {
        var id = resetFor;
        var group = byId(id);

        if (!id || !group) { return; }

        var mode = $("kgResetMode").value;
        var body = { mode: mode };

        if (mode === "timestamp") {
            if (!$("kgResetAt").value) {
                toast("Укажите дату и время", "danger");
                return;
            }
            body.timestamp = $("kgResetAt").value.replace("T", " ");
        }

        var label = $("kgResetMode")
            .options[$("kgResetMode").selectedIndex].text;
        var question = "Сбросить оффсеты группы «" + id + "» (" + label +
            ")? Затронет " + group.partitions + " партиций.";

        var doIt = function () {
            api("/api/kafka/clusters/" + clusterId() + "/groups/" +
                encodeURIComponent(id) + "/reset", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            }).then(function (r) {
                if (r.status !== 200 || !r.data.ok) {
                    toast(r.data.message || "Не удалось сбросить", "danger");
                    return;
                }

                var failed = (r.data.failed || []).length;

                toast(failed
                    ? "Сброшено " + r.data.done + ", не удалось " + failed
                    : "Оффсеты сброшены: " + r.data.done + " партиций",
                    failed ? "warning" : "success");

                closeReset();
                loadAudit();
                load(true);
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

    function dropGroup(id) {
        var question = "Удалить группу «" + id + "»? Её закоммиченные " +
            "оффсеты будут потеряны.";

        var doIt = function () {
            api("/api/kafka/clusters/" + clusterId() + "/groups/" +
                encodeURIComponent(id), { method: "DELETE" })
                .then(function (r) {
                    if (r.status !== 200 || !r.data.ok) {
                        toast(r.data.message || "Не удалось удалить",
                            "danger");
                        return;
                    }

                    toast("Группа удалена", "success");
                    loadAudit();
                    load(true);
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

    function paintAudit(records) {
        if (!records.length) {
            $("kgAudit").innerHTML =
                '<div class="kg-empty">Действий пока не было.</div>';
            return;
        }

        $("kgAudit").innerHTML = records.map(function (r) {
            var bad = r.result !== "ok" ? " bad" : "";
            var extra = r.details && r.details.mode
                ? " · " + esc(r.details.mode) : "";

            return '<div class="kg-audit' + bad + '">' + esc(r.created_at) +
                " · " + esc(r.action) + " · " + esc(r.target || "") +
                extra + " · " + esc(r.result) + "</div>";
        }).join("");
    }

    function loadAudit() {
        var id = clusterId();

        if (!id) { return; }

        api("/api/kafka/clusters/" + id + "/audit").then(function (r) {
            paintAudit((r.data && r.data.records) || []);
        });
    }

    function paintAll() {
        $("kgSnap").textContent = snapshot && snapshot.taken_at
            ? "срез от " + snapshot.taken_at : "среза нет";
        repaint();
    }

    function load(force) {
        var id = clusterId();

        if (!id) { paintAll(); return; }

        var url = "/api/kafka/clusters/" + id + "/groups" +
            (force ? "/refresh" : "");

        if (force) { $("kgRefresh").disabled = true; }

        api(url, force ? { method: "POST" } : {}).then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                showError(r.data.message || "Не удалось получить данные");
                return;
            }

            showError("");
            snapshot = r.data.groups;
            openGroup = null;
            closeReset();
            paintAll();
        }).catch(function (e) {
            showError(String(e));
        }).then(function () {
            $("kgRefresh").disabled = !clusterId();
        });
    }

    function wire() {
        if (!$("kgRoot") || !$("kgCluster")) { return; }

        $("kgCluster").onchange = function () {
            snapshot = null;
            paintAll();
            load(false);
            loadAudit();
        };

        $("kgRefresh").onclick = function () { load(true); };
        $("kgFilter").oninput = repaint;
        $("kgOnlyLag").onchange = repaint;

        $("kgResetMode").onchange = function () {
            $("kgResetAt").hidden = $("kgResetMode").value !== "timestamp";
        };

        $("kgResetGo").onclick = runReset;
        $("kgResetCancel").onclick = closeReset;

        paintAll();
        load(false);
        loadAudit();
    }

    document.addEventListener("DOMContentLoaded", wire);
}());
```

- [ ] **Шаг 3: добавить пункт меню**

В `templates/base.html`, в `<nav class="sb-sec-body sub" id="secBody-kfk">`,
после ссылки на «Обзор кластера»:

```html
                <a href="/kafka/groups" class="{% if p.startswith('/kafka/groups') %}active{% endif %}">
                    <span class="ic"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="7" r="4"/><path d="M2 21v-2a5 5 0 0 1 5-5h4a5 5 0 0 1 5 5v2"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg></span>
                    <span class="lbl">Консьюмер-группы</span></a>
```

И в блок выбора заголовка, **перед** веткой `p.startswith('/kafka')`:

```html
{% elif p.startswith('/kafka/groups') %}
    {% set page_title, page_crumb = 'Консьюмер-группы', 'кто читает и насколько отстаёт' %}
```

- [ ] **Шаг 4: проверить и прогнать всё**

```bash
node --check static/js/kafka_groups.js
python -m pytest tests -q
python -m flake8 --select=E9,F63,F7,F82 .
```

Ожидается: `node --check` без вывода, все тесты проходят (221 прежний + 30
новых: 3 в test_kafka_audit, 3 в test_kafka_client, 13 в test_kafka_groups,
11 в test_kafka_groups_api = `251 passed`), flake8 без вывода.

- [ ] **Шаг 5: коммит**

```bash
git add templates/kafka_groups.html templates/base.html static/js/kafka_groups.js
git commit -m "feat(kafka): экран консьюмер-групп"
```

---

### Задача 6: проверка в браузере

**Файлы:** правки только если проверка что-то вскроет.

- [ ] **Шаг 1: поднять предпросмотр**

Через `preview_start` (не через Bash — Flask не перечитывает модули,
для перезапуска нужны `preview_stop` + `preview_start`). Открыть
`/kafka/groups`.

- [ ] **Шаг 2: пустое состояние**

Через `read_page` убедиться: страница открывается, бейдж «среза нет», в
списке «Среза ещё нет — нажмите «Обновить срез»», журнал говорит
«Действий пока не было».

- [ ] **Шаг 3: отрисовка на подменённых данных**

Через `javascript_tool` подменить `window.fetch` так, чтобы `/groups`
вернул срез с тремя группами: `Stable` с двумя участниками и лагом,
`Empty` без участников и с нулевым лагом, и группа с `lag: null`.
Проверить в DOM:

- группы отсортированы по убыванию лага, `null` в конце;
- у группы с `lag: null` в колонке лага прочерк, а не «0»;
- галка «только с лагом» скрывает группы с нулевым и пустым лагом;
- разворот показывает партиции с владельцем и не сбрасывает `scrollTop`
  контейнера `#kgList`.

- [ ] **Шаг 4: опасные действия**

Подменить ответ `/reset` на 409 и убедиться, что показан тост с текстом
про участников, а срез на экране не изменился. Затем подменить на успешный
ответ и проверить, что появился тост с числом партиций и перезагрузился
журнал.

- [ ] **Шаг 5: снять скриншот и закоммитить правки**

```bash
git add -A
git commit -m "fix(kafka): правки по итогам проверки в браузере"
```

Если правок не потребовалось, коммит не создаётся.

---

## Что остаётся за рамками этапа 2

Управление топиками (создание, удаление, retention, число партиций) —
этап 3. Просмотр и отправка сообщений — этап 4.

Сброс оффсетов по отдельным партициям (а не по всей группе или выбранным
топикам) в этот этап не входит: API его позволяет, но интерфейс усложнится
заметно, а спрос неочевиден.
