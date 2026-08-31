# Kafka Manager, этап 1 — план реализации

> **Для исполнителя:** этот план выполняется задача за задачей.
> Шаги отмечаются чекбоксами (`- [ ]`). Каждая задача заканчивается
> зелёными тестами и коммитом.

**Спека:** `docs/superpowers/specs/2026-08-31-kafka-manager-stage1-design.md`

**Цель:** вкладка Kafka в направлении Data Flow: подключения к кластерам и
обзор — брокеры, топики, партиции, репликация, объём.

**Архитектура:** весь контакт с библиотекой заперт в `modules/kafka_client.py`;
`modules/kafka_clusters.py` хранит подключения, `modules/kafka_overview.py`
собирает и кэширует срез в SQLite, `kafka_routes.py` — Flask Blueprint,
`templates/kafka.html` + `static/js/kafka.js` — экран.

**Стек:** Python 3, Flask, SQLite, `kafka-python`, ванильный JS без сборки.

## Общие требования

- Комментарии, тексты интерфейса и сообщения об ошибках — по-русски.
- Даты — строки `YYYY-MM-DD HH:MM:SS`, как `job_manager.now_str()`.
- Работа с SQLite только через `db.sqlite_cursor()`, никаких своих коннектов.
- `import kafka` разрешён **только** в `modules/kafka_client.py`.
- Тесты не требуют работающего брокера: библиотека импортируется лениво,
  клиенты подменяются через `monkeypatch`.
- Пароли не попадают ни в командную строку, ни в списочные ответы API.
- `python -m flake8 --select=E9,F63,F7,F82 .` обязан быть чистым — это гейт CI.
- Автообновления на странице нет: кластер опрашивается только по кнопке.

## Карта файлов

| Файл | Ответственность |
|------|-----------------|
| `db.py` | три новые таблицы в `init_db()` |
| `modules/kafka_clusters.py` | CRUD подключений, нормализация bootstrap |
| `modules/kafka_client.py` | единственный `import kafka`, соединения и сырые метаданные |
| `modules/kafka_overview.py` | чистая сборка среза + хранение в SQLite |
| `kafka_routes.py` | Blueprint: страница и API |
| `app.py` | одна строка регистрации Blueprint |
| `templates/kafka.html` | разметка экрана |
| `static/js/kafka.js` | поведение экрана |
| `templates/base.html` | заглушка в меню → ссылка |
| `static/css/style.css` | стили `.kf-*` |
| `requirements.txt` | `kafka-python>=2.0.2` |
| `tests/test_kafka_clusters.py` | CRUD и нормализация |
| `tests/test_kafka_client.py` | сборка аргументов и обработка ошибок |
| `tests/test_kafka_overview.py` | чистая сборка среза и снапшоты |
| `tests/test_kafka_api.py` | роуты через `test_client` |

---

### Задача 1: таблицы и CRUD подключений

**Файлы:**
- Изменить: `db.py` — внутрь `init_db()`, сразу после блока
  `CREATE TABLE IF NOT EXISTS table_sets` (около строки 358), не выходя из
  `with sqlite_cursor(commit=True) as cur:`
- Создать: `modules/kafka_clusters.py`
- Тест: `tests/test_kafka_clusters.py`

**Интерфейсы:**
- Использует: `db.sqlite_cursor(commit=False)`.
- Даёт наружу: `normalize_bootstrap(value) -> str`,
  `list_clusters() -> list[dict]`, `get_cluster(cluster_id) -> dict | None`,
  `create_cluster(data: dict) -> int`,
  `update_cluster(cluster_id, data: dict) -> bool`,
  `delete_cluster(cluster_id) -> bool`.

- [ ] **Шаг 1: написать падающий тест**

Создать `tests/test_kafka_clusters.py`:

```python
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
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_clusters.py -q`
Ожидается: `ModuleNotFoundError: No module named 'modules.kafka_clusters'`

- [ ] **Шаг 3: добавить таблицы в `db.py`**

Вставить в `init_db()` сразу после блока `CREATE TABLE IF NOT EXISTS table_sets`:

```python
        # --- Kafka (spec: docs/superpowers/specs/
        #     2026-08-31-kafka-manager-stage1-design.md) ---

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS kafka_clusters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                bootstrap_servers TEXT NOT NULL,
                security_protocol TEXT NOT NULL DEFAULT 'PLAINTEXT',
                sasl_mechanism TEXT,
                sasl_username TEXT,
                sasl_password TEXT,
                ssl_cafile TEXT,
                ssl_certfile TEXT,
                ssl_keyfile TEXT,
                request_timeout_ms INTEGER NOT NULL DEFAULT 15000,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS kafka_snapshots (
                cluster_id INTEGER PRIMARY KEY,
                taken_at TEXT NOT NULL,
                payload BLOB NOT NULL,
                brokers_total INTEGER NOT NULL DEFAULT 0,
                topics_total INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS kafka_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id INTEGER,
                action TEXT NOT NULL,
                target TEXT,
                details_json TEXT,
                result TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
```

- [ ] **Шаг 4: написать `modules/kafka_clusters.py`**

```python
# -*- coding: utf-8 -*-
"""Подключения к Kafka-кластерам: хранение и нормализация адресов."""

from datetime import datetime

from db import sqlite_cursor

DEFAULT_PORT = 9092
DEFAULT_TIMEOUT_MS = 15000


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_bootstrap(value):
    """
    Приводит адреса брокеров к "host:port,host:port".

    Пользователь пишет их как придётся: через запятую, с пробелами, без
    порта. Дальше эта строка уходит в клиент как есть, поэтому чистим здесь
    один раз, а не в каждом вызывающем месте.
    """
    if isinstance(value, (list, tuple)):
        parts = list(value)
    else:
        parts = str(value or "").split(",")

    seen = []

    for part in parts:
        item = str(part or "").strip()

        if not item:
            continue

        if ":" not in item:
            item = "{}:{}".format(item, DEFAULT_PORT)

        if item not in seen:
            seen.append(item)

    if not seen:
        raise ValueError("Укажите хотя бы один адрес брокера")

    return ",".join(seen)


def _clean(data, require_name=True):
    """Из присланного словаря — только известные поля, уже нормализованные."""
    out = {}

    name = str(data.get("name") or "").strip()

    if require_name and not name:
        raise ValueError("Укажите имя кластера")

    if name:
        out["name"] = name

    if data.get("bootstrap_servers") is not None:
        out["bootstrap_servers"] = normalize_bootstrap(
            data.get("bootstrap_servers"))

    protocol = str(data.get("security_protocol") or "").strip().upper()

    if protocol:
        out["security_protocol"] = protocol

    for field in ("sasl_mechanism", "sasl_username", "sasl_password",
                  "ssl_cafile", "ssl_certfile", "ssl_keyfile"):
        if data.get(field) is not None:
            out[field] = str(data.get(field)).strip() or None

    if data.get("request_timeout_ms") is not None:
        try:
            timeout = int(data.get("request_timeout_ms"))
        except (TypeError, ValueError):
            raise ValueError("Таймаут должен быть числом миллисекунд")

        out["request_timeout_ms"] = max(1000, min(timeout, 300000))

    return out


def list_clusters():
    """Список для интерфейса: пароль наружу не отдаём, только флаг."""
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                name,
                bootstrap_servers,
                security_protocol,
                sasl_mechanism,
                sasl_username,
                CASE
                    WHEN sasl_password IS NULL OR sasl_password = ''
                    THEN 0 ELSE 1
                END AS has_password,
                ssl_cafile,
                ssl_certfile,
                ssl_keyfile,
                request_timeout_ms,
                created_at,
                updated_at
            FROM kafka_clusters
            ORDER BY id DESC
            """
        )

        rows = []

        for row in cur.fetchall():
            item = dict(row)
            item["has_password"] = bool(item["has_password"])
            rows.append(item)

        return rows


def get_cluster(cluster_id):
    """Полная запись, включая пароль — для внутреннего кода."""
    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT * FROM kafka_clusters WHERE id = ?", (int(cluster_id),)
        )
        row = cur.fetchone()

    return dict(row) if row else None


def create_cluster(data):
    fields = _clean(data)

    if not fields.get("bootstrap_servers"):
        raise ValueError("Укажите хотя бы один адрес брокера")

    fields.setdefault("security_protocol", "PLAINTEXT")
    fields.setdefault("request_timeout_ms", DEFAULT_TIMEOUT_MS)
    fields["created_at"] = _now()

    names = ", ".join(fields)
    marks = ", ".join("?" for _ in fields)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "INSERT INTO kafka_clusters ({}) VALUES ({})".format(names, marks),
            list(fields.values()),
        )
        return int(cur.lastrowid)


def update_cluster(cluster_id, data):
    fields = _clean(data, require_name=False)

    if not fields:
        return False

    fields["updated_at"] = _now()
    sets = ", ".join("{} = ?".format(k) for k in fields)
    params = list(fields.values()) + [int(cluster_id)]

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE kafka_clusters SET {} WHERE id = ?".format(sets), params
        )
        return cur.rowcount > 0


def delete_cluster(cluster_id):
    """Вместе с кластером уходит и его срез — он больше ни к чему."""
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "DELETE FROM kafka_snapshots WHERE cluster_id = ?",
            (int(cluster_id),),
        )
        cur.execute(
            "DELETE FROM kafka_clusters WHERE id = ?", (int(cluster_id),)
        )
        return cur.rowcount > 0
```

- [ ] **Шаг 5: убедиться, что тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_clusters.py -q`
Ожидается: `7 passed`

- [ ] **Шаг 6: коммит**

```bash
git add db.py modules/kafka_clusters.py tests/test_kafka_clusters.py
git commit -m "feat(kafka): таблицы и подключения к кластерам"
```

---

### Задача 2: транспорт до Kafka

**Файлы:**
- Создать: `modules/kafka_client.py`
- Изменить: `requirements.txt`
- Тест: `tests/test_kafka_client.py`

**Интерфейсы:**
- Использует: запись кластера из `modules.kafka_clusters.get_cluster()`.
- Даёт наружу: `KafkaUnavailable(Exception)`, `library_available() -> bool`,
  `client_kwargs(cluster) -> dict`, `open_admin(cluster)`,
  `open_consumer(cluster)`,
  `ping(cluster) -> {"ok": bool, "message": str, "brokers": int}`,
  `fetch_cluster_meta(cluster) -> (cluster_meta: dict, topics_meta: list)`,
  `fetch_offsets(cluster, pairs) -> (begin: dict, end: dict)`, где `pairs` —
  список кортежей `(topic, partition)`, а ключи результата — те же кортежи.

`fetch_cluster_meta` и `fetch_offsets` живут здесь, а не в
`kafka_overview.py`, потому что им нужен `TopicPartition` из библиотеки —
иначе `import kafka` протёк бы во второй файл.

- [ ] **Шаг 1: написать падающий тест**

Создать `tests/test_kafka_client.py`:

```python
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


def test_missing_library_is_explained(monkeypatch):
    def no_library():
        raise ImportError("no kafka")

    monkeypatch.setattr(kafka_client, "_import_kafka", no_library)

    with pytest.raises(KafkaUnavailable) as err:
        kafka_client.open_admin(PLAIN)

    assert "kafka-python" in str(err.value)
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_client.py -q`
Ожидается: `ModuleNotFoundError: No module named 'modules.kafka_client'`

- [ ] **Шаг 3: написать `modules/kafka_client.py`**

```python
# -*- coding: utf-8 -*-
"""
Единственное место, знающее про библиотеку kafka-python.

Всё остальное приложение работает со словарями и получает понятные ошибки,
а не NoBrokersAvailable из недр клиента. Импорт ленивый: без установленной
библиотеки приложение запускается, не открывается только вкладка Kafka.
"""

DEFAULT_TIMEOUT_MS = 15000

INSTALL_HINT = (
    "Библиотека kafka-python не установлена. На сервере: "
    "pip install -r requirements.txt и перезапуск app.py"
)


class KafkaUnavailable(Exception):
    """Кластер недоступен или библиотеки нет — с текстом для человека."""


def _import_kafka():
    """Вынесено отдельно, чтобы тесты могли подменить импорт."""
    import kafka

    return kafka


def library_available():
    try:
        _import_kafka()
    except ImportError:
        return False

    return True


def _servers(cluster):
    raw = str(cluster.get("bootstrap_servers") or "")
    return [part.strip() for part in raw.split(",") if part.strip()]


def client_kwargs(cluster):
    """Аргументы, общие для админ-клиента и консьюмера."""
    timeout = int(cluster.get("request_timeout_ms") or DEFAULT_TIMEOUT_MS)
    protocol = str(cluster.get("security_protocol") or "PLAINTEXT").upper()

    kwargs = {
        "bootstrap_servers": _servers(cluster),
        "security_protocol": protocol,
        "request_timeout_ms": timeout,
        "client_id": "opsentri",
    }

    if protocol.startswith("SASL"):
        kwargs["sasl_mechanism"] = cluster.get("sasl_mechanism") or "PLAIN"
        kwargs["sasl_plain_username"] = cluster.get("sasl_username")
        kwargs["sasl_plain_password"] = cluster.get("sasl_password")

    for field in ("ssl_cafile", "ssl_certfile", "ssl_keyfile"):
        if cluster.get(field):
            kwargs[field] = cluster.get(field)

    return kwargs


def _fail(cluster, error):
    timeout = int(cluster.get("request_timeout_ms") or DEFAULT_TIMEOUT_MS)

    return KafkaUnavailable(
        "Кластер недоступен: {} не ответил за {} с ({})".format(
            ", ".join(_servers(cluster)) or "адрес не задан",
            round(timeout / 1000.0),
            error,
        )
    )


def open_admin(cluster):
    try:
        kafka = _import_kafka()
    except ImportError:
        raise KafkaUnavailable(INSTALL_HINT)

    try:
        return kafka.KafkaAdminClient(**client_kwargs(cluster))
    except Exception as error:
        raise _fail(cluster, error)


def open_consumer(cluster):
    try:
        kafka = _import_kafka()
    except ImportError:
        raise KafkaUnavailable(INSTALL_HINT)

    try:
        return kafka.KafkaConsumer(
            enable_auto_commit=False,
            consumer_timeout_ms=int(
                cluster.get("request_timeout_ms") or DEFAULT_TIMEOUT_MS),
            **client_kwargs(cluster)
        )
    except Exception as error:
        raise _fail(cluster, error)


def ping(cluster):
    """Проверка связи для кнопки в интерфейсе. Никогда не бросает."""
    try:
        admin = open_admin(cluster)
    except KafkaUnavailable as error:
        return {"ok": False, "message": str(error), "brokers": 0}

    try:
        meta = admin.describe_cluster()
        brokers = len(meta.get("brokers") or [])

        return {
            "ok": True,
            "message": "Связь есть, брокеров: {}".format(brokers),
            "brokers": brokers,
        }
    except Exception as error:
        return {"ok": False, "message": str(_fail(cluster, error)),
                "brokers": 0}
    finally:
        try:
            admin.close()
        except Exception:
            pass


def fetch_cluster_meta(cluster):
    """(описание кластера, список топиков) сырыми структурами библиотеки."""
    admin = open_admin(cluster)

    try:
        return admin.describe_cluster(), admin.describe_topics()
    except Exception as error:
        raise _fail(cluster, error)
    finally:
        try:
            admin.close()
        except Exception:
            pass


def fetch_offsets(cluster, pairs):
    """
    Границы оффсетов по списку [(topic, partition), ...].

    Спрашиваем одним вызовом на все партиции: по одной это десятки
    round-trip'ов даже на маленьком кластере.
    """
    if not pairs:
        return {}, {}

    try:
        kafka = _import_kafka()
    except ImportError:
        raise KafkaUnavailable(INSTALL_HINT)

    consumer = open_consumer(cluster)

    try:
        tps = [kafka.TopicPartition(topic, part) for topic, part in pairs]
        begin = consumer.beginning_offsets(tps)
        end = consumer.end_offsets(tps)

        return (
            {(tp.topic, tp.partition): int(v or 0)
             for tp, v in begin.items()},
            {(tp.topic, tp.partition): int(v or 0) for tp, v in end.items()},
        )
    except Exception as error:
        raise _fail(cluster, error)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
```

- [ ] **Шаг 4: добавить зависимость**

В конец `requirements.txt` дописать строку:

```
kafka-python>=2.0.2
```

- [ ] **Шаг 5: убедиться, что тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_client.py -q`
Ожидается: `6 passed`

- [ ] **Шаг 6: коммит**

```bash
git add modules/kafka_client.py requirements.txt tests/test_kafka_client.py
git commit -m "feat(kafka): слой транспорта до кластера"
```

---

### Задача 3: срез кластера

**Файлы:**
- Создать: `modules/kafka_overview.py`
- Тест: `tests/test_kafka_overview.py`

**Интерфейсы:**
- Использует: `modules.kafka_clusters.get_cluster(cluster_id)`,
  `modules.kafka_client.fetch_cluster_meta(cluster)`,
  `modules.kafka_client.fetch_offsets(cluster, pairs)`.
- Даёт наружу:
  `build_overview(cluster_meta: dict, topics_meta: list, begin_offsets: dict, end_offsets: dict) -> dict`,
  `empty_overview(cluster_id) -> dict`,
  `load_snapshot(cluster_id) -> dict | None`,
  `save_snapshot(cluster_id, data) -> None`,
  `collect_overview(cluster_id, force=False) -> dict`.

Возврат `collect_overview` и `load_snapshot` — словарь среза плюс служебные
ключи `taken_at` (строка или `None`), `empty` (bool), `cluster_id` (int).

Импорты `fetch_cluster_meta` и `fetch_offsets` делаются на уровне модуля
(`from modules.kafka_client import ...`) — тесты подменяют их через
`monkeypatch.setattr(kafka_overview, "fetch_cluster_meta", ...)`.

- [ ] **Шаг 1: написать падающий тест**

Создать `tests/test_kafka_overview.py`:

```python
# -*- coding: utf-8 -*-
"""Сборка среза кластера и его хранение в SQLite."""

from modules import kafka_overview
from modules.kafka_clusters import create_cluster, delete_cluster
from modules.kafka_overview import (
    build_overview,
    collect_overview,
    empty_overview,
    load_snapshot,
    save_snapshot,
)

CLUSTER_META = {
    "cluster_id": "MkU3OEVBNTcwNTJENDM2Qk",
    "controller_id": 1,
    "brokers": [
        {"node_id": 1, "host": "kfk1", "port": 9092, "rack": None},
        {"node_id": 2, "host": "kfk2", "port": 9092, "rack": "b"},
    ],
}

TOPICS_META = [
    {
        "topic": "orders",
        "is_internal": False,
        "partitions": [
            {"partition": 0, "leader": 1, "replicas": [1, 2], "isr": [1, 2]},
            {"partition": 1, "leader": 2, "replicas": [1, 2], "isr": [2]},
        ],
    },
    {
        "topic": "__consumer_offsets",
        "is_internal": True,
        "partitions": [
            {"partition": 0, "leader": 1, "replicas": [1], "isr": [1]},
        ],
    },
]

BEGIN = {("orders", 0): 0, ("orders", 1): 100, ("__consumer_offsets", 0): 0}
END = {("orders", 0): 500, ("orders", 1): 700, ("__consumer_offsets", 0): 3}


def test_build_overview_counts_messages():
    data = build_overview(CLUSTER_META, TOPICS_META, BEGIN, END)

    orders = [t for t in data["topics"] if t["name"] == "orders"][0]

    assert data["controller_id"] == 1
    assert len(data["brokers"]) == 2
    assert data["brokers"][0]["host"] == "kfk1"
    assert orders["partitions"] == 2
    assert orders["replication"] == 2
    # (500 - 0) + (700 - 100)
    assert orders["messages"] == 1100


def test_build_overview_marks_under_replicated():
    data = build_overview(CLUSTER_META, TOPICS_META, BEGIN, END)

    orders = [t for t in data["topics"] if t["name"] == "orders"][0]

    # у партиции 1 в ISR только одна реплика из двух
    assert orders["under_replicated"] is True


def test_build_overview_marks_internal_and_sorts():
    data = build_overview(CLUSTER_META, TOPICS_META, BEGIN, END)

    internal = [t for t in data["topics"]
                if t["name"] == "__consumer_offsets"][0]

    assert internal["internal"] is True
    # сортировка по имени: подчёркивания идут раньше букв
    assert data["topics"][0]["name"] == "__consumer_offsets"


def test_build_overview_survives_missing_offsets():
    data = build_overview(CLUSTER_META, TOPICS_META, {}, {})

    orders = [t for t in data["topics"] if t["name"] == "orders"][0]

    assert orders["messages"] == 0


def test_snapshot_roundtrip():
    cluster_id = create_cluster({
        "name": "Snap", "bootstrap_servers": "kfk1:9092"})

    assert load_snapshot(cluster_id) is None

    data = build_overview(CLUSTER_META, TOPICS_META, BEGIN, END)
    save_snapshot(cluster_id, data)

    loaded = load_snapshot(cluster_id)

    assert loaded["empty"] is False
    assert loaded["taken_at"]
    assert len(loaded["topics"]) == 2

    delete_cluster(cluster_id)

    assert load_snapshot(cluster_id) is None


def test_empty_overview_shape():
    data = empty_overview(42)

    assert data["empty"] is True
    assert data["cluster_id"] == 42
    assert data["topics"] == []
    assert data["taken_at"] is None


def test_collect_reads_snapshot_without_force(monkeypatch):
    cluster_id = create_cluster({
        "name": "Cached", "bootstrap_servers": "kfk1:9092"})

    def never(*args, **kwargs):
        raise AssertionError("без force кластер трогать нельзя")

    monkeypatch.setattr(kafka_overview, "fetch_cluster_meta", never)

    save_snapshot(cluster_id, build_overview(
        CLUSTER_META, TOPICS_META, BEGIN, END))

    data = collect_overview(cluster_id)

    assert data["empty"] is False
    assert len(data["topics"]) == 2

    delete_cluster(cluster_id)


def test_collect_with_force_asks_cluster(monkeypatch):
    cluster_id = create_cluster({
        "name": "Live", "bootstrap_servers": "kfk1:9092"})

    monkeypatch.setattr(kafka_overview, "fetch_cluster_meta",
                        lambda c: (CLUSTER_META, TOPICS_META))
    monkeypatch.setattr(kafka_overview, "fetch_offsets",
                        lambda c, pairs: (BEGIN, END))

    data = collect_overview(cluster_id, force=True)

    assert data["empty"] is False
    assert data["taken_at"]
    # и сохранился, чтобы следующий заход был без опроса
    assert load_snapshot(cluster_id) is not None

    delete_cluster(cluster_id)
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_overview.py -q`
Ожидается: `ModuleNotFoundError: No module named 'modules.kafka_overview'`

- [ ] **Шаг 3: написать `modules/kafka_overview.py`**

```python
# -*- coding: utf-8 -*-
"""
Обзор кластера: сборка среза из метаданных и его хранение в SQLite.

Источник опрашивается только по явной команде — страница живёт на срезе,
как вкладка грантов.
"""

import json
import zlib
from datetime import datetime

from db import sqlite_cursor
from modules.kafka_client import fetch_cluster_meta, fetch_offsets
from modules.kafka_clusters import get_cluster


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _pack(payload):
    """Срез — это килобайты JSON на каждый кластер, кладём его сжатым."""
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


def build_overview(cluster_meta, topics_meta, begin_offsets, end_offsets):
    """
    Метаданные библиотеки → структура среза. Чистая функция: ни базы,
    ни сети, поэтому проверяется на обычных словарях.
    """
    brokers = []

    for broker in (cluster_meta or {}).get("brokers") or []:
        brokers.append({
            "id": broker.get("node_id"),
            "host": broker.get("host"),
            "port": broker.get("port"),
            "rack": broker.get("rack"),
        })

    brokers.sort(key=lambda b: (b["id"] is None, b["id"]))

    topics = []

    for topic in topics_meta or []:
        name = topic.get("topic")
        parts = []
        messages = 0
        under = False
        replication = 0

        for part in topic.get("partitions") or []:
            number = part.get("partition")
            replicas = list(part.get("replicas") or [])
            isr = list(part.get("isr") or [])
            begin = int(begin_offsets.get((name, number), 0) or 0)
            end = int(end_offsets.get((name, number), 0) or 0)

            replication = max(replication, len(replicas))
            messages += max(0, end - begin)

            if len(isr) < len(replicas):
                under = True

            parts.append({
                "p": number,
                "leader": part.get("leader"),
                "replicas": replicas,
                "isr": isr,
                "begin": begin,
                "end": end,
            })

        parts.sort(key=lambda p: (p["p"] is None, p["p"]))

        topics.append({
            "name": name,
            "internal": bool(topic.get("is_internal")),
            "partitions": len(parts),
            "replication": replication,
            "messages": messages,
            "under_replicated": under,
            "parts": parts,
        })

    topics.sort(key=lambda t: t["name"] or "")

    return {
        "cluster_id": (cluster_meta or {}).get("cluster_id"),
        "controller_id": (cluster_meta or {}).get("controller_id"),
        "brokers": brokers,
        "topics": topics,
    }


def empty_overview(cluster_id):
    """Ответ, пока срез ни разу не собирали."""
    return {
        "cluster_id": int(cluster_id),
        "empty": True,
        "taken_at": None,
        "kafka_cluster_id": None,
        "controller_id": None,
        "brokers": [],
        "topics": [],
    }


def load_snapshot(cluster_id):
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT taken_at, payload
            FROM kafka_snapshots
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
        "kafka_cluster_id": data.get("cluster_id"),
        "controller_id": data.get("controller_id"),
        "brokers": data.get("brokers") or [],
        "topics": data.get("topics") or [],
    }


def save_snapshot(cluster_id, data):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO kafka_snapshots (
                cluster_id, taken_at, payload, brokers_total, topics_total
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cluster_id) DO UPDATE SET
                taken_at = excluded.taken_at,
                payload = excluded.payload,
                brokers_total = excluded.brokers_total,
                topics_total = excluded.topics_total
            """,
            (
                int(cluster_id),
                _now(),
                _pack(data),
                len(data.get("brokers") or []),
                len(data.get("topics") or []),
            ),
        )


def collect_overview(cluster_id, force=False):
    """
    Без force — срез из базы. С force — опрос кластера и сохранение.

    Если сохранить не удалось, обзор всё равно возвращается: показать
    данные важнее, чем закэшировать их.
    """
    cluster = get_cluster(cluster_id)

    if not cluster:
        raise ValueError("Кластер не найден: {}".format(cluster_id))

    if not force:
        return load_snapshot(cluster_id) or empty_overview(cluster_id)

    cluster_meta, topics_meta = fetch_cluster_meta(cluster)

    pairs = [
        (topic.get("topic"), part.get("partition"))
        for topic in topics_meta or []
        for part in topic.get("partitions") or []
    ]

    begin, end = fetch_offsets(cluster, pairs)
    data = build_overview(cluster_meta, topics_meta, begin, end)

    saved = True

    try:
        save_snapshot(cluster_id, data)
    except Exception:
        saved = False

    if saved:
        stored = load_snapshot(cluster_id)

        if stored:
            return stored

    return {
        "cluster_id": int(cluster_id),
        "empty": False,
        "taken_at": _now(),
        "saved": False,
        "kafka_cluster_id": data.get("cluster_id"),
        "controller_id": data.get("controller_id"),
        "brokers": data.get("brokers"),
        "topics": data.get("topics"),
    }
```

- [ ] **Шаг 4: убедиться, что тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_overview.py -q`
Ожидается: `8 passed`

- [ ] **Шаг 5: коммит**

```bash
git add modules/kafka_overview.py tests/test_kafka_overview.py
git commit -m "feat(kafka): срез кластера с кэшем в SQLite"
```

---

### Задача 4: роуты

**Файлы:**
- Создать: `kafka_routes.py` (в корне, рядом с `app.py`)
- Создать: `templates/kafka.html` — заглушка (полная разметка в задаче 5)
- Изменить: `app.py` — импорт и регистрация Blueprint
- Тест: `tests/test_kafka_api.py`

**Интерфейсы:**
- Использует: `modules.kafka_clusters` (весь CRUD),
  `modules.kafka_client.ping`, `modules.kafka_client.library_available`,
  `modules.kafka_client.KafkaUnavailable`,
  `modules.kafka_overview.collect_overview`.
- Даёт наружу: объект `kafka_bp` (`flask.Blueprint`), который `app.py`
  регистрирует вызовом `app.register_blueprint(kafka_bp)`.

Формат ответов: успех — `{"ok": true, ...}`; ошибка ввода — 400
`{"ok": false, "message": "..."}`; кластер не найден — 404; недоступный
кластер — 502.

- [ ] **Шаг 1: создать заглушку шаблона**

Создать `templates/kafka.html` одной строкой, иначе `/kafka` вернёт 500:

```html
{% extends "base.html" %}{% block content %}<div id="kfRoot"></div>{% endblock %}
```

- [ ] **Шаг 2: написать падающий тест**

Создать `tests/test_kafka_api.py`:

```python
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
```

- [ ] **Шаг 3: убедиться, что тест падает**

Запуск: `python -m pytest tests/test_kafka_api.py -q`
Ожидается: `ModuleNotFoundError: No module named 'kafka_routes'`

- [ ] **Шаг 4: написать `kafka_routes.py`**

```python
# -*- coding: utf-8 -*-
"""Роуты вкладки Kafka. Отдельный Blueprint: app.py и так на 3400 строк."""

from flask import Blueprint, jsonify, render_template, request

from modules.kafka_client import KafkaUnavailable, library_available, ping
from modules.kafka_clusters import (
    create_cluster,
    delete_cluster,
    get_cluster,
    list_clusters,
    update_cluster,
)
from modules.kafka_overview import collect_overview

kafka_bp = Blueprint("kafka", __name__)


def _fail(message, code=400):
    return jsonify({"ok": False, "message": str(message)}), code


def _cluster_or_404(cluster_id):
    cluster = get_cluster(cluster_id)

    if not cluster:
        raise LookupError("Кластер не найден: {}".format(cluster_id))

    return cluster


@kafka_bp.route("/kafka")
def kafka_page():
    return render_template(
        "kafka.html",
        clusters=list_clusters(),
        library_ready=library_available(),
    )


@kafka_bp.route("/api/kafka/clusters", methods=["GET"])
def api_kafka_clusters():
    return jsonify({"ok": True, "clusters": list_clusters()})


@kafka_bp.route("/api/kafka/clusters", methods=["POST"])
def api_kafka_cluster_create():
    try:
        cluster_id = create_cluster(request.get_json(silent=True) or {})
    except ValueError as error:
        return _fail(error)

    return jsonify({"ok": True, "id": cluster_id})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>", methods=["PUT"])
def api_kafka_cluster_update(cluster_id):
    try:
        _cluster_or_404(cluster_id)
        changed = update_cluster(
            cluster_id, request.get_json(silent=True) or {})
    except LookupError as error:
        return _fail(error, 404)
    except ValueError as error:
        return _fail(error)

    return jsonify({"ok": True, "changed": changed})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>", methods=["DELETE"])
def api_kafka_cluster_delete(cluster_id):
    return jsonify({"ok": bool(delete_cluster(cluster_id))})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/ping", methods=["POST"])
def api_kafka_ping(cluster_id):
    try:
        cluster = _cluster_or_404(cluster_id)
    except LookupError as error:
        return _fail(error, 404)

    return jsonify(ping(cluster))


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/overview",
                methods=["GET"])
def api_kafka_overview(cluster_id):
    return _overview(cluster_id, force=False)


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/overview/refresh",
                methods=["POST"])
def api_kafka_overview_refresh(cluster_id):
    return _overview(cluster_id, force=True)


def _overview(cluster_id, force):
    try:
        _cluster_or_404(cluster_id)
        data = collect_overview(cluster_id, force=force)
    except LookupError as error:
        return _fail(error, 404)
    except KafkaUnavailable as error:
        # 502: приложение живо, недоступен внешний кластер
        return _fail(error, 502)
    except ValueError as error:
        return _fail(error)

    return jsonify({"ok": True, "overview": data})
```

- [ ] **Шаг 5: зарегистрировать Blueprint в `app.py`**

Импорт положить рядом с остальными импортами модулей проекта (после
`from modules...`), регистрацию — сразу после создания `app`:

```python
from kafka_routes import kafka_bp

app.register_blueprint(kafka_bp)
```

- [ ] **Шаг 6: убедиться, что тесты зелёные**

Запуск: `python -m pytest tests/test_kafka_api.py -q`
Ожидается: `8 passed`

- [ ] **Шаг 7: коммит**

```bash
git add kafka_routes.py app.py templates/kafka.html tests/test_kafka_api.py
git commit -m "feat(kafka): роуты подключений и обзора"
```

---

### Задача 5: экран

**Файлы:**
- Изменить: `templates/kafka.html` (заменить заглушку из задачи 4)
- Создать: `static/js/kafka.js`
- Изменить: `templates/base.html` — блок `secBody-flow`, строка
  `<div class="sb-stub"><span class="tk-logo kf">KF</span> Kafka …`
- Изменить: `static/css/style.css` — стили `.kf-*` в конец файла

**Интерфейсы:**
- Использует API из задачи 4 и глобальные помощники из `static/js/ui.js`:
  `window.gpToast(message, type)`, `window.gpKeepScroll(root, render)`.
- Наружу ничего не отдаёт: скрипт самодостаточен, вешается на
  `DOMContentLoaded`.

- [ ] **Шаг 1: заменить `templates/kafka.html`**

```html
{% extends "base.html" %}

{% block content %}
<div class="kf-wrap" id="kfRoot">

  {% if not library_ready %}
  <div class="kf-banner warn">
    Библиотека <code>kafka-python</code> не установлена. На сервере выполните
    <code>pip install -r requirements.txt</code> и перезапустите app.py.
  </div>
  {% endif %}

  <div class="card kf-head">
    <select id="kfCluster" class="form-select">
      {% for c in clusters %}
      <option value="{{ c.id }}">{{ c.name }} — {{ c.bootstrap_servers }}</option>
      {% endfor %}
    </select>
    <button class="gpp-btn sm" id="kfPing" type="button">Проверить связь</button>
    <div class="sp"></div>
    <span class="kf-snap" id="kfSnap">среза нет</span>
    <button class="gpp-btn sm primary" id="kfRefresh" type="button"
            {% if not library_ready %}disabled{% endif %}>Обновить срез</button>
  </div>

  <div class="kf-banner err" id="kfError" style="display: none;"></div>

  <div class="card">
    <h2 class="card-ttl">Брокеры</h2>
    <div id="kfBrokers"></div>
  </div>

  <div class="card">
    <h2 class="card-ttl">Топики</h2>
    <div class="kf-tools">
      <input type="search" id="kfFilter" class="form-control"
             placeholder="поиск по имени топика">
      <label class="kf-check">
        <input type="checkbox" id="kfInternal"> показывать системные
      </label>
      <span class="gpp-hint" id="kfCount"></span>
    </div>
    <div id="kfTopics"></div>
  </div>

</div>

<script src="{{ url_for('static', filename='js/kafka.js') }}"></script>
{% endblock %}
```

- [ ] **Шаг 2: написать `static/js/kafka.js`**

```javascript
/* Вкладка Kafka: обзор кластера. Автообновления нет — только по кнопке. */
(function () {
    "use strict";

    var overview = null;      // последний показанный срез
    var openTopic = null;     // какой топик развёрнут

    function $(id) { return document.getElementById(id); }

    function esc(s) {
        return String(s === null || s === undefined ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }

    function fmtN(n) {
        return Number(n || 0).toLocaleString("ru-RU");
    }

    function clusterId() {
        var sel = $("kfCluster");
        return sel && sel.value ? Number(sel.value) : null;
    }

    function api(url, options) {
        return fetch(url, options || {}).then(function (r) {
            return r.json().then(function (data) {
                return { status: r.status, data: data };
            });
        });
    }

    function showError(message) {
        var box = $("kfError");
        if (!message) { box.style.display = "none"; return; }
        box.textContent = message;
        box.style.display = "";
    }

    function paintBrokers() {
        var rows = (overview && overview.brokers) || [];

        if (!rows.length) {
            $("kfBrokers").innerHTML =
                '<div class="gpp-hint">Среза ещё нет — нажмите ' +
                "«Обновить срез».</div>";
            return;
        }

        $("kfBrokers").innerHTML = '<div class="kf-list">' +
            rows.map(function (b) {
                var boss = b.id === overview.controller_id
                    ? ' <span class="kf-badge">контроллер</span>' : "";
                return '<div class="kf-row"><span><b>' + esc(b.id) +
                    "</b> · " + esc(b.host) + ":" + esc(b.port) +
                    (b.rack ? " · rack " + esc(b.rack) : "") + boss +
                    "</span></div>";
            }).join("") + "</div>";
    }

    function visibleTopics() {
        var all = (overview && overview.topics) || [];
        var needle = ($("kfFilter").value || "").trim().toLowerCase();
        var withInternal = $("kfInternal").checked;

        return all.filter(function (t) {
            if (!withInternal && t.internal) { return false; }
            if (needle && t.name.toLowerCase().indexOf(needle) < 0) {
                return false;
            }
            return true;
        });
    }

    function partsHtml(topic) {
        if (openTopic !== topic.name) { return ""; }

        return '<div class="kf-parts">' + (topic.parts || []).map(
            function (p) {
                var size = Math.max(0, (p.end || 0) - (p.begin || 0));
                return '<div class="kf-part">п. <b>' + esc(p.p) +
                    "</b> · лидер " + esc(p.leader) +
                    " · реплики " + esc((p.replicas || []).join(", ")) +
                    " · ISR " + esc((p.isr || []).join(", ")) +
                    " · " + fmtN(p.begin) + "–" + fmtN(p.end) +
                    " (" + fmtN(size) + ")</div>";
            }).join("") + "</div>";
    }

    function paintTopics() {
        var rows = visibleTopics();
        var total = ((overview && overview.topics) || []).length;

        $("kfCount").textContent = total
            ? "показано " + fmtN(rows.length) + " из " + fmtN(total)
            : "";

        if (!rows.length) {
            $("kfTopics").innerHTML = '<div class="gpp-hint">' +
                (total ? "Ничего не найдено."
                       : "Среза ещё нет — нажмите «Обновить срез».") +
                "</div>";
            return;
        }

        $("kfTopics").innerHTML = '<div class="kf-list">' +
            rows.map(function (t) {
                var warn = t.under_replicated
                    ? ' <span class="kf-badge crit">под-реплицирован</span>'
                    : "";
                return '<div class="kf-row topic" data-topic="' +
                    esc(t.name) + '"><span><b>' + esc(t.name) + "</b>" +
                    (t.internal ? ' <span class="kf-badge">системный</span>'
                                : "") + warn +
                    '</span><span class="cols">' + fmtN(t.partitions) +
                    " парт. · RF " + fmtN(t.replication) + " · " +
                    fmtN(t.messages) + " сообщ. (оценка)</span></div>" +
                    partsHtml(t);
            }).join("") + "</div>";

        Array.prototype.forEach.call(
            $("kfTopics").querySelectorAll("[data-topic]"),
            function (row) {
                row.onclick = function () {
                    var name = row.getAttribute("data-topic");
                    openTopic = openTopic === name ? null : name;
                    repaintTopics();
                };
            }
        );
    }

    function repaintTopics() {
        // прокрутка не должна уезжать к началу при разворачивании строки
        if (window.gpKeepScroll) {
            window.gpKeepScroll($("kfTopics"), paintTopics);
        } else {
            paintTopics();
        }
    }

    function paintAll() {
        // saved === false — данные собрали, но в базу они не легли
        var note = overview && overview.saved === false
            ? " · срез не сохранён" : "";

        $("kfSnap").textContent = (overview && overview.taken_at
            ? "срез от " + overview.taken_at
            : "среза нет") + note;

        paintBrokers();
        repaintTopics();
    }

    function loadOverview(force) {
        var id = clusterId();

        if (!id) { return; }

        var url = "/api/kafka/clusters/" + id + "/overview" +
            (force ? "/refresh" : "");
        var options = force ? { method: "POST" } : {};

        if (force) { $("kfRefresh").disabled = true; }

        api(url, options).then(function (r) {
            if (r.status !== 200 || !r.data.ok) {
                // старый срез оставляем на экране — он всё ещё полезен
                showError(r.data.message || "Не удалось получить данные");
                return;
            }

            showError("");
            overview = r.data.overview;
            openTopic = null;
            paintAll();
        }).catch(function (e) {
            showError(String(e));
        }).then(function () {
            $("kfRefresh").disabled = false;
        });
    }

    function wire() {
        if (!$("kfRoot") || !$("kfCluster")) { return; }

        $("kfCluster").onchange = function () {
            overview = null;
            openTopic = null;
            paintAll();
            loadOverview(false);
        };

        $("kfRefresh").onclick = function () { loadOverview(true); };

        $("kfPing").onclick = function () {
            var id = clusterId();
            if (!id) { return; }

            api("/api/kafka/clusters/" + id + "/ping", { method: "POST" })
                .then(function (r) {
                    window.gpToast(r.data.message,
                        r.data.ok ? "success" : "danger");
                });
        };

        $("kfFilter").oninput = repaintTopics;
        $("kfInternal").onchange = repaintTopics;

        loadOverview(false);
    }

    document.addEventListener("DOMContentLoaded", wire);
}());
```

- [ ] **Шаг 3: добавить стили в конец `static/css/style.css`**

```css
/* --- Kafka --- */
.kf-wrap { display: flex; flex-direction: column; gap: 16px; }
.kf-head { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.kf-head .sp { flex: 1; }
.kf-head select { max-width: 420px; }
.kf-snap { color: var(--muted); font-size: 13px; }
.kf-banner { padding: 10px 14px; border-radius: 10px; font-size: 14px; }
.kf-banner.warn { background: rgba(234, 179, 8, .12); color: var(--warn); }
.kf-banner.err { background: rgba(239, 68, 68, .12); color: var(--crit); }
.kf-tools { display: flex; align-items: center; gap: 12px;
            flex-wrap: wrap; margin-bottom: 10px; }
.kf-tools input[type="search"] { max-width: 280px; }
.kf-check { display: flex; align-items: center; gap: 6px;
            font-size: 13px; color: var(--muted); }
.kf-list { max-height: 420px; overflow: auto; }
.kf-row { display: flex; justify-content: space-between; gap: 12px;
          padding: 7px 10px; border-radius: 8px; }
.kf-row.topic { cursor: pointer; }
.kf-badge { font-size: 11px; padding: 1px 6px; border-radius: 6px;
            color: var(--muted); }
.kf-badge.crit { background: rgba(239, 68, 68, .15); color: var(--crit); }
.kf-parts { padding: 4px 0 10px 20px; }
.kf-part { font-size: 12px; color: var(--muted); padding: 2px 0;
           font-variant-numeric: tabular-nums; }
```

Перед вставкой проверить, что переменные `--muted`, `--crit`, `--warn` в
файле объявлены (`grep -n "\-\-crit" static/css/style.css`). Если какой-то
нет — взять ближайший существующий аналог из соседних правил, а не заводить
новую переменную.

- [ ] **Шаг 4: заменить заглушку в меню**

В `templates/base.html`, в блоке `<div class="sb-sec-body" id="secBody-flow">`
заменить

```html
            <div class="sb-stub"><span class="tk-logo kf">KF</span>
                Kafka <span class="soon">скоро</span></div>
```

на

```html
            <a href="/kafka" class="{% if p.startswith('/kafka') %}active{% endif %}">
                <span class="tk-logo kf">KF</span>
                <span class="lbl">Kafka</span></a>
```

- [ ] **Шаг 5: проверить синтаксис и прогнать весь набор тестов**

```bash
node --check static/js/kafka.js
python -m pytest tests -q
python -m flake8 --select=E9,F63,F7,F82 .
```

Ожидается: `node --check` без вывода, `pytest` — 185 прежних + 29 новых =
`214 passed`, flake8 без вывода.

- [ ] **Шаг 6: коммит**

```bash
git add templates/kafka.html templates/base.html static/js/kafka.js static/css/style.css
git commit -m "feat(kafka): экран обзора кластера"
```

---

### Задача 6: проверка в браузере

**Файлы:** изменений кода не предполагается; правки — только если проверка
что-то вскроет.

**Интерфейсы:** ничего нового не появляется, проверяется собранное в
задачах 1–5.

- [ ] **Шаг 1: поставить зависимость локально**

```bash
python -m pip install -r requirements.txt
```

Ожидается: `Successfully installed kafka-python-...` либо
`Requirement already satisfied`.

- [ ] **Шаг 2: поднять предпросмотр**

Запустить дев-сервер через `preview_start` (не через Bash — Flask не
перечитывает `app.py`, для перезапуска нужны `preview_stop` +
`preview_start`). Открыть `/kafka`.

- [ ] **Шаг 3: проверить пустое состояние**

Через `read_page` убедиться: страница открывается, бейдж показывает
«среза нет», в карточках брокеров и топиков текст «Среза ещё нет — нажмите
«Обновить срез»».

- [ ] **Шаг 4: проверить отрисовку на подменённых данных**

Через `javascript_tool` подменить `window.fetch` так, чтобы
`/api/kafka/clusters/<id>/overview` вернул срез с двумя брокерами и тремя
топиками (один `internal: true`, один `under_replicated: true`), вызвать
перезагрузку обзора и проверить в DOM:

- системный топик скрыт, пока галка «показывать системные» снята;
- под-реплицированный топик получил элемент с классом `kf-badge crit`;
- клик по строке разворачивает партиции, повторный клик сворачивает;
- при разворачивании `scrollTop` контейнера `#kfTopics` не обнуляется.

- [ ] **Шаг 5: проверить поведение при недоступном кластере**

Подменить ответ `/overview/refresh` на 502 с текстом и убедиться, что
появился красный баннер `#kfError`, а ранее показанный срез остался на
экране.

- [ ] **Шаг 6: снять скриншот и закоммитить правки**

Если на шагах 3–5 что-то расходится с ожиданием — править исходники и
повторять проверку. Итог:

```bash
git add -A
git commit -m "fix(kafka): правки по итогам проверки в браузере"
```

Если правок не потребовалось, коммит не создаётся.

---

## Что остаётся за рамками этапа 1

Отдельная форма создания и редактирования кластеров в интерфейсе: API для
неё готово (задача 4), но экран этапа 1 показывает уже заведённые
подключения. Форму делаем в этапе 3, когда появятся действия, ради которых
её стоит открывать; до тех пор кластер заводится POST-запросом к
`/api/kafka/clusters`.

Консьюмер-группы и лаг, управление топиками, просмотр сообщений — этапы 2,
3 и 4, каждый со своей спекой.
