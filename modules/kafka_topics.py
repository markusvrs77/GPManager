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
