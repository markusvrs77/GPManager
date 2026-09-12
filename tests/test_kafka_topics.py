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
