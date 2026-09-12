# -*- coding: utf-8 -*-
"""Правила доступа Kafka: разбор, сборка и шаблоны."""

import pytest

from modules.kafka_acl import (
    build_acl_spec,
    build_filter_spec,
    expand_preset,
    format_acl,
    validate_principal,
)


class FakeEnum(object):
    """Перечисления библиотеки числовые, наружу нужны имена."""

    def __init__(self, name):
        self.name = name


class FakePattern(object):
    def __init__(self, rtype, name, ptype):
        self.resource_type = FakeEnum(rtype)
        self.resource_name = name
        self.pattern_type = FakeEnum(ptype)


class FakeAcl(object):
    def __init__(self):
        self.principal = "User:svc_etl"
        self.host = "*"
        self.operation = FakeEnum("READ")
        self.permission_type = FakeEnum("ALLOW")
        self.resource_pattern = FakePattern("TOPIC", "orders", "LITERAL")


def test_principal_gets_user_prefix():
    assert validate_principal("svc_etl") == "User:svc_etl"
    assert validate_principal(" User:svc_etl ") == "User:svc_etl"
    # чужой префикс не ломаем
    assert validate_principal("Group:analysts") == "Group:analysts"


def test_principal_rejects_empty():
    for bad in ("", "   ", None, "User:"):
        with pytest.raises(ValueError):
            validate_principal(bad)


def test_build_acl_spec_defaults():
    spec = build_acl_spec({"principal": "svc_etl", "resource_name": "orders",
                           "operations": ["READ"]})

    assert spec == {
        "principal": "User:svc_etl",
        "host": "*",
        "resource_type": "TOPIC",
        "resource_name": "orders",
        "pattern_type": "LITERAL",
        "operations": ["READ"],
        "permission": "ALLOW",
    }


def test_build_acl_spec_rejects_bad_input():
    with pytest.raises(ValueError):
        build_acl_spec({"principal": "svc", "resource_name": "orders",
                        "operations": []})

    with pytest.raises(ValueError):
        build_acl_spec({"principal": "svc", "resource_name": "orders",
                        "operations": ["НЕЧТО"]})

    with pytest.raises(ValueError):
        build_acl_spec({"principal": "svc", "resource_name": "",
                        "operations": ["READ"]})

    with pytest.raises(ValueError):
        build_acl_spec({"principal": "svc", "resource_name": "orders",
                        "resource_type": "НЕЧТО", "operations": ["READ"]})


def test_build_acl_spec_cluster_needs_no_name():
    spec = build_acl_spec({"principal": "svc", "resource_type": "CLUSTER",
                           "operations": ["DESCRIBE"]})

    # у кластера имя ресурса всегда kafka-cluster
    assert spec["resource_name"] == "kafka-cluster"


def test_filter_spec_allows_any():
    spec = build_filter_spec({})

    assert spec == {
        "principal": None,
        "host": None,
        "resource_type": "ANY",
        "resource_name": None,
        "pattern_type": "ANY",
        "operation": "ANY",
        "permission": "ANY",
    }


def test_filter_spec_keeps_given_values():
    spec = build_filter_spec({"principal": "svc_etl",
                              "resource_type": "TOPIC",
                              "resource_name": "orders"})

    assert spec["principal"] == "User:svc_etl"
    assert spec["resource_type"] == "TOPIC"
    assert spec["resource_name"] == "orders"


def test_preset_reader_covers_group():
    specs = expand_preset("reader", {"principal": "svc_etl",
                                     "topic": "orders", "group": "etl"})

    kinds = [(s["resource_type"], s["resource_name"], tuple(s["operations"]))
             for s in specs]

    # про права на группу забывают чаще всего — потребитель падает в проде
    assert ("TOPIC", "orders", ("READ", "DESCRIBE")) in kinds
    assert ("GROUP", "etl", ("READ",)) in kinds


def test_preset_writer_is_topic_only():
    specs = expand_preset("writer", {"principal": "svc", "topic": "orders"})

    assert len(specs) == 1
    assert specs[0]["operations"] == ["WRITE", "DESCRIBE"]


def test_preset_full():
    specs = expand_preset("full", {"principal": "svc", "topic": "orders"})

    assert specs[0]["operations"] == ["ALL"]


def test_preset_rejects_unknown_and_missing_topic():
    with pytest.raises(ValueError):
        expand_preset("нечто", {"principal": "svc", "topic": "orders"})

    with pytest.raises(ValueError):
        expand_preset("reader", {"principal": "svc", "topic": ""})


def test_format_acl_turns_enums_into_names():
    row = format_acl(FakeAcl())

    assert row == {
        "principal": "User:svc_etl",
        "host": "*",
        "operation": "READ",
        "permission": "ALLOW",
        "resource_type": "TOPIC",
        "resource_name": "orders",
        "pattern_type": "LITERAL",
    }
