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
