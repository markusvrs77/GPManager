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
