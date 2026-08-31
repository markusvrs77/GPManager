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
