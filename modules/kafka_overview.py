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
