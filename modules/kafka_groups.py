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
