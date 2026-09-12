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
