# -*- coding: utf-8 -*-
"""Срез прав живёт в SQLite: страница не должна ходить в источник."""

from db import sqlite_cursor
from modules.grants import (
    collect_grants,
    load_schema_snapshot,
    load_snapshot,
    save_schema_snapshot,
    save_snapshot,
)

SNAP = {
    "ok": True,
    "summary": {"users": 2, "tables": 7, "schemas": 1},
    "users": [{"name": "s.ivanov", "kind": "user", "tables_count": 7}],
    "generated_at": "2026-07-30 12:53:00",
    "duration_seconds": 3.4,
}


def _clean(connection_id):
    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM grants_snapshots WHERE connection_id = ?",
                    (connection_id,))
        cur.execute(
            "DELETE FROM grants_schema_snapshots WHERE connection_id = ?",
            (connection_id,))


def test_snapshot_round_trip():
    _clean(901)
    assert load_snapshot(901) is None

    save_snapshot(901, SNAP)
    stored = load_snapshot(901)

    assert stored["generated_at"] == "2026-07-30 12:53:00"
    assert stored["duration_seconds"] == 3.4
    assert stored["from_snapshot"] is True
    assert stored["summary"]["tables"] == 7
    assert stored["users"][0]["name"] == "s.ivanov"

    # повторное сохранение перезаписывает, а не плодит строки
    save_snapshot(901, dict(SNAP, generated_at="2026-07-31 09:00:00"))

    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM grants_snapshots WHERE connection_id = ?",
            (901,))
        assert cur.fetchone()["n"] == 1

    assert load_snapshot(901)["generated_at"] == "2026-07-31 09:00:00"
    _clean(901)


def test_collect_grants_reads_snapshot_without_touching_source():
    """Без force берём сохранённое — подключения даже не существует."""
    _clean(902)
    save_snapshot(902, SNAP)

    data = collect_grants(902)

    assert data["from_snapshot"] is True
    assert data["summary"]["tables"] == 7
    _clean(902)


def test_collect_grants_without_snapshot_returns_empty():
    """Среза нет — отдаём пустой ответ, а не лезем в источник."""
    _clean(903)

    data = collect_grants(903)

    assert data["ok"] is True
    assert data["empty"] is True
    assert data["generated_at"] is None
    assert data["summary"]["users"] == 0
    assert data["users"] == []
    assert data["graph"]["nodes"] == []


def test_new_snapshot_drops_schema_matrices():
    """Обновили срез — старые матрицы схем больше не показываем."""
    _clean(904)
    save_schema_snapshot(904, "dwh", {"ok": True, "schema": "dwh",
                                      "rows": ["u1"], "cols": ["t1"],
                                      "cells": {}, "tables_total": 1,
                                      "generated_at": "2026-07-30 12:53:00"})

    assert load_schema_snapshot(904, "dwh")["from_snapshot"] is True

    save_snapshot(904, SNAP)

    assert load_schema_snapshot(904, "dwh") is None
    _clean(904)
