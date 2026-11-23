"""Полное копирование: сравнение таблиц с источником по числу строк."""

import modules.gpcopy_partition as gp
from modules.gpcopy_partition import classify_table_counts


A, B, C, D = ("s", "a"), ("s", "b"), ("s", "c"), ("s", "d")


def test_classify_counts():
    pairs = [(A, A), (B, B), (C, C), (D, D)]
    rows = classify_table_counts(pairs, {A: 10, B: 10, C: 5}, {A: 10, B: 7, D: 1})

    assert [r["action"] for r in rows] == [
        "skip", "copy_changed", "copy_missing", "no_source"]
    assert rows[1]["src"] == 10 and rows[1]["dest"] == 7
    assert rows[0]["target"] is None


def test_mapped_target_is_compared_and_labelled():
    rows = classify_table_counts([(A, ("arch", "a_copy"))],
                                 {A: 3}, {("arch", "a_copy"): 3})

    assert rows[0]["action"] == "skip"
    assert rows[0]["target"] == "arch.a_copy"


class _Conn(object):
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_stats_sum_partitions_and_exact_overrides(monkeypatch):
    src, dst = _Conn(), _Conn()
    conns = iter([src, dst])
    monkeypatch.setattr(gp, "open_psycopg2_connection_by_cfg",
                        lambda cfg: next(conns))

    def stats(conn, tables):
        if conn is src:
            # секционированная: строк у корня нет, сумма по партициям
            return {A: {"p1": {"rows": 4}, "p2": {"rows": 6}},
                    B: {"b": {"rows": 5}}}
        return {A: {"p1": {"rows": 10}}}

    monkeypatch.setattr(gp, "fetch_leaf_stats", stats)

    rows = gp.diff_tables_counts({}, {}, [A, B])
    assert [(r["src"], r["dest"], r["action"]) for r in rows] == [
        (10, 10, "skip"), (5, None, "copy_missing")]
    assert src.closed and dst.closed

    asked = []
    conns = iter([src, dst])
    monkeypatch.setattr(gp, "open_psycopg2_connection_by_cfg",
                        lambda cfg: next(conns))

    def counts(conn, tables):
        asked.append(list(tables))
        return {A: 12} if conn is src else {A: 9}

    monkeypatch.setattr(gp, "fetch_table_counts", counts)

    rows = gp.diff_tables_counts({}, {}, [A, B], exact=True)
    # в приёмнике считаем только существующие таблицы
    assert asked == [[A, B], [A]]
    assert (rows[0]["src"], rows[0]["dest"], rows[0]["action"]) == (
        12, 9, "copy_changed")
    assert rows[1]["src"] == 5     # не посчиталась — осталась оценка


def test_route(client, monkeypatch):
    monkeypatch.setattr("db.get_connection_by_id", lambda cid: {"id": cid})
    seen = {}

    def fake(src, dst, tables, exact=False, targets=None):
        seen["args"] = (tables, exact)
        return [{"schema": "s", "table": "a", "target": None, "src": 1,
                 "dest": 2, "action": "copy_changed"},
                {"schema": "s", "table": "b", "target": None, "src": 1,
                 "dest": 1, "action": "skip"}]

    monkeypatch.setattr(gp, "diff_tables_counts", fake)

    r = client.post("/api/gpcopy/table-diff/preview", json={
        "source_connection_id": 1, "dest_connection_id": 2, "exact": True,
        "tables": [{"schema": "s", "table": "a"}, {"schema": "s", "table": "b"},
                   {"schema": "s", "table": "a"}],
    })

    body = r.get_json()
    assert r.status_code == 200 and body["differ"] == 1
    assert seen["args"] == ([("s", "a"), ("s", "b")], True)


def test_route_needs_tables(client, monkeypatch):
    monkeypatch.setattr("db.get_connection_by_id", lambda cid: {"id": cid})

    r = client.post("/api/gpcopy/table-diff/preview", json={
        "source_connection_id": 1, "dest_connection_id": 2, "tables": []})

    assert r.status_code == 400
