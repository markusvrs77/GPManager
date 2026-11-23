"""Проверка DDL: партиции, которых нет в приёмнике, и их досоздание."""

import modules.ddl_check as ddl
from modules.ddl_check import (
    build_create_partition_sql, missing_partitions,
)

ROOT = ("dwh_dm", "dm_stock_lot")


def _tree(*leaves):
    nodes = [(ROOT[0], ROOT[1], 0)]
    nodes += [(ROOT[0], t, 1) for t in leaves]
    return {ROOT: nodes}


def test_missing_leaves_are_found():
    src = _tree("p_20250113", "p_20250114", "p_20270120")
    dst = _tree("p_20250113")

    miss = missing_partitions(src, dst)

    assert [p["table"] for p in miss[ROOT]] == ["p_20250114", "p_20270120"]


def test_root_missing_in_dest_is_left_to_create_objects():
    """Корень целиком создаёт «Создать недостающие объекты»."""
    assert missing_partitions(_tree("p1"), {}) == {}


def test_full_match_gives_nothing():
    assert missing_partitions(_tree("p1", "p2"), _tree("p2", "p1")) == {}


def test_middle_level_goes_before_its_leaves():
    src = {ROOT: [(ROOT[0], ROOT[1], 0), ("dwh_dm", "leaf_a", 2),
                  ("dwh_dm", "mid_2025", 1)]}
    dst = {ROOT: [(ROOT[0], ROOT[1], 0)]}

    miss = missing_partitions(src, dst)

    assert [p["table"] for p in miss[ROOT]] == ["mid_2025", "leaf_a"]


def test_middle_level_keeps_its_partition_by():
    sql = build_create_partition_sql(
        "dwh_dm", "mid_2025", "dwh_dm", "dm_stock_lot",
        "FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
        options=["compresstype=zstd"], access_method="ao_column",
        partition_by="PARTITION BY RANGE (lot_date)",
    )

    assert (sql.index("FOR VALUES") < sql.index("PARTITION BY RANGE")
            < sql.index('USING "ao_column"') < sql.index("WITH ("))


class _Cur(object):
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.log.append(sql)


class _Conn(object):
    def __init__(self, name):
        self.name = name
        self.log = []
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return _Cur(self.log)

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def _patch(monkeypatch, src_tree, dst_tree, ddl_by_table):
    conns = {1: _Conn("src"), 2: _Conn("dst")}

    monkeypatch.setattr(ddl, "get_connection_by_id", lambda cid: {"id": cid})
    monkeypatch.setattr(ddl, "open_psycopg2_connection_by_cfg",
                        lambda cfg: conns[cfg["id"]])
    monkeypatch.setattr(
        ddl, "fetch_partition_trees",
        lambda conn, roots: src_tree if conn is conns[1] else dst_tree)

    def fake_ddl(cur, schema, table):
        text = ddl_by_table[table]
        if isinstance(text, Exception):
            raise text
        return text

    monkeypatch.setattr(ddl, "_partition_ddl", fake_ddl)
    return conns


def test_create_runs_only_on_dest_and_reports_each(monkeypatch):
    conns = _patch(
        monkeypatch, _tree("p1", "p2", "p3"), _tree("p1"),
        {"p2": "CREATE p2", "p3": ValueError("не декларативная партиция")},
    )

    res = ddl.create_missing_partitions(1, 2, [
        {"schema": ROOT[0], "table": ROOT[1]},
        {"schema": ROOT[0], "table": ROOT[1]},      # повтор не задваивает
    ])
    out = res["results"]

    assert res["remaining"] == 0
    assert conns[2].log == ["CREATE p2"]
    assert conns[1].log == []                     # в источник ничего не пишем
    assert conns[2].autocommit is True
    assert [(r["table"], r["ok"]) for r in out] == [("p2", True), ("p3", False)]
    assert "не декларативная" in out[1]["error"]
    assert conns[1].closed and conns[2].closed


def test_create_goes_in_chunks_skipping_failed(monkeypatch):
    """Тысяча партиций — не за один запрос; упавшие пропускаются offset'ом."""
    leaves = ["p%02d" % i for i in range(1, 8)]
    conns = _patch(monkeypatch, _tree(*leaves), _tree(),
                   dict((t, "CREATE " + t) for t in leaves))

    res = ddl.create_missing_partitions(
        1, 2, [{"schema": ROOT[0], "table": ROOT[1]}], limit=3, offset=2)

    assert conns[2].log == ["CREATE p03", "CREATE p04", "CREATE p05"]
    assert res["remaining"] == 2


def test_precheck_reports_missing_parts(monkeypatch):
    _patch(monkeypatch, _tree("p1", "p2"), _tree("p1"), {})
    cols = {ROOT: [{"name": "id", "type": "integer"}]}
    monkeypatch.setattr(ddl, "fetch_columns", lambda conn, tables: cols)
    monkeypatch.setattr(ddl, "dest_columns_by_source",
                        lambda conn, tables, targets=None: cols)
    monkeypatch.setattr(ddl, "analyze_dependencies", lambda s, d, t: [])

    res = ddl.precheck_tables(1, 2, [{"schema": ROOT[0], "table": ROOT[1]}])

    assert res["results"][0]["missing_parts"] == [
        {"schema": "dwh_dm", "table": "p2"}]
    assert res["summary"]["missing_parts"] == 1


def test_create_partitions_route(client, monkeypatch):
    seen = {}

    def fake_create(src, dst, tables, limit=0, offset=0):
        seen["args"] = (src, dst, tables, limit, offset)
        return {"results": [
            {"schema": "s", "table": "p1", "ok": True, "error": ""},
            {"schema": "s", "table": "p2", "ok": False, "error": "x"}],
            "remaining": 5}

    monkeypatch.setattr(ddl, "create_missing_partitions", fake_create)

    r = client.post("/api/gpcopy/create-partitions", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "root"}],
        "limit": 20, "offset": 3,
    })

    body = r.get_json()
    assert r.status_code == 200
    assert body["created"] == 1 and body["failed"] == 1
    assert body["remaining"] == 5
    assert seen["args"][2:] == ([{"schema": "s", "table": "root"}], 20, 3)


def test_create_partitions_route_needs_tables(client):
    r = client.post("/api/gpcopy/create-partitions", json={
        "source_connection_id": 1, "dest_connection_id": 2, "tables": [],
    })

    assert r.status_code == 400


# ------------------------------------------------- другая нарезка партиций

from modules.ddl_check import (  # noqa: E402
    bounds_overlap, parse_bound, partition_scheme_conflicts,
)

DAY = "FOR VALUES FROM ('2023-01-02') TO ('2023-01-03')"
WEEK = "FOR VALUES FROM ('2023-01-01') TO ('2023-01-08')"


def test_parse_range_and_list_bounds():
    assert parse_bound(DAY) == ("range", ("s", "2023-01-02"),
                                ("s", "2023-01-03"))
    assert parse_bound("FOR VALUES FROM ('2023-01-01'::date) TO (MAXVALUE)")[2] \
        == ("max",)
    assert parse_bound("FOR VALUES IN ('a', 'b')") == (
        "list", {("s", "a"), ("s", "b")})
    assert parse_bound("FOR VALUES FROM (1) TO (10)")[1] == ("n", 1.0)
    assert parse_bound("DEFAULT") is None
    assert parse_bound("FOR VALUES FROM ('a', 1) TO ('b', 2)") is None


def test_day_inside_week_overlaps_but_neighbours_do_not():
    assert bounds_overlap(parse_bound(DAY), parse_bound(WEEK))
    nxt = parse_bound("FOR VALUES FROM ('2023-01-08') TO ('2023-01-15')")
    assert not bounds_overlap(parse_bound(WEEK), nxt)   # TO не включается
    assert bounds_overlap(parse_bound("FOR VALUES IN ('a')"),
                          parse_bound("FOR VALUES IN ('b', 'a')"))


def test_daily_source_against_weekly_dest_is_a_conflict():
    src = {ROOT: {"prt_20230102": DAY,
                  "prt_20230103": "FOR VALUES FROM ('2023-01-03') TO ('2023-01-04')"}}
    dst = {ROOT: {"prt_20230101": WEEK}}
    missing = {ROOT: [{"schema": "dwh_dm", "table": "prt_20230102"},
                      {"schema": "dwh_dm", "table": "prt_20230103"}]}

    conf = partition_scheme_conflicts(src, dst, missing)

    assert conf[ROOT]["count"] == 2
    assert conf[ROOT]["src"] == "prt_20230102"
    assert conf[ROOT]["dst"] == "prt_20230101"


def test_new_partitions_after_dest_range_are_not_a_conflict():
    """Приёмник просто отстал: новых дней нет — их можно досоздать."""
    src = {ROOT: {"prt_20230109": "FOR VALUES FROM ('2023-01-09') TO ('2023-01-10')"}}
    dst = {ROOT: {"prt_20230101": WEEK}}
    missing = {ROOT: [{"schema": "dwh_dm", "table": "prt_20230109"}]}

    assert partition_scheme_conflicts(src, dst, missing) == {}
