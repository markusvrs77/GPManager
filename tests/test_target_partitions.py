"""
Секционированная таблица, которая грузится под другим именем.

gpcopy льёт каждую партицию источника в партицию цели с заменённым
префиксом корня (dm_stock_lot_prt_20260614 -> dm_stock_lot_new_prt_20260614).
Цель без партиций давала «relation ..._new_prt_... does not exist» на
каждой партиции — поэтому цель создаётся секционированной.
"""

import pytest

from modules import ddl_check


class _Cur:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.log.append(sql)


class _Conn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return _Cur(self.log)


def _fake_source(monkeypatch, partitioned=True):
    meta = {"oid": 1, "relkind": "p" if partitioned else "r",
            "options": [], "partition_by": "PARTITION BY RANGE (d)"
            if partitioned else None,
            "distributed_by": "DISTRIBUTED BY (id)", "definition": None,
            "access_method": None}
    monkeypatch.setattr(ddl_check, "_table_meta", lambda cur, s, t: meta)
    monkeypatch.setattr(ddl_check, "_table_columns", lambda cur, oid: [
        {"name": "id", "type": "bigint", "not_null": True,
         "default": "nextval('dwh_dm.seq')"},
        {"name": "d", "type": "date", "not_null": False, "default": None},
    ])
    monkeypatch.setattr(ddl_check, "_partition_children", lambda cur, oid: [
        {"schema": "dwh_dm", "table": "dm_stock_lot_prt_20260614",
         "bound": "FOR VALUES FROM ('2026-06-14') TO ('2026-06-15')",
         "options": [], "access_method": None},
        {"schema": "dwh_dm", "table": "dm_stock_lot_prt_20230416",
         "bound": "FOR VALUES FROM ('2023-04-16') TO ('2023-04-17')",
         "options": [], "access_method": None},
    ])


def test_leaf_name_follows_gpcopy_rule():
    assert ddl_check.target_leaf_name(
        "dm_stock_lot", "dm_stock_lot_prt_20260614", "dm_stock_lot_new"
    ) == "dm_stock_lot_new_prt_20260614"


def test_too_long_leaf_name_is_refused():
    with pytest.raises(ValueError):
        ddl_check.target_leaf_name("t", "t_prt_1", "x" * 60)


def test_partitioned_source_gives_partitioned_target(monkeypatch):
    _fake_source(monkeypatch)

    ddl = ddl_check.fetch_target_ddl(_Conn(), "dwh_dm", "dm_stock_lot",
                                     "dwh_dm", "dm_stock_lot_new")

    assert ddl["kind"] == "partitioned"
    root, *leaves = ddl["statements"]
    assert '"dm_stock_lot_new"' in root and "PARTITION BY RANGE (d)" in root
    assert "DISTRIBUTED BY (id)" in root
    assert "nextval" not in root          # DEFAULT источника не переносится
    assert '"dm_stock_lot_new_prt_20260614" PARTITION OF ' \
           '"dwh_dm"."dm_stock_lot_new"' in leaves[0]
    assert "FOR VALUES FROM ('2026-06-14')" in leaves[0]
    assert '"dm_stock_lot_new_prt_20230416"' in leaves[1]


def test_plain_source_stays_plain(monkeypatch):
    _fake_source(monkeypatch, partitioned=False)

    ddl = ddl_check.fetch_target_ddl(_Conn(), "s", "t", "s", "t_new")

    assert ddl["kind"] == "table" and len(ddl["statements"]) == 1
    assert "PARTITION BY" not in ddl["statements"][0]


def test_plain_target_for_partitioned_source_fails_early(monkeypatch):
    """Ровно случай из лога: цель уже создана обычной таблицей."""
    _fake_source(monkeypatch)
    monkeypatch.setattr(ddl_check, "_relkind", lambda conn, s, t: "r")
    dst = _Conn()

    with pytest.raises(ValueError) as err:
        ddl_check.ensure_target_table(_Conn(), dst, "dwh_dm", "dm_stock_lot",
                                      "dwh_dm", "dm_stock_lot_new")

    assert "Пересоздать" in str(err.value)
    assert dst.log == []                  # в приёмнике ничего не выполнено


def test_partitioned_target_gets_missing_leaves(monkeypatch):
    _fake_source(monkeypatch)
    monkeypatch.setattr(ddl_check, "_relkind", lambda conn, s, t: "p")
    dst = _Conn()

    created = ddl_check.ensure_target_table(
        _Conn(), dst, "dwh_dm", "dm_stock_lot", "dwh_dm", "dm_stock_lot_new")

    assert created is False
    assert len(dst.log) == 2
    assert all("IF NOT EXISTS" in s and "PARTITION OF" in s for s in dst.log)


def test_existing_plain_target_for_plain_source_is_left_alone(monkeypatch):
    _fake_source(monkeypatch, partitioned=False)
    monkeypatch.setattr(ddl_check, "_relkind", lambda conn, s, t: "r")
    dst = _Conn()

    assert ddl_check.ensure_target_table(
        _Conn(), dst, "s", "t", "s", "t_new") is False
    assert dst.log == []
