"""
Секционированная таблица, которая грузится под другим именем.

Два случая с живого кластера:
1. Цели нет — создавалась обычной таблицей, и gpcopy падал на каждой
   партиции («relation ..._new_prt_... does not exist»). Теперь цель
   создаётся секционированной.
2. Цель есть, но её партиции названы по-старому (после RENAME таблицы:
   dm_stock_lot_new -> dm_stock_lot_prt_20230101). Сопоставление по имени
   пыталось досоздать «недостающие» партиции и падало на пересечении
   границ. Теперь партиции сопоставляются по границам, а полная замена
   идёт в gpcopy попартиционно.
"""

import pytest

from modules import ddl_check
from modules import gpcopy


B1 = "FOR VALUES FROM ('2026-06-14') TO ('2026-06-15')"
B2 = "FOR VALUES FROM ('2023-04-16') TO ('2023-04-17')"


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


def _leaf(name, bound):
    return {"schema": "dwh_dm", "table": name, "bound": bound,
            "options": [], "access_method": None, "relkind": "r"}


def _fake_catalog(monkeypatch, partitioned=True, dst=None, dst_children=None):
    """
    Каталог источника (dm_stock_lot) и, если задан dst, цели.
    dst — relkind цели ('p' / 'r') или None, если её нет.
    """
    def meta(oid, relkind, partition_by):
        return {"oid": oid, "relkind": relkind, "options": [],
                "partition_by": partition_by,
                "distributed_by": "DISTRIBUTED BY (id)", "definition": None,
                "access_method": None}

    metas = {
        "dm_stock_lot": meta(1, "p" if partitioned else "r",
                             "PARTITION BY RANGE (d)" if partitioned else None),
    }

    if dst:
        metas["dm_stock_lot_new"] = meta(
            2, dst, "PARTITION BY RANGE (d)" if dst == "p" else None)

    children = {
        1: [_leaf("dm_stock_lot_prt_20260614", B1),
            _leaf("dm_stock_lot_prt_20230416", B2)],
        2: dst_children or [],
    }

    monkeypatch.setattr(ddl_check, "_table_meta",
                        lambda cur, s, t: metas.get(t))
    monkeypatch.setattr(ddl_check, "_table_columns", lambda cur, oid: [
        {"name": "id", "type": "bigint", "not_null": True,
         "default": "nextval('dwh_dm.seq')"},
        {"name": "d", "type": "date", "not_null": False, "default": None},
    ])
    monkeypatch.setattr(ddl_check, "_partition_children",
                        lambda cur, oid: children.get(oid, []))
    monkeypatch.setattr(ddl_check, "_relkind",
                        lambda conn, s, t: (metas.get(t) or {}).get("relkind"))


# ------------------------------------------------------------ имена и DDL

def test_leaf_name_follows_gpcopy_rule():
    assert ddl_check.target_leaf_name(
        "dm_stock_lot", "dm_stock_lot_prt_20260614", "dm_stock_lot_new"
    ) == "dm_stock_lot_new_prt_20260614"


def test_too_long_leaf_name_is_refused():
    with pytest.raises(ValueError):
        ddl_check.target_leaf_name("t", "t_prt_1", "x" * 60)


def test_partitioned_source_gives_partitioned_target(monkeypatch):
    _fake_catalog(monkeypatch)

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


def test_plain_source_stays_plain(monkeypatch):
    _fake_catalog(monkeypatch, partitioned=False)

    ddl = ddl_check.fetch_target_ddl(_Conn(), "dwh_dm", "dm_stock_lot",
                                     "dwh_dm", "dm_stock_lot_new")

    assert ddl["kind"] == "table" and len(ddl["statements"]) == 1
    assert "PARTITION BY" not in ddl["statements"][0]


# ------------------------------------------------------------ существующая цель

def test_plain_target_for_partitioned_source_fails_early(monkeypatch):
    """Цель уже создана обычной таблицей — ошибка до gpcopy, без DDL."""
    _fake_catalog(monkeypatch, dst="r")
    dst = _Conn()

    with pytest.raises(ValueError) as err:
        ddl_check.ensure_target_table(_Conn(), dst, "dwh_dm", "dm_stock_lot",
                                      "dwh_dm", "dm_stock_lot_new")

    assert "Пересоздать" in str(err.value)
    assert dst.log == []


def test_renamed_target_partitions_match_by_bound(monkeypatch):
    """
    Случай из лога: партиции цели названы по-старому. Те же границы —
    та же партиция; ничего не досоздаётся, пересечения нет.
    """
    _fake_catalog(monkeypatch, dst="p", dst_children=[
        _leaf("dm_stock_lot_prt_20260614", B1),
        _leaf("dm_stock_lot_prt_20230416", B2),
    ])
    dst = _Conn()

    created = ddl_check.ensure_target_table(
        _Conn(), dst, "dwh_dm", "dm_stock_lot", "dwh_dm", "dm_stock_lot_new")

    assert created is False
    assert dst.log == []

    pairs, missing = ddl_check.match_target_leaves(
        _Conn(), _Conn(), "dwh_dm", "dm_stock_lot", "dwh_dm",
        "dm_stock_lot_new")

    assert missing == []
    assert ("dwh_dm", "dm_stock_lot_prt_20260614",
            "dwh_dm", "dm_stock_lot_prt_20260614") in pairs


def test_only_partitions_with_new_bounds_are_created(monkeypatch):
    _fake_catalog(monkeypatch, dst="p", dst_children=[
        _leaf("old_name_for_b1", B1),
    ])
    dst = _Conn()

    ddl_check.ensure_target_table(_Conn(), dst, "dwh_dm", "dm_stock_lot",
                                  "dwh_dm", "dm_stock_lot_new")

    assert len(dst.log) == 1
    assert '"dm_stock_lot_new_prt_20230416" PARTITION OF' in dst.log[0]
    assert "2023-04-16" in dst.log[0]


def test_taken_leaf_name_is_reported_not_skipped(monkeypatch):
    """IF NOT EXISTS молча пропустил бы занятое имя — это ошибка."""
    _fake_catalog(monkeypatch, dst="p", dst_children=[])
    monkeypatch.setattr(ddl_check, "_relkind", lambda conn, s, t: "p"
                        if t == "dm_stock_lot_new" else "r")
    dst = _Conn()

    with pytest.raises(ValueError) as err:
        ddl_check.ensure_target_table(_Conn(), dst, "dwh_dm", "dm_stock_lot",
                                      "dwh_dm", "dm_stock_lot_new")

    assert "уже занято" in str(err.value)
    assert dst.log == []


def test_existing_plain_target_for_plain_source_is_left_alone(monkeypatch):
    _fake_catalog(monkeypatch, partitioned=False, dst="r")
    dst = _Conn()

    assert ddl_check.ensure_target_table(
        _Conn(), dst, "dwh_dm", "dm_stock_lot", "dwh_dm",
        "dm_stock_lot_new") is False
    assert dst.log == []


# ------------------------------------------------------------ gpcopy JSON

ITEMS = [{"id": 7, "schema_name": "dwh_dm", "table_name": "dm_stock_lot"},
         {"id": 8, "schema_name": "dwh_dm", "table_name": "plain"}]
TARGETS = {"dwh_dm.dm_stock_lot": "dwh_dm.dm_stock_lot_new"}
LEAVES = {("dwh_dm", "dm_stock_lot"): [
    ("dwh_dm", "dm_stock_lot_prt_20260614", "dwh_dm", "dm_stock_lot_prt_20260614"),
    ("dwh_dm", "dm_stock_lot_prt_20230416", "dwh_dm", "old_name_for_b2"),
]}


def test_partitioned_mapped_table_goes_leaf_to_leaf():
    entries = gpcopy.build_full_include_json(ITEMS, "adb", "adb", TARGETS,
                                             LEAVES)

    assert {"source": "adb.dwh_dm.dm_stock_lot_prt_20230416",
            "dest": "adb.dwh_dm.old_name_for_b2"} in entries
    # корень целиком не отправляется — иначе gpcopy снова выведет имена сам
    assert not any(e["source"] == "adb.dwh_dm.dm_stock_lot" for e in entries)
    assert {"source": "adb.dwh_dm.plain", "dest": "adb.dwh_dm.plain"} in entries


def test_without_leaf_map_json_is_as_before():
    entries = gpcopy.build_full_include_json(ITEMS, "adb", "adb", TARGETS)

    assert entries[0] == {"source": "adb.dwh_dm.dm_stock_lot",
                          "dest": "adb.dwh_dm.dm_stock_lot_new"}


def test_log_lines_of_leaves_are_attributed_to_root():
    keys = gpcopy.leaf_owner_keys(ITEMS, LEAVES)

    assert gpcopy.find_owner_item("dwh_dm", "old_name_for_b2", keys) == 7
    assert gpcopy.find_owner_item(
        "dwh_dm", "dm_stock_lot_prt_20230416", keys) == 7
