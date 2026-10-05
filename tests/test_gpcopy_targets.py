# -*- coding: utf-8 -*-
"""
Загрузка в другую таблицу приёмника (карта targets) на странице Greenplum:
маршруты запуска, include-JSON gpcopy, срезы по датам, watermark, sync,
подготовка приёмника и атрибуция прогресса по логу gpcopy.

Без живого Greenplum: раннеры останавливаются перед запуском gpcopy,
соединения — фейковые (tests/pg_fakes.py).
"""

import json

import pytest

import app as app_module
import modules.connections as connections
import modules.ddl_check as ddl
import modules.gpcopy as gp
import modules.gpcopy_increment as inc
import modules.gpcopy_sync as gsync
import modules.table_catalog as catalog
from job_manager import create_job, get_job, get_job_items
from tests.pg_fakes import FakeConn

CONN = {"host": "gp-host", "port": 5432, "username": "gpadmin",
        "database_name": "adb"}


class Stop(Exception):
    """Останавливает раннер до запуска настоящего gpcopy."""


# ------------------------------------------------------------ маршруты

@pytest.fixture
def captured(monkeypatch):
    """Перехватывает создание задачи и поток раннера."""
    box = {}

    def fake_create_job(job_type, connection_id, config, **kw):
        box["job_type"] = job_type
        box["config"] = config
        return 901

    monkeypatch.setattr(app_module, "create_job", fake_create_job)
    monkeypatch.setattr(app_module, "create_job_items",
                        lambda **kw: box.setdefault("items", kw["items"]))
    monkeypatch.setattr(app_module, "run_gpcopy_job", lambda job_id: None)
    monkeypatch.setattr(app_module, "run_gpcopy_sync_job", lambda job_id: None)
    monkeypatch.setattr(app_module, "run_gpcopy_increment_job",
                        lambda job_id: None)
    monkeypatch.setattr(app_module, "run_gpcopy_partition_diff_job",
                        lambda job_id: None)
    monkeypatch.setattr(app_module, "run_copy_pipe_job", lambda job_id: None)
    monkeypatch.setattr(connections, "get_connection_by_id",
                        lambda cid: {"db_type": "greenplum"})
    return box


def _start(client, **extra):
    body = {"source_connection_id": 1, "dest_connection_id": 2,
            "tables": [{"schema": "s", "table": "a"},
                       {"schema": "s", "table": "c"}]}
    body.update(extra)
    return client.post("/api/gpcopy/start", json=body)


def test_start_stores_normalized_targets(client, captured):
    r = _start(client, targets={"s.a": "a_copy", "s.c": ""}, truncate=True)

    assert r.status_code == 200
    assert captured["config"]["targets"] == {"s.a": "s.a_copy"}
    # строки задачи — по-прежнему имена источника
    assert [i["table_name"] for i in captured["items"]] == ["a", "c"]


def test_start_without_targets_keeps_config_as_before(client, captured):
    assert _start(client).status_code == 200
    assert "targets" not in captured["config"]


def test_start_rejects_bad_target_name(client, captured):
    r = _start(client, targets={"s.a": "Arch.A"})

    assert r.status_code == 400
    assert r.get_json()["ok"] is False
    assert "config" not in captured


def test_start_rejects_two_tables_into_one_target(client, captured):
    r = _start(client, targets={"s.a": "arch.t", "s.c": "arch.t"})

    assert r.status_code == 400
    assert "config" not in captured


def test_start_rejects_target_of_other_selected_table(client, captured):
    r = _start(client, targets={"s.a": "s.c"})

    assert r.status_code == 400


def test_start_rejects_targets_outside_selection(client, captured):
    """Fallback-задача получает карту только своей части выбора."""
    r = _start(client, targets={"s.zzz": "s.b"})

    assert r.status_code == 400


def test_partition_mode_rejects_targets(client, captured):
    r = client.post("/api/gpcopy/partition-diff/start", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a"}],
        "targets": {"s.a": "s.b"},
    })

    assert r.status_code == 400
    assert "партиций" in r.get_json()["message"]
    assert "config" not in captured


def test_partition_mode_ignores_empty_targets(client, captured):
    r = client.post("/api/gpcopy/partition-diff/start", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a"}],
        "targets": {"s.a": ""},
    })

    assert r.status_code == 200
    assert "targets" not in captured["config"]


def test_start_date_fills_dest_from_targets(client, captured):
    r = client.post("/api/gpcopy/start-date", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "date_from": "2026-09-01", "date_to": "2026-09-02",
        "window_cleanup": True,
        "table_configs": [
            {"schema": "s", "table": "a", "source": "s.a", "dest": "s.a",
             "date_column": "d"},
            {"schema": "s", "table": "c", "source": "s.c", "dest": "s.c",
             "date_column": "d"},
        ],
        "targets": {"s.a": "arch.a"},
    })

    assert r.status_code == 200
    cfg = captured["config"]
    assert cfg["targets"] == {"s.a": "arch.a"}
    assert [t["dest"] for t in cfg["table_configs"]] == ["arch.a", "s.c"]
    # очистка окна пойдёт по цели
    assert gp.window_targets(cfg["table_configs"]) == [
        ("arch", "a", "d"), ("s", "c", "d")]


def test_start_date_rejects_bad_targets(client, captured):
    r = client.post("/api/gpcopy/start-date", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "date_from": "2026-09-01", "date_to": "2026-09-02",
        "table_configs": [{"schema": "s", "table": "a", "date_column": "d"}],
        "targets": {"s.a": "x y"},
    })

    assert r.status_code == 400
    assert "config" not in captured


def test_window_preview_counts_in_target(client, monkeypatch):
    seen = {}

    def fake_clear(dest_id, configs, date_from, date_to, dry_run=False):
        seen["targets"] = gp.window_targets(configs)
        return [("arch", "a", 5)]

    monkeypatch.setattr(app_module, "clear_window_in_dest", fake_clear)
    monkeypatch.setattr(gp, "missing_dest_tables",
                        lambda dest_id, pairs: set())   # цель уже есть

    r = client.post("/api/gpcopy/window-preview", json={
        "dest_connection_id": 2, "date_from": "2026-09-01",
        "date_to": "2026-09-02",
        "table_configs": [{"schema": "s", "table": "a", "source": "s.a",
                           "dest": "s.a", "date_column": "d"}],
        "targets": {"s.a": "arch.a"},
    })

    assert r.status_code == 200
    assert seen["targets"] == [("arch", "a", "d")]


def test_sync_apply_fills_target(client, captured):
    r = client.post("/api/gpcopy/sync/apply", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "table_configs": [
            {"schema": "s", "table": "a", "key_columns": ["id"]},
            {"schema": "s", "table": "c", "key_columns": ["id"]},
        ],
        "targets": {"s.a": "a_copy"},
    })

    assert r.status_code == 200
    cfgs = captured["config"]["table_configs"]
    assert cfgs[0]["target"] == "s.a_copy"
    assert "target" not in cfgs[1]
    assert captured["config"]["targets"] == {"s.a": "s.a_copy"}


def test_sync_apply_rejects_bad_targets(client, captured):
    r = client.post("/api/gpcopy/sync/apply", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "table_configs": [{"schema": "s", "table": "a",
                           "key_columns": ["id"]}],
        "targets": {"s.a": "S.A"},
    })

    assert r.status_code == 400
    assert "config" not in captured


def test_increment_start_stores_normalized_targets(client, captured):
    r = client.post("/api/gpcopy/increment/start", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a", "watermark_column": "id"}],
        "targets": {"s.a": "a_log"},
    })

    assert r.status_code == 200
    assert captured["config"]["targets"] == {"s.a": "s.a_log"}


def test_increment_start_rejects_bad_targets(client, captured):
    r = client.post("/api/gpcopy/increment/start", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a", "watermark_column": "id"}],
        "targets": {"s.a": "a-log"},
    })

    assert r.status_code == 400
    assert "config" not in captured


def test_precheck_route_passes_targets(client, monkeypatch):
    seen = {}

    def fake_precheck(src, dst, tables, targets=None):
        seen["targets"] = targets
        return {"results": [], "deps": [], "summary": {}}

    monkeypatch.setattr(ddl, "precheck_tables", fake_precheck)

    r = client.post("/api/gpcopy/precheck", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a"}],
        "targets": {"s.a": "b"},
    })

    assert r.status_code == 200
    assert seen["targets"] == {"s.a": "s.b"}


def test_create_tables_route_rejects_bad_targets(client):
    r = client.post("/api/gpcopy/create-tables", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a"}],
        "targets": {"s.a": "B"},
    })

    assert r.status_code == 400


# ------------------------------------------------------------ полная замена

def test_full_include_json_has_dest_for_mapped_tables():
    items = [{"schema_name": "s", "table_name": "a"},
             {"schema_name": "s", "table_name": "c"}]

    assert gp.build_full_include_json(items, "src", "dst",
                                      {"s.a": "arch.a_copy"}) == [
        {"source": "src.s.a", "dest": "dst.arch.a_copy"},
        {"source": "src.s.c", "dest": "dst.s.c"},
    ]


@pytest.fixture
def full_run(monkeypatch):
    """Прогоняет полный раннер до gpcopy: что ушло бы в команду."""
    seen = {"prepared": []}

    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: {})
    monkeypatch.setattr(gp, "get_connection_by_id", lambda cid: CONN)
    monkeypatch.setattr(gp, "write_dest_mapping_file", lambda cfg: None)

    def fake_prepare(src_id, dst_id, targets, pairs):
        seen["prepared"].append((dict(targets), list(pairs)))
        return {}

    monkeypatch.setattr(gp, "prepare_mapped_targets", fake_prepare)
    # таблицы в этих тестах не секционированы — попартиционной карты нет
    monkeypatch.setattr(gp, "mapped_source_leaves",
                        lambda c, t, p: {})

    def fake_command(**kwargs):
        seen["command"] = kwargs

        if kwargs.get("include_json_file"):
            with open(kwargs["include_json_file"], encoding="utf-8") as f:
                seen["json"] = json.load(f)

        if kwargs.get("include_tables_file"):
            with open(kwargs["include_tables_file"], encoding="utf-8") as f:
                seen["file"] = f.read().split()

        raise Stop()

    monkeypatch.setattr(gp, "build_gpcopy_command", fake_command)

    def run(config):
        cfg = {"source_connection_id": 1, "dest_connection_id": 2,
               "tables": [{"schema": "s", "table": "a"},
                          {"schema": "s", "table": "c"}],
               "truncate": True, "gpcopy_path": "/usr/local/bin/gpcopy"}
        cfg.update(config)
        job_id = create_job("gpcopy", 1, cfg)
        gp.run_gpcopy_job(job_id)
        seen["job_id"] = job_id
        return seen

    return run


def test_full_runner_without_targets_uses_include_file(full_run):
    seen = full_run({})

    assert seen["command"]["include_json_file"] is None
    assert seen["file"] == ["adb.s.a", "adb.s.c"]
    assert seen["prepared"] == []


def test_full_runner_with_targets_uses_json_with_dest(full_run):
    seen = full_run({"targets": {"s.a": "arch.a_copy"}})

    assert seen["command"]["include_tables_file"] is None
    assert seen["json"] == [
        {"source": "adb.s.a", "dest": "adb.arch.a_copy"},
        {"source": "adb.s.c", "dest": "adb.s.c"},
    ]
    assert seen["command"]["truncate"] is True
    # цель проверяется (и создаётся) до gpcopy
    assert seen["prepared"] == [({"s.a": "arch.a_copy"},
                                 [("s", "a"), ("s", "c")])]


def test_full_runner_target_failure(monkeypatch):
    """Не вышло создать цель — эта таблица падает, остальные идут дальше."""
    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: {})
    monkeypatch.setattr(gp, "get_connection_by_id", lambda cid: CONN)
    monkeypatch.setattr(gp, "write_dest_mapping_file", lambda cfg: None)
    monkeypatch.setattr(
        gp, "prepare_mapped_targets",
        lambda s, d, t, p: {("s", "a"): "Цели нет: permission denied"})
    monkeypatch.setattr(gp, "mapped_source_leaves",
                        lambda c, t, p: {})
    seen = {}

    def fake_command(**kwargs):
        with open(kwargs["include_json_file"], encoding="utf-8") as f:
            seen["json"] = json.load(f)
        raise Stop()

    monkeypatch.setattr(gp, "build_gpcopy_command", fake_command)

    job_id = create_job("gpcopy", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a"},
                   {"schema": "s", "table": "c"},
                   {"schema": "s", "table": "e"}],
        "targets": {"s.a": "arch.a", "s.e": "arch.e"},
        "append": True, "gpcopy_path": "/usr/local/bin/gpcopy",
    })

    gp.run_gpcopy_job(job_id)

    by_name = {i["table_name"]: i for i in get_job_items(job_id)}
    assert by_name["a"]["status"] == "failed"
    assert "permission denied" in by_name["a"]["error_message"]
    # в gpcopy ушли только остальные
    assert [e["source"] for e in seen["json"]] == ["adb.s.c", "adb.s.e"]


def test_date_runner_cleans_window_in_target_and_skips_failed(monkeypatch):
    """
    Конфиг без dest (как из расписания): очистка окна всё равно идёт по
    цели, а таблица, чью цель не создали, в gpcopy не уходит.
    """
    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: {})
    monkeypatch.setattr(gp, "get_connection_by_id",
                        lambda cid: dict(CONN, database="adb"))
    monkeypatch.setattr(gp, "write_dest_mapping_file", lambda cfg: None)
    monkeypatch.setattr(
        gp, "fetch_leaves_by_key",
        lambda conn, entries, date_from="", date_to="": {})
    monkeypatch.setattr(gp, "mapped_source_leaves", lambda c, t, p: {})
    monkeypatch.setattr(
        gp, "prepare_mapped_targets",
        lambda s, d, t, p: {("s", "c"): "Цели нет: permission denied"})
    seen = {}

    def fake_clear(dest_id, configs, date_from, date_to, dry_run=False):
        seen["cleared"] = gp.window_targets(configs)
        return []

    def fake_command(**kwargs):
        with open(kwargs["include_json_file"], encoding="utf-8") as f:
            seen["json"] = json.load(f)
        raise Stop()

    monkeypatch.setattr(gp, "clear_window_in_dest", fake_clear)
    monkeypatch.setattr(gp, "build_gpcopy_command", fake_command)

    job_id = create_job("gpcopy", 1, {
        "mode": "date_filter",
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a"},
                   {"schema": "s", "table": "c"}],
        "table_configs": [
            {"schema": "s", "table": "a", "date_column": "d",
             "sql": "SELECT 1"},
            {"schema": "s", "table": "c", "date_column": "d",
             "sql": "SELECT 2"},
        ],
        "targets": {"s.a": "arch.a", "s.c": "arch.c"},
        "date_from": "2026-09-01", "date_to": "2026-09-02",
        "window_cleanup": True, "append": True,
        "gpcopy_path": "/usr/local/bin/gpcopy",
    })

    gp.run_gpcopy_job(job_id)

    assert seen["cleared"] == [("arch", "a", "d")]
    assert seen["json"] == [{"source": "adb.s.a", "dest": "adb.arch.a",
                             "sql": "SELECT 1"}]
    by_name = {i["table_name"]: i["status"] for i in get_job_items(job_id)}
    assert by_name["c"] == "failed"


def test_runner_rejects_bad_targets_from_schedule(full_run):
    """Расписание минует маршрут: раннер проверяет карту сам."""
    seen = full_run({"targets": {"s.a": "Bad Name"}})

    assert "command" not in seen
    job = get_job(seen["job_id"])
    assert job["status"] == "failed"


# ------------------------------------------------------------ окно по датам

def _date_preview(monkeypatch, leaves, **config):
    monkeypatch.setattr(gp, "get_connection_by_id",
                        lambda cid: {"database": "adb", "host": "h"})
    monkeypatch.setattr(
        gp, "fetch_leaves_by_key",
        lambda conn, entries, date_from="", date_to="": leaves)

    cfg = {
        "source_connection_id": 1, "dest_connection_id": 2,
        "date_from": "2026-07-01", "date_to": "2026-07-02",
        "table_configs": [{
            "schema": "s", "table": "f", "source": "s.f", "dest": "s.f",
            "date_column": "d", "sql": "SELECT 1",
        }],
    }
    cfg.update(config)
    return gp.build_gpcopy_date_include_json_preview(cfg)


LEAVES = {("s", "f"): [("s", "f_1_prt_1"), ("s", "f_1_prt_2")]}


def test_date_json_mapped_partitions_go_to_target_root(monkeypatch):
    items = _date_preview(monkeypatch, LEAVES, targets={"s.f": "arch.f2"})

    assert [i["source"] for i in items] == ["adb.s.f_1_prt_1",
                                             "adb.s.f_1_prt_2"]
    assert [i["dest"] for i in items] == ["adb.arch.f2", "adb.arch.f2"]
    assert all('"f_1_prt_' in i["sql"] for i in items)


def test_date_json_without_targets_keeps_leaf_names(monkeypatch):
    items = _date_preview(monkeypatch, LEAVES)

    assert [i["dest"] for i in items] == ["adb.s.f_1_prt_1",
                                           "adb.s.f_1_prt_2"]


def test_date_json_mapped_plain_table_dest_is_target(monkeypatch):
    items = _date_preview(monkeypatch, {("s", "f"): [("s", "f")]},
                          targets={"s.f": "f_copy"})

    assert items == [{"source": "adb.s.f", "dest": "adb.s.f_copy",
                      "sql": "SELECT 1"}]


def test_date_json_shared_target_with_truncate_is_refused(monkeypatch):
    """Каждая партиция с --truncate очистила бы общую цель заново."""
    with pytest.raises(ValueError):
        _date_preview(monkeypatch, LEAVES, targets={"s.f": "arch.f2"},
                      truncate=True)


def test_date_json_scheduled_selection_uses_targets(monkeypatch):
    """Конфиг расписания: selected_tables + колонка даты, без table_configs."""
    items = _date_preview(
        monkeypatch, {("s", "f"): [("s", "f")]},
        table_configs=[], selected_tables=[{"schema": "s", "table": "f"}],
        date_filter_column="d", targets={"s.f": "arch.f"})

    assert items[0]["dest"] == "adb.arch.f"


# ------------------------------------------------------------ watermark

def test_increment_items_dest_is_target():
    items = inc.build_increment_items(
        [{"schema": "s", "table": "a", "watermark_column": "id"}],
        {("s", "a"): 10}, "src", "dst", {"s.a": "arch.a_log"})

    assert items[0]["dest"] == "dst.arch.a_log"
    assert items[0]["source"] == "src.s.a"
    assert items[0]["sql"].endswith('"id" > 10')


def test_increment_watermark_is_read_from_target(monkeypatch):
    seen = []

    monkeypatch.setattr(inc, "get_connection_by_id",
                        lambda cid: {"database": "adb"})
    monkeypatch.setattr(
        inc, "get_dest_watermark",
        lambda cfg, schema, table, column: seen.append((schema, table)) or 7)

    path = inc.build_increment_include_json_file({
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a", "watermark_column": "id"}],
        "targets": {"s.a": "arch.a_log"},
    })

    with open(path, encoding="utf-8") as f:
        items = json.load(f)

    assert seen == [("arch", "a_log")]
    assert items[0]["dest"] == "adb.arch.a_log"


# ------------------------------------------------------------ по ключу

def test_sync_targets_fill_target_and_validate():
    cfgs, targets = gsync.apply_sync_targets(
        [{"schema": "s", "table": "a"}, {"source": "s.c", "target": "s.c"}],
        {"s.a": "arch.a"})

    assert targets == {"s.a": "arch.a"}
    assert gsync.resolve_sync_names(cfgs[0]) == ("s.a", "arch.a")
    assert gsync.resolve_sync_names(cfgs[1]) == ("s.c", "s.c")

    with pytest.raises(ValueError):
        gsync.apply_sync_targets([{"schema": "s", "table": "a"}],
                                 {"s.a": "Arch"})


def test_sync_without_targets_is_unchanged():
    cfgs, targets = gsync.apply_sync_targets(
        [{"source": "s.a", "target": "x.y"}], None)

    assert targets == {}
    assert cfgs == [{"source": "s.a", "target": "x.y"}]


# ------------------------------------------------------------ подготовка приёмника

def _ddl_conns(monkeypatch, src, dst):
    monkeypatch.setattr(ddl, "get_connection_by_id",
                        lambda cid: {"id": cid})
    monkeypatch.setattr(ddl, "open_psycopg2_connection_by_cfg",
                        lambda cfg: src if cfg["id"] == 1 else dst)


def test_precheck_compares_source_with_target(monkeypatch):
    src, dst = FakeConn(), FakeConn()
    _ddl_conns(monkeypatch, src, dst)
    asked = {}

    def fake_columns(conn, tables):
        side = "src" if conn is src else "dst"
        asked[side] = [(t["schema"], t["table"]) for t in tables]

        if side == "src":
            return {("s", "a"): [{"name": "id", "type": "integer"},
                                 {"name": "v", "type": "text"}]}

        return {("arch", "a2"): [{"name": "id", "type": "integer"}]}

    monkeypatch.setattr(ddl, "fetch_columns", fake_columns)

    result = ddl.precheck_tables(1, 2, [{"schema": "s", "table": "a"}],
                                 targets={"s.a": "arch.a2"})

    assert asked["dst"] == [("arch", "a2")]
    row = result["results"][0]
    assert (row["schema"], row["table"]) == ("s", "a")
    assert row["target"] == "arch.a2"
    assert row["status"] == "diff"
    assert [c["name"] for c in row["missing_in_dest"]] == ["v"]


def test_add_columns_goes_to_target(monkeypatch):
    dst = FakeConn()
    _ddl_conns(monkeypatch, None, dst)

    ddl.add_missing_columns(
        2, [{"schema": "s", "table": "a",
             "columns": [{"name": "v", "type": "text"}]}],
        targets={"s.a": "arch.a2"})

    assert 'ALTER TABLE "arch"."a2" ADD COLUMN "v" text' in dst.sql_text()


def _source_catalog():
    return FakeConn(responses=[
        ("LEFT JOIN pg_am", [(42, "r", ["appendonly=true"], "ao_column")]),
        ("pg_get_table_distributedby", [("DISTRIBUTED BY (id)",)]),
        ("LEFT JOIN pg_attrdef", [
            ("id", "integer", True, "nextval('s.a_id_seq'::regclass)"),
            ("v", "text", False, None),
        ]),
    ])


def test_create_tables_creates_target_by_source_structure(monkeypatch):
    src = _source_catalog()
    dst = FakeConn(responses=[("FROM pg_namespace", [])])
    _ddl_conns(monkeypatch, src, dst)
    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: {})

    rows = ddl.create_missing_objects(
        1, 2, [{"schema": "s", "table": "a"}], targets={"s.a": "arch.a2"})

    assert rows[0]["ok"] is True, rows
    assert rows[0]["target"] == "arch.a2"
    text = dst.sql_text()
    assert 'CREATE SCHEMA IF NOT EXISTS "arch"' in text
    assert 'CREATE TABLE IF NOT EXISTS "arch"."a2"' in text
    assert '"id" integer NOT NULL' in text
    assert "DISTRIBUTED BY (id)" in text
    assert 'USING "ao_column"' in text
    # DEFAULT с последовательностью источника не переносится
    assert "nextval" not in text
    # источник только читается
    assert not any(q.lstrip().upper().startswith(("CREATE", "ALTER", "DROP"))
                   for q, _ in src.executed)


def test_ensure_mapped_targets_creates_only_missing(monkeypatch):
    src = _source_catalog()
    existing = {("arch", "c2")}

    def rel(params):
        return [(1,)] if tuple(params) in existing else []

    dst = FakeConn(responses=[
        ("FROM pg_class c", rel),
        ("FROM pg_namespace", [(1,)]),
    ])
    _ddl_conns(monkeypatch, src, dst)

    errors = ddl.ensure_mapped_targets(
        1, 2, {"s.a": "arch.a2", "s.c": "arch.c2"},
        [("s", "a"), ("s", "c"), ("s", "e")])

    assert errors == {}
    created = [q for q, _ in dst.executed if q.startswith("CREATE TABLE")]
    assert len(created) == 1
    assert '"arch"."a2"' in created[0]
    assert src.session.get("readonly") is True


def test_ensure_mapped_targets_reports_failure(monkeypatch):
    src = FakeConn()   # источника нет — нечего брать за образец
    dst = FakeConn(responses=[("FROM pg_class c", [])])
    _ddl_conns(monkeypatch, src, dst)

    errors = ddl.ensure_mapped_targets(1, 2, {"s.a": "arch.a2"}, [("s", "a")])

    assert "arch.a2" in errors[("s", "a")]


# ------------------------------------------------------------ прогресс по логу

def test_log_line_with_target_name_is_attributed_to_source_item():
    items = [{"id": 1, "schema_name": "s", "table_name": "a"},
             {"id": 2, "schema_name": "s", "table_name": "c"}]
    keys = gp.owner_item_keys(items, {"s.a": "arch.a2"})

    assert gp.find_owner_item("arch", "a2", keys) == 1
    assert gp.find_owner_item("arch", "a2_1_prt_x", keys) == 1
    assert gp.find_owner_item("s", "a", keys) == 1
    assert gp.owner_item_keys(items) == keys[:2]


def test_swap_prefers_source_names():
    items = [{"id": 1, "schema_name": "a", "table_name": "x"},
             {"id": 2, "schema_name": "a", "table_name": "y"}]
    keys = gp.owner_item_keys(items, {"a.x": "a.y", "a.y": "a.x"})

    assert gp.find_owner_item("a", "y", keys) == 2
    assert gp.item_owns_leaf("a", "y", "a", "x", {"a.x": "a.y"},
                             [("a", "x"), ("a", "y")]) is False


def test_finalize_marks_mapped_table_by_target_name(monkeypatch):
    job_id = create_job("gpcopy", 1, {
        "tables": [{"schema": "s", "table": "a"},
                   {"schema": "s", "table": "c"}],
    })
    items = get_job_items(job_id)
    config = {"targets": {"s.a": "arch.a2", "s.c": "arch.c2"}}

    log = (
        'Finished copying table "adb"."arch"."a2" => "adb"."arch"."a2"\n'
        'Failed to copy table "adb"."arch"."c2"\n'
    )

    gp.finalize_gpcopy_job(job_id, items, 1, log, "", "gpcopy", 1.0, config)

    by_name = {i["table_name"]: i["status"] for i in get_job_items(job_id)}
    assert by_name == {"a": "done", "c": "failed"}
    # упавшее — под именем источника: по нему строилась бы дозагрузка
    assert config["failed_leaves"] == [["s", "c"]]


def test_retry_refuses_leaves_of_mapped_tables():
    config = {"targets": {"s.f": "arch.f2"}}

    with pytest.raises(ValueError) as error:
        gp.build_retry_config(config, [("s", "f_1_prt_3")])

    assert "s.f" in str(error.value)


def test_retry_of_unmapped_leaves_drops_targets():
    retry = gp.build_retry_config(
        {"targets": {"s.f": "arch.f2"}}, [("s", "g_1_prt_3")])

    assert "targets" not in retry
    assert retry["selected_tables"] == [{"schema": "s", "table": "g_1_prt_3"}]


# ------------------------------------------------------------ ревью: режимы и окно

def test_start_rejects_skip_existing_with_targets(client, captured):
    """Цель создаётся заранее: skip-existing дал бы done без данных."""
    r = _start(client, targets={"s.a": "a_copy"}, skip_existing=True)

    assert r.status_code == 400
    assert "skip-existing" in r.get_json()["message"]
    assert "config" not in captured


def test_start_requires_existing_mode_with_targets(client, captured):
    r = _start(client, targets={"s.a": "a_copy"})

    assert r.status_code == 400
    assert "truncate" in r.get_json()["message"]
    assert "config" not in captured


def test_start_without_targets_needs_no_mode(client, captured):
    assert _start(client).status_code == 200


def test_start_date_rejects_skip_existing_with_targets(client, captured):
    r = client.post("/api/gpcopy/start-date", json={
        "source_connection_id": 1, "dest_connection_id": 2,
        "date_from": "2026-09-01", "date_to": "2026-09-02",
        "skip_existing": True,
        "table_configs": [{"schema": "s", "table": "a", "date_column": "d"}],
        "targets": {"s.a": "arch.a"},
    })

    assert r.status_code == 400
    assert "config" not in captured


def test_runner_refuses_skip_existing_with_targets(full_run):
    """Расписание минует маршрут — раннер не даёт пометить done без данных."""
    seen = full_run({"targets": {"s.a": "arch.a"}, "truncate": False,
                     "skip_existing": True})

    assert "command" not in seen
    assert seen["prepared"] == []
    assert get_job(seen["job_id"])["status"] == "failed"
    assert all(i["status"] != "done" for i in get_job_items(seen["job_id"]))


def test_window_preview_target_not_created_yet(client, monkeypatch):
    called = {}

    def fake_clear(*a, **kw):
        called["yes"] = True
        return []

    monkeypatch.setattr(app_module, "clear_window_in_dest", fake_clear)
    monkeypatch.setattr(gp, "missing_dest_tables",
                        lambda dest_id, pairs: {("arch", "a")})

    r = client.post("/api/gpcopy/window-preview", json={
        "dest_connection_id": 2, "date_from": "2026-09-01",
        "date_to": "2026-09-02",
        "table_configs": [{"schema": "s", "table": "a", "source": "s.a",
                           "dest": "s.a", "date_column": "d"}],
        "targets": {"s.a": "arch.a"},
    })

    body = r.get_json()
    assert r.status_code == 200, body
    assert "yes" not in called
    assert body["total_rows"] == 0
    assert body["tables"][0]["table"] == "a"
    assert body["tables"][0]["schema"] == "arch"
    assert "создаст" in body["tables"][0]["note"]


def test_window_preview_missing_target_still_checks_bounds(client, monkeypatch):
    monkeypatch.setattr(gp, "missing_dest_tables",
                        lambda dest_id, pairs: {("arch", "a")})

    r = client.post("/api/gpcopy/window-preview", json={
        "dest_connection_id": 2, "date_from": "2026-09-05",
        "date_to": "2026-09-02",
        "table_configs": [{"schema": "s", "table": "a", "date_column": "d"}],
        "targets": {"s.a": "arch.a"},
    })

    assert r.status_code == 400


def test_inherited_uppercase_schema_is_quoted_for_gpcopy():
    """Короткая форма берёт схему источника как есть, gpcopy — в кавычках."""
    items = [{"schema_name": "Sales", "table_name": "a"}]

    assert gp.build_full_include_json(items, "src", "dst",
                                      {"Sales.a": "Sales.a_copy"}) == [
        {"source": 'src."Sales".a', "dest": 'dst."Sales".a_copy'}]

    inc_items = inc.build_increment_items(
        [{"schema": "Sales", "table": "a", "watermark_column": "id"}],
        {}, "src", "dst", {"Sales.a": "Sales.a_log"})

    assert inc_items[0]["dest"] == 'dst."Sales".a_log'


# ------------------------------------------------------------ секционированная цель

PART_LEAVES = {("s", "a"): [("s", "a_prt_1"), ("s", "a_prt_2")]}


class _FakePg:
    autocommit = False

    def set_session(self, **kw):
        pass

    def close(self):
        pass


def _part_run(full_run, monkeypatch, config, leaves=None):
    from modules import gpcopy_stage

    created = []
    monkeypatch.setattr(gp, "mapped_source_leaves",
                        lambda c, t, p: dict(leaves or PART_LEAVES))
    monkeypatch.setattr(gp, "open_psycopg2_connection_by_cfg",
                        lambda cfg: _FakePg())
    monkeypatch.setattr(gpcopy_stage, "create_stages",
                        lambda src, dst, merges: created.append(merges))
    seen = full_run(config)
    seen["created"] = created
    seen["config"] = json.loads(get_job(seen["job_id"])["config_json"])
    return seen


def test_partitioned_target_goes_through_stage_tables(full_run, monkeypatch):
    """
    gpcopy 2.7: «Multiple source tables ... cannot be transferred to the
    same dest table». Каждая партиция — в свою промежуточную таблицу.
    """
    seen = _part_run(full_run, monkeypatch, {
        "targets": {"s.a": "arch.a_copy"}})

    job = seen["job_id"]
    stage = "adb.opsentri_gpcopy_stage.j{}_0000".format(job)
    assert seen["json"] == [
        {"source": "adb.s.a_prt_1", "dest": stage + "1",
         "sql": 'SELECT * FROM "s"."a_prt_1"'},
        {"source": "adb.s.a_prt_2", "dest": stage + "2",
         "sql": 'SELECT * FROM "s"."a_prt_2"'},
        {"source": "adb.s.c", "dest": "adb.s.c"},
    ]
    dests = [e["dest"] for e in seen["json"]]
    assert len(dests) == len(set(dests))      # ни одной общей цели

    merges = seen["config"]["stage_merges"]
    assert merges == [{
        "item": ["s", "a"], "target": ["arch", "a_copy"], "truncate": True,
        "stages": [["opsentri_gpcopy_stage", "j{}_00001".format(job)],
                   ["opsentri_gpcopy_stage", "j{}_00002".format(job)]],
    }]
    assert seen["created"] == [merges]
    # общий --truncate остаётся: другим таблицам он нужен, промежуточные пусты
    assert seen["command"]["truncate"] is True


def test_partitioned_target_with_append_does_not_truncate(full_run,
                                                          monkeypatch):
    seen = _part_run(full_run, monkeypatch, {
        "targets": {"s.a": "arch.a_copy"}, "truncate": False,
        "append": True})

    assert seen["config"]["stage_merges"][0]["truncate"] is False


def test_partitioned_target_refuses_drop(full_run, monkeypatch):
    seen = _part_run(full_run, monkeypatch, {
        "targets": {"s.a": "arch.a_copy"}, "truncate": False, "drop": True})

    items = {i["table_name"]: i for i in get_job_items(seen["job_id"])}
    assert items["a"]["status"] == "failed"
    assert "drop" in items["a"]["error_message"]
    assert [e["source"] for e in seen["json"]] == ["adb.s.c"]
    assert seen["created"] == []


# ------------------------------------------------------------ перелив в цель

def _staged_job(monkeypatch, merge_errors=None):
    calls = []

    def fake_finish(config, apply):
        calls.append(apply)
        return dict(merge_errors or {}) if apply else {}

    monkeypatch.setattr(gp, "finish_stage_merges", fake_finish)

    cfg = {"source_connection_id": 1, "dest_connection_id": 2,
           "tables": [{"schema": "s", "table": "a"},
                      {"schema": "s", "table": "c"}],
           "targets": {"s.a": "arch.a_copy"},
           "stage_merges": [{"item": ["s", "a"], "target": ["arch", "a_copy"],
                             "truncate": True, "stages": [["st", "j1_00001"]]}]}
    job_id = create_job("gpcopy", 1, cfg)
    return job_id, cfg, calls


def test_success_merges_then_marks_done(monkeypatch):
    job_id, cfg, calls = _staged_job(monkeypatch)

    gp.finalize_gpcopy_job(job_id, get_job_items(job_id), 0, "", "", "cmd",
                           1, cfg)

    assert calls == [True]
    assert {i["status"] for i in get_job_items(job_id)} == {"done"}
    assert "stage_merges" not in json.loads(get_job(job_id)["config_json"])


def test_failed_merge_fails_only_that_table(monkeypatch):
    job_id, cfg, calls = _staged_job(
        monkeypatch, {("s", "a"): "перенести не удалось: no partition"})

    gp.finalize_gpcopy_job(job_id, get_job_items(job_id), 0, "", "", "cmd",
                           1, cfg)

    by_name = {i["table_name"]: i for i in get_job_items(job_id)}
    assert by_name["a"]["status"] == "failed"
    assert "no partition" in by_name["a"]["error_message"]
    assert by_name["c"]["status"] == "done"
    assert get_job(job_id)["status"] == "failed"


def test_gpcopy_failure_drops_stages_without_merge(monkeypatch):
    job_id, cfg, calls = _staged_job(monkeypatch)

    gp.finalize_gpcopy_job(job_id, get_job_items(job_id), 1, "boom", "",
                           "cmd", 1, cfg)

    assert calls == [False]
    by_name = {i["table_name"]: i for i in get_job_items(job_id)}
    assert by_name["a"]["status"] == "failed"
    assert "цель не изменена" in by_name["a"]["error_message"]


def test_date_runner_partitioned_target_uses_stage_tables(monkeypatch):
    """Окно дат: срезы партиций тоже идут в свои промежуточные таблицы."""
    from modules import gpcopy_stage

    monkeypatch.setattr(catalog, "fetch_partition_pairs", lambda cid: {})
    monkeypatch.setattr(gp, "get_connection_by_id",
                        lambda cid: dict(CONN, database="adb"))
    monkeypatch.setattr(gp, "write_dest_mapping_file", lambda cfg: None)
    monkeypatch.setattr(
        gp, "fetch_leaves_by_key",
        lambda conn, entries, date_from="", date_to="": {
            ("s", "a"): [("s", "a_prt_1"), ("s", "a_prt_2")]})
    monkeypatch.setattr(gp, "mapped_source_leaves",
                        lambda c, t, p: dict(PART_LEAVES))
    monkeypatch.setattr(gp, "prepare_mapped_targets", lambda s, d, t, p: {})
    monkeypatch.setattr(gp, "open_psycopg2_connection_by_cfg",
                        lambda cfg: _FakePg())
    monkeypatch.setattr(gpcopy_stage, "create_stages",
                        lambda src, dst, merges: None)
    seen = {}

    def fake_command(**kwargs):
        with open(kwargs["include_json_file"], encoding="utf-8") as f:
            seen["json"] = json.load(f)
        raise Stop()

    monkeypatch.setattr(gp, "build_gpcopy_command", fake_command)

    job_id = create_job("gpcopy", 1, {
        "mode": "date_filter",
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a"}],
        "table_configs": [{"schema": "s", "table": "a", "date_column": "d",
                           "sql": "SELECT 1"}],
        "targets": {"s.a": "arch.a_copy"},
        "date_from": "2026-09-01", "date_to": "2026-09-02",
        "append": True, "gpcopy_path": "/usr/local/bin/gpcopy",
    })

    gp.run_gpcopy_job(job_id)

    dests = [e["dest"] for e in seen["json"]]
    assert len(dests) == 2 and len(set(dests)) == 2
    assert all(".opsentri_gpcopy_stage.j{}_".format(job_id) in d
               for d in dests)
    merges = json.loads(get_job(job_id)["config_json"])["stage_merges"]
    assert merges[0]["target"] == ["arch", "a_copy"]
    assert merges[0]["truncate"] is False      # окно чистится своим DELETE
