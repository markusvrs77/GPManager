import os

import pytest

from modules.gpcopy_increment import build_increment_items


TABLES = [{"schema": "dwh", "table": "orders", "watermark_column": "id"}]


def test_no_watermark_copies_everything():
    items = build_increment_items(TABLES, {}, "prod", "test")
    assert len(items) == 1
    assert items[0]["source"] == "prod.dwh.orders"
    assert items[0]["dest"] == "test.dwh.orders"
    assert items[0]["sql"] == 'SELECT * FROM "dwh"."orders"'


def test_watermark_builds_where_greater_than():
    wm = {("dwh", "orders"): 1000}
    items = build_increment_items(TABLES, wm, "prod", "test")
    assert items[0]["sql"] == 'SELECT * FROM "dwh"."orders" WHERE "id" > 1000'


def test_string_watermark_is_escaped():
    wm = {("dwh", "orders"): "2026-07-20 10:00:00"}
    items = build_increment_items(TABLES, wm, "prod", "test")
    assert items[0]["sql"] == (
        'SELECT * FROM "dwh"."orders" WHERE "id" > \'2026-07-20 10:00:00\''
    )


def test_sql_injection_in_watermark_is_neutralised():
    wm = {("dwh", "orders"): "x'; DROP TABLE users;--"}
    items = build_increment_items(TABLES, wm, "prod", "test")
    assert "''" in items[0]["sql"]  # single quote doubled
    assert items[0]["sql"].count("'") % 2 == 0


def test_missing_watermark_column_raises():
    bad = [{"schema": "dwh", "table": "orders"}]
    with pytest.raises(ValueError):
        build_increment_items(bad, {("dwh", "orders"): 5}, "prod", "test")


def test_empty_tables_raises():
    with pytest.raises(ValueError):
        build_increment_items([], {}, "prod", "test")


class _Proc(object):
    pid = 4242
    returncode = 1


def _runner_env(monkeypatch, stop=False):
    import modules.gpcopy_increment as inc

    items = [{"id": 1, "schema_name": "s", "table_name": "a"},
             {"id": 2, "schema_name": "s", "table_name": "b"}]
    seen = {"blanket_failed": [], "cancelled": False}

    monkeypatch.setattr(inc, "get_job", lambda jid: {
        "id": jid, "config_json": '{"source_connection_id": 1, '
        '"dest_connection_id": 2, "tables": []}'})
    monkeypatch.setattr(inc, "get_job_items", lambda jid: items)
    monkeypatch.setattr(inc, "get_connection_by_id", lambda cid: {"id": cid})
    monkeypatch.setattr(inc, "clear_stop_flag", lambda jid: None)
    monkeypatch.setattr(inc, "mark_job_running", lambda jid: None)
    monkeypatch.setattr(inc, "refresh_job_progress", lambda jid: None)
    monkeypatch.setattr(inc, "job_targets", lambda cfg, pairs: {})
    monkeypatch.setattr(inc, "build_increment_include_json_file",
                        lambda cfg: None)
    monkeypatch.setattr(inc, "build_gpcopy_command", lambda **kw: ["gpcopy"])
    monkeypatch.setattr(inc, "write_dest_mapping_file", lambda cfg: None)
    monkeypatch.setattr(inc, "pick_rpc_port", lambda: 1)
    monkeypatch.setattr(inc, "get_conn_host", lambda cfg: "h")
    monkeypatch.setattr(inc, "get_conn_port", lambda cfg: 5432)
    monkeypatch.setattr(inc, "get_conn_user", lambda cfg: "u")
    monkeypatch.setattr(inc, "_job_log_path", lambda jid: os.devnull)
    monkeypatch.setattr(inc.subprocess, "Popen", lambda cmd, **kw: _Proc())
    monkeypatch.setattr(inc, "set_job_runtime",
                        lambda jid, pid=None, log_file=None:
                        seen.update(pid=pid))
    monkeypatch.setattr(inc, "_watch_gpcopy_log",
                        lambda jid, path, keys, process=None: "LOG")
    monkeypatch.setattr(inc, "is_stop_requested", lambda jid: stop
                        and seen.get("pid") is not None)
    monkeypatch.setattr(inc, "mark_job_cancelled",
                        lambda jid: seen.update(cancelled=True))
    monkeypatch.setattr(inc, "safe_mark_item_failed",
                        lambda iid, **kw: seen["blanket_failed"].append(iid))
    monkeypatch.setattr(inc, "safe_mark_job_failed",
                        lambda jid, msg: seen.update(job_failed=msg))
    monkeypatch.setattr(inc, "finalize_gpcopy_job",
                        lambda *a: seen.update(final=a))
    return inc, items, seen


def test_runner_judges_each_table_by_the_log(monkeypatch):
    inc, items, seen = _runner_env(monkeypatch)

    inc.run_gpcopy_increment_job(7)

    # итог по логу gpcopy, а не «одна упала — красим все»
    assert "job_failed" not in seen
    assert seen["blanket_failed"] == []
    job_id, got_items, rc, log_text = seen["final"][:4]
    assert (job_id, got_items, rc, log_text) == (7, items, 1, "LOG")
    assert seen["pid"] == 4242      # pid + лог — переподхват после рестарта


def test_runner_stop_cancels_without_finalizing(monkeypatch):
    inc, _items, seen = _runner_env(monkeypatch, stop=True)

    inc.run_gpcopy_increment_job(7)

    assert seen["cancelled"] and "final" not in seen
