"""«Продолжить с места остановки» для gpcopy: только то, что не доехало."""
import pytest


def _job(config, statuses):
    """gpcopy-задача со строками в заданных статусах."""
    from job_manager import create_job, get_job_items
    from db import sqlite_cursor

    tables = [{"schema": s, "table": t} for s, t, _ in statuses]
    job_id = create_job("gpcopy", 1, dict(config, tables=tables))

    with sqlite_cursor(commit=True) as cur:
        for item, (_, _, st) in zip(get_job_items(job_id), statuses):
            cur.execute("UPDATE job_items SET status = ? WHERE id = ?",
                        (st, item["id"]))
        cur.execute("UPDATE jobs SET status = 'cancelled' WHERE id = ?",
                    (job_id,))

    return job_id


BASE = {"source_connection_id": 1, "dest_connection_id": 2, "truncate": True}


def test_continue_takes_only_what_is_not_done():
    import modules.gpcopy as g

    items = [{"schema_name": "s", "table_name": "a", "status": "done"},
             {"schema_name": "s", "table_name": "b", "status": "cancelled"},
             {"schema_name": "s", "table_name": "c", "status": "queued"}]

    cont, left = g.build_continue_config(
        dict(BASE, failed_leaves=[["s", "b"]], smart_parent=7), items)

    assert left == [("s", "b"), ("s", "c")]
    assert cont["tables"] == [{"schema": "s", "table": "b"},
                              {"schema": "s", "table": "c"}]
    assert cont["truncate"] is True
    assert "failed_leaves" not in cont and "smart_parent" not in cont


def test_partly_copied_table_continues_with_missing_partitions_only():
    import modules.gpcopy as g

    items = [{"schema_name": "s", "table_name": "big", "status": "cancelled"}]
    finished = {("s", "big_1_prt_1"), ("s", "big_1_prt_2")}

    def leaves(schema, table):
        return [("s", "big_1_prt_1"), ("s", "big_1_prt_2"), ("s", "big_1_prt_3")]

    _, left = g.build_continue_config(dict(BASE), items, finished, leaves)

    assert left == [("s", "big_1_prt_3")]

    # «Пересоздать»: таблица уже пересоздана (часть партиций доехала) —
    # доливаем остаток с очисткой, без повторного DROP
    cont, left = g.build_continue_config(
        dict(BASE, drop=True, truncate=False), items, finished, leaves)
    assert left == [("s", "big_1_prt_3")]
    assert cont["drop"] is False and cont["truncate"] is True


def test_gp7_partition_names_are_recognised_by_catalog():
    """GP7: партиции не <табл>_1_prt_…, корень не должен уходить целиком."""
    import modules.gpcopy as g

    items = [{"schema_name": "dwh_dm", "table_name": "dm_stock_lot",
              "status": "cancelled"}]
    finished = {("dwh_dm", "dm_stock_lot_prt_20230102")}

    def leaves(schema, table):
        return [("dwh_dm", "dm_stock_lot_prt_20230102"),
                ("dwh_dm", "dm_stock_lot_prt_20230103")]

    _, left = g.build_continue_config(dict(BASE), items, finished, leaves)

    assert left == [("dwh_dm", "dm_stock_lot_prt_20230103")]


def test_drop_with_untouched_table_still_goes_whole():
    """Не начатой таблице нужен DROP, а флаг один на запуск gpcopy."""
    import modules.gpcopy as g

    items = [{"schema_name": "s", "table_name": "big", "status": "cancelled"},
             {"schema_name": "s", "table_name": "new", "status": "queued"}]
    finished = {("s", "big_1_prt_1")}

    def leaves(schema, table):
        if table == "big":
            return [("s", "big_1_prt_1"), ("s", "big_1_prt_2")]
        return [("s", table)]

    cont, left = g.build_continue_config(
        dict(BASE, drop=True, truncate=False), items, finished, leaves)

    assert left == [("s", "big"), ("s", "new")]
    assert cont["drop"] is True


def test_continue_refuses_when_nothing_left_or_other_mode():
    import modules.gpcopy as g

    done = [{"schema_name": "s", "table_name": "a", "status": "done"}]

    with pytest.raises(ValueError):
        g.build_continue_config(dict(BASE), done)

    with pytest.raises(ValueError):
        g.build_continue_config(dict(BASE, mode="date_filter"),
                                [{"schema_name": "s", "table_name": "a",
                                  "status": "cancelled"}])


def test_continue_route_starts_job_without_copied_tables(client, monkeypatch):
    """Остановленная на 1 из 3 задача продолжает 2 оставшиеся, не все 3."""
    import app as app_module
    import modules.gpcopy as g
    from job_manager import get_job, get_job_items
    import json

    started = []
    monkeypatch.setattr(app_module, "run_gpcopy_job", started.append)
    monkeypatch.setattr(g, "read_job_log", lambda job_id: (
        'Finished copying table "db"."s"."c"\n'))

    job_id = _job(BASE, [("s", "a", "done"), ("s", "b", "cancelled"),
                         ("s", "c", "cancelled")])

    r = client.post("/api/gpcopy/continue", json={"job_id": job_id})
    body = r.get_json()

    assert r.status_code == 200, body
    names = [i["table_name"] for i in get_job_items(body["job_id"])]
    # a — done, c — gpcopy успел скопировать (по логу)
    assert names == ["b"]
    assert body["total_items"] == 1

    cfg = json.loads(get_job(job_id)["config_json"])
    assert cfg["continued_by"] == body["job_id"]

    # второе нажатие, пока продолжение в очереди, — не плодит копию
    r2 = client.post("/api/gpcopy/continue", json={"job_id": job_id})
    assert r2.status_code == 409
    assert "уже идёт" in r2.get_json()["message"]


def test_partition_continue_uses_saved_plan_minus_copied():
    import modules.gpcopy as g

    items = [{"schema_name": "s", "table_name": "a", "status": "done"},
             {"schema_name": "s", "table_name": "b", "status": "cancelled"}]
    config = dict(BASE, tables=[{"schema": "s", "table": "a"},
                                {"schema": "s", "table": "b"}],
                  recompute=True, count_mode="stats",
                  planned_leaves=[["s", "a_1_prt_1"], ["s", "b_1_prt_1"],
                                  ["s", "b_1_prt_2"]])

    cont, roots = g.build_partition_continue_config(
        config, items, {("s", "b_1_prt_1")})

    assert roots == [("s", "b")]
    assert cont["tables"] == [{"schema": "s", "table": "b"}]
    assert cont["partitions"] == [{"schema": "s", "table": "b_1_prt_2"}]
    assert cont["recompute"] is False
    assert not cont.get("analyze")
    # для карточки: из плана в 3 партиции 2 уже скопированы (a — готова
    # целиком, b_1_prt_1 — Finished в логе)
    assert cont["continue_copied"] == 2

    # продолжение продолжения: счётчик копится по цепочке
    cont2, _ = g.build_partition_continue_config(
        dict(cont, planned_leaves=[["s", "b_1_prt_2"], ["s", "b_1_prt_3"]]),
        items, {("s", "b_1_prt_2")})
    assert cont2["partitions"] == [{"schema": "s", "table": "b_1_prt_3"}]
    assert cont2["continue_copied"] == 3


def test_partition_continue_recompute_drops_stale_copied_counter():
    import modules.gpcopy as g

    items = [{"schema_name": "s", "table_name": "b", "status": "failed"}]
    cont, _ = g.build_partition_continue_config(
        dict(BASE, continue_copied=5), items)

    assert cont["recompute"] is True
    assert "continue_copied" not in cont


def test_partition_continue_without_plan_recomputes_left_tables_only():
    import modules.gpcopy as g

    items = [{"schema_name": "s", "table_name": "a", "status": "done"},
             {"schema_name": "s", "table_name": "b", "status": "running"}]

    cont, roots = g.build_partition_continue_config(
        dict(BASE, recompute=False, partitions=[{"schema": "s", "table": "x"}]),
        items)

    assert roots == [("s", "b")]
    assert cont["recompute"] is True and "partitions" not in cont


def test_partition_job_does_not_analyze_unless_asked(monkeypatch):
    """ANALYZE каждой партиции занимал часы — теперь только по галочке."""
    import modules.gpcopy_partition as gp
    from job_manager import create_job, get_job
    import json

    seen = {}

    def fake_build(**kw):
        seen["analyze"] = kw.get("analyze")
        raise RuntimeError("stop here")

    monkeypatch.setattr(gp, "get_connection_by_id", lambda cid: {"id": cid})
    monkeypatch.setattr(gp, "skip_covered_items", lambda items, cid: (items, set()))
    monkeypatch.setattr(gp, "make_include_table_file", lambda items, db: None)
    monkeypatch.setattr(gp, "get_conn_dbname", lambda cfg: "db")
    monkeypatch.setattr(gp, "build_gpcopy_command", fake_build)

    for flag, expected in ((None, False), (True, True)):
        cfg = dict(BASE, tables=[{"schema": "s", "table": "t"}],
                   partitions=[{"schema": "s", "table": "t_1_prt_1"}])
        if flag is not None:
            cfg["analyze"] = flag
        job_id = create_job("gpcopy_partition_diff", 1, cfg)
        gp.run_gpcopy_partition_diff_job(job_id)

        assert seen["analyze"] is expected
        saved = json.loads(get_job(job_id)["config_json"])
        assert saved["planned_leaves"] == [["s", "t_1_prt_1"]]


def test_continue_route_runs_partition_job_with_partition_runner(client, monkeypatch):
    import app as app_module
    import modules.gpcopy as g
    from job_manager import create_job, get_job
    from db import sqlite_cursor

    ran = []
    monkeypatch.setattr(app_module, "run_gpcopy_partition_diff_job",
                        lambda jid: ran.append(("part", jid)))
    monkeypatch.setattr(app_module, "run_gpcopy_job",
                        lambda jid: ran.append(("full", jid)))
    monkeypatch.setattr(g, "read_job_log", lambda job_id: "")

    job_id = create_job("gpcopy_partition_diff", 1, dict(
        BASE, tables=[{"schema": "s", "table": "b"}], recompute=True))
    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE jobs SET status = 'cancelled' WHERE id = ?", (job_id,))

    r = client.post("/api/gpcopy/continue", json={"job_id": job_id})

    assert r.status_code == 200, r.get_json()
    new_id = r.get_json()["job_id"]
    assert get_job(new_id)["job_type"] == "gpcopy_partition_diff"
    import time
    for _ in range(50):
        if ran:
            break
        time.sleep(0.02)
    assert ran == [("part", new_id)]


def test_continue_route_rejects_running_job(client):
    from job_manager import create_job

    job_id = create_job("gpcopy", 1, dict(BASE, tables=[{"schema": "s", "table": "a"}]))

    r = client.post("/api/gpcopy/continue", json={"job_id": job_id})
    assert r.status_code == 409
