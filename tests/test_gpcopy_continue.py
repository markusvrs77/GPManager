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

    # drop пересоздал бы каждую партицию — такую таблицу льём целиком
    _, left = g.build_continue_config(dict(BASE, drop=True, truncate=False),
                                      items, finished, leaves)
    assert left == [("s", "big")]


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


def test_continue_route_rejects_running_job(client):
    from job_manager import create_job

    job_id = create_job("gpcopy", 1, dict(BASE, tables=[{"schema": "s", "table": "a"}]))

    r = client.post("/api/gpcopy/continue", json={"job_id": job_id})
    assert r.status_code == 409
