# -*- coding: utf-8 -*-
"""Остановленная задача не оставляет объекты в статусе running."""

from db import sqlite_cursor
from job_manager import (
    close_orphan_items,
    create_job,
    get_job,
    get_job_items,
    mark_item_done,
    mark_item_running,
    mark_job_cancelled,
    mark_job_failed,
)


def _job(names):
    tables = [{"schema": "dwh_stage", "table": n} for n in names]
    return create_job("gpcopy", 1, {"tables": tables, "action": "copy"})


def _by_name(job_id):
    return {i["table_name"]: i for i in get_job_items(job_id)}


def test_cancel_closes_open_items():
    job_id = _job(["s01_t_ord", "s01_t_bal", "s01_t_trndtl"])
    items = _by_name(job_id)

    mark_item_done(items["s01_t_ord"]["id"])
    mark_item_running(items["s01_t_bal"]["id"])
    # s01_t_trndtl остаётся в очереди

    mark_job_cancelled(job_id)

    after = _by_name(job_id)

    assert get_job(job_id)["status"] == "cancelled"
    # уже скопированное так и остаётся done
    assert after["s01_t_ord"]["status"] == "done"
    assert after["s01_t_bal"]["status"] == "cancelled"
    assert after["s01_t_trndtl"]["status"] == "cancelled"
    assert after["s01_t_bal"]["finished_at"]
    assert "остановлена" in after["s01_t_bal"]["error_message"]


def test_failure_closes_open_items_without_touching_done():
    job_id = _job(["a", "b"])
    items = _by_name(job_id)

    mark_item_done(items["a"]["id"])
    mark_item_running(items["b"]["id"])

    mark_job_failed(job_id, "gpcopy rc=1")

    after = _by_name(job_id)

    assert after["a"]["status"] == "done"
    assert after["b"]["status"] == "interrupted"
    # оборванный объект попадает в счётчик неудачных
    assert get_job(job_id)["failed_items"] == 1


def test_orphan_sweep_repairs_old_runs():
    job_id = _job(["old_one", "old_two"])
    items = _by_name(job_id)

    mark_item_running(items["old_one"]["id"])
    mark_item_done(items["old_two"]["id"])

    # так выглядели остановленные запуски до починки: задача закрыта,
    # а строки внутри остались «копируется»
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE jobs SET status = 'cancelled' WHERE id = ?", (job_id,)
        )

    assert close_orphan_items() >= 1

    after = _by_name(job_id)

    assert after["old_one"]["status"] == "cancelled"
    assert after["old_two"]["status"] == "done"

    # повторный проход уже ничего не находит
    assert close_orphan_items() == 0
