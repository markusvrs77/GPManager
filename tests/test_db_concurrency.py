# -*- coding: utf-8 -*-
"""Параллельные писатели не должны ронять приложение «database is locked»."""

import sqlite3
import threading
import time

from db import get_sqlite_connection, sqlite_cursor
from job_manager import create_job


def test_wal_and_busy_timeout_are_on():
    with sqlite_cursor() as cur:
        cur.execute("PRAGMA journal_mode")
        mode = cur.fetchone()[0]

        cur.execute("PRAGMA busy_timeout")
        timeout = cur.fetchone()[0]

    assert str(mode).lower() == "wal"
    assert timeout >= 10000


def test_writer_waits_instead_of_failing():
    """Пока один держит транзакцию, второй ждёт очереди, а не падает."""
    holder = get_sqlite_connection()
    holder.execute("BEGIN IMMEDIATE")
    holder.execute(
        "INSERT INTO jobs (job_type, status) VALUES ('lock_test', 'queued')")

    result = {}
    started = threading.Event()

    def writer():
        started.set()
        try:
            with sqlite_cursor(commit=True) as cur:
                cur.execute(
                    "INSERT INTO jobs (job_type, status) "
                    "VALUES ('lock_test', 'queued')")
            result["ok"] = True
        except sqlite3.OperationalError as e:
            result["error"] = str(e)

    thread = threading.Thread(target=writer)
    thread.start()
    started.wait(timeout=5)

    # держим блокировку заметно дольше, чем занял бы мгновенный отказ;
    # соединение трогаем только из своего потока — sqlite иначе не умеет
    time.sleep(0.4)
    holder.commit()
    holder.close()

    thread.join(timeout=25)

    assert result.get("ok"), result.get("error")

    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM jobs WHERE job_type = 'lock_test'")


def test_many_parallel_writers():
    """Восемь потоков пишут одновременно — все записи доезжают."""
    errors = []

    def worker(n):
        try:
            for _ in range(5):
                create_job("lock_test_bulk", 1,
                           {"marker": "lock_test_bulk", "n": n})
        except Exception as e:                      # noqa: BLE001
            errors.append(str(e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    with sqlite_cursor(commit=True) as cur:
        cur.execute("SELECT COUNT(*) FROM jobs WHERE job_type = ?",
                    ("lock_test_bulk",))
        count = cur.fetchone()[0]
        cur.execute("DELETE FROM job_items WHERE job_id IN "
                    "(SELECT id FROM jobs WHERE job_type = 'lock_test_bulk')")
        cur.execute("DELETE FROM jobs WHERE job_type = 'lock_test_bulk'")

    assert not errors, errors
    assert count == 40
