# -*- coding: utf-8 -*-
"""
Общие примитивы PG↔PG: подключение, хеш строки, поток COPY, стоп.
"""

import threading
import time

import pytest

import modules.pg_sync_common as common
from tests.pg_fakes import FakeConn, render

PG = {"id": 1, "name": "cashprod", "host": "h", "port": 5432,
      "username": "u", "database_name": "cash", "db_type": "postgres"}


@pytest.fixture
def connections(monkeypatch):
    kinds = {1: PG, 2: dict(PG, id=2, name="gp", db_type="greenplum")}
    opened = []

    def fake_open(cfg):
        conn = FakeConn()
        opened.append(conn)
        return conn

    monkeypatch.setattr(common, "get_connection_by_id",
                        lambda cid: kinds.get(int(cid)))
    monkeypatch.setattr(common, "open_psycopg2_connection_by_cfg", fake_open)
    return opened


def test_open_pg_rejects_a_greenplum_connection(connections):
    with pytest.raises(ValueError) as err:
        common.open_pg(2)

    assert "PostgreSQL" in str(err.value)
    assert connections == []


def test_open_pg_rejects_an_unknown_connection(connections):
    with pytest.raises(ValueError):
        common.open_pg(99)


def test_source_session_is_read_only_and_normalized(connections):
    conn = common.open_pg(1, readonly=True)

    assert conn.session.get("readonly") is True
    executed = conn.sql_text()
    settings = {params[0]: params[1] for text, params in conn.executed
                if params and "set_config" in text}

    assert settings == {
        "DateStyle": "ISO, YMD",
        "IntervalStyle": "postgres",
        "TimeZone": "UTC",
        "extra_float_digits": "3",
        "bytea_output": "hex",
    }
    assert "default_transaction_read_only" in executed
    # SET без коммита откатился бы вместе с первой же ошибкой
    assert conn.commits >= 1


def test_dest_session_is_normalized_but_writable(connections):
    conn = common.open_pg(1)

    assert "readonly" not in conn.session
    assert "default_transaction_read_only" not in conn.sql_text()
    assert "TimeZone" in [p[0] for _t, p in conn.executed if p]


def test_row_hash_sorts_columns_by_name():
    text = render(common.row_hash_sql("t", ["b", "a", "C"]))

    assert text == 'md5(ROW("t"."C", "t"."a", "t"."b")::text)'


def test_row_hash_quotes_odd_names():
    text = render(common.row_hash_sql(None, ['we"ird']))

    assert text == 'md5(ROW("we""ird")::text)'


def test_stream_copy_pipes_source_into_dest_with_explicit_columns():
    from psycopg2 import sql

    src = FakeConn(copy_out=b"1\tx\n2\ty\n3\tz\n")
    dst = FakeConn()

    rows = common.stream_copy(
        src, dst,
        sql.SQL("SELECT 1"),
        sql.Identifier("pgcmp_src"),
        ["k0", "h"],
    )

    assert rows == 3
    assert src.copies == ["COPY (SELECT 1) TO STDOUT"]
    assert dst.copies == ['COPY "pgcmp_src" ("k0", "h") FROM STDIN']
    assert dst.copied_in == [b"1\tx\n2\ty\n3\tz\n"]
    # транзакцией управляет вызывающий
    assert dst.commits == 0


def test_stream_copy_raises_the_source_error():
    from psycopg2 import sql

    src = FakeConn(copy_error=RuntimeError("source broke"))
    dst = FakeConn()

    with pytest.raises(RuntimeError):
        common.stream_copy(src, dst, sql.SQL("SELECT 1"),
                           sql.Identifier("t"), ["a"])


def test_stopwatch_cancels_connections_on_stop(monkeypatch):
    flag = {"stop": False}
    monkeypatch.setattr(common, "is_stop_requested", lambda jid: flag["stop"])
    a, b = FakeConn(), FakeConn()

    with common.StopWatch(7, [a, b], interval=0.01) as watch:
        assert watch.stopped is False
        flag["stop"] = True
        deadline = time.time() + 2
        while not watch.stopped and time.time() < deadline:
            time.sleep(0.01)

    assert watch.stopped is True
    assert a.cancelled == 1 and b.cancelled == 1


def test_stopwatch_thread_ends_with_the_block(monkeypatch):
    monkeypatch.setattr(common, "is_stop_requested", lambda jid: False)
    before = threading.active_count()

    with common.StopWatch(7, [], interval=0.01) as watch:
        pass

    time.sleep(0.05)
    assert watch.stopped is False
    assert threading.active_count() <= before


def test_stream_copy_reports_the_dest_error_not_the_broken_pipe():
    """Приёмник упал, источник жив и пишет дальше в закрытую трубу."""
    from psycopg2 import sql

    src = FakeConn(copy_out=b"x" * (4 * 1024 * 1024))
    dst = FakeConn(copy_error=RuntimeError("dest broke"))

    with pytest.raises(RuntimeError) as err:
        common.stream_copy(src, dst, sql.SQL("SELECT 1"),
                           sql.Identifier("t"), ["a"])

    assert "dest broke" in str(err.value)
    # поток pump к возврату завершён
    assert src.copy_finished is True


def test_stopwatch_cancels_queries_started_after_the_first_cancel(monkeypatch):
    """Каждый блокирующий запрос под стопом должен быть отменён сторожем."""
    monkeypatch.setattr(common, "is_stop_requested", lambda jid: True)
    conn = FakeConn()

    with common.StopWatch(7, [conn], interval=0.01) as watch:
        first = conn.cancel_event.wait(2)
        conn.cancel_event.clear()
        # второй запрос стартовал уже после первого cancel()
        second = conn.cancel_event.wait(2)

    assert first and second
    assert watch.stopped is True
