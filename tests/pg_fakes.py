# -*- coding: utf-8 -*-
"""
Фейковые соединения psycopg2 для тестов PG↔PG без живого PostgreSQL.

Запросы приходят строкой или psycopg2.sql.Composable; render() печатает
Composable без соединения, чтобы тест мог проверить итоговый SQL.
"""

import threading

from psycopg2 import sql


def render(query):
    """Composable → строка без соединения (Identifier в двойных кавычках)."""
    if isinstance(query, str):
        return query

    if isinstance(query, sql.Composed):
        return "".join(render(part) for part in query.seq)

    if isinstance(query, sql.SQL):
        return query.string

    if isinstance(query, sql.Identifier):
        return ".".join('"%s"' % s.replace('"', '""') for s in query.strings)

    if isinstance(query, sql.Literal):
        return repr(query.wrapped)

    if isinstance(query, sql.Placeholder):
        return "%s"

    raise TypeError("unexpected query object: %r" % (query,))


class FakeCursor(object):
    def __init__(self, conn):
        self.conn = conn
        self._rows = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        text = render(query)
        self.conn.executed.append((text, params))
        rows = self.conn.respond(text, params)
        self._rows = list(rows or [])
        self.rowcount = len(self._rows)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def copy_expert(self, query, stream):
        text = render(query)
        self.conn.copies.append(text)

        if self.conn.copy_error is not None:
            raise self.conn.copy_error

        if "TO STDOUT" in text:
            try:
                stream.write(self.conn.copy_out)
            finally:
                self.conn.copy_finished = True
            return

        data = stream.read()
        self.conn.copied_in.append(data)
        self.rowcount = len([ln for ln in data.split(b"\n") if ln])

    def close(self):
        pass


class FakeConn(object):
    """
    responses: [(подстрока SQL, rows или callable(params) -> rows)].
    Первый совпавший по подстроке ответ отдаётся курсору.
    """

    def __init__(self, responses=None, copy_out=b"", copy_error=None):
        self.responses = list(responses or [])
        self.copy_out = copy_out
        self.copy_error = copy_error
        self.executed = []
        self.copies = []
        self.copied_in = []
        self.session = {}
        self.commits = 0
        self.rollbacks = 0
        self.cancelled = 0
        self.closed = False
        self.copy_finished = False
        self.rollback_error = None
        self.cancel_event = threading.Event()

    def respond(self, text, params):
        for needle, rows in self.responses:
            if needle in text:
                return rows(params) if callable(rows) else rows
        return []

    def cursor(self):
        return FakeCursor(self)

    def set_session(self, **kwargs):
        self.session.update(kwargs)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1
        if self.rollback_error is not None:
            raise self.rollback_error

    def cancel(self):
        self.cancelled += 1
        self.cancel_event.set()

    def close(self):
        self.closed = True

    def sql_text(self):
        return "\n".join(text for text, _ in self.executed)
