# -*- coding: utf-8 -*-
"""
Общие примитивы PG↔PG для режима «Сравнение и разница» (Postgres Toolkit).

    open_pg       — подключение только к PostgreSQL, сессия нормализована;
                    источник открывается read-only
    table_columns — имена колонок таблицы в порядке attnum
    row_hash_sql  — md5(ROW(<колонки по имени>)::text): хеш строки,
                    одинаковый на обеих сторонах при одинаковом наборе колонок
    stream_copy   — COPY (SELECT ...) TO STDOUT источника → COPY ... FROM STDIN
                    приёмника через os.pipe; транзакцией управляет вызывающий
    StopWatch     — сторожевой поток: по запросу стопа зовёт conn.cancel()

Идентификаторы — только через psycopg2.sql.Identifier.
"""

import os
import threading

from psycopg2 import sql

from job_manager import is_stop_requested

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id

try:
    from modules.gpcopy import open_psycopg2_connection_by_cfg
except ImportError:
    from gpcopy import open_psycopg2_connection_by_cfg

try:
    from modules.sync_transport import fetch_table_columns, normalize_db_type
except ImportError:
    from sync_transport import fetch_table_columns, normalize_db_type


# Одинаковые форматы дат, интервалов, float и bytea на обоих серверах:
# иначе ROW(...)::text одной и той же строки дал бы разный хеш.
SESSION_SETTINGS = (
    ("DateStyle", "ISO, YMD"),
    ("IntervalStyle", "postgres"),
    ("TimeZone", "UTC"),
    ("extra_float_digits", "3"),
    ("bytea_output", "hex"),
)


def pg_connection_cfg(connection_id):
    """
    Конфиг подключения, если это PostgreSQL. Иначе ValueError с текстом
    для пользователя: сравнение и загрузка разницы — только PG↔PG.
    """
    try:
        cfg = get_connection_by_id(int(connection_id))
    except (TypeError, ValueError):
        cfg = None

    if not cfg:
        raise ValueError("Подключение %s не найдено" % connection_id)

    if normalize_db_type(cfg.get("db_type")) != "postgres":
        raise ValueError(
            "Подключение «%s» — не PostgreSQL. Сравнение баз работает "
            "только между двумя подключениями PostgreSQL."
            % (cfg.get("name") or connection_id)
        )

    return cfg


def normalize_session(conn, readonly=False):
    """SET форматов сессии (и read-only) с коммитом: откат их не снимет."""
    cur = conn.cursor()

    for name, value in SESSION_SETTINGS:
        cur.execute("SELECT set_config(%s, %s, false)", (name, value))

    if readonly:
        cur.execute("SET default_transaction_read_only = on")

    conn.commit()


def open_pg(connection_id, readonly=False):
    """
    Соединение psycopg2 с PostgreSQL-подключением.
    readonly=True — для источника: каждая транзакция только на чтение.
    """
    cfg = pg_connection_cfg(connection_id)
    conn = open_psycopg2_connection_by_cfg(cfg)

    try:
        if readonly:
            conn.set_session(readonly=True)
        normalize_session(conn, readonly=readonly)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise

    return conn


def table_columns(conn, schema, table):
    """Имена колонок таблицы в порядке attnum (без удалённых)."""
    return [c["name"] for c in fetch_table_columns(conn, schema, table)]


def table_column_types(conn, schema, table):
    """{колонка: format_type} — для сверки структуры по именам и типам."""
    return {c["name"]: c["type"]
            for c in fetch_table_columns(conn, schema, table)}


def _column_ref(alias, column):
    if alias:
        return sql.Identifier(alias, column)
    return sql.Identifier(column)


def row_hash_sql(alias, columns):
    """md5(ROW(<колонки, отсортированные по имени>)::text) как Composable."""
    if not columns:
        raise ValueError("Нет колонок для хеша строки")

    refs = [_column_ref(alias, c) for c in sorted(columns)]
    return sql.SQL("md5(ROW({})::text)").format(sql.SQL(", ").join(refs))


def stream_copy(src_conn, dst_conn, select_sql, dst_table, dst_columns):
    """
    COPY (select_sql) TO STDOUT источника → COPY dst_table (dst_columns)
    FROM STDIN приёмника. Возвращает число строк, принятых приёмником.

    Не коммитит и не откатывает: транзакцией приёмника управляет
    вызывающий (полная загрузка — TRUNCATE и COPY в одной транзакции).
    dst_table — Composable (sql.Identifier), select_sql — Composable.
    """
    copy_out = sql.SQL("COPY ({}) TO STDOUT").format(select_sql)
    copy_in = sql.SQL("COPY {} ({}) FROM STDIN").format(
        dst_table,
        sql.SQL(", ").join(sql.Identifier(c) for c in dst_columns),
    )

    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()

    r_fd, w_fd = os.pipe()
    reader = os.fdopen(r_fd, "rb")
    writer = os.fdopen(w_fd, "wb")
    # ошибки в порядке появления: ("src"|"dst", exc)
    errors = []
    lock = threading.Lock()

    def pump():
        try:
            src_cur.copy_expert(copy_out, writer)
        except Exception as e:
            with lock:
                errors.append(("src", e))
        finally:
            try:
                writer.close()
            except Exception:
                pass

    t = threading.Thread(target=pump, daemon=True)
    t.start()

    try:
        dst_cur.copy_expert(copy_in, reader)
    except Exception as e:
        with lock:
            errors.append(("dst", e))
        # источник дальше не нужен: снимаем его запрос, чтобы pump вышел
        _cancel_quietly(src_conn)
    finally:
        try:
            reader.close()
        except Exception:
            pass
        _finish_pump(t, src_conn)

    src_error = next((e for side, e in errors if side == "src"), None)
    dst_error = next((e for side, e in errors if side == "dst"), None)

    if dst_error is not None:
        # источник первичен, только если упал раньше и не на обрыве трубы
        if (src_error is not None and errors[0][0] == "src"
                and not isinstance(src_error, OSError)):
            raise src_error
        raise dst_error

    if src_error is not None:
        raise src_error

    return dst_cur.rowcount if dst_cur.rowcount is not None else -1


PUMP_JOIN_TIMEOUT = 60


def _cancel_quietly(conn):
    try:
        conn.cancel()
    except Exception:
        pass


def _finish_pump(thread, src_conn):
    """
    Дожидается потока pump. Если он завис, отменяет запрос источника,
    а если и это не помогло, закрывает соединение — им больше нельзя
    пользоваться.
    """
    thread.join(timeout=PUMP_JOIN_TIMEOUT)

    if thread.is_alive():
        _cancel_quietly(src_conn)
        thread.join(timeout=10)

    if thread.is_alive():
        try:
            src_conn.close()
        except Exception:
            pass
        raise RuntimeError("Поток COPY источника не завершился; "
                           "соединение источника закрыто")


class StopWatch(object):
    """
    Сторожевой поток на время блока with: раз в interval секунд проверяет
    is_stop_requested(job_id). Когда стоп запрошен, на КАЖДОМ тике зовёт
    cancel() у всех соединений из conns (список можно пополнять на ходу):
    так отменяются и запросы, начатые после первого cancel(). .stopped —
    был ли стоп; выставляется один раз под Lock.
    """

    def __init__(self, job_id, conns, interval=1.0):
        self.job_id = job_id
        self.conns = conns
        self.interval = interval
        self.stopped = False
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._thread = None

    def check(self):
        """Проверка стопа без ожидания (между таблицами)."""
        if self.stopped:
            return True

        if is_stop_requested(self.job_id):
            with self._lock:
                self.stopped = True
            self._cancel_all()

        return self.stopped

    def _cancel_all(self):
        for conn in list(self.conns or []):
            try:
                if conn is not None:
                    conn.cancel()
            except Exception:
                pass

    def _watch(self):
        while not self._done.wait(self.interval):
            try:
                if self.stopped:
                    self._cancel_all()
                else:
                    self.check()
            except Exception:
                # временная ошибка SQLite не должна убить сторожа
                continue

    def __enter__(self):
        self._thread = threading.Thread(target=self._watch, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._done.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        return False
