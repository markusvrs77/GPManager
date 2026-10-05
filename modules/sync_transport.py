# -*- coding: utf-8 -*-
"""
Универсальный слой транспортов для «Синхронизации данных».

Идея: GPManager остаётся инструментом для Greenplum, но вкладка
синхронизации умеет переносить данные и между другими СУБД.
Транспорт выбирается по паре типов (источник → назначение):

    greenplum → greenplum   gpcopy (быстрый, сегмент-в-сегмент)
    postgres  ↔ postgres/gp copy_pipe (COPY TO STDOUT → COPY FROM STDIN)
    mysql/oracle            зарезервировано (pgloader / ora2pg / PXF)

copy_pipe стримит данные без временных файлов: reader-поток льёт
COPY-вывод источника в os.pipe, приёмник читает его как COPY FROM STDIN.
"""

import json
import os
import threading

from job_manager import (
    get_job,
    get_job_items,
    is_stop_requested,
    mark_item_done,
    mark_item_failed,
    mark_item_running,
    mark_item_skipped,
    mark_job_cancelled,
    mark_job_done,
    mark_job_failed,
    mark_job_running,
    refresh_job_progress,
    set_item_bytes,
    set_item_size,
)

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id

try:
    from modules.gpcopy import open_psycopg2_connection_by_cfg
except ImportError:
    from gpcopy import open_psycopg2_connection_by_cfg

try:
    from modules.sync_targets import normalize_targets, target_of
except ImportError:
    from sync_targets import normalize_targets, target_of


DB_TYPES = ("greenplum", "postgres", "mysql", "oracle")

DB_TYPE_LABELS = {
    "greenplum": "Greenplum",
    "postgres": "PostgreSQL",
    "mysql": "MySQL",
    "oracle": "Oracle",
}

# семейство postgres-протокола: COPY работает в обе стороны
_PG_FAMILY = {"greenplum", "postgres"}


def normalize_db_type(value):
    v = str(value or "").strip().lower()
    return v if v in DB_TYPES else "greenplum"


def pick_transport(source_type, dest_type):
    """
    Возвращает имя транспорта для пары типов СУБД.
    Бросает ValueError с понятным сообщением, если пара не поддержана.
    """
    s = normalize_db_type(source_type)
    d = normalize_db_type(dest_type)

    if s == "greenplum" and d == "greenplum":
        return "gpcopy"

    if s in _PG_FAMILY and d in _PG_FAMILY:
        return "copy_pipe"

    raise ValueError(
        "Перенос %s → %s пока не поддерживается. Доступно: "
        "Greenplum→Greenplum (gpcopy), PostgreSQL↔PostgreSQL/Greenplum (COPY)."
        % (DB_TYPE_LABELS.get(s, s), DB_TYPE_LABELS.get(d, d))
    )


def qident(name):
    return '"' + str(name).replace('"', '""') + '"'


def fetch_table_sizes(conn, tables):
    """
    {(schema, table): size_bytes} по каталогу источника.

    Для партиционированной таблицы суммируем размер всего дерева
    (pg_partition_tree, PG12/GP7). Недоступную таблицу пропускаем (0).
    """
    sizes = {}
    cur = conn.cursor()

    for schema, table in tables:
        full = qident(schema) + "." + qident(table)

        try:
            cur.execute(
                """
                SELECT COALESCE(SUM(pg_total_relation_size(relid)), 0)
                FROM pg_partition_tree(%s::regclass)
                """,
                (full,),
            )
            sizes[(schema, table)] = int(cur.fetchone()[0] or 0)
        except Exception:
            conn.rollback()
            try:
                cur.execute("SELECT pg_total_relation_size(%s::regclass)", (full,))
                sizes[(schema, table)] = int(cur.fetchone()[0] or 0)
            except Exception:
                conn.rollback()
                sizes[(schema, table)] = 0

    return sizes


class _CountingReader(object):
    """Обёртка над потоком: считает прочитанные байты и зовёт on_bytes."""

    def __init__(self, stream, on_bytes):
        self._stream = stream
        self._on_bytes = on_bytes
        self.total = 0

    def read(self, size=-1):
        chunk = self._stream.read(size)
        if chunk:
            self.total += len(chunk)
            if self._on_bytes:
                self._on_bytes(self.total)
        return chunk

    def readline(self, *args):
        line = self._stream.readline(*args)
        if line:
            self.total += len(line)
            if self._on_bytes:
                self._on_bytes(self.total)
        return line

    def close(self):
        try:
            self._stream.close()
        except Exception:
            pass


# ------------------------------------------------------------------
# DDL: авто-создание таблицы на приёмнике по структуре источника
# ------------------------------------------------------------------

def table_exists(conn, schema, table):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        LIMIT 1
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def fetch_table_columns(conn, schema, table):
    """Колонки таблицы с типами в каноническом виде (format_type)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.attname,
               pg_catalog.format_type(a.atttypid, a.atttypmod) AS coltype
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (schema, table),
    )
    return [{"name": r[0], "type": r[1]} for r in cur.fetchall()]


def build_create_table_sql(schema, table, columns, distributed_randomly=False):
    """Чистый генератор CREATE TABLE — тестируется без БД."""
    if not columns:
        raise ValueError("Нет колонок для создания таблицы %s.%s" % (schema, table))

    cols = ",\n    ".join(
        qident(c["name"]) + " " + c["type"] for c in columns
    )
    sql = "CREATE TABLE %s.%s (\n    %s\n)" % (qident(schema), qident(table), cols)
    if distributed_randomly:
        sql += "\nDISTRIBUTED RANDOMLY"
    return sql


def ensure_dest_table(src_conn, dst_conn, schema, table, dest_is_greenplum,
                      dst_schema=None, dst_table=None):
    """
    Если таблицы нет на приёмнике — создаёт её по структуре источника.
    schema / table — таблица источника; dst_schema / dst_table — цель
    (карта targets), по умолчанию одноимённая. DDL — только на приёмнике.
    Возвращает True, если таблица была создана.
    """
    dst_schema = dst_schema or schema
    dst_table = dst_table or table

    if table_exists(dst_conn, dst_schema, dst_table):
        return False

    columns = fetch_table_columns(src_conn, schema, table)
    cur = dst_conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS %s" % qident(dst_schema))
        cur.execute(build_create_table_sql(
            dst_schema, dst_table, columns,
            distributed_randomly=dest_is_greenplum
        ))
        dst_conn.commit()
    except Exception:
        try:
            dst_conn.rollback()
        except Exception:
            pass
        raise
    return True


def mapped_copy_columns(src_conn, dst_conn, src_schema, src_table,
                        dst_schema, dst_table):
    """
    Колонки для COPY в таблицу с другим именем: колонки источника в его
    порядке; каждая должна быть в цели (порядок колонок цели может быть
    другим). Колонок источника нет в цели — ValueError, данные не трогаются.
    """
    src_cols = [c["name"] for c in
                fetch_table_columns(src_conn, src_schema, src_table)]
    dst_cols = {c["name"] for c in
                fetch_table_columns(dst_conn, dst_schema, dst_table)}

    if not src_cols:
        raise ValueError("Таблицы %s.%s нет в источнике"
                         % (src_schema, src_table))
    if not dst_cols:
        raise ValueError("Таблицы %s.%s нет в приёмнике"
                         % (dst_schema, dst_table))

    absent = [c for c in src_cols if c not in dst_cols]
    if absent:
        raise ValueError("В таблице %s.%s приёмника нет колонок источника: %s"
                         % (dst_schema, dst_table, ", ".join(absent)))
    return src_cols


def build_copy_pipe_sql(src_schema, src_table, dst_schema, dst_table,
                        columns=None):
    """
    (COPY ... TO STDOUT источника, COPY ... FROM STDIN приёмника).
    columns — явный список колонок обеих сторон (таблица с другим именем,
    порядок колонок цели может отличаться); None — как раньше, SELECT *.
    """
    src_full = qident(src_schema) + "." + qident(src_table)
    dst_full = qident(dst_schema) + "." + qident(dst_table)

    if columns is None:
        return ("COPY (SELECT * FROM %s) TO STDOUT" % src_full,
                "COPY %s FROM STDIN" % dst_full)

    if not columns:
        raise ValueError("Нет колонок для COPY %s.%s" % (src_schema, src_table))

    cols = ", ".join(qident(c) for c in columns)
    return ("COPY (SELECT %s FROM %s) TO STDOUT" % (cols, src_full),
            "COPY %s (%s) FROM STDIN" % (dst_full, cols))


def copy_table_pipe(src_conn, dst_conn, src_schema, src_table,
                    dst_schema, dst_table, truncate=False, on_bytes=None,
                    columns=None):
    """
    Стримит таблицу источника в приёмник: COPY TO STDOUT → COPY FROM STDIN
    через os.pipe + reader-поток. Возвращает число перенесённых строк.
    on_bytes(total) вызывается по мере перекачки — для live-прогресса.
    columns — явный список колонок (см. build_copy_pipe_sql).
    Коммитит приёмник; при ошибке откатывает и пробрасывает исключение.
    """
    dst_full = qident(dst_schema) + "." + qident(dst_table)
    copy_out, copy_in = build_copy_pipe_sql(src_schema, src_table,
                                            dst_schema, dst_table, columns)

    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()

    try:
        if truncate:
            dst_cur.execute("TRUNCATE TABLE %s" % dst_full)

        r_fd, w_fd = os.pipe()
        reader = os.fdopen(r_fd, "rb")
        writer = os.fdopen(w_fd, "wb")

        src_error = []

        def pump():
            try:
                src_cur.copy_expert(copy_out, writer)
            except Exception as e:
                src_error.append(e)
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

        t = threading.Thread(target=pump, daemon=True)
        t.start()

        counted = _CountingReader(reader, on_bytes) if on_bytes else reader

        try:
            dst_cur.copy_expert(copy_in, counted)
        finally:
            try:
                reader.close()
            except Exception:
                pass
            t.join(timeout=60)

        if src_error:
            raise src_error[0]

        rows = dst_cur.rowcount if dst_cur.rowcount is not None else -1
        dst_conn.commit()
        return rows
    except Exception:
        try:
            dst_conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            src_conn.rollback()  # снять снапшот-транзакцию источника
        except Exception:
            pass


def job_config(job):
    """
    Конфиг задачи словарём.

    get_job отдаёт его строкой config_json, а раннер читал job["config"],
    которого там никогда не было. Конфиг выходил пустым, и каждый перенос
    через COPY падал на первой же строке с невнятным 'source_connection_id'.
    """
    raw = job.get("config")

    if isinstance(raw, dict):
        return raw

    try:
        return json.loads(job.get("config_json") or "{}")
    except ValueError:
        return {}


def validated_targets(config, items):
    """
    Карта targets задачи, заново проверенная по её строкам (normalize_
    targets): ключи — среди таблиц задачи, у каждой своя цель, формат имён.
    Конфиг мог прийти не из маршрута (расписание, перезапуск), поэтому
    раннер ему не доверяет. Ошибка — ValueError с текстом для человека.
    """
    raw = config.get("targets")
    selected = ["%s.%s" % (i["schema_name"], i["table_name"])
                for i in items or []]
    try:
        return normalize_targets(raw, selected)
    except ValueError as e:
        raise ValueError("Карта «куда грузить» задачи неверна: %s" % e)


def run_copy_pipe_job(job_id):
    """
    Раннер job_type='copy_pipe': полный перенос выбранных таблиц
    между postgres-семейством (PG↔PG, PG↔GP) без gpcopy-бинаря.
    """
    job = get_job(job_id)
    if not job:
        return

    config = job_config(job)
    mark_job_running(job_id)

    src_conn = None
    dst_conn = None

    try:
        source_id = config.get("source_connection_id")
        dest_id = config.get("dest_connection_id")

        if not source_id or not dest_id:
            raise Exception("В задаче не указан источник или назначение")

        src_cfg = get_connection_by_id(int(source_id))
        dst_cfg = get_connection_by_id(int(dest_id))
        if not src_cfg or not dst_cfg:
            raise Exception("Источник или назначение не найдены")

        truncate = bool(config.get("truncate"))
        append = bool(config.get("append"))
        if not truncate and not append:
            truncate = True  # безопасный дефолт полного переноса

        dest_is_gp = normalize_db_type(dst_cfg.get("db_type")) == "greenplum"
        # карта проверяется заново: задачу может создать и расписание из
        # сохранённого конфига, минуя маршрут. Ошибка — задача failed до
        # того, как тронута хоть одна таблица
        targets = validated_targets(config, get_job_items(job_id))

        src_conn = open_psycopg2_connection_by_cfg(src_cfg)
        dst_conn = open_psycopg2_connection_by_cfg(dst_cfg)

        items = get_job_items(job_id)
        failed = 0

        # веса таблиц — для взвешенного общего прогресса по объёму данных
        try:
            sizes = fetch_table_sizes(
                src_conn,
                [(it["schema_name"], it["table_name"]) for it in items],
            )
            for it in items:
                set_item_size(
                    it["id"],
                    sizes.get((it["schema_name"], it["table_name"]), 0),
                )
        except Exception:
            sizes = {}

        refresh_job_progress(job_id)

        for item in items:
            # переподхват после рестарта: готовые строки не переделываем
            if item.get("status") in ("done", "failed", "skipped"):
                continue

            if is_stop_requested(job_id):
                # статусы в items сняты до цикла: таблицы, готовые в этом
                # запуске, там всё ещё queued — перечитываем, иначе они
                # записались бы в пропущенные. И строки создаются queued, а
                # не pending: прежняя проверка не находила ни одной
                for rest in get_job_items(job_id):
                    if rest["status"] in ("queued", "pending"):
                        mark_item_skipped(rest["id"], "остановлено пользователем")
                refresh_job_progress(job_id)
                mark_job_cancelled(job_id)
                return

            mark_item_running(item["id"])
            refresh_job_progress(job_id)

            # live-прогресс внутри таблицы: обновляем не чаще раза в ~2 МБ,
            # чтобы не заваливать SQLite апдейтами на больших таблицах
            state = {"last": 0}

            def on_bytes(total, _item_id=item["id"]):
                if total - state["last"] >= 2 * 1024 * 1024:
                    state["last"] = total
                    set_item_bytes(_item_id, total)
                    refresh_job_progress(job_id)

            schema, table = item["schema_name"], item["table_name"]
            dst_schema, dst_table = target_of(targets, schema, table)

            try:
                if (dst_schema, dst_table) == (schema, table):
                    # без карты — прежние команды байт-в-байт
                    ensure_dest_table(
                        src_conn, dst_conn, schema, table,
                        dest_is_greenplum=dest_is_gp,
                    )
                    copy_table_pipe(
                        src_conn, dst_conn, schema, table, schema, table,
                        truncate=truncate,
                        on_bytes=on_bytes,
                    )
                else:
                    # другая таблица приёмника: нет — создаём по источнику,
                    # COPY с явным списком колонок
                    ensure_dest_table(
                        src_conn, dst_conn, schema, table,
                        dest_is_greenplum=dest_is_gp,
                        dst_schema=dst_schema, dst_table=dst_table,
                    )
                    columns = mapped_copy_columns(
                        src_conn, dst_conn, schema, table,
                        dst_schema, dst_table,
                    )
                    copy_table_pipe(
                        src_conn, dst_conn, schema, table,
                        dst_schema, dst_table,
                        truncate=truncate,
                        on_bytes=on_bytes,
                        columns=columns,
                    )
                mark_item_done(item["id"])
            except Exception as e:
                failed += 1
                mark_item_failed(item["id"], str(e)[:500])

            refresh_job_progress(job_id)

        if failed:
            mark_job_failed(
                job_id, "%s таблиц(ы) не перенесены (copy_pipe)" % failed
            )
        else:
            mark_job_done(job_id)

    except Exception as e:
        mark_job_failed(job_id, str(e)[:500])
    finally:
        for c in (src_conn, dst_conn):
            try:
                if c:
                    c.close()
            except Exception:
                pass
