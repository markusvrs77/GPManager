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
)

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id

try:
    from modules.gpcopy import open_psycopg2_connection_by_cfg
except ImportError:
    from gpcopy import open_psycopg2_connection_by_cfg


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


def copy_table_pipe(src_conn, dst_conn, src_schema, src_table,
                    dst_schema, dst_table, truncate=False):
    """
    Стримит таблицу источника в приёмник: COPY TO STDOUT → COPY FROM STDIN
    через os.pipe + reader-поток. Возвращает число перенесённых строк.
    Коммитит приёмник; при ошибке откатывает и пробрасывает исключение.
    """
    src_full = qident(src_schema) + "." + qident(src_table)
    dst_full = qident(dst_schema) + "." + qident(dst_table)

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
                src_cur.copy_expert(
                    "COPY (SELECT * FROM %s) TO STDOUT" % src_full, writer
                )
            except Exception as e:
                src_error.append(e)
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

        t = threading.Thread(target=pump, daemon=True)
        t.start()

        try:
            dst_cur.copy_expert("COPY %s FROM STDIN" % dst_full, reader)
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


def run_copy_pipe_job(job_id):
    """
    Раннер job_type='copy_pipe': полный перенос выбранных таблиц
    между postgres-семейством (PG↔PG, PG↔GP) без gpcopy-бинаря.
    """
    job = get_job(job_id)
    if not job:
        return

    config = job.get("config") or {}
    mark_job_running(job_id)

    src_conn = None
    dst_conn = None

    try:
        src_cfg = get_connection_by_id(int(config["source_connection_id"]))
        dst_cfg = get_connection_by_id(int(config["dest_connection_id"]))
        if not src_cfg or not dst_cfg:
            raise Exception("Источник или назначение не найдены")

        truncate = bool(config.get("truncate"))
        append = bool(config.get("append"))
        if not truncate and not append:
            truncate = True  # безопасный дефолт полного переноса

        src_conn = open_psycopg2_connection_by_cfg(src_cfg)
        dst_conn = open_psycopg2_connection_by_cfg(dst_cfg)

        items = get_job_items(job_id)
        failed = 0

        for item in items:
            if is_stop_requested(job_id):
                for rest in items:
                    if rest["status"] == "pending":
                        mark_item_skipped(rest["id"], "остановлено пользователем")
                refresh_job_progress(job_id)
                mark_job_cancelled(job_id)
                return

            mark_item_running(item["id"])
            refresh_job_progress(job_id)

            try:
                copy_table_pipe(
                    src_conn, dst_conn,
                    item["schema_name"], item["table_name"],
                    item["schema_name"], item["table_name"],
                    truncate=truncate,
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
