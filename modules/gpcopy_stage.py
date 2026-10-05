"""
Промежуточные таблицы gpcopy для секционированной таблицы с картой targets.

gpcopy 2.7 не даёт загрузить секционированную таблицу в цель с другим
именем и другой нарезкой партиций:
  * корень в корень он льёт партиция-в-партицию и сам выводит имена
    партиций цели (dm_stock_lot_prt_X -> dm_stock_lot_new_prt_X) — у цели
    по дням вместо недель таких партиций нет;
  * SQL-срез по секционированной таблице не принимает;
  * несколько таблиц источника в одну таблицу приёмника не принимает
    («Multiple source tables ... cannot be transferred to the same dest
    table»).

Поэтому каждая партиция источника идёт своей записью gpcopy в свою
промежуточную таблицу приёмника (обычная таблица с колонками и
распределением источника), а после успешного gpcopy на приёмнике одной
транзакцией: TRUNCATE цели (если выбран truncate) и INSERT INTO цель
SELECT ... FROM каждой промежуточной. Строки по партициям раскладывает
сама цель. Промежуточные таблицы удаляются в любом исходе.

Источник только читается; DDL и DML — только на приёмнике, только в
схеме STAGE_SCHEMA и в целях из карты.
"""

STAGE_SCHEMA = "opsentri_gpcopy_stage"


def quote_ident(name):
    return '"' + str(name).replace('"', '""') + '"'


def stage_name(job_id, n):
    return "j{}_{:05d}".format(int(job_id), n)


def plan_stages(entries, roots, job_id, full_name):
    """
    Чистая функция. entries — записи include-table-json; roots —
    {dest как в JSON: {"item": [s, t], "target": [ds, dt],
    "truncate": bool}} для корней целей секционированных таблиц.

    Каждая запись с dest из roots получает свою промежуточную таблицу.
    full_name(schema, table) -> имя для JSON (с базой приёмника).
    -> (новые entries, merges), merges — [{"item", "target", "truncate",
    "stages": [[schema, table], ...]}] в порядке первого появления.
    """
    merges = {}
    order = []
    out = []
    n = 0

    for entry in entries:
        meta = roots.get(entry.get("dest"))

        if not meta:
            out.append(entry)
            continue

        n += 1
        name = stage_name(job_id, n)
        key = tuple(meta["target"])

        if key not in merges:
            merges[key] = {"item": list(meta["item"]),
                           "target": list(meta["target"]),
                           "truncate": bool(meta.get("truncate")),
                           "stages": []}
            order.append(key)

        merges[key]["stages"].append([STAGE_SCHEMA, name])
        out.append(dict(entry, dest=full_name(STAGE_SCHEMA, name)))

    return out, [merges[k] for k in order]


def create_stages(src_conn, dst_conn, merges):
    """
    Пустые промежуточные таблицы в приёмнике по структуре таблицы
    источника (обычные, без партиций). Имена содержат номер задачи —
    остаток прошлой попытки этой же задачи удаляется.
    """
    try:
        from modules.ddl_check import fetch_target_ddl
    except ImportError:
        from ddl_check import fetch_target_ddl

    with dst_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s",
                    (STAGE_SCHEMA,))

        if cur.fetchone() is None:
            cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(
                quote_ident(STAGE_SCHEMA)))

    for merge in merges:
        schema, table = merge["item"]

        for stage_schema, stage_table in merge["stages"]:
            ddl = fetch_target_ddl(src_conn, schema, table,
                                   stage_schema, stage_table, plain=True)

            if not ddl:
                raise ValueError("{}.{} нет в источнике".format(schema, table))

            with dst_conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                    quote_ident(stage_schema), quote_ident(stage_table)))

                for sql_text in ddl["statements"]:
                    cur.execute(sql_text)


def _stage_columns(cur, schema, table):
    cur.execute(
        """
        SELECT a.attname
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


def apply_merges(dst_conn, merges):
    """
    Перелить промежуточные таблицы в цели: по цели — одна транзакция
    (TRUNCATE, если выбран, и INSERT из каждой промежуточной). Ошибка
    одной цели откатывает только её. -> {(schema, table) источника: текст}.
    """
    errors = {}

    for merge in merges:
        dst_schema, dst_table = merge["target"]
        target = "{}.{}".format(quote_ident(dst_schema), quote_ident(dst_table))

        try:
            with dst_conn.cursor() as cur:
                if merge.get("truncate"):
                    cur.execute("TRUNCATE TABLE {}".format(target))

                columns = None

                for stage_schema, stage_table in merge["stages"]:
                    if columns is None:
                        columns = _stage_columns(cur, stage_schema,
                                                 stage_table)

                        if not columns:
                            raise ValueError(
                                "промежуточной таблицы {}.{} нет".format(
                                    stage_schema, stage_table))

                        cols = ", ".join(quote_ident(c) for c in columns)

                    cur.execute("INSERT INTO {} ({}) SELECT {} FROM {}.{}".format(
                        target, cols, cols, quote_ident(stage_schema),
                        quote_ident(stage_table)))

            dst_conn.commit()
            print("[gpcopy_stage] {}.{}: перелито {} промежуточных".format(
                dst_schema, dst_table, len(merge["stages"])))
        except Exception as e:
            try:
                dst_conn.rollback()
            except Exception:
                pass

            errors[tuple(merge["item"])] = (
                "gpcopy скопировал данные, но перенести их из промежуточных "
                "таблиц в {}.{} не удалось (цель не изменена): {}".format(
                    dst_schema, dst_table, str(e)[:600]))

    return errors


def drop_stages(dst_conn, merges):
    """Удалить промежуточные таблицы; ошибки не мешают остальным."""
    for merge in merges or []:
        for stage_schema, stage_table in merge.get("stages") or []:
            try:
                with dst_conn.cursor() as cur:
                    cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                        quote_ident(stage_schema), quote_ident(stage_table)))
                dst_conn.commit()
            except Exception:
                try:
                    dst_conn.rollback()
                except Exception:
                    pass
