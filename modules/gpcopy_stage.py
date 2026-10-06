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
                           "stages": [],
                           "entries": []}
            order.append(key)

        staged = dict(entry, dest=full_name(STAGE_SCHEMA, name))
        merges[key]["stages"].append([STAGE_SCHEMA, name])
        # какая партиция источника в какую промежуточную таблицу: по нему
        # после частичного сбоя дозагружаются только недоехавшие
        merges[key]["entries"].append(
            dict(staged, leaf=list(split_full_name(entry.get("source")))))
        out.append(staged)

    return out, [merges[k] for k in order]


def split_full_name(name):
    """
    'db.schema.table' (части могут быть в кавычках) -> (schema, table).
    Чистая функция.
    """
    import re

    part = r'(?:"((?:[^"]|"")*)"|([^."]+))'
    m = re.match(r"^{0}\.{0}\.{0}$".format(part), str(name or ""))

    if not m:
        return (None, None)

    groups = m.groups()

    def pick(i):
        quoted, plain = groups[2 * i], groups[2 * i + 1]
        return quoted.replace('""', '"') if quoted is not None else plain

    return pick(1), pick(2)


def kept_after_failure(merges, finished):
    """
    gpcopy упал: какие промежуточные таблицы сохранить для дозагрузки.

    finished — {(schema, table)} партиций источника, которые gpcopy
    отчитал как скопированные. Если у цели доехала хоть одна партиция,
    её промежуточные таблицы сохраняются целиком, а в «pending» уходят
    записи недоехавших — дозагрузка перельёт только их. Если не доехало
    ничего, сохранять нечего. Чистая функция.
    -> (kept, dropped)
    """
    kept, dropped = [], []
    finished = set(tuple(p) for p in (finished or []))

    for merge in merges or []:
        # done — доехало в прошлых попытках (дозагрузка их не повторяет,
        # и в её логе их нет)
        entries = [
            dict(e, done=bool(e.get("done"))
                 or tuple(e.get("leaf") or ()) in finished)
            for e in merge.get("entries") or []
        ]
        pending = [e for e in entries if not e["done"]]

        if entries and len(pending) < len(entries):
            kept.append(dict(merge, entries=entries, pending=pending))
        else:
            dropped.append(merge)

    return kept, dropped


def resume_plan(kept):
    """
    План дозагрузки по сохранённым промежуточным таблицам. Чистая функция.
    -> (merges для перелива — без «pending», записи gpcopy только по
    недоехавшим партициям — без служебных полей).
    """
    merges, entries = [], []

    for merge in kept or []:
        clean = {k: v for k, v in merge.items() if k != "pending"}
        merges.append(clean)

        for entry in clean.get("entries") or []:
            if not entry.get("done"):
                entries.append({k: v for k, v in entry.items()
                                if k not in ("leaf", "done")})

    return merges, entries


def staged_source_leaves(kept):
    """{(schema, partition)} источника, которые идут через промежуточные."""
    return {
        tuple(e.get("leaf") or ())
        for merge in kept or []
        for e in merge.get("entries") or []
    }


def missing_stage_tables(dst_conn, merges):
    """Какие промежуточные таблицы из планов уже исчезли из приёмника."""
    missing = []

    with dst_conn.cursor() as cur:
        for merge in merges or []:
            for schema, table in merge.get("stages") or []:
                cur.execute(
                    """
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                    """,
                    (schema, table),
                )

                if cur.fetchone() is None:
                    missing.append((schema, table))

    return missing


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
                create_unlogged(dst_conn, sql_text)


def unlogged_sql(sql_text):
    """
    CREATE TABLE -> CREATE UNLOGGED TABLE. Чистая функция.

    Промежуточная таблица живёт минуты и переливается в цель: журнал WAL
    и копия на зеркалах ей не нужны, а запись без них на проде легче.
    Упадёт кластер посреди задачи — содержимое пропадёт, задачу просто
    перезапускают.
    """
    head = "CREATE TABLE "
    start = sql_text.upper().find(head)

    if start < 0 or sql_text[:start].strip():
        return sql_text

    return (sql_text[:start] + "CREATE UNLOGGED TABLE "
            + sql_text[start + len(head):])


def create_unlogged(dst_conn, sql_text):
    """
    Создать промежуточную таблицу UNLOGGED, а если такой способ хранения
    кластер для неё не принимает — обычной (соединение в autocommit).
    """
    try:
        with dst_conn.cursor() as cur:
            cur.execute(unlogged_sql(sql_text))
    except Exception as e:
        print("[gpcopy_stage] UNLOGGED не принят ({}), создаю обычную".format(
            (str(e).splitlines() or [""])[0][:200]))

        with dst_conn.cursor() as cur:
            cur.execute(sql_text)


def stale_stage_tables(names, is_active, current_job_id=None):
    """
    Остатки прошлых задач в схеме промежуточных таблиц: имена j<задача>_N,
    чья задача уже не идёт. Таблицы идущих задач и текущей не трогаем,
    чужие имена (не j<число>_<число>) — тоже. Чистая функция.
    """
    import re

    pattern = re.compile(r"^j(\d+)_\d+$")
    stale = []

    for name in names:
        m = pattern.match(name)

        if not m:
            continue

        job_id = int(m.group(1))

        if job_id == current_job_id or is_active(job_id):
            continue

        stale.append(name)

    return stale


def sweep_stale_stages(dst_conn, is_active, current_job_id=None):
    """
    Удалить в STAGE_SCHEMA промежуточные таблицы завершённых задач —
    на случай, если процесс приложения убили между копированием и уборкой.
    -> сколько удалено. Ошибки уборки задачу не останавливают.
    """
    with dst_conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind IN ('r', 'p')
            """,
            (STAGE_SCHEMA,),
        )
        names = [r[0] for r in cur.fetchall()]

    dropped = 0

    for name in stale_stage_tables(names, is_active, current_job_id):
        try:
            with dst_conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                    quote_ident(STAGE_SCHEMA), quote_ident(name)))
            dropped += 1
        except Exception:
            pass

    if dropped:
        print("[gpcopy_stage] удалено остатков прошлых задач: {}".format(
            dropped))

    return dropped


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
