"""
gpcopy partition-diff: копирует только те leaf-партиции, что отличаются
от прода по числу строк (COUNT(*) source != dest, или партиция отсутствует
в dest). Определение leaf-партиций — рекурсивный CTE по pg_inherits (тот же
паттерн, что modules/reorganize.py:get_reorganize_targets).
"""

from __future__ import print_function

import os
import json
import time
import traceback
import subprocess

from psycopg2.extras import RealDictCursor

try:
    from job_manager import (
        get_job, get_job_items, mark_job_running, mark_job_done,
        is_stop_requested, clear_stop_flag, mark_job_cancelled,
        refresh_job_progress, set_item_parts, set_job_runtime,
        update_job_config,
    )
except ImportError:
    from modules.job_manager import (
        get_job, get_job_items, mark_job_running, mark_job_done,
        is_stop_requested, clear_stop_flag, mark_job_cancelled,
        refresh_job_progress, set_item_parts, set_job_runtime,
        update_job_config,
    )

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id

try:
    from modules.gpcopy import (
        owner_item_keys,
        skip_covered_items,
        quote_ident, build_gpcopy_command, write_dest_mapping_file, pick_rpc_port, open_psycopg2_connection_by_cfg,
        get_conn_dbname, get_conn_host, get_conn_port, get_conn_user,
        make_include_table_file, safe_mark_job_failed, get_item_value,
        DEFAULT_GPCOPY_PATH, _job_log_path, _watch_gpcopy_log,
        finalize_gpcopy_job, find_owner_item, to_bool,
        gpcopy_full_name, create_job_stages, drop_stages_quietly,
        safe_mark_item_failed,
    )
except ImportError:
    from gpcopy import (
        owner_item_keys,
        skip_covered_items,
        quote_ident, build_gpcopy_command, write_dest_mapping_file, pick_rpc_port, open_psycopg2_connection_by_cfg,
        get_conn_dbname, get_conn_host, get_conn_port, get_conn_user,
        make_include_table_file, safe_mark_job_failed, get_item_value,
        DEFAULT_GPCOPY_PATH, _job_log_path, _watch_gpcopy_log,
        finalize_gpcopy_job, find_owner_item, to_bool,
        gpcopy_full_name, create_job_stages, drop_stages_quietly,
        safe_mark_item_failed,
    )


_LEAF_CTE = """
    WITH RECURSIVE tree AS (
        SELECT c.oid, n.nspname AS schema_name, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        UNION ALL
        SELECT child.oid, cn.nspname, child.relname
        FROM tree
        JOIN pg_inherits i ON i.inhparent = tree.oid
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_namespace cn ON cn.oid = child.relnamespace
    )
    SELECT t.schema_name, t.table_name
    FROM tree t
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_inherits i2 WHERE i2.inhparent = t.oid
    )
    ORDER BY t.schema_name, t.table_name
"""


def list_leaf_partitions(conn, schema, table):
    """Возвращает [(schema, table)] leaf-партиций (или саму таблицу, если не партиц.)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(_LEAF_CTE, (schema, table))
        rows = cur.fetchall()

    return [(r["schema_name"], r["table_name"]) for r in rows]


def _count_rows(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM {}.{}".format(
                quote_ident(schema), quote_ident(table)
            )
        )
        return int(cur.fetchone()[0])


def classify_partition_diff(source_counts, dest_counts):
    """
    Чистая функция. source_counts / dest_counts — {partition_name: count}.
    Возвращает [{partition, src_count, dest_count, action}].
    action: copy_missing | copy_changed | skip.
    """
    rows = []

    for partition, src_count in source_counts.items():
        if partition not in dest_counts:
            action = "copy_missing"
            dest_count = None
        elif dest_counts[partition] != src_count:
            action = "copy_changed"
            dest_count = dest_counts[partition]
        else:
            action = "skip"
            dest_count = dest_counts[partition]

        rows.append({
            "partition": partition,
            "src_count": src_count,
            "dest_count": dest_count,
            "action": action,
        })

    return rows


def partitions_to_copy(diff_rows):
    """Имена партиций с action != skip."""
    return [r["partition"] for r in diff_rows if r["action"] != "skip"]


def diff_partitions(source_cfg, dest_cfg, schema, table):
    """Открывает соединения, считает строки leaf-партиций с обеих сторон, классифицирует."""
    source_conn = open_psycopg2_connection_by_cfg(source_cfg)
    dest_conn = open_psycopg2_connection_by_cfg(dest_cfg)

    try:
        source_leaves = list_leaf_partitions(source_conn, schema, table)

        source_counts = {}
        for s, t in source_leaves:
            source_counts[t] = _count_rows(source_conn, s, t)

        dest_counts = {}
        for s, t in source_leaves:
            try:
                dest_counts[t] = _count_rows(dest_conn, s, t)
            except Exception:
                # Партиции нет в dest — останется missing.
                dest_conn.rollback()

        return classify_partition_diff(source_counts, dest_counts), source_leaves

    finally:
        source_conn.close()
        dest_conn.close()


# ------------------------------------------------------------
# Быстрый diff по статистике каталога (reltuples, один запрос
# на сторону для всех таблиц сразу) + батч-COUNT для точного режима.
# ------------------------------------------------------------

_LEAF_STATS_CTE = """
    WITH RECURSIVE tree AS (
        SELECT c.oid, c.oid AS root_oid,
               n.nspname AS root_schema, c.relname AS root_table
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE (n.nspname, c.relname) IN %s
        UNION ALL
        SELECT child.oid, tree.root_oid, tree.root_schema, tree.root_table
        FROM tree
        JOIN pg_inherits i ON i.inhparent = tree.oid
        JOIN pg_class child ON child.oid = i.inhrelid
    )
    SELECT t.root_schema, t.root_table,
           n.nspname AS leaf_schema, c.relname AS leaf_table,
           c.reltuples::bigint AS rows
    FROM tree t
    JOIN pg_class c ON c.oid = t.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_inherits i2 WHERE i2.inhparent = t.oid
    )
    ORDER BY t.root_schema, t.root_table, c.relname
"""


def fetch_leaf_stats(conn, tables):
    """
    Один запрос: leaf-партиции и reltuples для ВСЕХ переданных таблиц.
    tables: [(schema, table)].
    -> {(root_schema, root_table): {leaf_table: {"schema","table","rows"}}}
    Непартиционированная таблица возвращается сама как единственный leaf.
    """
    if not tables:
        return {}

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(_LEAF_STATS_CTE, (tuple(tables),))
        rows = cur.fetchall()

    out = {}
    for r in rows:
        root = (r["root_schema"], r["root_table"])
        out.setdefault(root, {})[r["leaf_table"]] = {
            "schema": r["leaf_schema"],
            "table": r["leaf_table"],
            "rows": int(r["rows"]),
        }
    return out


def classify_stats_maps(src_stats, dest_stats):
    """
    Чистая функция: покорневая классификация по статистике.
    -> {root_key: [{partition, src_count, dest_count, action}]}
    """
    out = {}
    for root, leaves in src_stats.items():
        src_counts = {name: info["rows"] for name, info in leaves.items()}
        dest_counts = {
            name: info["rows"]
            for name, info in (dest_stats.get(root) or {}).items()
        }
        out[root] = classify_partition_diff(src_counts, dest_counts)
    return out


def build_batched_count_sql(leaves, chunk=50):
    """
    SQL-чанки для точного пересчёта: один запрос считает до `chunk` партиций
    через UNION ALL (вместо round-trip на каждую).
    leaves: [(schema, table)] -> ["SELECT 'p1', count(*) FROM ... UNION ALL ...", ...]
    """
    chunks = []
    for i in range(0, len(leaves), chunk):
        parts = [
            "SELECT '{}' AS partition, count(*) AS cnt FROM {}.{}".format(
                t.replace("'", "''"), quote_ident(s), quote_ident(t)
            )
            for s, t in leaves[i:i + chunk]
        ]
        chunks.append(" UNION ALL ".join(parts))
    return chunks


def fetch_counts_batched(conn, leaves, chunk=50):
    """Точные COUNT(*) батчами. Несуществующие партиции просто пропускаются."""
    counts = {}
    for sql in build_batched_count_sql(leaves, chunk):
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                for name, cnt in cur.fetchall():
                    counts[name] = int(cnt)
        except Exception:
            conn.rollback()
            # чанк с несуществующей таблицей — падаем на поштучный счёт
            for s, t in leaves:
                if t in counts:
                    continue
                try:
                    counts[t] = _count_rows(conn, s, t)
                except Exception:
                    conn.rollback()
    return counts


def diff_partitions_stats(source_cfg, dest_cfg, tables, exact=False):
    """
    Diff для пачки таблиц: по reltuples (мгновенно) или точный (COUNT батчами).
    tables: [(schema, table)].
    -> ({root_key: diff_rows}, {root_key: {leaf_name: (schema, table)}})
    """
    source_conn = open_psycopg2_connection_by_cfg(source_cfg)
    dest_conn = open_psycopg2_connection_by_cfg(dest_cfg)

    try:
        src_stats = fetch_leaf_stats(source_conn, tables)
        dest_stats = fetch_leaf_stats(dest_conn, tables)

        leaves_by_root = {
            root: {name: (info["schema"], info["table"])
                   for name, info in leaves.items()}
            for root, leaves in src_stats.items()
        }

        if not exact:
            return classify_stats_maps(src_stats, dest_stats), leaves_by_root

        # Точный режим: reltuples заменяем настоящими COUNT(*) батчами.
        all_leaves = [
            (info["schema"], info["table"])
            for leaves in src_stats.values()
            for info in leaves.values()
        ]
        src_counts_flat = fetch_counts_batched(source_conn, all_leaves)
        dest_leaves = [
            lv for root, leaves in leaves_by_root.items()
            for name, lv in leaves.items()
            if name in (dest_stats.get(root) or {})
        ]
        dest_counts_flat = fetch_counts_batched(dest_conn, dest_leaves)

        out = {}
        for root, leaves in src_stats.items():
            src_counts = {n: src_counts_flat.get(n, l["rows"])
                          for n, l in leaves.items()}
            dest_present = dest_stats.get(root) or {}
            dest_counts = {n: dest_counts_flat[n]
                           for n in dest_present if n in dest_counts_flat}
            out[root] = classify_partition_diff(src_counts, dest_counts)
        return out, leaves_by_root

    finally:
        source_conn.close()
        dest_conn.close()


# ------------------------------------------------------------
# Сравнение целых таблиц по числу строк (полное копирование):
# то же, что diff партиций, но итог — одна строка на таблицу.
# ------------------------------------------------------------

def classify_table_counts(pairs, src_counts, dest_counts):
    """
    Чистая функция. pairs: [((s, t) источника, (s, t) приёмника)];
    src_counts / dest_counts: {(s, t): rows}, нет ключа — таблицы нет.
    -> [{schema, table, target, src, dest, action}],
    action: copy_missing | copy_changed | skip | no_source.
    """
    rows = []

    for src_key, dst_key in pairs:
        src = src_counts.get(src_key)
        dest = dest_counts.get(dst_key)

        if src is None:
            action = "no_source"
        elif dest is None:
            action = "copy_missing"
        elif dest != src:
            action = "copy_changed"
        else:
            action = "skip"

        rows.append({
            "schema": src_key[0], "table": src_key[1],
            "target": ("{}.{}".format(*dst_key)
                       if dst_key != src_key else None),
            "src": src, "dest": dest, "action": action,
        })

    return rows


def fetch_table_counts(conn, tables, chunk=50):
    """
    Точные COUNT(*) таблиц (у секционированной — по всем партициям)
    батчами через UNION ALL. tables: [(s, t)] -> {(s, t): rows}.
    Таблицы, которые не посчитались, пропускаются.
    """
    counts = {}

    for i in range(0, len(tables), chunk):
        part = tables[i:i + chunk]
        sql = " UNION ALL ".join(
            "SELECT {} AS k, count(*) AS cnt FROM {}.{}".format(
                i + n, quote_ident(s), quote_ident(t))
            for n, (s, t) in enumerate(part)
        )

        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                for k, cnt in cur.fetchall():
                    counts[tables[int(k)]] = int(cnt)
        except Exception:
            conn.rollback()
            # таблица пропала посреди — считаем поштучно
            for s, t in part:
                try:
                    counts[(s, t)] = _count_rows(conn, s, t)
                except Exception:
                    conn.rollback()

    return counts


def _stats_totals(stats):
    """{root: {leaf: {rows}}} -> {root: сумма reltuples по партициям}."""
    return {root: sum(max(int(l["rows"]), 0) for l in leaves.values())
            for root, leaves in stats.items()}


def diff_tables_counts(source_cfg, dest_cfg, tables, exact=False,
                       targets=None):
    """
    Сравнение таблиц источника и приёмника по числу строк: по
    статистике каталога (reltuples, сумма по партициям — у корня GP7
    своих строк нет) или точным COUNT(*). Только чтение с обеих сторон.
    tables: [(s, t)]; targets — карта «источник -> цель».
    """
    try:
        from modules.sync_targets import target_of
    except ImportError:
        from sync_targets import target_of

    pairs = [((s, t), tuple(target_of(targets, s, t)) if targets else (s, t))
             for s, t in tables]

    source_conn = open_psycopg2_connection_by_cfg(source_cfg)
    dest_conn = open_psycopg2_connection_by_cfg(dest_cfg)

    try:
        src_counts = _stats_totals(fetch_leaf_stats(
            source_conn, [p[0] for p in pairs]))
        dest_counts = _stats_totals(fetch_leaf_stats(
            dest_conn, list(dict.fromkeys(p[1] for p in pairs))))

        if exact:
            # не посчиталась — остаётся оценка по статистике, а не «нет»
            src_counts.update(fetch_table_counts(
                source_conn, [k for k in (p[0] for p in pairs)
                              if k in src_counts]))
            dest_counts.update(fetch_table_counts(
                dest_conn, [k for k in dict.fromkeys(p[1] for p in pairs)
                            if k in dest_counts]))

        return classify_table_counts(pairs, src_counts, dest_counts)
    finally:
        source_conn.close()
        dest_conn.close()


def normalize_partition_config(config):
    """
    Конфиг задачи «синхронизация партиций». Если стоит recompute, список
    партиций из интерфейса выбрасываем: расхождение считается на старте
    самой задачи. Иначе в расписании каждый раз лился бы один и тот же
    список, снятый когда-то в UI. Чистая функция.
    """
    cfg = dict(config or {})

    if cfg.get("recompute"):
        cfg.pop("partitions", None)

    return cfg


def _dedupe_copy_items(copy_items):
    """Каждая партиция — одной строкой, в прежнем порядке (чистая)."""
    seen = set()
    out = []

    for ci in copy_items:
        key = (ci["schema_name"], ci["table_name"])

        if key in seen:
            continue

        seen.add(key)
        out.append(ci)

    return out


# ------------------------------------------------------------
# «Только общие колонки»: партиции таблиц, у которых колонки источника и
# приёмника различаются, идут через промежуточные таблицы
# (modules/gpcopy_stage) и переливаются в одноимённую партицию приёмника
# общими колонками.
# ------------------------------------------------------------

def partition_column_diffs(source_cfg, dest_cfg, roots):
    """
    Колонки корней источника и одноимённых корней приёмника (у всех
    партиций GP колонки корня — сравниваем корень один раз).
    -> ({root: diff} только отличающихся, {leaf: root}). Источник —
    только чтение.
    """
    try:
        from modules.gpcopy_columns import differing_tables
    except ImportError:
        from gpcopy_columns import differing_tables

    roots = list(dict.fromkeys(tuple(r) for r in roots))

    if not roots:
        return {}, {}

    src_conn = open_psycopg2_connection_by_cfg(source_cfg)

    try:
        try:
            src_conn.set_session(readonly=True, autocommit=True)
        except Exception:
            pass

        dst_conn = open_psycopg2_connection_by_cfg(dest_cfg)

        try:
            stats = fetch_leaf_stats(src_conn, roots)
            diffs = differing_tables(src_conn, dst_conn,
                                     [r + r for r in roots])
        finally:
            try:
                dst_conn.rollback()
            except Exception:
                pass
            dst_conn.close()
    finally:
        src_conn.close()

    leaf_root = {
        (info["schema"], info["table"]): root
        for root, leaves in stats.items()
        for info in leaves.values()
    }

    return {tuple(d["source"]): d for d in diffs}, leaf_root


def split_refused_leaves(copy_items, leaf_root, diffs):
    """
    Партиции корней, которые нельзя грузить только общими колонками
    (NOT NULL без DEFAULT только в цели, нет общих колонок), — вон из
    копирования. Чистая функция. -> (оставшиеся copy_items, {root: текст})
    """
    try:
        from modules.gpcopy_columns import refuse_reason
    except ImportError:
        from gpcopy_columns import refuse_reason

    errors = {}

    for root, diff in diffs.items():
        reason = refuse_reason(diff)

        if reason:
            errors[tuple(root)] = reason

    kept = []

    for ci in copy_items:
        leaf = (ci["schema_name"], ci["table_name"])

        if leaf_root.get(leaf, leaf) not in errors:
            kept.append(ci)

    return kept, errors


def plan_common_columns_stages(copy_items, leaf_root, diffs, job_id,
                               source_db, dest_db):
    """
    Чистая функция. Записи include-table-json для всех партиций
    (в одноимённые партиции приёмника); партиции отличающихся корней —
    каждая в свою промежуточную таблицу, перелив в одноимённую партицию
    приёмника (TRUNCATE + INSERT общих колонок).
    -> (entries, merges); merges пуст — промежуточные не нужны.
    """
    try:
        from modules import gpcopy_stage
    except ImportError:
        import gpcopy_stage

    entries = []
    roots = {}

    for ci in copy_items:
        leaf = (ci["schema_name"], ci["table_name"])
        dest = gpcopy_full_name(dest_db, *leaf)
        entries.append({"source": gpcopy_full_name(source_db, *leaf),
                        "dest": dest})
        root = leaf_root.get(leaf, leaf)

        if root in diffs:
            roots[dest] = {"item": list(root), "target": list(leaf),
                           "truncate": True, "common_columns": True}

    if not roots:
        return entries, []

    return gpcopy_stage.plan_stages(
        entries, roots, job_id,
        lambda schema, table: gpcopy_full_name(dest_db, schema, table))


def _write_include_json(entries):
    import tempfile

    fd, path = tempfile.mkstemp(prefix="gpcopy_include_part_",
                                suffix=".json", text=True)

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
    except Exception:
        try:
            os.remove(path)
        except Exception:
            pass
        raise

    return path


def _stage_common_columns(job_id, config, items, copy_items, source_cfg,
                          dest_cfg):
    """
    Раннер партиций, «Только общие колонки». Строки задачи с корнем,
    который так грузить нельзя, падают сразу. Промежуточные таблицы
    создаются, план перелива — в config["stage_merges"] (и в задаче).
    -> (copy_items, entries include JSON или None, {root: ошибка})
    """
    try:
        from modules.gpcopy_columns import describe
    except ImportError:
        from gpcopy_columns import describe

    roots = [
        (get_item_value(i, "schema_name"), get_item_value(i, "table_name"))
        for i in items if get_item_value(i, "status") != "skipped"
    ]
    diffs, leaf_root = partition_column_diffs(source_cfg, dest_cfg, roots)

    for diff in diffs.values():
        print("[gpcopy] колонки различаются, через промежуточные таблицы: "
              + describe(diff))

    copy_items, errors = split_refused_leaves(copy_items, leaf_root, diffs)

    for item in items:
        pair = (get_item_value(item, "schema_name"),
                get_item_value(item, "table_name"))

        if pair in errors:
            safe_mark_item_failed(get_item_value(item, "id"),
                                  error_message=errors[pair][:2000])

    if not copy_items:
        return copy_items, None, errors

    entries, merges = plan_common_columns_stages(
        copy_items, leaf_root, diffs, job_id,
        get_conn_dbname(source_cfg), get_conn_dbname(dest_cfg))

    if not merges:
        return copy_items, None, errors

    create_job_stages(job_id, merges, source_cfg, dest_cfg)
    config["stage_merges"] = merges

    # план перелива — в задачу: переподхват после рестарта доведёт его.
    # finalize перепишет конфиг целиком — план «Продолжить» не теряем
    saved = json.loads(get_item_value(get_job(job_id), "config_json") or "{}")
    saved["stage_merges"] = merges
    update_job_config(job_id, saved)

    if saved.get("planned_leaves") is not None:
        config["planned_leaves"] = saved["planned_leaves"]

    return copy_items, entries, errors


def run_gpcopy_partition_diff_job(job_id):
    include_file = None
    include_json_file = None
    config = None
    started = time.time()

    try:
        job = get_job(job_id)
        if not job:
            raise Exception("Job not found: {}".format(job_id))

        config = normalize_partition_config(
            json.loads(get_item_value(job, "config_json") or "{}"))

        source_cfg = get_connection_by_id(int(config["source_connection_id"]))
        dest_cfg = get_connection_by_id(int(config["dest_connection_id"]))

        tables = config.get("tables") or []
        if not tables:
            raise Exception("tables is empty")

        # партиции льются в одноимённые партиции приёмника — другой цели
        # у этого режима нет (маршрут отказывает раньше, это для расписаний)
        raw_targets = config.get("targets")

        if raw_targets and (not isinstance(raw_targets, dict) or any(
                str(v or "").strip() for v in raw_targets.values())):
            raise Exception(
                "Для режима партиций загрузка в другую таблицу "
                "не поддерживается")

        clear_stop_flag(job_id)
        mark_job_running(job_id)

        # партиция, выбранная вместе с корнем, иначе попала бы в gpcopy
        # дважды: от корня и отдельной строкой — а gpcopy льёт параллельно
        _kept, covered = skip_covered_items(
            get_job_items(job_id), config["source_connection_id"])

        explicit = config.get("partitions") or []

        if explicit:
            # UI уже показал diff и пользователь отметил, что переливать.
            copy_items = [
                {"schema_name": p.get("schema") or p.get("schema_name"),
                 "table_name": p.get("table") or p.get("table_name")}
                for p in explicit
            ]
        else:
            # Diff по всем таблицам сразу: stats (reltuples, мгновенно) —
            # дефолт; count_mode="exact" пересчитывает COUNT(*) батчами.
            exact = (config.get("count_mode") or "stats") == "exact"
            roots = []

            for entry in tables:
                key = (entry.get("schema") or entry.get("schema_name"),
                       entry.get("table") or entry.get("table_name"))

                if key not in covered and key not in roots:
                    roots.append(key)

            diff_by_root, leaves_by_root = diff_partitions_stats(
                source_cfg, dest_cfg, roots, exact=exact,
            )

            copy_items = []
            for root, diff_rows in diff_by_root.items():
                leaf_by_name = leaves_by_root.get(root) or {}
                for name in partitions_to_copy(diff_rows):
                    s, t = leaf_by_name[name]
                    copy_items.append({"schema_name": s, "table_name": t})

        copy_items = _dedupe_copy_items(copy_items)

        # что именно решили перелить — по этому «Продолжить» дольёт остаток,
        # не пересчитывая расхождение заново
        try:
            saved = json.loads(get_item_value(get_job(job_id), "config_json")
                               or "{}")
            saved["planned_leaves"] = [[ci["schema_name"], ci["table_name"]]
                                       for ci in copy_items]
            update_job_config(job_id, saved)
        except Exception:
            pass

        if is_stop_requested(job_id):
            mark_job_cancelled(job_id)
            return

        items = get_job_items(job_id)
        item_keys = owner_item_keys(items)

        if not copy_items:
            # Всё совпадает — копировать нечего, это успех.
            from job_manager import mark_item_skipped

            for i in items:
                if get_item_value(i, "status") == "skipped":
                    continue

                mark_item_skipped(
                    get_item_value(i, "id"), "Партиции совпадают"
                )

            refresh_job_progress(job_id)
            mark_job_done(job_id)
            return

        # «Только общие колонки»: партиции таблиц с другими колонками в
        # приёмнике — через промежуточные таблицы
        stage_entries = None

        if to_bool(config.get("common_columns"), False):
            copy_items, stage_entries, refused = _stage_common_columns(
                job_id, config, items, copy_items, source_cfg, dest_cfg)

            if refused:
                items = [i for i in items
                         if (get_item_value(i, "schema_name"),
                             get_item_value(i, "table_name")) not in refused]
                item_keys = owner_item_keys(items)

            if not copy_items:
                refresh_job_progress(job_id)
                safe_mark_job_failed(
                    job_id, "Ни одну таблицу нельзя загрузить только общими "
                            "колонками:\n{}".format(
                                "\n".join(refused.values()))[:4000])
                return

        # сколько партиций льём в каждую таблицу — для live-баров
        parts_by_item = {}

        for ci in copy_items:
            owner = find_owner_item(
                ci["schema_name"], ci["table_name"], item_keys
            )

            if owner is not None:
                parts_by_item[owner] = parts_by_item.get(owner, 0) + 1

        for item_id, total in parts_by_item.items():
            set_item_parts(item_id, parts_total=total)

        source_db = get_conn_dbname(source_cfg)

        if stage_entries:
            # имя приёмника (промежуточная таблица) задаёт только JSON
            include_json_file = _write_include_json(stage_entries)
        else:
            include_file = make_include_table_file(copy_items, source_db)

        gpcopy_path = (
            config.get("gpcopy_path")
            or os.environ.get("GPCOPY_PATH")
            or DEFAULT_GPCOPY_PATH
        )

        cmd = build_gpcopy_command(
            gpcopy_path=gpcopy_path,
            source_host=get_conn_host(source_cfg),
            dest_host=get_conn_host(dest_cfg),
            source_port=get_conn_port(source_cfg),
            dest_port=get_conn_port(dest_cfg),
            dest_user=get_conn_user(dest_cfg),
            include_tables_file=include_file,
            include_json_file=include_json_file,
            jobs=int(config.get("jobs") or 4),
            truncate=True,
            # ANALYZE каждой перелитой партиции — долго, поэтому только по
            # галочке. Без него в приёмнике остаётся старый reltuples, и
            # сравнение «по статистике» может снова увидеть расхождение —
            # тогда точнее «Точный пересчёт COUNT(*)»
            analyze=to_bool(config.get("analyze"), False),
            dest_mapping_file=write_dest_mapping_file(dest_cfg),
            rpc_port=pick_rpc_port(),
        )

        # общий каркас gpcopy: detached-процесс + лог + live-статусы,
        # переживает рестарт Opsentri (pid + log_file в задаче)
        log_path = _job_log_path(job_id)
        log_handle = open(log_path, "w", encoding="utf-8", errors="replace")

        popen_kwargs = {
            "stdout": log_handle,
            "stderr": subprocess.STDOUT,
            "universal_newlines": True,
        }

        if os.name == "posix":
            popen_kwargs["start_new_session"] = True
        else:
            popen_kwargs["creationflags"] = 0x00000200

        process = subprocess.Popen(cmd, **popen_kwargs)

        try:
            log_handle.close()
        except Exception:
            pass

        set_job_runtime(job_id, pid=process.pid, log_file=log_path)

        log_text = _watch_gpcopy_log(job_id, log_path, item_keys,
                                     process=process)

        if is_stop_requested(job_id):
            drop_stages_quietly(config)
            mark_job_cancelled(job_id)
            refresh_job_progress(job_id)
            return

        finalize_gpcopy_job(
            job_id, items, process.returncode, log_text, "",
            " ".join(cmd), time.time() - started, config,
        )

    except Exception as e:
        err = "{}\n{}".format(e, traceback.format_exc())

        # промежуточные таблицы «Только общих колонок» — удалить
        drop_stages_quietly(config)

        try:
            safe_mark_job_failed(job_id, err[:4000])
        except Exception:
            pass
    finally:
        for path in (include_file, include_json_file):
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass
