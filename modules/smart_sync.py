# -*- coding: utf-8 -*-
"""
Умная синхронизация прод -> тест: каждый день копируется только то, что
изменилось с прошлой синхронизации.

План синхронизации (smart_sync_plans) — пара подключений и волны. Волна —
набор таблиц по маскам «schema.table» (включить / исключить) со своим
временем старта и сроком: после срока новые таблицы не начинаются,
остаток уходит в следующее окно первым. Ни схемы, ни таблицы в коде не
зашиты — всё задаётся в плане.

Единица синхронизации — «лист»: обычная таблица или leaf-партиция. По
каждому листу из каталога прода (без чтения данных) берётся отпечаток:

  * relfilenode — меняется при TRUNCATE и перезаписи таблицы;
  * время последней DDL/TRUNCATE из pg_stat_last_operation (Greenplum),
    без VACUUM/ANALYZE — обслуживание данных не меняет;
  * счётчик DML (вставлено+обновлено+удалено) из сводной статистики по
    сегментам, если она есть, иначе — размер на сегментах.

Отпечаток не изменился с прошлой успешной синхронизации — лист
пропускается. Любое расхождение (и сброс статистики) — копируется: ошибка
возможна только в сторону лишнего копирования. Раз в неделю (по плану)
волна копируется целиком без проверки отпечатков.

Модуль только читает каталог прода; изменения — только в приёмнике.
"""

import fnmatch
import json
import re
import threading
from datetime import datetime

from db import sqlite_cursor

DEFAULT_SETTINGS = {
    "waves": [],
    "exclude": [],
    "big_gb": 50,          # лист больше — отдельный запуск gpcopy
    "batch_gb": 200,       # пачка мелких листьев на один запуск gpcopy
    "batch_tables": 500,
    "jobs": 4,
    "parallel": 1,         # пачек gpcopy одновременно
    "gpcopy_path": "/usr/local/gpdb/greenplum-db/bin/gpcopy",
    "full_weekday": None,  # 0=пн … 6=вс: копировать волну целиком
}

_MASK_RE = re.compile(r"^[A-Za-z0-9_$*?.\[\]\-]+$")
_TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
_PATH_RE = re.compile(r"^/[A-Za-z0-9_./\-]+$")

_SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "gp_toolkit",
                   "pg_aoseg", "pg_bitmapindex", "pg_toast")


# ------------------------------------------------------------------ план

def _masks(value, label):
    if isinstance(value, str):
        value = re.split(r"[\s,;]+", value)

    out = []

    for mask in value or []:
        mask = str(mask).strip()

        if not mask:
            continue
        if "." not in mask or not _MASK_RE.match(mask):
            raise ValueError("{}: маска «{}» — нужно schema.table, можно * и ?".format(
                label, mask))
        if mask not in out:
            out.append(mask)

    return out


def _int(data, key, low, high, label):
    raw = data.get(key, DEFAULT_SETTINGS[key])

    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError("{}: нужно целое число".format(label))

    if not low <= value <= high:
        raise ValueError("{}: от {} до {}".format(label, low, high))

    return value


def normalize_settings(data):
    """Проверенные настройки плана; ValueError — что не так."""
    data = data or {}
    out = dict(DEFAULT_SETTINGS)
    waves = []

    for i, wave in enumerate(data.get("waves") or [], 1):
        name = str(wave.get("name") or "").strip()[:60] or "Волна {}".format(i)
        include = _masks(wave.get("include"), name)

        if not include:
            raise ValueError("{}: укажите, какие таблицы входят (маски)".format(name))

        for key in ("start", "deadline"):
            value = str(wave.get(key) or "").strip()

            if value and not _TIME_RE.match(value):
                raise ValueError("{}: время «{}» — формат ЧЧ:ММ".format(name, value))

        waves.append({
            "name": name,
            "include": include,
            "exclude": _masks(wave.get("exclude"), name),
            "start": str(wave.get("start") or "").strip(),
            "deadline": str(wave.get("deadline") or "").strip(),
        })

    if not waves:
        raise ValueError("Нужна хотя бы одна волна")

    out["waves"] = waves
    out["exclude"] = _masks(data.get("exclude"), "Исключить везде")
    out["big_gb"] = _int(data, "big_gb", 1, 100000, "Большая таблица, ГБ")
    out["batch_gb"] = _int(data, "batch_gb", 1, 100000, "Пачка, ГБ")
    out["batch_tables"] = _int(data, "batch_tables", 1, 5000, "Пачка, таблиц")
    out["jobs"] = _int(data, "jobs", 1, 32, "Потоки gpcopy")
    out["parallel"] = _int(data, "parallel", 1, 8, "Пачек одновременно")

    path = str(data.get("gpcopy_path") or DEFAULT_SETTINGS["gpcopy_path"]).strip()

    if not _PATH_RE.match(path) or ".." in path.split("/"):
        raise ValueError("Путь к gpcopy: абсолютный, без пробелов и ..")
    out["gpcopy_path"] = path

    weekday = data.get("full_weekday")

    if weekday in (None, "", "none"):
        out["full_weekday"] = None
    else:
        try:
            weekday = int(weekday)
        except (TypeError, ValueError):
            raise ValueError("День полной сверки: 0 (пн) … 6 (вс)")
        if not 0 <= weekday <= 6:
            raise ValueError("День полной сверки: 0 (пн) … 6 (вс)")
        out["full_weekday"] = weekday

    return out


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _row_plan(row):
    plan = dict(row)
    plan["settings"] = json.loads(plan.pop("settings_json") or "{}")
    return plan


def list_plans():
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM smart_sync_plans ORDER BY id")
        return [_row_plan(r) for r in cur.fetchall()]


def get_plan(plan_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM smart_sync_plans WHERE id = ?", (int(plan_id),))
        row = cur.fetchone()
    return _row_plan(row) if row else None


def save_plan(data, plan_id=None, user=None):
    """Создать или обновить план -> его id. ValueError — что не так."""
    name = str(data.get("name") or "").strip()[:100]

    if not name:
        raise ValueError("Название плана обязательно")

    try:
        src = int(data.get("source_connection_id"))
        dst = int(data.get("dest_connection_id"))
    except (TypeError, ValueError):
        raise ValueError("Источник и приёмник обязательны")

    if src == dst:
        raise ValueError("Источник и приёмник должны различаться")

    settings = json.dumps(normalize_settings(data.get("settings") or {}),
                          ensure_ascii=False)

    with sqlite_cursor(commit=True) as cur:
        if plan_id is None:
            cur.execute(
                "INSERT INTO smart_sync_plans (name, source_connection_id, "
                "dest_connection_id, settings_json, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (name, src, dst, settings, user, _now(), _now()))
            return cur.lastrowid

        cur.execute(
            "UPDATE smart_sync_plans SET name = ?, source_connection_id = ?, "
            "dest_connection_id = ?, settings_json = ?, updated_at = ? WHERE id = ?",
            (name, src, dst, settings, _now(), int(plan_id)))

        if not cur.rowcount:
            raise LookupError("План не найден")

        return int(plan_id)


def delete_plan(plan_id):
    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM smart_sync_state WHERE plan_id = ?", (int(plan_id),))
        cur.execute("DELETE FROM smart_sync_analysis WHERE plan_id = ?", (int(plan_id),))
        cur.execute("DELETE FROM smart_sync_plans WHERE id = ?", (int(plan_id),))
        return cur.rowcount


def load_state(plan_id):
    """{(schema, table): {fingerprint, synced_at}} — последняя успешная синхронизация."""
    with sqlite_cursor() as cur:
        cur.execute("SELECT schema_name, table_name, fingerprint, synced_at "
                    "FROM smart_sync_state WHERE plan_id = ?", (int(plan_id),))
        return {(r[0], r[1]): {"fingerprint": r[2], "synced_at": r[3]}
                for r in cur.fetchall()}


def save_state(plan_id, entries, job_id=None):
    """entries: [((schema, table), fingerprint)] — синхронизировано успешно."""
    now = _now()

    with sqlite_cursor(commit=True) as cur:
        cur.executemany(
            "INSERT INTO smart_sync_state (plan_id, schema_name, table_name, "
            "fingerprint, synced_at, last_job_id) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(plan_id, schema_name, table_name) DO UPDATE SET "
            "fingerprint = excluded.fingerprint, synced_at = excluded.synced_at, "
            "last_job_id = excluded.last_job_id",
            [(int(plan_id), s, t, fp, now, job_id) for (s, t), fp in entries])


# ------------------------------------------------------------- волны

def _match(name, masks):
    low = name.lower()
    return any(fnmatch.fnmatch(low, m.lower()) for m in masks)


def wave_of(root, settings):
    """Номер волны (0…) для корня (schema, table) или None — вне плана."""
    name = "{}.{}".format(*root)

    if _match(name, settings.get("exclude") or []):
        return None

    for i, wave in enumerate(settings["waves"]):
        if _match(name, wave["include"]) and not _match(name, wave["exclude"]):
            return i

    return None


# ------------------------------------------------------------- каталог

_REL_SQL = """
SELECT c.oid::bigint, n.nspname, c.relname, c.relkind, c.relfilenode::bigint,
       (SELECT i.inhparent::bigint FROM pg_inherits i
        WHERE i.inhrelid = c.oid LIMIT 1)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND n.nspname NOT IN {system}
  AND n.nspname NOT LIKE 'pg_temp%'
  AND n.nspname NOT LIKE 'pg_toast%'
  {storage}
"""

_LAST_OP_SQL = """
SELECT objid::bigint, max(statime)::text
FROM pg_stat_last_operation
WHERE classid = 'pg_class'::regclass
  AND staactionname NOT IN ('VACUUM', 'ANALYZE')
GROUP BY objid
"""

_SIZE_SQL = """
SELECT c.oid::bigint, sum(pg_total_relation_size(c.oid))::bigint
FROM gp_dist_random('pg_class') c
WHERE c.oid IN (SELECT v.o FROM (VALUES {values}) v(o))
GROUP BY c.oid
"""


def _exists(cur, name):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (name,))
    return bool(cur.fetchone()[0])


def _has_column(cur, rel, column):
    cur.execute("SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass(%s) "
                "AND attname = %s AND NOT attisdropped", (rel, column))
    return cur.fetchone() is not None


def _stats_view(cur):
    """Сводная по сегментам статистика DML (Greenplum 7) или None."""
    for name in ("pg_catalog.gp_stat_all_tables_summary",
                 "gp_toolkit.gp_stat_all_tables_summary"):
        if _exists(cur, name) and all(_has_column(cur, name, c) for c in
                                      ("relid", "n_tup_ins", "n_tup_upd", "n_tup_del")):
            return name
    return None


def read_catalog(conn):
    """
    Каталог базы (только чтение) -> {"relations": {oid: {...}},
    "last_op": {oid: время}, "dml": {oid: n} | None, "stats": имя вида | None}.
    """
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")

    storage = ""
    # Greenplum 6: внешние таблицы — relkind 'r' с relstorage 'x'
    if _has_column(cur, "pg_catalog.pg_class", "relstorage"):
        storage = "AND c.relstorage NOT IN ('x', 'v', 'f')"

    cur.execute(_REL_SQL.format(
        system="(" + ", ".join("'{}'".format(s) for s in _SYSTEM_SCHEMAS) + ")",
        storage=storage))
    relations = {
        int(oid): {"oid": int(oid), "schema": s, "table": t, "kind": kind,
                   "filenode": int(fn or 0), "parent": int(parent) if parent else None}
        for oid, s, t, kind, fn, parent in cur.fetchall()
    }

    last_op = {}

    if _exists(cur, "pg_catalog.pg_stat_last_operation"):
        cur.execute(_LAST_OP_SQL)
        last_op = {int(oid): ts for oid, ts in cur.fetchall()}

    dml, stats = None, _stats_view(cur)

    if stats:
        cur.execute("SELECT relid::bigint, (COALESCE(n_tup_ins, 0) + COALESCE(n_tup_upd, 0)"
                    " + COALESCE(n_tup_del, 0))::bigint FROM {}".format(stats))
        dml = {int(oid): int(n or 0) for oid, n in cur.fetchall()}

    return {"relations": relations, "last_op": last_op, "dml": dml, "stats": stats}


SIZE_CHUNK = 100
SIZE_LOCK_TIMEOUT = "2s"


def _is_lock_timeout(error):
    return (getattr(error, "pgcode", None) == "55P03"
            or "lock timeout" in str(error).lower())


def read_sizes(conn, oids, locked=None):
    """
    {oid: байт на сегментах}; на обычном Postgres — на сервере.

    pg_total_relation_size берёт AccessShareLock на каждую таблицу до конца
    запроса. Чтобы не ждать ETL и не держать его: пачки по SIZE_CHUNK,
    чужую блокировку ждём не дольше SIZE_LOCK_TIMEOUT. Пачка упёрлась в
    блокировку — её таблицы по одной; занятая ETL таблица остаётся без
    размера (её oid — в locked): размер неизвестен, а не ноль.
    """
    cur = conn.cursor()
    oids = sorted(set(int(o) for o in oids))
    out = {}
    mode = {"distributed": True}
    cur.execute("SET lock_timeout = %s", (SIZE_LOCK_TIMEOUT,))
    cur.execute("SET statement_timeout = '300s'")

    def run(chunk):
        if mode["distributed"]:
            values = ", ".join("({}::oid)".format(o) for o in chunk)
            cur.execute(_SIZE_SQL.format(values=values))
        else:
            cur.execute("SELECT oid::bigint, pg_total_relation_size(oid)::bigint "
                        "FROM pg_class WHERE oid = ANY(%s::oid[])", (chunk,))
        return cur.fetchall()

    try:
        for i in range(0, len(oids), SIZE_CHUNK):
            chunk = oids[i:i + SIZE_CHUNK]

            try:
                rows = run(chunk)
            except Exception as e:
                if _is_lock_timeout(e):
                    rows = []

                    for oid in chunk:
                        try:
                            rows += run([oid])
                        except Exception as one:
                            if not _is_lock_timeout(one):
                                raise
                            if locked is not None:
                                locked.append(oid)
                elif mode["distributed"]:
                    # обычный Postgres: gp_dist_random нет
                    mode["distributed"] = False
                    rows = run(chunk)
                else:
                    raise

            for oid, size in rows:
                out[int(oid)] = int(size or 0)
    finally:
        try:
            cur.execute("SET lock_timeout = 0")
            cur.execute("SET statement_timeout = '600s'")
        except Exception:
            pass

    return out


# ------------------------------------------------------------- листья

def build_tree(relations):
    """
    {root: {"oid", "leaves": {(s, t): oid}}} — корни (таблицы без родителя)
    и их листья (без потомков). Обычная таблица — сама себе лист.
    """
    parents = {rel["parent"] for rel in relations.values()
               if rel["parent"] in relations}

    def top(oid):
        seen = set()

        while relations[oid]["parent"] in relations and oid not in seen:
            seen.add(oid)
            oid = relations[oid]["parent"]
        return oid

    roots = {}

    for rel in relations.values():
        if rel["oid"] in parents:
            continue                    # промежуточный уровень или корень

        r = relations[top(rel["oid"])]
        entry = roots.setdefault((r["schema"], r["table"]), {"oid": r["oid"], "leaves": {}})
        entry["leaves"][(rel["schema"], rel["table"])] = rel["oid"]

    return roots


def fingerprint(catalog, oid, size=None):
    """Отпечаток листа: строка JSON, сравнивается целиком."""
    rel = catalog["relations"][oid]
    fp = {"fn": rel["filenode"], "op": catalog["last_op"].get(oid) or ""}

    if catalog.get("dml") is not None:
        fp["dml"] = catalog["dml"].get(oid, 0)
    else:
        fp["size"] = size if size is not None else -1

    return json.dumps(fp, sort_keys=True)


def plan_wave(catalog, dest_catalog, settings, wave_index, state, sizes=None,
              full=False, src_tree=None, dst_tree=None):
    """
    Что делать с таблицами волны. Чистая функция.

    -> {"roots": n, "units": n, "unchanged": n, "items": [...]}; items — по
    корню, которому есть что делать:
      action "new"     — таблицы нет в приёмнике: копируется целиком;
             "changed" — копируются изменившиеся листья (leaves);
      missing — партиции, которых нет в приёмнике (создать и залить),
      extra   — партиции приёмника, которых нет на проде (только отчёт),
      fingerprints — {(s, t): отпечаток} для сохранения после успеха.
    """
    sizes = sizes or {}
    src_tree = src_tree if src_tree is not None else build_tree(catalog["relations"])
    dst_tree = dst_tree if dst_tree is not None else build_tree(dest_catalog["relations"])
    items, roots, units, unchanged = [], 0, 0, 0

    for root, entry in sorted(src_tree.items()):
        if wave_of(root, settings) != wave_index:
            continue

        roots += 1
        units += len(entry["leaves"])
        fps = {leaf: fingerprint(catalog, oid, sizes.get(oid))
               for leaf, oid in entry["leaves"].items()}
        oldest = min((state.get(leaf) or {}).get("synced_at") or "" for leaf in fps)
        total = sum(sizes.get(o, 0) for o in entry["leaves"].values())

        if root not in dst_tree:
            items.append({"root": root, "action": "new", "leaves": sorted(fps),
                          "missing": [], "extra": [], "fingerprints": fps,
                          "synced_at": oldest, "bytes": total})
            continue

        partitioned = root not in fps
        dst_leaves = set(dst_tree[root]["leaves"])
        missing = sorted(set(fps) - dst_leaves) if partitioned else []
        extra = sorted(dst_leaves - set(fps)) if partitioned else []
        changed = sorted(
            leaf for leaf, fp in fps.items()
            if full or leaf in missing
            or (state.get(leaf) or {}).get("fingerprint") != fp)

        unchanged += len(fps) - len(changed)

        if changed or extra:
            items.append({"root": root, "action": "changed", "leaves": changed,
                          "missing": missing, "extra": extra,
                          "fingerprints": {l: fps[l] for l in changed},
                          "synced_at": oldest,
                          "bytes": sum(sizes.get(entry["leaves"][l], 0) for l in changed)})

    # давно не синхронизированное — первым
    items.sort(key=lambda it: (it["synced_at"] or "", it["root"]))
    return {"roots": roots, "units": units, "unchanged": unchanged, "items": items}


def make_batches(items, sizes_by_leaf, settings):
    """
    Пачки для gpcopy: [{"leaves": [(s, t)], "bytes": n, "roots": [...]}].
    Лист больше big_gb — отдельно; мелкие — пачками до batch_gb /
    batch_tables. Корень «new» копируется целиком одним объектом.
    """
    big = settings["big_gb"] * 1024 ** 3
    cap = settings["batch_gb"] * 1024 ** 3
    limit = settings["batch_tables"]
    batches = []
    current = {"leaves": [], "bytes": 0, "roots": []}

    def flush():
        if current["leaves"]:
            batches.append({"leaves": list(current["leaves"]),
                            "bytes": current["bytes"], "roots": list(current["roots"])})
        current.update({"leaves": [], "bytes": 0, "roots": []})

    for it in items:
        units = [it["root"]] if it["action"] == "new" else it["leaves"]

        for leaf in units:
            size = it["bytes"] if it["action"] == "new" else sizes_by_leaf.get(leaf, 0)

            if size >= big:
                batches.append({"leaves": [leaf], "bytes": size, "roots": [it["root"]]})
                continue

            if current["leaves"] and (current["bytes"] + size > cap
                                      or len(current["leaves"]) >= limit):
                flush()

            current["leaves"].append(leaf)
            current["bytes"] += size
            if it["root"] not in current["roots"]:
                current["roots"].append(it["root"])

    flush()
    return batches


# ------------------------------------------------------------- анализ

def _open(connection_id):
    from db import get_connection_by_id
    from modules.gpcopy import open_psycopg2_connection_by_cfg

    cfg = get_connection_by_id(int(connection_id))

    if not cfg:
        raise ValueError("Подключение {} не найдено".format(connection_id))

    conn = open_psycopg2_connection_by_cfg(cfg)
    conn.autocommit = True
    return conn


def is_full_day(settings, when=None):
    weekday = settings.get("full_weekday")
    return weekday is not None and (when or datetime.now()).weekday() == int(weekday)


def analyze(plan, wave_index=None, full=None):
    """
    Что изменилось на проде с прошлой синхронизации (только чтение) ->
    {"mode", "waves": [...], ...}. Размеры читаются только у изменившегося
    (без статистики DML размер — часть отпечатка, тогда у всех).
    """
    settings = plan["settings"]
    src = _open(plan["source_connection_id"])

    try:
        dst = _open(plan["dest_connection_id"])
    except Exception:
        src.close()
        raise

    try:
        catalog = read_catalog(src)
        dest_catalog = read_catalog(dst)
        state = load_state(plan["id"])
        full = is_full_day(settings) if full is None else bool(full)
        src_tree = build_tree(catalog["relations"])
        dst_tree = build_tree(dest_catalog["relations"])
        indexes = (range(len(settings["waves"])) if wave_index is None
                   else [int(wave_index)])
        sizes = {}
        locked = []   # размер не прочитан: таблицу держит ETL

        if catalog["dml"] is None:
            in_plan = [o for root, e in src_tree.items()
                       if wave_of(root, settings) in indexes
                       for o in e["leaves"].values()]
            sizes = read_sizes(src, in_plan, locked)

        waves = []

        for i in indexes:
            draft = plan_wave(catalog, dest_catalog, settings, i, state, sizes=sizes,
                              full=full, src_tree=src_tree, dst_tree=dst_tree)
            need = []

            for it in draft["items"]:
                wanted = set(it["leaves"])

                for leaf, oid in src_tree[it["root"]]["leaves"].items():
                    if (it["action"] == "new" or leaf in wanted) and oid not in sizes:
                        need.append(oid)

            if need:
                sizes.update(read_sizes(src, need, locked))

            result = plan_wave(catalog, dest_catalog, settings, i, state, sizes=sizes,
                               full=full, src_tree=src_tree, dst_tree=dst_tree)
            result.update({"name": settings["waves"][i]["name"], "index": i,
                           "full": full,
                           "bytes": sum(it["bytes"] for it in result["items"])})
            waves.append(result)

        in_plan_roots = sum(1 for root in src_tree if wave_of(root, settings) is not None)

        return {
            "mode": ("статистика DML ({})".format(catalog["stats"]) if catalog["stats"]
                     else "размер на сегментах"),
            "last_op": bool(catalog["last_op"]),
            "waves": waves,
            "tables_total": len(src_tree),
            "outside": len(src_tree) - in_plan_roots,
            "sizes": sizes,
            "sizes_locked": len(set(locked)),
            "catalog": catalog,
            "src_tree": src_tree,
        }
    finally:
        src.close()
        dst.close()


def public_analysis(result, limit=300):
    """Анализ для страницы: без каталога, кортежи -> строки, списки урезаны."""
    def name(pair):
        return "{}.{}".format(*pair)

    waves = []

    for w in result["waves"]:
        items = w["items"]
        waves.append({
            "index": w["index"], "name": w["name"], "full": w["full"],
            "roots": w["roots"], "units": w["units"], "unchanged": w["unchanged"],
            "changed_tables": len(items),
            "changed_units": sum(len(it["leaves"]) for it in items),
            "new_tables": sum(1 for it in items if it["action"] == "new"),
            "missing_parts": sum(len(it["missing"]) for it in items),
            "extra_parts": sum(len(it["extra"]) for it in items),
            "bytes": w["bytes"],
            "items": [{
                "table": name(it["root"]), "action": it["action"],
                "parts": len(it["leaves"]), "missing": len(it["missing"]),
                "extra": len(it["extra"]), "bytes": it["bytes"],
                "synced_at": it["synced_at"] or None,
            } for it in items[:limit]],
        })

    return {"mode": result["mode"], "last_op": result["last_op"],
            "sizes_locked": result.get("sizes_locked", 0),
            "tables_total": result["tables_total"], "outside": result["outside"],
            "waves": waves}


# ------------------------------------------------------------- запуск

def deadline_of(wave, started):
    """Срок волны после старта (через полночь — на следующий день) или None."""
    from datetime import timedelta

    text = (wave or {}).get("deadline") or ""

    if not text:
        return None

    hh, mm = (int(x) for x in text.split(":"))
    end = started.replace(hour=hh, minute=mm, second=0, microsecond=0)

    return end if end > started else end + timedelta(days=1)


def _name(pair):
    return "{}.{}".format(*pair)


def _run_child(parent_id, child_id, runner):
    """Пачка — обычная задача gpcopy; «Стоп» родителя останавливает и её."""
    import threading

    from job_manager import is_stop_requested, request_stop_job

    done = threading.Event()

    def watch():
        while not done.wait(3):
            if is_stop_requested(parent_id):
                request_stop_job(child_id)
                return

    threading.Thread(target=watch, daemon=True).start()

    try:
        runner(child_id)
    finally:
        done.set()


def _set_item_volumes(item_ids, items):
    """Объём и число партиций у строк таблиц — прогресс волны по объёму."""
    rows = [(int(it["bytes"] or 0), len(it["leaves"]) or 1, item_ids[root])
            for root, it in items.items() if item_ids.get(root)]

    with sqlite_cursor(commit=True) as cur:
        cur.executemany("UPDATE job_items SET size_bytes = ?, parts_total = ?, parts_done = 0, "
                        "bytes_done = 0 WHERE id = ?", rows)


def _set_item_part_progress(item_id, it, res):
    """Сколько партиций таблицы уже доехало (и доля объёма) — после каждой пачки."""
    if not item_id:
        return

    total = len(it["leaves"]) or 1
    done = min(len(res["done"]), total)

    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE job_items SET parts_done = ?, bytes_done = ? WHERE id = ?",
                    (done, int((it["bytes"] or 0) * done / total), item_id))


def _close_item(item_id, it, res, stopped, deadline):
    """Итог по таблице: failed / done / extra / left — и статус её строки."""
    from job_manager import mark_item_done, mark_item_failed, mark_item_skipped

    want = set(it["leaves"])

    if res["failed"]:
        leaf, why = next(iter(res["failed"].items()))
        mark_item_failed(item_id, "Не скопировано {} из {}: {} — {}".format(
            len(res["failed"]), len(want) or 1, _name(leaf), why)[:2000])
        return "failed"

    if want and want <= res["done"]:
        mark_item_done(item_id)
        return "done"

    if not want and it["extra"]:
        # копировать нечего, только лишние партиции в тесте
        mark_item_skipped(item_id, "В тесте лишние партиции ({}), на проде их нет: "
                                   "{}".format(len(it["extra"]),
                                               ", ".join(_name(x) for x in it["extra"][:5])))
        return "extra"

    mark_item_skipped(item_id, (
        "Остановлено — продолжится в следующий запуск" if stopped else
        "Не успели до {} — в следующее окно первыми".format(
            deadline.strftime("%H:%M") if deadline else "срока")))
    return "left"


def _covered(it, unit):
    """Какие листья закрывает единица копирования: новая таблица — все свои."""
    return it["leaves"] if it["action"] == "new" else [unit]


MAX_PARALLEL = 8               # потолок «gpcopy одновременно»
PARALLEL_REREAD_SECONDS = 10   # как быстро волна видит новое число из плана


def _parallel_of(settings):
    try:
        return max(1, min(int((settings or {}).get("parallel") or 1), MAX_PARALLEL))
    except (TypeError, ValueError):
        return 1


def _active_batches(smart):
    """{child_id: {n, units, save}} — идущие пачки; «batch» — запись до параллели."""
    out = {}

    for key, meta in (smart.get("active") or {}).items():
        out[int(key)] = meta

    legacy = smart.get("batch") or {}

    if legacy.get("job"):
        out.setdefault(int(legacy["job"]), legacy)

    return out


def batch_summary(smart):
    """Для страницы: начато пачек n из of, идёт сейчас running — или None."""
    active = _active_batches(smart or {})

    if not active:
        return None

    return {"n": int((smart or {}).get("started_batches")
                     or max(int(m.get("n") or 0) for m in active.values())),
            "of": (smart or {}).get("batches"), "running": len(active),
            "limit": (smart or {}).get("parallel")}


def continue_job(job_id):
    """
    «Продолжить» прерванный / остановленный / недоделанный запуск волны: та же
    задача, недоехавшие таблицы снова в очередь. Скопированное не повторяется
    (отпечатки), срок — заново от «сейчас» по времени волны.
    """
    from job_manager import clear_stop_flag, get_job

    job = get_job(job_id)

    if not job or job.get("job_type") != "smart_sync":
        raise ValueError("Это не запуск автосинхронизации")

    if job.get("status") not in ("interrupted", "cancelled", "failed"):
        raise ValueError("Продолжить можно только прерванный, остановленный или "
                         "недоделанный запуск")

    cfg = _cfg(job)
    twin = _live_twin(cfg.get("plan_id"), int(cfg.get("wave") or 0), job_id)

    if twin:
        raise ValueError("Эта волна уже идёт: задача #{}".format(twin))

    smart = cfg.get("smart") or {}

    if smart:
        smart["deadline"] = None
        cfg["smart"] = smart

    from job_manager import update_job_config

    update_job_config(job_id, cfg)

    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE job_items SET status = 'queued', error_message = NULL, "
                    "finished_at = NULL WHERE job_id = ? AND status NOT IN ('done')",
                    (int(job_id),))
        cur.execute("UPDATE jobs SET status = 'queued', finished_at = NULL, "
                    "error_message = NULL, stop_requested = 0 WHERE id = ?", (int(job_id),))

    clear_stop_flag(job_id)
    return job_id


def _cfg(job):
    try:
        return json.loads(job.get("config_json") or "{}")
    except ValueError:
        return {}


def _sleep(seconds):
    import time

    time.sleep(seconds)


def _live_twin(plan_id, wave_index, job_id=None):
    """Id другой идущей задачи той же волны плана или None."""
    from job_manager import list_unfinished_jobs

    for j in list_unfinished_jobs():
        if (j.get("job_type") != "smart_sync" or j.get("status") not in ("running", "stopping")
                or (job_id is not None and int(j["id"]) == int(job_id))):
            continue

        cfg = _cfg(j)

        if str(cfg.get("plan_id")) == str(plan_id) and int(cfg.get("wave") or 0) == int(wave_index):
            return int(j["id"])

    return None


def _wait_leftover_children(plan, job_id):
    """
    gpcopy этого плана, которые ещё идут (gpcopy — внешний процесс и рестарт
    Opsentri его не обрывает): ждём конца, затем зачитываем доехавшее —
    по отпечаткам, которые пачка сохранила в задаче-родителе.
    """
    from job_manager import (get_job, get_job_items, is_stop_requested, list_recent_jobs,
                             list_unfinished_jobs, request_stop_job)

    # свои пачки и пачки оборванных запусков; живая соседняя волна ждёт свои сама
    parents = {int(j["id"]): _cfg(j) for j in list_recent_jobs(["smart_sync"], limit=100)
               if int(j["id"]) == int(job_id)
               or j.get("status") not in ("queued", "running", "stopping")}
    parents = {k: v for k, v in parents.items() if str(v.get("plan_id")) == str(plan["id"])}
    waiting = []

    for j in list_unfinished_jobs():
        if j.get("job_type") != "gpcopy" or j.get("status") not in ("running", "stopping"):
            continue

        parent = _cfg(j).get("smart_parent")

        if parent is not None and int(parent) in parents:
            waiting.append((int(j["id"]), int(parent)))

    for child, parent in waiting:
        while (get_job(child) or {}).get("status") in ("queued", "running", "stopping"):
            if is_stop_requested(job_id):
                request_stop_job(child)
            _sleep(10)

        meta = _active_batches(parents[parent].get("smart") or {}).get(child)

        if not meta:
            continue

        save = meta.get("save") or {}
        saved = [((s, t), fp)
                 for row in get_job_items(child) if row["status"] == "done"
                 for s, t, fp in save.get("{}.{}".format(row["schema_name"], row["table_name"]), [])]

        if saved:
            save_state(plan["id"], saved, job_id=parent)


def _close_already_synced(plan_id, job, existing, items):
    """
    Продолжение после рестарта: строки задачи, которых нет в свежем анализе,
    уже совпадают с продом (их отпечаток сохранён после копирования).
    """
    from job_manager import mark_item_done, mark_item_skipped

    state = load_state(plan_id)
    since = job.get("started_at") or ""

    for key, row in existing.items():
        if key in items or row["status"] in ("done", "failed", "skipped"):
            continue

        if since and ((state.get(key) or {}).get("synced_at") or "") >= since:
            mark_item_done(row["id"])
        else:
            mark_item_skipped(row["id"], "Изменений больше нет — таблица уже совпадает с продом")


# ------------------------------------------------------------- ход запуска

def _job_progress(job, cfg):
    """Ход одного запуска волны: итоги и статус каждой таблицы (по строкам задачи)."""
    from job_manager import get_job_items

    smart = cfg.get("smart") or {}
    rows = get_job_items(job["id"])
    active = _active_batches(smart)
    live = {}

    # внутри идущих пачек — какие их партиции уже доехали (строки дочерних gpcopy)
    for child, meta in (active.items() if job.get("status") == "running" else ()):
        units = meta.get("units") or {}

        for row in get_job_items(child):
            unit = "{}.{}".format(row["schema_name"], row["table_name"])
            root = units.get(unit)

            if not root:
                continue

            if unit == root:            # новая таблица целиком: её собственный счётчик
                live[root] = live.get(root, 0) + (row["parts_done"] or 0 if row["status"] != "done"
                                                  else row["parts_total"] or 1)
            elif row["status"] == "done":
                live[root] = live.get(root, 0) + 1

    # запуск оборвался (рестарт Opsentri до продолжения после рестарта): строки
    # таблиц не закрыты — берём, что реально доехало, из строк его пачек gpcopy
    copied = {}

    if job.get("status") in ("interrupted", "cancelled", "failed"):
        for child in smart.get("children") or []:
            for row in get_job_items(child):
                if row["status"] == "done":
                    copied["{}.{}".format(row["schema_name"], row["table_name"])] = (
                        row["finished_at"] or "")

    tables, counts, failed = {}, {}, []
    bytes_total = bytes_done = 0

    for row in rows:
        name = "{}.{}".format(row["schema_name"], row["table_name"])
        st = row["status"]
        total = row["parts_total"] or 0
        done = row["parts_done"] or 0

        if st not in ("done", "failed", "skipped") and name in copied:
            row = dict(row, finished_at=copied[name])
            st, done = "done", total

        if st == "running":
            done = min(done + live.get(name, 0), total or done + live.get(name, 0))

        counts[st] = counts.get(st, 0) + 1
        size = row["size_bytes"] or 0
        bytes_total += size
        bytes_done += size if st == "done" else (int(size * done / total) if total else 0)

        t = {"s": st, "p": done, "n": total,
             "t": (row["finished_at"] or "")[11:16] if st != "running" else
                  (row["started_at"] or "")[11:16]}

        if st in ("failed", "skipped") and row["error_message"]:
            t["e"] = row["error_message"][:400]

        if st == "failed" and len(failed) < 30:
            failed.append({"table": name, "error": (row["error_message"] or "")[:400]})

        tables[name] = t

    return {
        "job_id": job["id"], "status": job.get("status"),
        "started_at": job.get("started_at"), "finished_at": job.get("finished_at"),
        "progress": job.get("progress_percent") or 0,
        "error": (job.get("error_message") or "")[:300] if job.get("status") != "done" else "",
        "deadline": smart.get("deadline"), "full": bool(cfg.get("full")),
        "batch": batch_summary(smart),
        "batches": smart.get("batches"),
        "total": len(rows), "counts": counts,
        "bytes_total": bytes_total, "bytes_done": bytes_done,
        # доля доехавшего: по объёму, без объёма — по таблицам
        "pct": round(100.0 * bytes_done / bytes_total, 1) if bytes_total else
               (round(100.0 * counts.get("done", 0) / len(rows), 1) if rows else 0),
        "resumed": int(smart.get("resumed") or 0),
        "failed": failed, "tables": tables,
    }


def run_progress(plan_id):
    """{"<волна>": ход последнего запуска этой волны} — для страницы, на лету."""
    from job_manager import list_recent_jobs

    out = {}

    for job in list_recent_jobs(["smart_sync"], limit=100):
        try:
            cfg = json.loads(job.get("config_json") or "{}")
        except ValueError:
            continue

        if str(cfg.get("plan_id")) != str(plan_id):
            continue

        wave = str(int(cfg.get("wave") or 0))

        if wave not in out:
            out[wave] = _job_progress(job, cfg)

    return out


def run_smart_sync_job(job_id, runner=None, clock=datetime.now):
    """
    Одна волна плана: что изменилось -> недостающие партиции в приёмнике ->
    пачки gpcopy (--truncate по листьям, под сторожем блокировок) до срока
    волны. Отпечаток листа сохраняется только после его успешного
    копирования — снятый ДО копирования: изменения во время копирования
    будут видны в следующий раз.
    """
    from job_manager import (create_job, create_job_items, get_job, get_job_items,
                             is_stop_requested, mark_item_done, mark_item_failed,
                             mark_item_running, mark_item_skipped, mark_job_cancelled,
                             mark_job_done, mark_job_failed, mark_job_running,
                             refresh_job_progress, update_job_config)

    if runner is None:
        from modules.gpcopy import run_gpcopy_job as runner

    try:
        job = get_job(job_id)
        config = json.loads(job.get("config_json") or "{}")
        plan = get_plan(config.get("plan_id"))

        if not plan:
            raise ValueError("План синхронизации не найден")

        wave_index = int(config.get("wave") or 0)
        settings = plan["settings"]

        if wave_index >= len(settings["waves"]):
            raise ValueError("В плане нет волны №{}".format(wave_index + 1))

        twin = _live_twin(plan["id"], wave_index, job_id)

        if twin:
            mark_job_failed(job_id, "Эта волна уже идёт: задача #{} — второй запуск "
                                    "не начат".format(twin))
            return

        # продолжение после рестарта Opsentri: срок — прежний, а не от «сейчас»
        prev = config.get("smart") or {}
        started = clock()

        if prev.get("deadline"):
            deadline = datetime.strptime(prev["deadline"], "%Y-%m-%d %H:%M")
        else:
            deadline = deadline_of(settings["waves"][wave_index], started)

        mark_job_running(job_id)

        # gpcopy этого плана, ещё идущие (наша пачка до рестарта или прерванный
        # запуск): ждём их — две копии одной таблицы с --truncate недопустимы
        _wait_leftover_children(plan, job_id)

        result = analyze(plan, wave_index, full=config.get("full"))
        w = result["waves"][0]
        summary = public_analysis(result)["waves"][0]
        summary.pop("items", None)
        config["smart"] = dict(summary, mode=result["mode"], plan_name=plan["name"],
                               deadline=deadline.strftime("%Y-%m-%d %H:%M")
                               if deadline else None)

        if prev:
            config["smart"]["children"] = prev.get("children") or []
            config["smart"]["resumed"] = int(prev.get("resumed") or 0) + 1

        update_job_config(job_id, config)

        items = {it["root"]: it for it in w["items"]}
        existing = {(i["schema_name"], i["table_name"]): i for i in get_job_items(job_id)}
        _close_already_synced(plan["id"], job, existing, items)

        if not w["items"]:
            refresh_job_progress(job_id)
            mark_job_done(job_id)
            return

        create_job_items(job_id, [{"schema_name": it["root"][0], "table_name": it["root"][1],
                                   "action": "SMART SYNC"} for it in w["items"]
                                  if it["root"] not in existing])
        item_ids = {(i["schema_name"], i["table_name"]): i["id"]
                    for i in get_job_items(job_id)}
        outcome = {root: {"done": set(), "failed": {}} for root in items}
        _set_item_volumes(item_ids, items)

        # партиции, которых нет в тесте (новые дни на проде), — по DDL прода
        missing_owner = {leaf: it["root"] for it in w["items"] for leaf in it["missing"]}

        if missing_owner:
            from modules.ddl_check import create_missing_objects

            for res in create_missing_objects(
                    plan["source_connection_id"], plan["dest_connection_id"],
                    [{"schema": s, "table": t} for s, t in missing_owner]):
                leaf = (res.get("schema"), res.get("table"))

                if not res.get("ok") and leaf in missing_owner:
                    outcome[missing_owner[leaf]]["failed"][leaf] = (
                        "партицию не создать в приёмнике: {}".format(res.get("error")))[:300]

        # единица копирования -> таблица: новая таблица целиком, иначе листья
        owner, todo, sizes = {}, [], {}

        for it in w["items"]:
            tree = result["src_tree"][it["root"]]["leaves"]

            for leaf, oid in tree.items():
                sizes[leaf] = result["sizes"].get(oid, 0)

            if it["action"] == "new":
                owner[it["root"]] = it
                todo.append(it)
                continue

            leaves = [l for l in it["leaves"] if l not in outcome[it["root"]]["failed"]]

            for leaf in leaves:
                owner[leaf] = it

            if leaves:
                todo.append(dict(it, leaves=leaves))

        batches = make_batches(todo, sizes, settings)
        children = list(config["smart"].get("children") or [])
        config["smart"]["batches"] = len(batches)
        config["smart"].pop("batch", None)

        # что ещё не скопировано по таблице: закончились единицы — статус таблицы
        # ставим сразу, а не в конце волны (страница показывает это на лету)
        pending = {it["root"]: set([it["root"]] if it["action"] == "new" else it["leaves"])
                   for it in todo}
        closed = {}
        queue = list(enumerate(batches, 1))
        active = {}               # идущие пачки: child -> что в ней (страница, рестарт)
        flags = {"late": False, "error": None}
        lock = threading.Lock()
        cond = threading.Condition(lock)
        limit = {"n": _parallel_of(settings), "at": None}

        def current_limit():
            """«gpcopy одновременно» из плана — меняется на ходу, без перезапуска волны."""
            import time

            now = time.monotonic()

            if limit["at"] is None or now - limit["at"] >= PARALLEL_REREAD_SECONDS:
                limit["at"] = now

                try:
                    fresh = get_plan(plan["id"])
                except Exception:
                    fresh = None

                if fresh:
                    limit["n"] = _parallel_of(fresh["settings"])

            return limit["n"]

        def publish():
            config["smart"]["children"] = list(children)
            config["smart"]["active"] = {str(k): v for k, v in active.items()}
            config["smart"]["parallel"] = limit["n"]
            update_job_config(job_id, config)

        def take():
            """Следующая пачка — если не «Стоп», срок не прошёл и есть свободное место."""
            with cond:
                while True:
                    if flags["late"] or flags["error"] or not queue or is_stop_requested(job_id):
                        return None

                    if len(active) < current_limit():
                        break

                    cond.wait(max(PARALLEL_REREAD_SECONDS, 0.05))

                if deadline and clock() >= deadline:
                    flags["late"] = True
                    return None

                n, batch = queue.pop(0)
                child = create_job("gpcopy", plan["source_connection_id"], {
                    "source_connection_id": plan["source_connection_id"],
                    "dest_connection_id": plan["dest_connection_id"],
                    "tables": [{"schema": s, "table": t} for s, t in batch["leaves"]],
                    "truncate": True,
                    "jobs": settings["jobs"],
                    "gpcopy_path": settings["gpcopy_path"],
                    "smart_parent": job_id,
                    "item_action": "SMART SYNC {}/{}".format(n, len(batches)),
                })
                children.append(child)
                # отпечатки пачки — чтобы после рестарта Opsentri зачесть то, что
                # gpcopy докопировала без нас (см. _wait_leftover_children)
                active[child] = {
                    "n": n,
                    "units": {_name(u): _name(owner[u]["root"]) for u in batch["leaves"]},
                    "save": {_name(u): [[l[0], l[1], owner[u]["fingerprints"][l]]
                                        for l in _covered(owner[u], u)
                                        if l in owner[u]["fingerprints"]]
                             for u in batch["leaves"]}}
                config["smart"]["started_batches"] = n
                publish()

                for root in batch["roots"]:
                    if item_ids.get(root):
                        mark_item_running(item_ids[root])

                return batch, child

        def finish(batch, child):
            """Итог пачки -> отпечатки, строки таблиц, прогресс волны."""
            rows = {(i["schema_name"], i["table_name"]): i for i in get_job_items(child)}

            with lock:
                saved = []

                for unit in batch["leaves"]:
                    it = owner[unit]
                    row = rows.get(unit) or {}

                    if row.get("status") == "done":
                        covered = _covered(it, unit)
                        outcome[it["root"]]["done"].update(covered)
                        saved += [(l, it["fingerprints"][l]) for l in covered
                                  if l in it["fingerprints"]]
                    else:
                        outcome[it["root"]]["failed"][unit] = (
                            row.get("error_message")
                            or "не скопировано (задача #{})".format(child))[:300]

                if saved:
                    save_state(plan["id"], saved, job_id=job_id)

                for root in {owner[u]["root"] for u in batch["leaves"]}:
                    pending[root].difference_update(batch["leaves"])
                    it, res, iid = items[root], outcome[root], item_ids.get(root)
                    _set_item_part_progress(iid, it, res)

                    if not pending[root] and (res["failed"] or set(it["leaves"]) <= res["done"]):
                        closed[root] = _close_item(iid, it, res, False, deadline)

                active.pop(child, None)
                publish()
                refresh_job_progress(job_id)
                cond.notify_all()          # место освободилось — следующая пачка

        def worker():
            while True:
                try:
                    got = take()
                except Exception as e:     # задачу gpcopy не создать — волна падает целиком
                    with cond:
                        flags["error"] = flags["error"] or e
                        cond.notify_all()
                    return

                if not got:
                    with cond:
                        cond.notify_all()
                    return

                batch, child = got

                try:
                    _run_child(job_id, child, runner)
                except Exception:
                    pass          # пачка не доехала — finish() отметит её листья ошибкой
                finally:
                    finish(batch, child)

        # потоков — сколько можно максимум; сколько из них копирует, решает
        # current_limit(): число из плана можно поднять или снизить во время волны
        pool = [threading.Thread(target=worker, daemon=True)
                for _ in range(max(1, min(MAX_PARALLEL, len(batches))))]

        for t in pool:
            t.start()

        for t in pool:
            t.join()

        if flags["error"]:
            raise flags["error"]

        late = flags["late"]
        stopped = is_stop_requested(job_id)

        for root, it in items.items():
            if root not in closed:
                closed[root] = _close_item(item_ids.get(root), it, outcome[root],
                                           stopped, deadline)

        left = sum(1 for kind in closed.values() if kind == "left")
        failed = sum(1 for kind in closed.values() if kind == "failed")

        refresh_job_progress(job_id)
        config["smart"].pop("batch", None)
        config["smart"].pop("active", None)
        config["smart"].update({"left": left, "failed": failed, "late": late})
        update_job_config(job_id, config)

        if stopped:
            mark_job_cancelled(job_id, "Остановлено: не скопировано таблиц {}".format(left))
        elif failed or left:
            mark_job_failed(job_id, "Скопировано {} из {} таблиц; ошибок {}; {} {}".format(
                len(items) - failed - left, len(items), failed,
                "не успели до срока" if late else "осталось", left))
        else:
            mark_job_done(job_id)

    except Exception as e:
        import traceback

        try:
            mark_job_failed(job_id, "{}\n{}".format(e, traceback.format_exc())[:4000])
        except Exception:
            pass


# ------------------------------------------------------------- расписание

_DAYS_RE = re.compile(r"^(\*|[0-6](-[0-6])?(,[0-6](-[0-6])?)*)$")


def plan_schedules(plan_id):
    """{номер волны: расписание} этого плана."""
    import scheduler_store as store

    out = {}

    for s in store.list_schedules():
        if s.get("job_type") != "smart_sync":
            continue
        try:
            cfg = json.loads(s.get("config_json") or "{}")
        except ValueError:
            continue
        if str(cfg.get("plan_id")) == str(plan_id):
            out[int(cfg.get("wave") or 0)] = s

    return out


def apply_schedules(plan, days="*", enabled=True):
    """
    Расписание по волнам плана: старт волны -> cron «ММ ЧЧ * * дни».
    Волна без времени старта — без расписания (запуск вручную).
    -> [{wave, schedule_id, cron}].
    """
    import scheduler_store as store

    days = str(days or "*").strip()

    if not _DAYS_RE.match(days):
        raise ValueError("Дни недели: * или 0-6 (0 — вс), например 1-5")

    existing = plan_schedules(plan["id"])
    out = []

    for i, wave in enumerate(plan["settings"]["waves"]):
        current = existing.pop(i, None)

        if not wave.get("start"):
            if current:
                store.delete_schedule(current["id"])
            continue

        hh, mm = (int(x) for x in wave["start"].split(":"))
        cron = "{} {} * * {}".format(mm, hh, days)
        data = {
            "name": "{} · {}".format(plan["name"], wave["name"])[:120],
            "job_type": "smart_sync",
            "config_json": json.dumps({
                "plan_id": plan["id"], "wave": i,
                "source_connection_id": plan["source_connection_id"],
                "dest_connection_id": plan["dest_connection_id"],
            }),
            "cron_expr": cron,
            "overlap_policy": "skip",
            "enabled": 1 if enabled else 0,
        }

        if current:
            store.update_schedule(current["id"], data)
            sid = current["id"]
        else:
            sid = store.create_schedule(data)

        out.append({"wave": i, "schedule_id": sid, "cron": cron})

    # волн стало меньше — их расписания не нужны
    for s in existing.values():
        store.delete_schedule(s["id"])

    return out


def drop_schedules(plan_id):
    import scheduler_store as store

    for s in plan_schedules(plan_id).values():
        store.delete_schedule(s["id"])


# ------------------------------------------------------------- последняя проверка

_analysis_lock = threading.Lock()
_analysis_running = set()


def _spawn(target, *args):
    threading.Thread(target=target, args=args, daemon=True).start()


def _save_analysis(plan_id, **fields):
    cols = ", ".join("{} = ?".format(k) for k in fields)

    with sqlite_cursor(commit=True) as cur:
        cur.execute("INSERT OR IGNORE INTO smart_sync_analysis (plan_id) VALUES (?)",
                    (int(plan_id),))
        cur.execute("UPDATE smart_sync_analysis SET {} WHERE plan_id = ?".format(cols),
                    list(fields.values()) + [int(plan_id)])


def get_analysis(plan_id):
    """
    Последняя проверка плана: {status: running|done|failed, full, started_at,
    finished_at, result, error} или None. «Идёт», а потока нет — Opsentri
    перезапускали посреди проверки.
    """
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM smart_sync_analysis WHERE plan_id = ?", (int(plan_id),))
        row = cur.fetchone()

    if not row:
        return None

    out = dict(row)

    if out["status"] == "running" and int(plan_id) not in _analysis_running:
        out["status"] = "failed"
        out["error"] = "Проверка прервана рестартом Opsentri — запустите её заново"
        _save_analysis(plan_id, status="failed", error=out["error"])

    try:
        out["result"] = json.loads(out.pop("result_json") or "null")
    except ValueError:
        out["result"] = None

    out["full"] = bool(out.get("full"))
    return out


def start_analysis(plan, full=False):
    """
    Проверка «что изменилось» в фоне; результат сохраняется и переживает
    перезагрузку страницы. Уже идёт — вторую не запускаем.
    """
    plan_id = int(plan["id"])

    with _analysis_lock:
        if plan_id in _analysis_running:
            return get_analysis(plan_id)
        _analysis_running.add(plan_id)

    # прошлый результат остаётся, пока новый не готов
    _save_analysis(plan_id, status="running", full=1 if full else 0,
                   started_at=_now(), error=None)
    _spawn(_analysis_worker, plan, bool(full))
    return get_analysis(plan_id)


def _analysis_worker(plan, full):
    plan_id = int(plan["id"])

    try:
        # весь список таблиц, не первые 300: страница фильтрует и ищет сама
        result = public_analysis(analyze(plan, None, full=full or None), limit=None)
        _save_analysis(plan_id, status="done", finished_at=_now(), error=None,
                       result_json=json.dumps(result, ensure_ascii=False))
    except Exception as e:
        _save_analysis(plan_id, status="failed", finished_at=_now(),
                       error=str(e)[:1000])
    finally:
        with _analysis_lock:
            _analysis_running.discard(plan_id)
